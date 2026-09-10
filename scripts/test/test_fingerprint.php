<?php
require __DIR__ . '/../src/Pipelines/StudyFingerprint.php';
require __DIR__ . '/../src/Pipelines/FingerprintTracker.php';
use LORIS\Pipelines\{StudyFingerprint, FingerprintTracker};

$pass=0; $fail=0;
function ok($cond,$label){ global $pass,$fail; if($cond){$pass++; printf("  PASS  %s\n",$label);} else {$fail++; printf("  FAIL  %s\n",$label);} }

$d='/tmp/tfp'; exec("rm -rf $d /tmp/tfp.json"); mkdir("$d/s1",0777,true); mkdir("$d/s2",0777,true);
file_put_contents("$d/s1/a.dcm", str_repeat('a',2000));
file_put_contents("$d/s1/b.dcm", str_repeat('b',2000));
file_put_contents("$d/s2/c.dcm", str_repeat('c',2000));

echo "STUDY FINGERPRINT\n";
$f = StudyFingerprint::listFiles("$d/s1");
ok(count($f)===2, "listFiles finds both files");

$m1 = StudyFingerprint::manifestHash($f, "$d/s1");
$c1 = StudyFingerprint::contentHash($f, "$d/s1");
ok(strlen($m1)===64 && strlen($c1)===64, "hashes are sha256 hex");
ok($m1 !== $c1, "manifest and content hashes differ");

// relative paths: moving the tree must not change the manifest
exec("cp -r $d/s1 $d/s1copy");
$fc = StudyFingerprint::listFiles("$d/s1copy");
ok(StudyFingerprint::manifestHash($fc,"$d/s1copy") === $m1, "manifest is path-relative (move-safe)");
ok(StudyFingerprint::contentHash($fc,"$d/s1copy") === $c1, "content is path-independent");

// rename: content must be stable
rename("$d/s1copy/a.dcm", "$d/s1copy/zzz.dcm");
$fr = StudyFingerprint::listFiles("$d/s1copy");
ok(StudyFingerprint::contentHash($fr,"$d/s1copy") === $c1, "rename does not change content hash");
ok(StudyFingerprint::manifestHash($fr,"$d/s1copy") !== $m1, "rename DOES change manifest hash");

// one byte
file_put_contents("$d/s1copy/zzz.dcm", str_repeat('a',1999).'X');
$fb = StudyFingerprint::listFiles("$d/s1copy");
ok(StudyFingerprint::contentHash($fb,"$d/s1copy") !== $c1, "one byte changes content hash");

// order independence
ok(StudyFingerprint::manifestHash(array_reverse($f),"$d/s1") === $m1, "manifest is order-independent");
ok(StudyFingerprint::contentHash(array_reverse($f),"$d/s1") === $c1, "content is order-independent");

echo "\nDECISION TABLE\n";
$t = new FingerprintTracker('/tmp/tfp.json');
$c = $t->check('s1', "$d/s1");
ok($c['state']===StudyFingerprint::NEW && $c['reprocess'], "unseen -> new, reprocess");
$t->succeeded('s1', "$d/s1", ['status'=>'success'], $c);

$t2 = new FingerprintTracker('/tmp/tfp.json');
$c = $t2->check('s1', "$d/s1");
ok($c['state']===StudyFingerprint::UNCHANGED && !$c['reprocess'], "unchanged -> skip");

rename("$d/s1/a.dcm","$d/s1/renamed.dcm"); $t2->invalidate("$d/s1");
$c = $t2->check('s1', "$d/s1");
ok($c['state']===StudyFingerprint::RELOCATED && !$c['reprocess'], "renamed same bytes -> relocated, skip");
$t2->skipped('s1',"$d/s1",$c);

file_put_contents("$d/s1/renamed.dcm","different"); $t2->invalidate("$d/s1");
$c = $t2->check('s1', "$d/s1");
ok($c['state']===StudyFingerprint::CHANGED && $c['reprocess'], "bytes differ -> changed, reprocess");

$t3 = new FingerprintTracker('/tmp/tfp2.json'); @unlink('/tmp/tfp2.json'); $t3 = new FingerprintTracker('/tmp/tfp2.json');
$r = new ReflectionClass($t3); $p=$r->getProperty('entries'); $p->setAccessible(true);
$p->setValue($t3, ['legacy'=>['status'=>'success','timestamp'=>'2020-01-01']]);
$c = $t3->check('legacy', "$d/s2");
ok($c['state']===StudyFingerprint::BASELINE && !$c['reprocess'], "legacy entry -> baseline, no reprocess");

echo "\nTRACKER RULES\n";
$t4 = new FingerprintTracker('/tmp/tfp3.json'); @unlink('/tmp/tfp3.json'); $t4 = new FingerprintTracker('/tmp/tfp3.json');
$c = $t4->check('s2', "$d/s2"); $t4->succeeded('s2',"$d/s2",['status'=>'success'],$c);
ok(isset($t4->get('s2')['content_hash']), "success stores a fingerprint");
$t4->recordFailure('s2',['status'=>'failed','detail'=>'boom']);
ok(!isset($t4->get('s2')['content_hash']), "failure strips the fingerprint");
$c = $t4->check('s2', "$d/s2");
ok($c['reprocess'], "after failure -> retried");

$t5 = new FingerprintTracker('/tmp/tfp4.json'); @unlink('/tmp/tfp4.json'); $t5 = new FingerprintTracker('/tmp/tfp4.json');
$c = $t5->check('s2',"$d/s2"); $t5->succeeded('s2',"$d/s2",['status'=>'success','detail'=>'keepme','custom'=>42],$c);
$e = $t5->get('s2');
ok(($e['detail']??'')==='keepme' && ($e['custom']??0)===42, "pipeline fields survive alongside hashes");

echo "\nSELF-EXCLUSION\n";
$d2='/tmp/tfp_self'; exec("rm -rf $d2"); mkdir($d2,0777,true);
file_put_contents("$d2/x.dcm", str_repeat('x',500));
$inside = "$d2/.tracking.json";
$t6 = new FingerprintTracker($inside);
$c = $t6->check('k', $d2); $t6->succeeded('k',$d2,['status'=>'success'],$c);
$t7 = new FingerprintTracker($inside);
$c = $t7->check('k', $d2);
ok($c['state']===StudyFingerprint::UNCHANGED, "tracking file inside tree is excluded from its own hash");

echo "\nEDGE CASES\n";
$empty='/tmp/tfp_empty'; exec("rm -rf $empty"); mkdir($empty,0777,true);
$t8 = new FingerprintTracker('/tmp/tfp5.json'); @unlink('/tmp/tfp5.json'); $t8 = new FingerprintTracker('/tmp/tfp5.json');
$c = $t8->check('e', $empty);
ok($c['state']===StudyFingerprint::NEW, "empty directory handled");
file_put_contents('/tmp/tfp6.json','{ not json');
$t9 = new FingerprintTracker('/tmp/tfp6.json');
ok($t9->all()===[], "corrupt tracking file treated as empty, no crash");

printf("\n%d passed, %d failed\n", $pass, $fail);
exit($fail>0?1:0);
