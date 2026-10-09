#!/usr/bin/env php
<?php
/**
 * ARCHIMEDES pipeline preflight check.
 *
 * Run as lorisadmin before running the clinical pipeline.
 *
 * READ-ONLY: this script changes nothing. It prints the values it read so
 * you can review them, reports PASS / WARN / FAIL, and prints a suggested
 * fix for anything that needs attention. Applying a fix is up to you.
 *
 * Security: passwords, tokens and secrets are never printed (not even their
 * length), the LORIS token stays in memory, data file contents are never
 * shown, and TLS verification follows verify_ssl in the config.
 *
 * Usage:
 *   php scripts/test/preflight_check.php [--project=<name>]
 *       [--loris-root=/var/www/loris]
 *       [--loris-config=<path to LORIS config.xml, if not in the usual places>]
 *       [--upload-dir=/data/web_uploads/instrument_manager]
 *       [--warn-if-contains=<text>[,<text>...]]   e.g. a dev hostname
 *       [--no-prompt]   do not ask to confirm notification recipients
 *                       (also skipped automatically when not run in a terminal)
 *
 * Exit code: 0 = no FAIL, 1 = at least one FAIL.
 */

$autoload = __DIR__ . '/../../vendor/autoload.php';
if (is_file($autoload)) {
    require $autoload;
}

use GuzzleHttp\Client;

$opts      = getopt('', ['project:', 'loris-root:', 'loris-config:', 'upload-dir:', 'warn-if-contains:', 'no-prompt']);
$askConfirm = !isset($opts['no-prompt']) && function_exists('posix_isatty') && posix_isatty(STDIN);
$onlyProj  = $opts['project'] ?? null;
$lorisRoot = rtrim($opts['loris-root'] ?? '/var/www/loris', '/');
$uploadDir = rtrim($opts['upload-dir'] ?? '/data/web_uploads/instrument_manager', '/');
$warnIfContains = array_values(array_filter(array_map('trim', explode(',', (string) ($opts['warn-if-contains'] ?? '')))));
$root      = realpath(__DIR__ . '/../..');
$cfgDir    = "$root/config";

$counts = ['PASS' => 0, 'WARN' => 0, 'FAIL' => 0];
$currentSection = null;
$sectionStatus  = [];
function out(string $status, string $msg, string $fix = ''): void
{
    global $counts, $currentSection, $sectionStatus;
    $counts[$status]++;
    if ($currentSection !== null) {
        $rank = ['OK' => 0, 'WARN' => 1, 'FAIL' => 2];
        $new  = $status === 'PASS' ? 'OK' : $status;
        if ($rank[$new] > $rank[$sectionStatus[$currentSection] ?? 'OK']) {
            $sectionStatus[$currentSection] = $new;
        }
    }
    $icon = ['PASS' => "\033[32mPASS\033[0m", 'WARN' => "\033[33mWARN\033[0m", 'FAIL' => "\033[31mFAIL\033[0m"][$status];
    echo "  [$icon] $msg\n";
    if ($fix !== '' && $status !== 'PASS') {
        echo "         suggested fix: $fix\n";
    }
}
function section(string $t): void
{
    global $currentSection, $sectionStatus;
    $currentSection = $t;
    $sectionStatus[$t] = $sectionStatus[$t] ?? 'OK';
    echo "\n== $t ==\n";
}
/** Mount point and filesystem type holding $path, from /proc/mounts. */
function mountOf(string $path): string
{
    $real = realpath($path) ?: $path;
    $best = ['/', '?'];
    foreach (@file('/proc/mounts', FILE_IGNORE_NEW_LINES) ?: [] as $l) {
        [$src, $mp, $type] = array_pad(explode(' ', $l), 3, '');
        $mp = str_replace('\\040', ' ', $mp);
        if (($real === $mp || str_starts_with($real, rtrim($mp, '/') . '/')) && strlen($mp) >= strlen($best[0])) {
            $best = [$mp, $type];
        }
    }
    return "{$best[0]} ({$best[1]})";
}
function yn(bool $b): string { return $b ? "\033[32myes\033[0m" : "\033[31mno\033[0m"; }
function line(string $label, string $value): void { printf("    %-22s : %s\n", $label, $value); }
function mode(string $p): string { return substr(sprintf('%o', fileperms($p)), -4); }
function ownerName(string $p): string
{
    $u = function_exists('posix_getpwuid') ? posix_getpwuid(fileowner($p)) : null;
    return $u['name'] ?? (string) fileowner($p);
}
function groupName(string $p): string
{
    $g = function_exists('posix_getgrgid') ? posix_getgrgid(filegroup($p)) : null;
    return $g['name'] ?? (string) filegroup($p);
}
function canWrite(string $dir): bool
{
    // Permission check only; nothing is written.
    return is_writable($dir);
}
function show(string $label, $value): void
{
    printf("    %-24s %s\n", $label, is_array($value) ? implode(', ', $value) : (string) $value);
}
function loadJson(string $file, string $label): ?array
{
    if (!is_file($file)) {
        out('FAIL', "$label not found: $file", 'Copy ' . str_replace('.json', '.example.json', basename($file)) . ' to ' . basename($file) . ' and fill in the values');
        return null;
    }
    if (!is_readable($file)) {
        out('FAIL', "$label not readable by " . get_current_user(), "chown lorisadmin $file");
        return null;
    }
    $data = json_decode(file_get_contents($file), true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        out('FAIL', "$label invalid JSON: " . json_last_error_msg(), "Fix the syntax in $file");
        return null;
    }
    out('PASS', "$label parses");
    return $data;
}

$me = function_exists('posix_geteuid') ? (posix_getpwuid(posix_geteuid())['name'] ?? '?') : get_current_user();
echo "ARCHIMEDES preflight  " . date('Y-m-d H:i:s') . "  host=" . gethostname() . "  user=$me\n";

// ---------------------------------------------------------------- server
section('Server');
version_compare(PHP_VERSION, '8.1', '>=')
    ? out('PASS', 'PHP ' . PHP_VERSION)
    : out('FAIL', 'PHP ' . PHP_VERSION . ' is older than 8.1', 'Install PHP 8.1 or newer');
foreach (['curl', 'json', 'mbstring', 'posix'] as $ext) {
    extension_loaded($ext)
        ? out('PASS', "PHP extension $ext")
        : out('FAIL', "PHP extension $ext missing", "Install php-$ext");
}
$sendmail = trim(explode(' ', (string) ini_get('sendmail_path'))[0] ?: '/usr/sbin/sendmail');
is_executable($sendmail)
    ? out('PASS', "mail binary $sendmail")
    : out('WARN', "mail binary $sendmail not found (no notification emails)", 'Install postfix or sendmail');
$me === 'lorisadmin'
    ? out('PASS', 'running as lorisadmin')
    : out('WARN', "running as $me, not lorisadmin (permission results may differ)", 'sudo -u lorisadmin php ' . $argv[0]);

// ---------------------------------------------------------------- install
section('Install');
ownerName($root) === 'lorisadmin'
    ? out('PASS', "$root owned by lorisadmin")
    : out('FAIL', "$root owned by " . ownerName($root), "sudo chown -R lorisadmin:lorisadmin $root");
is_file("$root/vendor/autoload.php")
    ? out('PASS', 'composer dependencies installed')
    : out('FAIL', 'vendor/autoload.php missing', "cd $root && composer install");
$commit = trim((string) @shell_exec("git -C " . escapeshellarg($root) . " rev-parse --short HEAD 2>/dev/null"));
$commit !== '' ? out('PASS', "code at commit $commit") : out('WARN', 'not a git checkout; commit unknown');

// ---------------------------------------------------------------- config
section('Config files');
$cfgFile = "$cfgDir/loris_client_config.json";
$eviFile = "$cfgDir/evidata_config.json";
$cfg = loadJson($cfgFile, 'loris_client_config.json');
$evi = is_file($eviFile) ? loadJson($eviFile, 'evidata_config.json') : null;
if ($evi === null && !is_file($eviFile)) {
    out('WARN', 'evidata_config.json absent (EviData treated as disabled)');
}
foreach ([$cfgFile] as $f) {   // evidata_config.json holds no secrets (only env var NAMES)
    if (is_file($f)) {
        ownerName($f) === 'lorisadmin' || out('WARN', basename($f) . ' owned by ' . ownerName($f), "sudo chown lorisadmin $f");
    }
}
if ($cfg === null) {
    echo "\nCannot continue without loris_client_config.json.\n";
    exit(1);
}

$required = ['api.base_url', 'api.username', 'api.password', 'notification_defaults', 'collections'];
foreach ($required as $key) {
    $v = $cfg;
    foreach (explode('.', $key) as $k) {
        $v = $v[$k] ?? null;
    }
    empty($v) ? out('FAIL', "missing required key $key", "Add $key to $cfgFile") : out('PASS', "key $key set");
}
$raw = file_get_contents($cfgFile);
// Placeholders from the .example.json files, plus anything passed with
// --warn-if-contains (e.g. a dev hostname you copied the config from).
$leftovers = array_merge(['example', '<', 'secure_password_here', 'api_user'], $warnIfContains);
foreach ($leftovers as $leftover) {
    if (stripos($raw, $leftover) !== false) {
        out('WARN', "config still contains '$leftover' (placeholder or value from another server?)", 'Replace it with the value for this server');
    }
}

// ---------------------------------------------------------------- values
section('Values read from config (review these)');
show('api.base_url', $cfg['api']['base_url'] ?? '(not set)');
show('api.username', $cfg['api']['username'] ?? '(not set)');
show('verify_ssl', var_export($cfg['verify_ssl'] ?? true, true));
foreach ($cfg['notification_defaults'] ?? [] as $k => $v) {
    show("email $k", (array) $v);
}
foreach ($cfg['collections'] ?? [] as $c) {
    if (isset($c['enabled']) && !$c['enabled']) {
        continue;
    }
    show('collection', $c['name'] ?? '?');
    show('  base_path', $c['base_path'] ?? '(not set)');
    foreach ($c['projects'] ?? [] as $p) {
        if (!empty($p['enabled'])) {
            show('  project', $p['name'] ?? '?');
        }
    }
}
show('evidata.enabled', var_export(!empty($evi['enabled']), true));
if (!empty($evi['enabled'])) {
    show('evidata.api_base_url', $evi['api_base_url'] ?? '(not set)');
}
echo "    WARN: check every value above is correct for THIS server before running the pipeline.\n";

// ---------------------------------------------------------------- evidata
// Same checks as test_evidata_connection.php, one line each. Nothing secret
// is printed; credentials and tokens stay in memory.
section('EviData');
$eviOn  = !empty($evi['enabled']);
$eviBad = $eviOn ? 'FAIL' : 'WARN';   // disabled: problems do not block the run
out('PASS', 'EviData ' . ($eviOn ? 'enabled' : 'disabled (setup still checked; problems are WARN)'));
if ($evi === null) {
    out($eviBad, 'EviData not set up (no evidata_config.json)', 'Copy evidata_config.example.json to evidata_config.json if EviData will be used');
} else {
    // 1. config keys
    $missingKeys = array_filter(['api_base_url', 'token_url', 'client_id'],
        fn($k) => empty($evi[$k]) || str_contains((string) $evi[$k], '<'));
    $missingKeys
        ? out($eviBad, 'evidata_config.json missing: ' . implode(', ', $missingKeys), 'Set these keys in evidata_config.json')
        : out('PASS', 'evidata_config.json keys set');

    // 2. env file and the three credentials
    $env     = getenv('EVIDATA_ENV_FILE') ?: ($evi['env_file'] ?? '/home/lorisadmin/evidata/evidata.env');
    $envVars = [];
    if (!is_file($env) || !is_readable($env)) {
        out($eviBad, "env file missing or not readable by $me: $env", 'Create the env_file named in evidata_config.json, owned by lorisadmin, chmod 600');
    } else {
        foreach (preg_split('/\R/', (string) file_get_contents($env)) as $l) {
            if (preg_match('/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/', $l, $m)) {
                $envVars[$m[1]] = trim($m[2], " \t\"'");
            }
        }
        (fileperms($env) & 0077) === 0
        || out($eviBad, 'env file mode ' . mode($env) . ' (holds secrets; group/others can read)', "chmod 600 $env");
    }
    $cred = [];
    foreach (['client_secret' => $evi['client_secret_env'] ?? 'EVIDATA_CLIENT_SECRET',
                 'username'      => $evi['username_env'] ?? 'EVIDATA_USERNAME',
                 'password'      => $evi['password_env'] ?? 'EVIDATA_PASSWORD'] as $k => $var) {
        $cred[$k] = getenv($var) ?: ($envVars[$var] ?? '');
    }
    $unset = array_keys(array_filter($cred, fn($v) => $v === ''));
    if (is_file($env)) {
        $unset
            ? out($eviBad, 'env file: ' . count($unset) . ' of 3 credentials not set (' . implode(', ', $unset) . ')', "Fill them in $env")
            : out('PASS', 'env file OK (' . mode($env) . ', 3 credentials set)');
    }

    $evClient = class_exists(Client::class)
        ? new Client(['timeout' => 20, 'verify' => $cfg['verify_ssl'] ?? true, 'http_errors' => false]) : null;
    $apiB = rtrim((string) ($evi['api_base_url'] ?? ''), '/');

    // 3+4. reachability and health (no auth)
    if ($evClient && $apiB && !$missingKeys) {
        try {
            $r = $evClient->get("$apiB/health");
            $h = json_decode((string) $r->getBody(), true) ?: [];
            ($r->getStatusCode() === 200 && ($h['status'] ?? '') === 'healthy' && !empty($h['keycloak_configured']))
                ? out('PASS', 'EviData API reachable and healthy')
                : out($eviBad, 'EviData API health: HTTP ' . $r->getStatusCode() . ', status=' . ($h['status'] ?? '?')
                . ', keycloak_configured=' . var_export((bool) ($h['keycloak_configured'] ?? false), true), 'Ask the EviData server owner to check the stack');
        } catch (\Throwable $e) {
            out($eviBad, 'EviData API not reachable (' . parse_url($apiB, PHP_URL_HOST) . ')', 'Check api_base_url, DNS and firewall (port 443)');
        }
    }

    // 5+6. Keycloak token, then authenticated /datasets
    if ($evClient && !$unset && !$missingKeys) {
        $evToken = null;
        try {
            $r = $evClient->post($evi['token_url'], ['form_params' => [
                'grant_type' => 'password', 'client_id' => $evi['client_id'],
                'client_secret' => $cred['client_secret'], 'username' => $cred['username'],
                'password' => $cred['password'], 'scope' => $evi['scope'] ?? 'openid profile email',
            ]]);
            $evToken = json_decode((string) $r->getBody(), true)['access_token'] ?? null;
            ($r->getStatusCode() === 200 && $evToken)
                ? out('PASS', 'EviData login OK')
                : out($eviBad, 'EviData login failed (HTTP ' . $r->getStatusCode() . ')', 'Check the env file credentials, client_id and token_url');
        } catch (\Throwable $e) {
            out($eviBad, 'EviData token_url not reachable', 'Check token_url, DNS and firewall');
        }
        if ($evToken) {
            $r = $evClient->get("$apiB/datasets", ['headers' => ['Authorization' => "Bearer $evToken"]]);
            $r->getStatusCode() === 200
                ? out('PASS', 'EviData authenticated request OK (/datasets 200)')
                : out($eviBad, 'EviData /datasets returned HTTP ' . $r->getStatusCode(), 'Token issued but rejected; check scope includes openid');
        }
        unset($evToken);
    }
    unset($cred, $envVars);
}

// ---------------------------------------------------------------- LORIS API
// Same checks as test_authentication.php, one line each.
section('Authentication (LORIS API)');
$baseUrl  = rtrim($cfg['api']['base_url'], '/');
$http     = class_exists(Client::class)
    ? new Client(['timeout' => 30, 'verify' => $cfg['verify_ssl'] ?? true, 'http_errors' => false]) : null;
$token    = null;
$apiBase  = null;
if (!$http) {
    out('FAIL', 'not tested: composer dependencies missing', "cd $root && composer install");
} else {
    // 1. base URL reachable
    $reachable = false;
    try {
        $r = $http->get($baseUrl);
        $reachable = $r->getStatusCode() < 500;
        $reachable
            ? out('PASS', "LORIS reachable ($baseUrl, HTTP " . $r->getStatusCode() . ')')
            : out('FAIL', "LORIS returned HTTP " . $r->getStatusCode(), 'Check the LORIS web server: sudo tail -40 /var/log/apache2/error.log');
    } catch (\Throwable $e) {
        out('FAIL', "LORIS not reachable ($baseUrl)", 'Check api.base_url, DNS, firewall, and verify_ssl / certificate');
    }
    // 2. login, trying the configured API version first
    if ($reachable) {
        foreach (array_unique([$cfg['api']['api_version'] ?? 'v0.0.3', 'v0.0.3', 'v0.0.4', 'v0.0.4-dev']) as $ver) {
            try {
                $r = $http->post("$baseUrl/api/$ver/login", ['json' => [
                    'username' => $cfg['api']['username'], 'password' => $cfg['api']['password'],
                ]]);
                $t = json_decode((string) $r->getBody(), true)['token'] ?? null;
                if ($r->getStatusCode() === 200 && $t) {
                    $token = $t;
                    $apiBase = "$baseUrl/api/$ver";
                    break;
                }
            } catch (\Throwable $e) {
            }
        }
        $token
            ? out('PASS', 'login OK (API ' . basename($apiBase) . ')')
            : out('FAIL', 'login failed for api.username on all API versions', "Check api.username and api.password; reset with php $lorisRoot/tools/resetpassword.php <user>");
    }
    // 3. token works on an authenticated endpoint
    if ($token) {
        $r = $http->get("$apiBase/candidates", ['headers' => ['Authorization' => "Bearer $token"]]);
        $r->getStatusCode() === 200
            ? out('PASS', 'authenticated request OK (/candidates 200)')
            : out('FAIL', 'token rejected: /candidates returned HTTP ' . $r->getStatusCode(), 'Check the API user has access to candidates');
    }
}

// ---------------------------------------------------------------- path report helpers

/**
 * Print one path as a yes/no block and count PASS/FAIL.
 * $who = 'lorisadmin' (checks this process can read/write) or 'www-data'
 * (checks group www-data + group-writable, since we do not switch user).
 */
function pathReport(string $label, string $path, bool $needWrite, string $who, string $fix = '', string $missingFix = '', string $sev = 'FAIL'): bool
{
    global $me;
    echo "\n  $label\n";
    line('path', $path);
    $exists = is_dir($path);
    line('exists', yn($exists));
    if (!$exists) {
        out($sev, "$label missing", $missingFix ?: ($fix ?: "sudo mkdir -p $path"));
        return false;
    }
    line('owner:group mode', ownerName($path) . ':' . groupName($path) . ' ' . mode($path));
    if (fileperms($path) & 0002) {
        line('world-writable', "\033[33myes\033[0m (any user on the server can write here)");
        out('WARN', "$label is world-writable (" . mode($path) . ')', "sudo chown lorisadmin:www-data $path && sudo chmod 2775 $path   (keeps www-data access, removes world write)");
    }
    if ($who === 'www-data') {
        $perm = fileperms($path);
        $ok = (ownerName($path) === 'www-data' && ($perm & 0200))
            || (groupName($path) === 'www-data' && ($perm & 0020))
            || ($perm & 0002);
        line('writable by www-data', yn((bool) $ok) . ($ok ? '' : '   (confirm: sudo -u www-data test -w ' . $path . ' && echo yes)'));
        $ok ? out('PASS', "$label OK") : out($sev, "$label not writable by www-data", $fix ?: "sudo chown lorisadmin:www-data $path && sudo chmod 2775 $path");
        return (bool) $ok;
    }
    $r = is_readable($path) && is_executable($path);
    line("readable by $me", yn($r));
    $ok = $r;
    if ($needWrite) {
        $w = is_writable($path);
        line("writable by $me", yn($w));
        $ok = $ok && $w;
    }
    $ok ? out('PASS', "$label OK")
        : out('FAIL', "$label not " . ($r ? 'writable' : 'readable') . " by $me", $fix ?: "Give lorisadmin " . ($r ? 'write' : 'read') . " access to $path (e.g. chown lorisadmin:www-data, chmod 2775)");
    return $ok;
}

// ---------------------------------------------------------------- LORIS folders
section('LORIS folders');
pathReport('LORIS project folder', "$lorisRoot/project", false, 'lorisadmin', "Check --loris-root (default /var/www/loris)");
// These are only used by the LORIS Instrument Manager (clinical/tabular
// ingestion). Other pipelines do not need them, so problems are WARN.
echo "\n  Used by the LORIS Instrument Manager (clinical/tabular ingestion only).\n";
echo "  A problem here does not block other pipelines; fix it before clinical ingestion.\n";
foreach ([
             "$lorisRoot/project/instruments" => 'Instrument Manager: install new instruments',
             "$lorisRoot/project/tables_sql"  => 'Instrument Manager: install new instruments',
             $uploadDir                       => 'Instrument Manager: every clinical data upload',
         ] as $d => $usedFor) {
    pathReport(basename($d) . " ($usedFor)", $d, true, 'www-data',
        "sudo mkdir -p $d && sudo chown lorisadmin:www-data $d && sudo chmod 2775 $d", '', 'WARN');
}

echo "\n  LORIS DB admin user (Instrument Manager: install new instruments)\n";
// Find the DB config the way LORIS 26 does (NDB_Config::configFilePath):
// config.xml in project/, then /etc/loris/, then /usr/local/etc/loris/
// (LORIS_DB_CONFIG, if Apache sets it, overrides; pass it with --loris-config).
// Then follow every <include> in that file, resolved the same way.
$resolve = function (string $name) use ($lorisRoot, $opts): ?string {
    foreach (["$lorisRoot/project/$name", "/etc/loris/$name", "/usr/local/etc/loris/$name", $name] as $c) {
        if (is_file($c)) {
            return $c;
        }
    }
    return null;
};
$mainCfg    = $opts['loris-config'] ?? (getenv('LORIS_DB_CONFIG') ?: $resolve('config.xml'));
$queue      = $mainCfg ? [$mainCfg] : [];
$seen       = [];
$adminState = 'missing';   // found | unreadable | missing
while ($queue && $adminState !== 'found') {
    $xml = array_shift($queue);
    if (isset($seen[$xml])) {
        continue;
    }
    $seen[$xml] = true;
    if (!is_readable($xml)) {
        $adminState = 'unreadable';
        continue;
    }
    $txt = (string) file_get_contents($xml);
    if (preg_match('#<adminUser>\s*[^<\s][^<]*</adminUser>#', $txt)) {
        $adminState = 'found';
        break;
    }
    preg_match_all('#<include>\s*([^<]+?)\s*</include>#', $txt, $m);
    foreach ($m[1] as $inc) {
        ($f = $resolve($inc)) ? $queue[] = $f : null;
    }
}
if (!$mainCfg) {
    line('LORIS config.xml', 'not found');
}
if ($adminState === 'found') {
    line('adminUser', 'found');
    out('PASS', 'DB adminUser found');
} elseif ($adminState === 'unreadable') {
    line('adminUser', "could not be checked (a LORIS config file is not readable by $me)");
    out('WARN', 'DB adminUser not checked', 'Run as a user that can read the LORIS config, or ask the LORIS admin to confirm adminUser and adminPassword are set');
} else {
    line('adminUser', 'not found');
    out('WARN', 'DB adminUser not found (Instrument Manager cannot install new instruments)', 'Set adminUser and adminPassword in the LORIS database config');
}

// ---------------------------------------------------------------- projects
// ---------------------------------------------------------------- notification emails
/**
 * Recipients a pipeline will use for one modality, resolved like the pipeline
 * code: clinical uses project.json only; the others fall back to
 * notification_defaults when a list is missing. Returns label => [list, fromDefault].
 */
function resolveRecipients(string $modality, array $block, array $cfg): array
{
    $def  = $cfg['notification_defaults'] ?? [];
    $pick = fn(string $k, string $d) => array_key_exists($k, $block)
        ? [$block[$k], false]
        : ($modality === 'clinical' ? [[], false] : [$def[$d] ?? [], true]);
    return $modality === 'evidata'
        ? ['privacy fail' => $pick('on_check_failed', 'default_on_evidata_failed')]
        : ['success' => $pick('on_success', 'default_on_success'), 'error' => $pick('on_error', 'default_on_error')];
}

/** Show who gets emailed per enabled modality, flag bad addresses, ask to confirm. */
function notificationReport(string $project, ?array $pj, array $cfg, bool $ask): void
{
    echo "\n  Notification emails\n";
    $problems = [];
    $shown    = 0;
    foreach ((array) ($pj['notification_emails'] ?? []) as $modality => $block) {
        if (!is_array($block) || empty($block['enabled'])) {
            continue;
        }
        $shown++;
        foreach (resolveRecipients((string) $modality, $block, $cfg) as $label => [$list, $fromDefault]) {
            $list = is_array($list) ? $list : [$list];
            $text = $list ? implode(', ', $list) : '(none)';
            printf("    %-20s %-13s %s%s\n", $modality, $label, $text, $fromDefault ? '  (default)' : '');
            foreach ($list as $a) {
                if (stripos((string) $a, 'example') !== false || str_contains((string) $a, '<')
                    || filter_var(trim((string) $a), FILTER_VALIDATE_EMAIL) === false) {
                    $problems[] = "$modality $label: '$a'";
                }
            }
            if (!$list && $label !== 'success') {
                out('WARN', "$modality: no $label recipient, failures will not be emailed");
            }
        }
    }

    if ($shown === 0) {
        out('WARN', 'no enabled modality in notification_emails, so no emails for this project');
        return;
    }
    $problems
        ? out('FAIL', 'invalid email: ' . implode('; ', $problems), "Fix notification_emails in $project/project.json")
        : out('PASS', 'email addresses valid');

    if ($ask) {
        echo "    Are these the right recipients for $project? [y/N] ";
        in_array(strtolower(trim((string) fgets(STDIN))), ['y', 'yes'], true)
            ? out('PASS', 'recipients confirmed')
            : out('WARN', 'recipients not confirmed', "Update notification_emails in $project/project.json");
    }
}

section('Project paths');

// Overview: every collection and project listed in loris_client_config.json.
function ynPlain(bool $b): string { return $b ? 'yes' : 'NO'; }
echo "\n  Enabled projects in loris_client_config.json\n";
$rows = [];
foreach ($cfg['collections'] ?? [] as $col) {
    if (isset($col['enabled']) && !$col['enabled']) {
        continue;
    }
    $base = rtrim($col['base_path'] ?? '', '/');
    $baseOk = is_dir($base) && is_readable($base) && is_executable($base);
    foreach ($col['projects'] ?? [] as $p) {
        if (empty($p['enabled']) || ($onlyProj && $p['name'] !== $onlyProj)) {
            continue;
        }
        $dir = "$base/" . ($p['name'] ?? '?');
        // Data folders the pipeline reads (only those that exist).
        $dataDirs = array_filter(['deidentified-raw', 'deidentified-raw/clinical', 'deidentified-raw/preclinical',
            'deidentified-raw/bids/phenotype', 'documentation/data_dictionary'], fn($d) => is_dir("$dir/$d"));
        $dataOk = $dataDirs && !array_filter($dataDirs, fn($d) => !(is_readable("$dir/$d") && is_executable("$dir/$d")));
        $rows[] = [$col['name'] ?? '?', $p['name'] ?? '?', $base ?: '(none)', is_dir($base) ? mountOf($base) : '-',
            ynPlain($baseOk), ynPlain(is_dir($dir)),
            ynPlain(is_dir($dir) && is_readable($dir) && is_executable($dir)),
            ynPlain(is_dir($dir) && is_writable($dir)),
            is_dir($dir) ? ($dataDirs ? ynPlain($dataOk) : 'NONE') : '-'];
    }
}
if (!$rows) {
    echo "    (none enabled)\n";
} else {
    $hdr = ['COLLECTION', 'PROJECT', 'BASE PATH', 'MOUNT', 'BASE READABLE', 'PROJECT EXISTS', 'PROJECT READABLE', 'PROJECT WRITABLE', 'DATA READABLE'];
    $w = array_map(fn($i) => max(array_map(fn($r) => strlen($r[$i]), array_merge([$hdr], $rows))), array_keys($hdr));
    $fmt = '    ' . implode('  ', array_map(fn($x) => "%-{$x}s", $w)) . "\n";
    vprintf($fmt, $hdr);
    foreach ($rows as $r) {
        vprintf($fmt, $r);
    }
    echo "    (checked as $me. DATA READABLE = deidentified-raw/* and documentation/data_dictionary. Details below.)\n";
}

$found = 0;
foreach ($cfg['collections'] ?? [] as $col) {
    $colName = $col['name'] ?? '?';
    if (isset($col['enabled']) && !$col['enabled']) {
        continue;
    }
    $base = rtrim($col['base_path'] ?? '', '/');
    echo "\n  Collection $colName is on mount " . (is_dir($base) ? mountOf($base) : '(path missing)') . "\n";
    if (!pathReport("Base path (collection $colName)", $base, false, 'lorisadmin',
        'Check the mount is present and lorisadmin can read it (ask the storage admin)')) {
        continue;
    }
    foreach ($col['projects'] ?? [] as $p) {
        $name = $p['name'] ?? '?';
        if ($onlyProj && $name !== $onlyProj) {
            continue;
        }
        if (empty($p['enabled'])) {
            continue;
        }
        $found++;
        $dir = "$base/$name";
        echo "\n  ===== Project $name (enabled) =====";
        pathReport('Project folder', $dir, true, 'lorisadmin',
            "Give lorisadmin write access to $dir (the pipeline creates logs/ and processed/ there)",
            "Check the project name in loris_client_config.json matches the folder name exactly (case-sensitive)");
        if (!is_dir($dir) || !is_readable($dir) || !is_executable($dir)) {
            continue;   // nothing below can be read
        }
        foreach (['logs', 'processed'] as $sub) {
            if (is_dir("$dir/$sub")) {
                pathReport("$sub/", "$dir/$sub", true, 'lorisadmin', "sudo chown -R lorisadmin:www-data $dir/$sub && sudo chmod -R 2775 $dir/$sub");
            }
        }

        echo "\n  project.json\n";
        $pjFile = "$dir/project.json";
        line('exists', yn(is_file($pjFile)));
        $pj = null;
        if (is_file($pjFile)) {
            line("readable by $me", yn(is_readable($pjFile)));
            $pj = is_readable($pjFile) ? json_decode((string) file_get_contents($pjFile), true) : null;
            $valid = is_array($pj);
            line('valid JSON', yn($valid) . ($valid ? '' : '  (' . json_last_error_msg() . ')'));
            $valid ? out('PASS', 'project.json valid') : out('FAIL', 'project.json invalid or unreadable', "Fix $pjFile");
        } else {
            out('FAIL', 'project.json missing', "Add $pjFile");
        }

        notificationReport($name, $pj, $cfg, $askConfirm);

        echo "\n  Data folders (read by the pipeline)\n";
        foreach (['deidentified-raw', 'deidentified-raw/clinical', 'deidentified-raw/preclinical',
                     'deidentified-raw/bids/phenotype', 'documentation', 'documentation/data_dictionary'] as $sub) {
            if (!is_dir("$dir/$sub")) {
                continue;
            }
            $ok = is_readable("$dir/$sub") && is_executable("$dir/$sub");
            line($sub, 'readable: ' . yn($ok) . '   (' . ownerName("$dir/$sub") . ':' . groupName("$dir/$sub") . ' ' . mode("$dir/$sub") . ')');
            $ok || out('FAIL', "$sub not readable by $me", "Ask the data owner to give lorisadmin read access to $dir/$sub");
        }

        echo "\n  Data dictionary\n";
        $ddDir = "$dir/documentation/data_dictionary";
        $dd = is_dir($ddDir) ? (glob("$ddDir/*") ?: []) : [];
        line('folder', $ddDir);
        line('files found', (string) count($dd));
        $dd ? out('PASS', 'data dictionary present') : out('FAIL', 'no data dictionary', "Add the dictionary to $ddDir");

        echo "\n  Data files\n";
        $dataFiles = [];
        foreach (['deidentified-raw/clinical', 'deidentified-raw/preclinical', 'deidentified-raw/bids/phenotype'] as $sub) {
            if (!is_dir("$dir/$sub")) {
                continue;
            }
            $files = glob("$dir/$sub/*.{csv,tsv,CSV,TSV}", GLOB_BRACE) ?: [];
            $unread = array_filter($files, fn($f) => !is_readable($f));
            line($sub, count($files) . ' file(s), readable: ' . yn(!$unread));
            $dataFiles = array_merge($dataFiles, $files);
            if ($unread) {
                out('FAIL', count($unread) . " file(s) in $sub not readable, e.g. " . basename(reset($unread)), 'Give lorisadmin read access');
            }
        }
        $rawDir = "$dir/deidentified-raw";
        if (is_dir($rawDir) && !(is_readable($rawDir) && is_executable($rawDir))) {
            line('data files', "cannot look: deidentified-raw not readable by $me");
        } else {
            $dataFiles
                ? out('PASS', count($dataFiles) . ' data file(s) found')
                : out('FAIL', 'no data files in deidentified-raw/clinical, preclinical or bids/phenotype', "Add the data files under $rawDir/clinical/");
        }

        // Visit values in the data vs visits in LORIS (after visit_mappings).
        if ($token && $dataFiles) {
            $map    = is_array($pj) ? ($pj['visit_mappings'] ?? []) : [];
            $values = [];
            foreach ($dataFiles as $f) {
                if (!is_readable($f) || !($fh = fopen($f, 'r'))) {
                    continue;
                }
                $delim = str_ends_with(strtolower($f), '.tsv') ? "\t" : ',';
                $hdr = array_map(fn($h) => strtolower(trim($h, " \t\n\r\0\x0B\xEF\xBB\xBF\"")), fgetcsv($fh, 0, $delim) ?: []);
                $c = false;
                foreach (['redcap_event_name', 'visit_label'] as $cn) {
                    if (($c = array_search($cn, $hdr, true)) !== false) {
                        break;
                    }
                }
                if ($c !== false) {
                    while (($row = fgetcsv($fh, 0, $delim)) !== false) {
                        $v = trim($row[$c] ?? '');
                        if ($v !== '') {
                            $values[$map[$v] ?? $v] = true;
                        }
                    }
                }
                fclose($fh);
            }
            $lorisProject = trim((string) (is_array($pj) ? ($pj['candidate_defaults']['project'] ?? '') : '')) ?: $name;
            echo "\n  LORIS project and visits\n";
            line('LORIS project', $lorisProject);
            $r = $http->get("$apiBase/projects/" . rawurlencode($lorisProject), ['headers' => ['Authorization' => "Bearer $token"]]);
            $exists = $r->getStatusCode() === 200;
            line('exists in LORIS', yn($exists));
            if (!$exists) {
                out('WARN', "LORIS project '$lorisProject' not found via API (HTTP " . $r->getStatusCode() . ')', 'Check the LORIS project exists and candidate_defaults.project in project.json');
            } elseif ($values) {
                $visits  = json_decode((string) $r->getBody(), true)['Visits'] ?? [];
                $missing = array_diff(array_keys($values), $visits);
                line('visits in data', implode(', ', array_keys($values)));
                line('all found in LORIS', yn(!$missing));
                $missing
                    ? out('WARN', 'visit values not in LORIS project: ' . implode(', ', $missing), 'Add the visit in LORIS, or map it in project.json visit_mappings')
                    : out('PASS', 'visit values match LORIS');
            }
        }
    }
}
if ($found === 0) {
    out('FAIL', $onlyProj ? "project '$onlyProj' not enabled in config" : 'no enabled projects in config', 'Add { "name": "<PROJECT>", "enabled": true } to collections[].projects');
}

// ---------------------------------------------------------------- summary
$currentSection = null;
echo "\n== Readiness ==\n";
foreach ($sectionStatus as $name => $st) {
    if ($name === 'Values read from config (review these)') {
        continue;
    }
    $c = ['OK' => "\033[32m", 'WARN' => "\033[33m", 'FAIL' => "\033[31m"][$st];
    printf("  %-34s %s%s\033[0m\n", $name, $c, $st);
}
printf("\n  PASS %d   WARN %d   FAIL %d\n", $counts['PASS'], $counts['WARN'], $counts['FAIL']);
if ($counts['FAIL'] === 0) {
    echo "\n  \033[32mPipeline ready to run: YES\033[0m" . ($counts['WARN'] ? "  (review the WARN lines above)" : '') . "\n";
} else {
    echo "\n  \033[31mPipeline ready to run: NO\033[0m  fix every FAIL above, then rerun this check.\n";
}
exit($counts['FAIL'] === 0 ? 0 : 1);