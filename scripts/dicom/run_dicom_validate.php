<?php

/**
 * run_dicom_validate.php
 *
 * Check a DICOM upload against "ARCHIMEDES - Preparing DICOM Data for
 * Submission" (the DICOM user guide) BEFORE participant sync, reidentification
 * and import, and write a report the site can fix their data from.
 *
 * Read-only. Nothing in the upload, processed/, deidentified-lorisid/ or LORIS is
 * created or changed: the organizer runs as a dry run into a working folder under
 * logs/dicom/ (removed afterwards), and LORIS is only read (projects, sites,
 * candidates). The one file kept is the report, in logs/dicom/.
 *
 * What is checked, by guide section:
 *   1 Folder structure  participants.tsv at the top; sub-<ID>/ses-<visit>/;
 *                       files or folders outside that shape
 *   2 Naming            identifiers and visit labels letters/digits only;
 *                       visits listed in project.json "visits" (when declared)
 *   3 Spreadsheets      tab-separated (not CSV / .xlsx); required columns;
 *                       participant_id matches the folders; sex / dob values;
 *                       sub-<ID>/sessions.tsv (or sub-<ID>_sessions.tsv) with session_id = the ses- folders;
 *                       acq_date format
 *   4 DICOM headers     PatientName = participant ID without sub-; one
 *                       PatientName per study; SeriesDescription kept;
 *                       free-text / identifying fields populated
 *   5 Examinations      unreadable files, a study in two folders, conflicting
 *                       or duplicate files (same checks as the real run)
 *   LORIS values        candidate_defaults / participants.tsv project and
 *                       site exist in LORIS (LORIS 26 API v0.0.4-dev); for a
 *                       participant already in LORIS, participants.tsv dob / sex
 *                       still match the candidate
 *
 * Usage:
 *   php scripts/dicom/run_dicom_validate.php --collection=archimedes --project=FDG-PET
 *   php scripts/dicom/run_dicom_validate.php --collection=archimedes --project=FDG-PET --offline
 *   php scripts/dicom/run_dicom_validate.php --all --report=/tmp/validation.txt
 *
 * Exit codes: 0 ready, 1 problems to fix, 2 usage error, 3 nothing to fix found
 * but some checks could not run (LORIS unreachable or refused).
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\Cli;
use LORIS\Pipelines\Dicom\DicomPreflight;
use LORIS\Pipelines\Dicom\DicomStudyOrganizer;
use LORIS\Endpoints\LorisApiClientAdapter;
use LORIS\Pipelines\Shared\ParticipantsTsv;
use LORIS\Pipelines\Shared\ParticipantSync;
use LORIS\Pipelines\Shared\ParticipantsCheck;
use LORIS\Pipelines\Shared\SessionsTsv;
use LORIS\Utils\Dob;
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;

const EXIT_OK      = 0;
const EXIT_FAILURE = 1;
const EXIT_USAGE   = 2;
const EXIT_INCOMPLETE = 3;   // nothing to fix found, but some checks could not run (LORIS)

const DEFAULT_CONFIG = __DIR__ . '/../../config/loris_client_config.json';

/** Header fields that usually hold free text or identifying details (guide section 4). */
const DEID_FIELDS = [
    '0008,0050' => 'AccessionNumber',
    '0008,1030' => 'StudyDescription',
    '0008,0080' => 'InstitutionName',
    '0008,0081' => 'InstitutionAddress',
    '0008,0090' => 'ReferringPhysicianName',
    '0008,1048' => 'PhysiciansOfRecord',
    '0008,1050' => 'PerformingPhysicianName',
    '0008,1070' => 'OperatorsName',
    '0010,1000' => 'OtherPatientIDs',
    '0010,1001' => 'OtherPatientNames',
    '0010,1040' => 'PatientAddress',
    '0010,2154' => 'PatientTelephoneNumbers',
    '0010,4000' => 'PatientComments',
    '0032,4000' => 'StudyComments',
];

/** Top-level files the guide allows, or that are harmless. */
const ALLOWED_TOP_FILES = [
    'participants.tsv', 'participants.json', 'dataset_description.json',
    'README', 'README.md', 'README.txt', 'CHANGES', 'DICOMDIR',
];

function usage(int $code = EXIT_USAGE): void
{
    fwrite(
        $code === EXIT_OK ? STDOUT : STDERR,
        <<<TXT
Usage: php scripts/dicom/run_dicom_validate.php [options]

Checks a DICOM upload against the DICOM user guide before sync, reidentification
and import, and writes a report the site can fix their data from. Read-only:
nothing is organized, created or ingested.

Selection:
  --all                All enabled projects with Imaging
  --collection=NAME    All enabled projects in a collection
  --project=NAME       One project (requires --collection)

Options:
  --offline            Do not contact LORIS (project and sites
                       are then not checked)
  --report=FILE        Write the report here (default, per project:
                       <project>/logs/dicom/dicom_validation_<date>.txt)
  --config=FILE        Config file (default: config/loris_client_config.json)
  -v, --verbose        Detailed output
  --help               Show this message

Exit codes: 0 ready, 1 problems to fix, 2 usage error,
            3 nothing to fix found but some checks could not run (LORIS)

TXT
    );
    exit($code);
}

function readJson(string $path): array
{
    if (!is_file($path)) {
        throw new RuntimeException("File not found: {$path}");
    }
    $data = json_decode((string) file_get_contents($path), true);
    if (!is_array($data)) {
        throw new RuntimeException("Invalid JSON in {$path}: " . json_last_error_msg());
    }
    return $data;
}

/**
 * Same selection rule as the other DICOM scripts.
 *
 * @return array<int, array{collection: string, project: string, path: string}>
 */
function resolveTargets(array $config, array $options): array
{
    $targets        = [];
    $wantAll        = isset($options['all']);
    $wantCollection = $options['collection'] ?? null;
    $wantProject    = $options['project'] ?? null;

    if ($wantProject !== null && $wantCollection === null) {
        fwrite(STDERR, "--project requires --collection\n\n");
        usage();
    }

    foreach ($config['collections'] ?? [] as $collection) {
        $collectionName = $collection['name'] ?? null;
        if ($collectionName === null) {
            continue;
        }
        if (empty($collection['enabled']) && $wantCollection !== $collectionName) {
            continue;
        }
        if (!$wantAll && $wantCollection !== null && $wantCollection !== $collectionName) {
            continue;
        }
        $basePath = rtrim($collection['base_path'] ?? '', '/');

        foreach ($collection['projects'] ?? [] as $project) {
            $projectName = $project['name'] ?? null;
            if ($projectName === null || ($wantProject !== null && $wantProject !== $projectName)) {
                continue;
            }
            if (!DicomStudyOrganizer::acceptProject(
                "{$collectionName}/{$projectName}",
                $basePath . '/' . $projectName,
                !empty($collection['enabled']) && !empty($project['enabled']),
                $wantProject === $projectName
                || (empty($collection['enabled']) && $wantCollection === $collectionName)
            )) {
                continue;
            }
            $targets[] = [
                'collection' => $collectionName,
                'project'    => $projectName,
                'path'       => $basePath . '/' . $projectName,
            ];
        }
    }

    return $targets;
}

function rmTree(string $dir): void
{
    if (!is_dir($dir)) {
        return;
    }
    $it = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::CHILD_FIRST
    );
    foreach ($it as $f) {
        $f->isDir() && !$f->isLink() ? @rmdir($f->getPathname()) : @unlink($f->getPathname());
    }
    @rmdir($dir);
}

/** Any regular file below $dir (hidden files ignored)? */
function hasFiles(string $dir): bool
{
    return walkFiles($dir)['files'] > 0;
}

/** PatientName / participant ID compared as the organizer does: case and ^ ignored. */
function pnKey(string $v): string
{
    return strtolower(trim(rtrim(trim($v), '^')));
}

/**
 * The concrete fix for a PatientName problem, read against participants.tsv
 * (PatientName must be the participant_id without sub-, guide section 4).
 *
 * @param array<string, string> $tsvIds normalised ID => participant_id
 */
function patientNameFix(string $msg, array $tsvIds): string
{
    if (preg_match('/PatientName "([^"]*)" does not match the folder sub-([A-Za-z0-9_-]+)/', $msg, $m)) {
        [$all, $pn, $folder] = $m;
        $owner = $tsvIds[pnKey($pn)] ?? null;
        if ($owner !== null && $owner !== "sub-{$folder}") {
            return "PatientName '{$pn}' is participant {$owner} in participants.tsv - if these files are "
                . "{$owner}'s, move them to {$owner}/; otherwise set PatientName to {$folder} (guide section 4)";
        }
        return "PatientName '{$pn}' matches no participant in participants.tsv - set PatientName to {$folder} "
            . '(the participant_id without sub-), or file the scans under the right participant (guide section 4)';
    }
    if (str_contains($msg, 'more than one PatientName within a single study')) {
        return 'one examination holds files of more than one participant - separate them, and set PatientName '
            . 'to the participant ID (without sub-) in every file (guide section 4)';
    }
    return '';
}

/**
 * Collects findings in three groups, each with where / what / how to fix.
 */
final class Findings
{
    /** @var array<string, array<int, array{area: string, where: string, what: string, fix: string}>> */
    public array $items = ['fix' => [], 'unlinked' => [], 'check' => [], 'nocheck' => [], 'note' => []];

    public function fix(string $area, string $where, string $what, string $fix = ''): void
    {
        $this->add('fix', $area, $where, $what, $fix);
    }

    public function unlinked(string $area, string $where, string $what, string $fix = ''): void
    {
        $this->add('unlinked', $area, $where, $what, $fix);
    }

    public function check(string $area, string $where, string $what, string $fix = ''): void
    {
        $this->add('check', $area, $where, $what, $fix);
    }

    /** Reported for information only: fine as it is, no action needed. */
    public function note(string $area, string $where, string $what, string $fix = ''): void
    {
        $this->add('note', $area, $where, $what, $fix);
    }

    /** A check that could not run (LORIS unreachable / refused, validator error). */
    public function nocheck(string $area, string $where, string $what, string $fix = ''): void
    {
        $this->add('nocheck', $area, $where, $what, $fix);
    }

    private function add(string $group, string $area, string $where, string $what, string $fix): void
    {
        $key = $area . '|' . $where . '|' . $what;
        foreach ($this->items[$group] as $i) {
            if ($i['area'] . '|' . $i['where'] . '|' . $i['what'] === $key) {
                return;
            }
        }
        $this->items[$group][] = compact('area', 'where', 'what', 'fix');
    }
}

// =============================================================================
//  Checks
// =============================================================================

/**
 * A tab-separated file saved the wrong way (guide section 3): CSV, or not text.
 */
function spreadsheetFormatProblem(string $path): ?string
{
    $head = (string) @file_get_contents($path, false, null, 0, 4096);
    if (str_starts_with($head, "PK\x03\x04") || str_starts_with($head, "\xD0\xCF\x11\xE0")) {
        return 'is an Excel workbook, not a text file - save it as "Tab Delimited Text" (Excel) '
            . 'or "Tab-separated values" (Google Sheets)';
    }
    $first = strtok(str_replace("\r", '', $head), "\n") ?: '';
    if (!str_contains($first, "\t") && (str_contains($first, ',') || str_contains($first, ';'))) {
        return 'is comma- or semicolon-separated - save it as "Tab Delimited Text" (Excel) '
            . 'or "Tab-separated values" (Google Sheets), not CSV';
    }
    return null;
}

/**
 * Walk a folder: regular files, files the pipeline cannot read (the organizer
 * skips those silently, so their slices would be lost), and an error when a
 * folder itself cannot be listed. Hidden files are ignored.
 *
 * @return array{files: int, unreadable: string[], error: ?string}
 */
function walkFiles(string $dir): array
{
    $out = ['files' => 0, 'unreadable' => [], 'error' => null];
    try {
        $it = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS)
        );
        foreach ($it as $file) {
            if (!$file->isFile() || str_starts_with($file->getFilename(), '.')) {
                continue;
            }
            $out['files']++;
            if (!$file->isReadable()) {
                $out['unreadable'][] = $file->getPathname();
            }
        }
    } catch (UnexpectedValueException $e) {
        $out['error'] = $e->getMessage();
    }
    return $out;
}

/** Read a TSV for the checks; null (and a finding) when it cannot be read. */
function readTsv(string $path, string $where, Findings $f, bool $mustFix): ?array
{
    try {
        return ParticipantsTsv::readRows($path);
    } catch (Throwable $e) {
        $mustFix
            ? $f->fix('permissions', $where, 'cannot be read: ' . $e->getMessage(), 'make it readable by the pipeline user')
            : $f->check('permissions', $where, 'cannot be read: ' . $e->getMessage(), 'make it readable by the pipeline user');
        return null;
    }
}

/**
 * Guide sections 1-3: the folder tree, both spreadsheets.
 *
 * @param string[] $outsideRoots    out: top-level names reported as outside sub-/ses-
 * @param array    $emptyPlaces     out: "sub-X/ses-Y" => true for empty visit folders
 * @param array    $reportedLabels  out: "sub-X/ses-Y" => true for invalid labels reported here
 *
 * @return array<string, string[]> subject => ses- labels found on disk
 */
function checkStructure(
    string $dicomDir,
    Findings $f,
    array $projectConfig,
    array &$outsideRoots = [],
    array &$emptyPlaces = [],
    array &$reportedLabels = [],
    array &$blockedSubjects = []
): array {
    $tree = [];
    $tsv  = "{$dicomDir}/participants.tsv";

    // --- top level -----------------------------------------------------------
    foreach (scandir($dicomDir) ?: [] as $name) {
        if ($name === '.' || $name === '..' || str_starts_with($name, '.')) {
            continue;
        }
        $path = "{$dicomDir}/{$name}";

        if (is_dir($path)) {
            if (str_starts_with($name, 'sub-')) {
                $tree[substr($name, 4)] = [];
                continue;
            }
            $outsideRoots[] = $name;
            $f->unlinked('structure', "{$name}/",
                'folder is not a participant folder (sub-<ID>) - nothing in it can be matched to a participant',
                'move its visits into sub-<ID>/ses-<visit>/ (guide section 1)');
            continue;
        }

        if (in_array($name, ALLOWED_TOP_FILES, true)) {
            continue;
        }
        if (preg_match('/^participants\.(csv|xlsx?|txt)$/i', $name)) {
            is_file($tsv)
                ? $f->check('participants.tsv', $name, 'extra participants file next to participants.tsv - only participants.tsv is read',
                'remove it to avoid confusion')
                : $f->fix('participants.tsv', $name, 'participants file has the wrong name or format',
                'save it as participants.tsv, tab-separated (guide section 3)');
            continue;
        }
        $outsideRoots[] = pathinfo($name, PATHINFO_FILENAME);
        $f->unlinked('structure', $name,
            'file sits at the top level, outside any sub-<ID>/ses-<visit>/ folder',
            'move it into the right participant and visit folder (guide section 1)');
    }

    if (empty($tree)) {
        $f->fix('structure', 'dicoms/', 'no participant folders (sub-<ID>) found',
            'organize the upload as sub-<ID>/ses-<visit>/ (guide section 1)');
    }

    // --- participants.tsv ----------------------------------------------------
    if (!is_file($tsv)) {
        $f->fix('participants.tsv', 'participants.tsv',
            'missing - without it no study can be matched to a participant',
            'add participants.tsv at the top level, next to the sub- folders (guide section 3)');
    } elseif (($why = spreadsheetFormatProblem($tsv)) !== null) {
        $f->fix('participants.tsv', 'participants.tsv', $why);
    } elseif (($parsed = readTsv($tsv, 'participants.tsv', $f, true)) !== null) {
        $columns = $parsed['columns'];

        if ($columns === []) {
            $f->fix('participants.tsv', 'participants.tsv', 'is empty',
                'add a header row (participant_id, dob, sex) and one row per participant (guide section 3)');
        } elseif (!in_array('participant_id', $columns, true)) {
            $f->fix('participants.tsv', 'participants.tsv', 'no participant_id column',
                'add the participant_id column, e.g. sub-ARCHI0001 (guide section 3)');
        } elseif (empty($parsed['rows'])) {
            $f->fix('participants.tsv', 'participants.tsv', 'has a header but no participant rows - nothing can be matched',
                'add one row per participant (guide section 3)');
        }

        $seen = [];
        foreach ($parsed['rows'] as ['line' => $line, 'row' => $row]) {
            $pid = $row['participant_id'] ?? '';
            if ($pid === '') {
                if (in_array('participant_id', $columns, true)) {
                    $f->fix('participants.tsv', "line {$line}", 'participant_id is empty', 'fill it in, e.g. sub-ARCHI0001');
                }
                continue;
            }
            if (isset($seen[$pid])) {
                $blockedSubjects[ParticipantSync::subjectId($pid)] = 'listed twice in participants.tsv';
                $f->fix('participants.tsv', "line {$line}",
                    "{$pid} is listed twice (also line {$seen[$pid]}) - the whole project stops until fixed",
                    'keep one row per participant');
            }
            $seen[$pid] = $line;
            if (!str_starts_with($pid, 'sub-')) {
                $f->fix('participants.tsv', "line {$line}", "participant_id '{$pid}' does not start with sub-",
                    "write it exactly as the folder name, e.g. sub-{$pid}");
                continue;
            }
            // An external ID column different from the folder ID: sync creates the
            // candidate under that value, but reidentification looks the
            // participant up by the folder ID - the study would be rejected.
            $folderId = substr($pid, 4);
            $extId    = ParticipantSync::externalId($row, $folderId);
            if ($extId !== $folderId) {
                $blockedSubjects[$folderId] = 'external ID differs from the folder ID';
                $f->fix('participants.tsv', "line {$line} ({$pid})",
                    "external ID '{$extId}' differs from the folder ID '{$folderId}' - reidentification would not find the participant",
                    'remove the external_id / ExtStudyID column, or make it equal to the ID after sub-');
            }
        }
    }

    // --- each participant folder ---------------------------------------------
    foreach (array_keys($tree) as $subject) {
        $subDir   = "{$dicomDir}/sub-{$subject}";
        $label    = "sub-{$subject}/";
        $sessions = [];

        if (!preg_match('/^[A-Za-z0-9]+$/', (string) $subject)) {
            $blockedSubjects[(string) $subject] = 'participant identifier not letters and digits';
            $f->fix('naming', $label,
                'participant identifier must be letters and digits only (no _ - spaces)',
                'rename the folder and the participants.tsv row, e.g. sub-ARCHI0001 (guide section 2)');
        }

        $walk = walkFiles($subDir);
        if ($walk['error'] !== null) {
            $f->fix('permissions', $label, 'a folder cannot be read: ' . $walk['error'],
                'make the upload readable by the pipeline user');
        }
        if (!empty($walk['unreadable'])) {
            $rel = array_map(static fn ($p) => substr($p, strlen($dicomDir) + 1), $walk['unreadable']);
            $f->fix('permissions', $label,
                count($rel) . ' file(s) cannot be read - they would be skipped and their images lost: '
                . implode(', ', array_slice($rel, 0, 5)) . (count($rel) > 5 ? ' ...' : ''),
                'make them readable by the pipeline user');
        }

        $checkVisit = static function (string $visit, string $rel, string $path) use ($f, $subject, &$sessions, &$emptyPlaces, &$reportedLabels): void {
            $sessions[] = $visit;
            if (!preg_match('/^[A-Za-z0-9]+$/', $visit)) {
                $reportedLabels["sub-{$subject}/ses-{$visit}"] = true;
                $f->fix('naming', $rel, 'visit label must be letters and digits only (no _ - spaces)',
                    'rename it, e.g. ses-V01 (guide section 2)');
            }
            if (walkFiles($path)['files'] === 0) {
                $emptyPlaces["sub-{$subject}/ses-{$visit}"] = true;
                $f->check('structure', $rel, 'visit folder is empty - nothing to ingest', 'add the DICOM files, or remove the folder');
            }
        };

        foreach (scandir($subDir) ?: [] as $name) {
            if ($name === '.' || $name === '..' || str_starts_with($name, '.')) {
                continue;
            }
            $path = "{$subDir}/{$name}";
            if (is_dir($path) && str_starts_with($name, 'ses-')) {
                $checkVisit(substr($name, 4), "{$label}{$name}/", $path);
                continue;
            }
            if (is_dir($path) && str_starts_with($name, 'sub-')) {
                $f->check('structure', "{$label}{$name}/",
                    'participant folder inside another participant folder (copied one level too deep?)',
                    "move it up next to {$label}");
                continue;
            }
            if (is_file($path) && in_array($name, SessionsTsv::names($subject), true)) {
                continue;
            }
            if (is_file($path) && preg_match('/(^|_)sessions\.(tsv|csv|xlsx?)$/i', $name)) {
                $f->check('sessions.tsv', "{$label}{$name}",
                    "sessions file should be named sessions.tsv (or sub-{$subject}_sessions.tsv), tab-separated",
                    'rename / re-save it (guide section 3)');
                continue;
            }
            // A folder that holds visit folders further down: the organizer still
            // finds them, but the guide fixes sub-/ses- as the top two levels.
            $nested = is_dir($path) ? (glob($path . '/{,*/,*/*/}ses-*', GLOB_ONLYDIR | GLOB_BRACE) ?: []) : [];
            if (!empty($nested)) {
                foreach ($nested as $n) {
                    $checkVisit(substr(basename($n), 4), $label . substr($n, strlen($subDir) + 1) . '/', $n);
                }
                $f->check('structure', "{$label}{$name}/",
                    'visit folder(s) are inside ' . $name . '/ instead of directly in ' . $label,
                    "move ses-<visit>/ up to {$label} (guide section 1)");
                continue;
            }
            $f->unlinked('structure', "{$label}{$name}",
                (is_dir($path) ? 'folder' : 'file') . ' is not inside a visit folder (ses-<visit>) - its scans cannot be matched to a visit',
                "move it into {$label}ses-<visit>/ (guide section 1)");
        }

        if (empty($sessions)) {
            $f->check('structure', $label, 'no visit folders (ses-<visit>)',
                "put the scans in {$label}ses-<visit>/ (guide section 1)");
        }

        // sub-<ID>_sessions.tsv - rules shared with the ingestion (Shared\SessionsTsv)
        $sessTsv = SessionsTsv::path($dicomDir, $subject);
        $sessRel = $label . basename($sessTsv);
        if (SessionsTsv::both($dicomDir, $subject)) {
            $f->check('sessions.tsv', $label, "both sessions.tsv and sub-{$subject}_sessions.tsv are present - only sessions.tsv is read",
                'keep one of them');
        }
        if (is_file($sessTsv) && ($why = spreadsheetFormatProblem($sessTsv)) !== null) {
            $f->check('sessions.tsv', $sessRel, $why);
        } elseif (is_file($sessTsv) && ($err = SessionsTsv::read($dicomDir, $subject)['error']) !== null) {
            $f->check('permissions', $sessRel, 'cannot be read: ' . $err, 'make it readable by the pipeline user');
        } else {
            foreach (SessionsTsv::check($dicomDir, $subject, $sessions) as $p) {
                switch ($p['kind']) {
                    case SessionsTsv::MISSING:
                        $f->fix('sessions.tsv', $label, $p['message'], $p['fix']);
                        break;
                    case SessionsTsv::NO_COLUMN:
                        $f->fix('sessions.tsv', $sessRel, $p['message'], $p['fix']);
                        break;
                    case SessionsTsv::NOT_LISTED:
                        $f->fix('sessions.tsv', "{$label}ses-{$p['visit']}/", $p['message'], $p['fix']);
                        break;
                    case SessionsTsv::BAD_ACQ_DATE:
                        $f->check('sessions.tsv', "{$sessRel} line {$p['line']}", $p['message']);
                        break;
                    default:   // LISTED_NO_FOLDER
                        $f->check('sessions.tsv', $sessRel, $p['message'], $p['fix']);
                }
            }
        }

        $tree[$subject] = $sessions;
    }

    return $tree;
}

/**
 * Guide section 4: SeriesDescription kept, identifying fields blank.
 * One dcmdump per series (its first file).
 */
function checkHeaders(array $studies, string $dcmdump, Findings $f, bool $verbose, string $dicomDir = ''): void
{
    if (!is_executable($dcmdump)) {
        $f->check('headers', 'all studies', "de-identification fields not checked: {$dcmdump} not found");
        return;
    }

    $populated = [];   // field => [places]
    $tags      = implode(' ', array_map(static fn ($t) => '+P ' . escapeshellarg($t), array_keys(DEID_FIELDS)));

    foreach ($studies as $study) {
        $where = sprintf('sub-%s/ses-%s', $study['subject'] ?? '?', $study['session'] ?? '?');

        // Guide section 4: StudyDate orders examinations within a visit.
        if (trim((string) ($study['study_date'] ?? '')) === '') {
            $f->check('headers', $where, 'StudyDate is empty', 'keep StudyDate in the export (guide section 4)');
        }
        // Text in another character set: substituted on our copy, shown as ?.
        foreach (['patient_name' => 'PatientName'] as $k => $field) {
            if (str_contains((string) ($study[$k] ?? ''), "\u{FFFD}")) {
                $f->check('headers', $where, "{$field} has characters that are not UTF-8 (SpecificCharacterSet)",
                    'use plain letters and digits');
            }
        }

        foreach ($study['series'] ?? [] as $series) {
            $sWhere = "{$where} series " . ($series['series_number'] ?? '?') . ' (' . (($series['modality'] ?? '') ?: '?') . ')';
            if (trim((string) ($series['modality'] ?? '')) === '') {
                $f->fix('headers', $sWhere, 'Modality is empty - the examination cannot be archived by type',
                    'keep Modality (MR, PT, CT ...) in the export (guide section 4)');
            }
            if (str_contains((string) ($series['description'] ?? ''), "\u{FFFD}")) {
                $f->check('headers', $sWhere, 'SeriesDescription has characters that are not UTF-8 - they will show as ?',
                    'use plain characters in SeriesDescription');
            }
            $desc = trim((string) ($series['description'] ?? ''));
            if ($desc === '') {
                $f->check('headers', "{$where} series " . ($series['series_number'] ?? '?')
                    . ' (' . ($series['modality'] ?? '?') . ')',
                    'SeriesDescription is empty - the scan will have no label in ARCHIMEDES',
                    'keep SeriesDescription unless it holds identifying text (guide section 4)');
            }

        }
    }

    // Identifying fields: read the upload itself (the dry run copies nothing),
    // one file from every folder under each matched visit.
    $places = [];
    foreach ($studies as $study) {
        $places[sprintf('sub-%s/ses-%s', $study['subject'] ?? '?', $study['session'] ?? '?')] = true;
    }
    foreach (array_keys($places) as $where) {
        $root = rtrim($dicomDir, '/') . '/' . $where;
        if (!is_dir($root)) {
            continue;
        }
        $seenDirs = [];
        try {
            $it = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS));
            $files = iterator_to_array($it, false);
        } catch (UnexpectedValueException) {
            continue;   // reported by the structure check (permissions)
        }
        foreach ($files as $file) {
            $dir = $file->getPath();
            if (isset($seenDirs[$dir]) || !$file->isFile()
                || str_starts_with($file->getFilename(), '.') || strcasecmp($file->getFilename(), 'DICOMDIR') === 0
            ) {
                continue;
            }
            $seenDirs[$dir] = true;
            $out = (string) shell_exec(escapeshellarg($dcmdump) . ' -q ' . $tags . ' '
                . escapeshellarg($file->getPathname()) . ' 2>/dev/null');
            foreach (explode("\n", $out) as $line) {
                if (preg_match('/^\(([0-9a-f]{4},[0-9a-f]{4})\)\s+\S\S\s+\[(.*)\]/i', trim($line), $m)
                    && trim($m[2]) !== ''
                ) {
                    $name = DEID_FIELDS[strtolower($m[1])] ?? $m[1];
                    $populated[$name][$where] = true;
                }
            }
        }
    }

    foreach ($populated as $field => $places) {
        $list = array_keys($places);
        $f->check('headers', implode(', ', array_slice($list, 0, 5)) . (count($list) > 5 ? ' ...' : ''),
            "{$field} is filled in (" . count($list) . ' visit(s)) - it often holds names or notes',
            'blank it before uploading unless you are sure it is not identifying (guide section 4)');
    }
}

/**
 * What a LORIS refusal means for the pipeline user. The CBIGR override of the
 * api module answers 403 to every /api/... call (except login) without the
 * loris_api permission.
 */
function lorisHint(string $msg): string
{
    if (str_contains($msg, 'HTTP 403')) {
        return 'the pipeline API user is not allowed: give it the loris_api permission '
            . '(CBIGR api override), then re-run - see docs/dicom.md section 11';
    }
    if (str_contains($msg, 'HTTP 401') || str_contains($msg, 'did not return a token')) {
        return 'check api.base_url and the pipeline user credentials, or run with --offline';
    }
    return 'check api.base_url / credentials, or run with --offline';
}

/**
 * LORIS values: project, site (LORIS 26 API v0.0.4-dev); cohort and visits are checked by LORIS
 * when the session is created.
 */
function checkLorisValues(LorisApiClientAdapter $api, array $projectConfig, ?ParticipantsTsv $roster, Findings $f): void
{
    $defaults = $projectConfig['candidate_defaults'] ?? [];
    $values   = ['project' => [], 'cohort' => [], 'site' => []];

    foreach (array_keys($values) as $col) {
        if (!empty($defaults[$col])) {
            $values[$col][(string) $defaults[$col]] = 'project.json candidate_defaults.' . $col;
        }
        foreach ($roster?->all() ?? [] as $pid => $row) {
            $v = trim((string) ($row[$col] ?? ''));
            if ($v !== '' && !isset($values[$col][$v])) {
                $values[$col][$v] = "participants.tsv ({$pid})";
            }
        }
    }

    // project
    try {
        $projects = $api->getProjectNames();
        foreach ($values['project'] as $v => $from) {
            if (!in_array($v, $projects, true)) {
                $f->fix('LORIS', $from, "project '{$v}' does not exist in LORIS",
                    'use the exact LORIS project name: ' . implode(', ', $projects));
            }
        }
    } catch (Throwable $e) {
        $f->nocheck('LORIS', 'projects', 'cannot read the LORIS projects: ' . $e->getMessage(),
            lorisHint($e->getMessage()));
    }

    // cohort: not checked here. The only API call that lists a project's
    // cohorts (GET /projects/{name}) also returns every instrument; LORIS
    // checks the cohort when the session is created, and the run stops there
    // if it is wrong.

    // participants already in LORIS: participants.tsv must still agree
    // (an edited dob / sex is not applied to LORIS by the ingestion)
    foreach ($roster?->all() ?? [] as $pid => $row) {
        $subject = ParticipantSync::subjectId((string) $pid);
        $extId   = ParticipantSync::externalId($row, $subject);
        try {
            $existing = $api->findCandidateByExternalId($extId);
            if ($existing === null) {
                continue;    // new participant: created by the participant sync
            }
            $diff = ParticipantSync::lorisDifferences($api, $existing, $row);
            if ($diff !== []) {
                $f->fix('LORIS', "participants.tsv ({$pid})",
                    "already in LORIS as {$existing['pscid']}, but " . implode('; ', $diff)
                    . ' - ingestion does not change LORIS, so the two would stay different',
                    'correct participants.tsv, or ask the ARCHIMEDES team to correct the candidate in LORIS');
            }
        } catch (Throwable $e) {
            $f->nocheck('LORIS', "participants.tsv ({$pid})", 'cannot compare with LORIS: ' . $e->getMessage(),
                lorisHint($e->getMessage()));
            break;
        }
    }

    // site
    if (empty($values['site'])) {
        // no site anywhere: reported once, by the pre-flight (ParticipantsCheck::columnSources)
    } else {
        try {
            $sites = $api->getSites();
            $names = array_column($sites, 'Name');
            foreach ($values['site'] as $v => $from) {
                if (in_array($v, $names, true)) {
                    continue;
                }
                $hint = null;
                foreach ($sites as $s) {
                    if (strcasecmp($s['Alias'], $v) === 0 || strcasecmp($s['Name'], $v) === 0) {
                        $hint = $s['Name'];
                    }
                }
                $f->fix('LORIS', $from, "site '{$v}' is not a LORIS site name the pipeline user is affiliated with"
                    . ' (candidate creation needs that affiliation)',
                    $hint !== null ? "use the site name '{$hint}'" : 'use one of: ' . implode(', ', $names));
            }
        } catch (Throwable $e) {
            $f->nocheck('LORIS', 'sites', 'cannot read the LORIS sites: ' . $e->getMessage(),
                lorisHint($e->getMessage()));
        }
    }
}

// =============================================================================
//  Report
// =============================================================================

/**
 * @param array<string, array{line: string, status: string, why: string}> $studies
 *
 * @return array{0: string, 1: string} report text, status READY|NOT READY|INCOMPLETE
 */
function renderReport(string $label, string $dicomDir, Findings $f, array $studies, bool $offline): array
{
    $n       = $f->items;
    $ready   = array_filter($studies, static fn ($s) => $s['status'] === 'READY');
    $blocked = array_filter($studies, static fn ($s) => $s['status'] !== 'READY');
    $nFix    = count($n['fix']);

    // READY only when every examination will go in: nothing to fix, nothing
    // blocked, nothing left unmatched, and at least one examination.
    if ($nFix > 0 || !empty($blocked) || !empty($n['unlinked']) || empty($ready)) {
        $status = 'NOT READY';
    } elseif (!empty($n['nocheck'])) {
        $status = 'INCOMPLETE';
    } else {
        $status = 'READY';
    }

    $out   = [];
    $out[] = str_repeat('=', 78);
    $out[] = " DICOM upload check - {$label}";
    $out[] = ' ' . date('Y-m-d H:i T') . ($offline ? '   (offline: LORIS values not checked)' : '');
    $out[] = " Upload: {$dicomDir}";
    $out[] = str_repeat('=', 78);
    $out[] = match ($status) {
        'READY'      => sprintf(' RESULT: READY - all %d examination(s) can be ingested', count($ready)),
        'INCOMPLETE' => sprintf(' RESULT: NO PROBLEMS FOUND, BUT NOT EVERYTHING COULD BE CHECKED - %d examination(s)', count($ready)),
        default      => empty($studies) && $nFix === 0
            ? ' RESULT: NOT READY - no examination can be ingested'
            : sprintf(' RESULT: NOT READY - %d problem(s) to fix; %d examination(s) ready, %d blocked',
                $nFix, count($ready), count($blocked)),
    };
    if (!empty($n['unlinked'])) {
        $out[] = sprintf('         %d item(s) cannot be matched to a participant/visit and will not be ingested', count($n['unlinked']));
    }
    if (!empty($n['check'])) {
        $out[] = sprintf('         %d thing(s) to check', count($n['check']));
    }
    if (!empty($n['nocheck'])) {
        $out[] = sprintf('         %d check(s) could not run - see COULD NOT CHECK', count($n['nocheck']));
    }

    $section = static function (string $title, array $items) use (&$out, $dicomDir): void {
        if (empty($items)) {
            return;
        }
        $out[] = '';
        $out[] = $title;
        $out[] = str_repeat('-', strlen($title));
        $i = 0;
        foreach ($items as $it) {
            $i++;
            $clean = static fn (string $t): string => str_replace($dicomDir . '/', '', $t);
            $out[] = sprintf('%3d. [%s] %s', $i, $it['area'], $clean($it['where']));
            $out[] = '     ' . $clean($it['what']);
            if ($it['fix'] !== '') {
                $out[] = '     -> ' . $clean($it['fix']);
            }
        }
    };

    $section('MUST FIX - blocks ingestion of the affected data', $n['fix']);
    $section('NOT INGESTED - cannot be matched to a participant or visit', $n['unlinked']);
    $section('PLEASE CHECK', $n['check']);
    $section('COULD NOT CHECK - for the ARCHIMEDES team, not the site', $n['nocheck']);
    $section('FROM project.json - OK, no action needed', $n['note']);

    $out[] = '';
    $out[] = 'EXAMINATIONS';
    $out[] = '------------';
    if (empty($studies)) {
        $out[] = '  (none found)';
    }
    foreach ($ready as $s) {
        $out[] = '  READY    ' . $s['line'];
    }
    foreach ($blocked as $s) {
        $out[] = '  BLOCKED  ' . $s['line'] . '  - ' . $s['why'];
    }
    $out[] = '';
    $out[] = 'Guide: "ARCHIMEDES - Preparing DICOM Data for Submission". Fix the items above, re-upload,';
    $out[] = 'and run this check again. Nothing was created or ingested by this check.';
    $out[] = '';

    return [implode("\n", $out), $status];
}

// =============================================================================
//  Main
// =============================================================================

$shortOpts = 'v';
$longOpts  = ['all', 'collection:', 'project:', 'offline', 'report:', 'config:', 'verbose', 'help'];
$options = getopt($shortOpts, $longOpts);
// getopt() ignores options it does not know; a typo or a removed option
// must not run with the default instead.
Cli::exitOnUnknownOptions($shortOpts, $longOpts);

if ($options === false || isset($options['help'])) {
    usage(isset($options['help']) ? EXIT_OK : EXIT_USAGE);
}
if (!isset($options['all']) && !isset($options['collection'])) {
    fwrite(STDERR, "Specify --all or --collection=NAME\n\n");
    usage();
}

$offline    = isset($options['offline']);
$verbose    = isset($options['v']) || isset($options['verbose']);
$configPath = $options['config'] ?? DEFAULT_CONFIG;

try {
    $config = readJson($configPath);
} catch (RuntimeException $e) {
    fwrite(STDERR, 'ERROR ' . $e->getMessage() . "\n");
    exit(EXIT_USAGE);
}

$targets = resolveTargets($config, $options);
if (empty($targets)) {
    fwrite(STDERR, "No enabled projects with Imaging matched the selection.\n");
    exit(EXIT_USAGE);
}

$logger = new Logger('dicom_validate');
$logger->pushHandler(new StreamHandler('php://stderr', $verbose ? Level::Debug : Level::Error));

$dicomInfoPath = $config['imaging']['get_dicom_info_path']
    ?? rtrim($config['imaging']['mri_code_path'] ?? '', '/') . '/dicom-archive/get_dicom_info.pl';
$dcmdump = $config['imaging']['dcmdump_path'] ?? '/usr/bin/dcmdump';

$exitCode   = EXIT_OK;
$reportFile = $options['report'] ?? null;
$firstWrite = true;

// LORIS lookups during a check should not hang for the pipeline's long import
// timeout: the validator only reads small lists.
$checkConfig            = $config;
$checkConfig['timeout'] = min((int) ($config['timeout'] ?? 60), 60);

foreach ($targets as $target) {
    $label       = "{$target['collection']}/{$target['project']}";
    $projectPath = $target['path'];
    $dicomDir    = $projectPath . '/deidentified-raw/imaging/dicoms';
    $f           = new Findings();
    $studiesOut  = [];   // uid => ['line' => ..., 'status' => READY|BLOCKED, 'why' => ...]
    $api         = null;
    $tmp         = null;

    fwrite(STDERR, "=== {$label} ===\n");

    try {
        $projectConfig = readJson($projectPath . '/project.json');

        if (!is_dir($dicomDir)) {
            $f->fix('structure', 'deidentified-raw/imaging/dicoms/', 'upload folder does not exist',
                'upload into <project>/deidentified-raw/imaging/dicoms/ (guide section 1)');
        } else {
            validateUpload($f, $studiesOut, $api, $tmp, $config, $checkConfig, $projectConfig,
                $projectPath, $dicomDir, $offline, $verbose, $logger, $dicomInfoPath, $dcmdump);
        }
    } catch (Throwable $e) {
        $f->nocheck('validator', $label, 'the check stopped early: ' . $e->getMessage(),
            'fix the cause and run the check again; the report below is incomplete');
    } finally {
        if ($tmp !== null) {
            rmTree($tmp);
        }
    }

    // --- report ---------------------------------------------------------------
    [$report, $status] = renderReport($label, $dicomDir, $f, $studiesOut, $offline);

    if ($reportFile === null) {
        $logDir = $projectPath . '/logs/dicom';
        @mkdir($logDir, 0775, true);
        $path = $logDir . '/dicom_validation_' . date('Y-m-d_His') . '.txt';
        $ok   = @file_put_contents($path, $report) !== false;
    } else {
        $path       = $reportFile;
        $ok         = @file_put_contents($path, $report . "\n", $firstWrite ? 0 : FILE_APPEND) !== false;
        $firstWrite = false;
    }

    echo $report, "\n";
    fwrite(STDERR, $ok ? "  report: {$path}\n\n" : "  WARNING report could not be written to {$path}\n\n");

    // Across projects the worst wins: to fix (1) > incomplete (3) > ready (0).
    $code     = ['READY' => EXIT_OK, 'INCOMPLETE' => EXIT_INCOMPLETE][$status] ?? EXIT_FAILURE;
    $rank     = [EXIT_OK => 0, EXIT_INCOMPLETE => 1, EXIT_FAILURE => 2];
    $exitCode = $rank[$code] > $rank[$exitCode] ? $code : $exitCode;
}

exit($exitCode);

/**
 * Every check for one upload. Fills $f and $studiesOut; $tmp is the working
 * folder (removed by the caller, whatever happens here).
 */
function validateUpload(
    Findings $f,
    array &$studiesOut,
    ?LorisApiClientAdapter &$api,
    ?string &$tmp,
    array $config,
    array $checkConfig,
    array $projectConfig,
    string $projectPath,
    string $dicomDir,
    bool $offline,
    bool $verbose,
    Logger $logger,
    string $dicomInfoPath,
    string $dcmdump
): void {
    // --- 1-3: folders and spreadsheets -------------------------------------------
    $outsideRoots   = [];
    $emptyPlaces    = [];
    $reportedLabels  = [];
    $blockedSubjects = [];
    checkStructure($dicomDir, $f, $projectConfig, $outsideRoots, $emptyPlaces, $reportedLabels, $blockedSubjects);
    $structureUnlinked = count($f->items['unlinked']);

    // Orphans, both directions (shared ParticipantSync::orphanFolders).
    $orphanSubjects = [];
    $tsvIds         = [];   // normalised participant ID (no sub-) => participant_id
    $tsv            = "{$dicomDir}/participants.tsv";
    $tsvUsable      = false;
    if (is_file($tsv) && spreadsheetFormatProblem($tsv) === null) {
        try {
            $parsed = ParticipantsTsv::readRows($tsv);
        } catch (Throwable) {
            $parsed = ['columns' => [], 'rows' => []];   // reported by the structure check
        }
        $tsvUsable = in_array('participant_id', $parsed['columns'], true) && !empty($parsed['rows']);
        $ids = [];
        foreach ($parsed['rows'] as ['row' => $row]) {
            if (($row['participant_id'] ?? '') !== '') {
                $ids[] = (string) $row['participant_id'];
            }
        }
        foreach ($ids as $id) {
            $tsvIds[pnKey(ParticipantSync::subjectId($id))] = $id;
        }
        if ($tsvUsable) {
            $orphans = ParticipantSync::orphanFolders($ids, $dicomDir);
            foreach ($orphans['dirs'] as $dir) {
                $orphanSubjects[substr($dir, 4)] = true;
                $f->unlinked('participants.tsv', "{$dir}/",
                    'no row in participants.tsv - nothing in this folder will be synced, reidentified or ingested',
                    "add a row for {$dir} (participant_id, dob, sex), or remove the folder (guide section 3)");
            }
            foreach ($orphans['rows'] as $pid) {
                if (str_starts_with($pid, 'sub-')) {
                    $f->check('participants.tsv', $pid,
                        "listed in participants.tsv, but there is no {$pid}/ folder in this upload",
                        'upload the folder, or remove the row if this participant has no imaging');
                }
            }
        }
    }

    $wait = DicomStudyOrganizer::uploadInProgress($dicomDir, (int) ($config['imaging']['delivery_quiet_minutes'] ?? 5));
    if ($wait !== null) {
        $f->check('upload', 'dicoms/', $wait . ' - this check may be incomplete', 'run it again once the upload has finished');
    }

    // --- 5: examinations - organizer dry run into a working folder in logs/ -------
    $tmp = $projectPath . '/logs/dicom/validate_tmp_' . date('Ymd_His') . '_' . bin2hex(random_bytes(3));
    if (!@mkdir($tmp, 0775, true) && !is_dir($tmp)) {
        $tmp = sys_get_temp_dir() . '/dicom_validate_' . bin2hex(random_bytes(6));
        @mkdir($tmp, 0700, true);
    }
    $manifestPath = "{$tmp}/dicom_studies.json";
    $studies      = [];

    $runOrganizer = static function (bool $ignoreParticipants) use ($config, $dicomInfoPath, $tmp, $logger, $dcmdump, $dicomDir, $manifestPath): array {
        $organizer = new DicomStudyOrganizer($dicomInfoPath, $tmp, $logger);
        $organizer->setPerl5Lib($config['imaging']['perl5lib'] ?? null);
        switch ($config['imaging']['header_reader'] ?? 'dcmdump') {
            case 'get_dicom_info':
                $organizer->useGetDicomInfoReader();
                break;
            case 'pydicom':
                $organizer->usePydicomReader(
                    $config['imaging']['python_bin']
                    ?? '/opt/LorisMRI/bin/mri/python_virtualenvs/loris-mri-python/bin/python',
                    $config['imaging']['header_dump_script'] ?? dirname(__DIR__, 2) . '/bin/dicom_header_dump.py'
                );
                break;
            default:
                $organizer->useDcmdumpReader($dcmdump);
        }
        $organizer->setDryRun(true);
        $organizer->setForce(true);
        $organizer->setIgnoreParticipants($ignoreParticipants);
        $organizer->organize($dicomDir);
        return is_file($manifestPath) ? (readJson($manifestPath)['studies'] ?? []) : [];
    };

    try {
        // participants.tsv unusable (already reported): still check the files.
        $studies = $runOrganizer(!$tsvUsable && is_file($tsv));
    } catch (Throwable $e) {
        $msg = str_replace($dicomDir . '/', '', $e->getMessage());
        str_contains($msg, 'No candidate files found')
            ? $f->fix('files', 'dicoms/', 'no DICOM files found in the upload', 'upload the DICOM files into sub-<ID>/ses-<visit>/ (guide section 1)')
            : $f->fix('files', 'dicoms/', 'the files could not be checked: ' . $msg);
    }

    // --- LORIS: reachable? (a refusal is "could not check", not the site's data) ----
    // Visits come from project.json "visits" (the LORIS API cannot list the
    // configured ones), so they are checked online and --offline alike.
    $declaredVisits = array_values(array_filter(array_map('strval', (array) ($projectConfig['visits'] ?? []))));
    $visitFn        = static fn (): array => $declaredVisits;
    $lorisProject   = null;
    if (!$offline) {
        try {
            $api = new LorisApiClientAdapter($checkConfig, $logger);
            $api->resolveProject($projectConfig, $checkConfig);
            $api->setSexMappings($projectConfig['sex_mappings'] ?? []);
            $projects     = $api->getProjectNames();
            $lorisProject = $api->getProjectName();
        } catch (Throwable $e) {
            $msg = $e->getMessage();
            if (str_contains($msg, 'candidate_defaults.project')) {
                $f->fix('project.json', 'candidate_defaults.project', 'not set - the LORIS project cannot be checked',
                    'set candidate_defaults.project to the LORIS project name');
            } else {
                $f->nocheck('LORIS', 'api', 'LORIS could not be read: ' . $msg, lorisHint($msg));
            }
            $api = null;
        }
    }

    $pre  = DicomPreflight::check($projectPath, $projectConfig, $visitFn, [], $manifestPath);
    $plan = DicomPreflight::plan($projectPath, $pre['issues'], $manifestPath);
    $studyById = [];
    foreach ($studies as $st) {
        $studyById[$st['study_instance_uid'] ?? ''] = $st;
    }
    $issueWhy = [];   // uid => reason (for the STUDIES list)

    foreach ($pre['issues'] as $issue) {
        $msg  = str_replace($dicomDir . '/', '', (string) $issue['message']);
        $area = str_starts_with($msg, 'organize: ') ? 'files' : 'participants';
        $msg  = preg_replace('/^organize: /', '', $msg);

        // Already reported, more precisely, by the structure check.
        if (str_contains($msg, 'identifier must be letters and digits only')
            || str_contains($msg, 'Duplicate participant_id')
            || (!$tsvUsable && str_starts_with($msg, 'participants.tsv'))
        ) {
            continue;
        }
        if (preg_match('#^(sub-[^:\s]+/ses-[^:\s]+): visit label must be letters and digits only#', $msg, $lm)
            && isset($reportedLabels[$lm[1]])
        ) {
            continue;
        }
        if (str_contains($msg, 'is not in project.json "visits"')
            && preg_match("/visit '([^']*)'/", $msg, $vm) && !preg_match('/^[A-Za-z0-9]+$/', $vm[1])
        ) {
            continue;   // the label itself is invalid: fix that first
        }
        if (str_contains($msg, 'is not in project.json "visits"')) {
            $area = 'project.json';
        }

        $uid = (string) ($issue['uid'] ?? '');
        if (preg_match('/in more than one participant\/visit folder \(([^)]*)\)/', $msg, $mm)) {
            $where = str_replace(', ', ' + ', $mm[1]);
            $f->fix($area, $where, $msg, 'keep the examination in one sub-<ID>/ses-<visit>/ folder and delete the other copy');
        } elseif (preg_match('#^(sub-[^:\s]+(?:/ses-[^:\s]+)?): (.*)$#s', $msg, $pm)) {
            $where = $pm[1];
            $f->fix($area, $where, $pm[2], patientNameFix($msg, $tsvIds));
        } else {
            $where = match ($issue['scope']) {
                'subject' => 'sub-' . ($issue['subject'] ?? '?'),
                'place'   => 'sub-' . ($issue['subject'] ?? '?') . (isset($issue['session']) ? '/ses-' . $issue['session'] : ''),
                'study'   => isset($studyById[$uid])
                    ? sprintf('sub-%s/ses-%s', $studyById[$uid]['subject'] ?? '?', $studyById[$uid]['session'] ?? '?')
                    : 'study ' . substr($uid, -12),
                default   => $area === 'files' ? 'dicoms/' : 'participants.tsv',
            };
            $f->fix($area, $where, $msg, patientNameFix($msg, $tsvIds));
        }

        // Rejected before organizing (PatientName mismatch, study in two folders):
        // not in the manifest, but still an examination the site sent.
        if ($issue['scope'] === 'study' && $uid !== '' && !isset($studyById[$uid])) {
            $studiesOut[$uid] = ['line' => sprintf('%-40s (not organized)', $where), 'status' => 'BLOCKED', 'why' => 'see MUST FIX'];
        }
    }
    foreach ($pre['warnings'] as $w) {
        $w = str_replace($dicomDir . '/', '', $w);
        if (str_contains($w, 'has no imaging in this delivery')) {
            continue;   // reported by the orphan check
        }
        if (str_contains($w, 'no readable DICOM files')
            && preg_match('#(sub-[^:\s]+/ses-[^:\s]+)#', $w, $em) && isset($emptyPlaces[$em[1]])
        ) {
            continue;   // reported as an empty visit folder
        }
        if (str_starts_with($w, 'UNLINKED ')) {
            // "UNLINKED <dir>: <advice>"
            [$dir, $advice] = array_pad(explode(': ', substr($w, 9), 2), 2, '');
            $bare = preg_replace('/^unlinked-/', '', $dir);
            if (preg_match('/^sub-([A-Za-z0-9]+)_/', $bare, $m) && isset($orphanSubjects[$m[1]])) {
                continue;   // its whole participant folder is already reported
            }
            if (!$tsvUsable && is_file($tsv)) {
                continue;   // everything is unlinked because participants.tsv is unusable (reported)
            }
            if ($structureUnlinked > 0 && str_contains($advice, 'not inside a sub-')) {
                continue;   // the misplaced folder/file is reported by the structure check
            }
            foreach ($outsideRoots as $root) {
                if ($bare === $root || str_starts_with($bare, $root . '_') || str_starts_with($bare, $root . '/')) {
                    continue 2;
                }
            }
            $f->unlinked('participants', $dir, 'cannot be matched to a participant and listed visit - will not be ingested', $advice);
            continue;
        }
        if (str_contains($w, 'UNLINKED')) {
            continue;   // the same study, reported once above
        }
        $f->check(str_starts_with($w, 'organize: ') ? 'files' : 'participants', 'dicoms/', preg_replace('/^organize: /', '', $w));
    }

    // --- 4: headers ---------------------------------------------------------------
    $linked = array_values(array_filter($studies, static fn ($st) => ($st['link_status'] ?? '') === 'linked'));
    checkHeaders($linked, $dcmdump, $f, $verbose, $dicomDir);

    // Reidentification copies the original PatientName into OtherPatientNames
    // with dcmodify, which cannot carry \ ^ = safely (DicomHeaderWriter stops).
    $badPn = [];
    foreach ($linked as $st) {
        $pn = (string) ($st['patient_name'] ?? '');
        if ($pn !== '' && strpbrk($pn, "\\^=") !== false) {
            $f->fix('headers', sprintf('sub-%s/ses-%s', $st['subject'] ?? '?', $st['session'] ?? '?'),
                "PatientName '{$pn}' contains \\, ^ or = - reidentification cannot write it",
                'set PatientName to the participant ID only, e.g. ' . ($st['subject'] ?? 'ARCHI0001') . ' (guide section 4)');
            $badPn[$st['study_instance_uid'] ?? ''] = true;
        }
        foreach ($st['series'] ?? [] as $se) {
            if (trim((string) ($se['modality'] ?? '')) === '') {
                $badPn[$st['study_instance_uid'] ?? ''] = true;   // Modality missing: reported above
            }
        }
    }

    // --- participants.tsv values taken from project.json (reported, OK) ------------
    // A value missing from the file AND from candidate_defaults is a MUST FIX
    // (pre-flight above). One missing from the file only is filled from
    // project.json - said here, so the site knows where it came from.
    $roster = null;
    if ($tsvUsable) {
        try {
            $roster = ParticipantSync::load($tsv, $projectConfig)['roster'];
        } catch (Throwable) {
            // reported by the structure check / pre-flight
        }
    }
    foreach ($roster !== null ? ParticipantsCheck::columnSources($roster)['defaults'] : [] as $line) {
        $f->note('participants.tsv', 'participants.tsv', $line);
    }

    // --- LORIS values -------------------------------------------------------------
    if ($api !== null) {
        checkLorisValues($api, $projectConfig, $roster, $f);
    }

    // --- every examination: READY or BLOCKED ----------------------------------------
    $excluded = array_flip($plan['exclude_uids'] ?? []);
    $abort    = !empty($plan['abort']);
    foreach ($linked as $st) {
        $uid  = (string) ($st['study_instance_uid'] ?? '');
        $line = sprintf('sub-%s/ses-%s  %-8s %2d series %5d files',
            $st['subject'] ?? '?', $st['session'] ?? '?',
            implode('+', (array) ($st['modalities'] ?? [])) ?: '?',
            (int) ($st['series_count'] ?? 0), (int) ($st['file_count'] ?? 0));
        $place = sprintf('sub-%s/ses-%s', $st['subject'] ?? '?', $st['session'] ?? '?');
        if ($abort) {
            $studiesOut[$uid] = ['line' => $line, 'status' => 'BLOCKED', 'why' => 'project-level problem above'];
        } elseif (isset($blockedSubjects[(string) ($st['subject'] ?? '')])) {
            $studiesOut[$uid] = ['line' => $line, 'status' => 'BLOCKED', 'why' => $blockedSubjects[(string) $st['subject']]];
        } elseif (isset($reportedLabels[$place])) {
            $studiesOut[$uid] = ['line' => $line, 'status' => 'BLOCKED', 'why' => 'visit label not letters and digits'];
        } elseif (isset($excluded[$uid]) || isset($badPn[$uid])) {
            $studiesOut[$uid] = ['line' => $line, 'status' => 'BLOCKED', 'why' => 'see MUST FIX'];
        } else {
            $studiesOut[$uid] = ['line' => $line, 'status' => 'READY', 'why' => ''];
        }
    }
}