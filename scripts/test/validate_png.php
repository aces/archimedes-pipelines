#!/usr/bin/env php
<?php
/**
 * ARCHIMEDES PNG validation - the one PNG check, run when a site shares files.
 *
 * Cross-checks what a site shared - deidentified-raw/imaging/png/ - against
 * the "Preparing PNG Images for Submission" user guide, the project's
 * project.json and LORIS, with the rules the PNG pipeline applies, BEFORE
 * anything is ingested, and lists everything that needs fixing.
 *
 * READ-ONLY. LORIS is only read (login, GET /projects, /sites, /candidates,
 * /candidates/{CandID}, the ExternalID mapper lookup): nothing is created or
 * changed. --offline skips LORIS. No EviData or email calls. The report is
 * printed on screen; with --save it is also written as .txt and .csv to
 * <project>/logs/test/png_validation/.
 *
 * The REDCap counterpart is scripts/test/validate_redcap.php.
 *
 * Run with --help for usage.
 */

// The repository root, wherever this script sits under scripts/.
$root = __DIR__;
while ($root !== dirname($root) && !is_file($root . '/composer.json')) {
    $root = dirname($root);
}
define('ARCHIMEDES_ROOT', $root);

if (is_readable(ARCHIMEDES_ROOT . '/vendor/autoload.php')) {
    require ARCHIMEDES_ROOT . '/vendor/autoload.php';
} else {
    // Lets the check run on a machine without `composer install`;
    // it only needs classes from src/.
    spl_autoload_register(function (string $class): void {
        if (str_starts_with($class, 'LORIS\\')) {
            $file = ARCHIMEDES_ROOT . '/src/' . str_replace('\\', '/', substr($class, 6)) . '.php';
            if (is_readable($file)) {
                require $file;
            }
        }
    });
}

use LORIS\Validation\PngValidator;

$opts = getopt('', [
    'collection:', 'project:', 'all', 'path:', 'png-dir:',
    'offline', 'config:', 'save', 'report-dir:', 'help',
]);

if (isset($opts['help']) || $opts === []) {
    echo <<<'HELP'

ARCHIMEDES PNG validation

Cross-checks the PNG images a site shared, and its participants.tsv and
sessions.tsv files, against the PNG user guide, the project settings and
LORIS, using the rules the PNG pipeline applies, and lists everything to fix.
Read-only: nothing is ingested, created in LORIS, uploaded or emailed.
The report is printed on screen.

Usage:
  php scripts/test/validate_png.php [WHAT] [--offline] [--save] [--report-dir=DIR]

WHAT (one of):
  --all                          every enabled project in loris_client_config.json
  --collection=NAME              every enabled project in one collection
  --collection=NAME --project=P  one configured project
  --path=/data/coll/project      any project folder (project.json read if present)
      [--png-dir=DIR]            the shared PNG folder, if not
                                 <project>/deidentified-raw/imaging/png

Options:
  --offline          no LORIS calls (folder, spreadsheet and image checks only)
  --config=FILE      LORIS connection (default config/loris_client_config.json)
  --save             also save the report as .txt and .csv (one row per
                     problem, to send to the site) in
                     <project>/logs/test/png_validation/<project>_<YYYY-MM-DD>_<HHMMSS>
  --report-dir=DIR   save the report files to DIR instead (implies --save)
  --help             show this help

What is checked:
  Project settings  modalities (Imaging); candidate_defaults project, site,
                    cohort; visits, cohorts, sites, png.scan_types; unfilled
                    template values
  Folder            the PNG folder exists; entries outside sub-<ID>/ses-<visit>/;
                    a sub- folder inside another
  participants.tsv  tab-separated (not comma-separated, not Excel), not empty,
                    participant_id, duplicate rows; sex and dob usable (as the
                    pipeline normalises them); site and cohort in project.json;
                    rows without a folder and folders without a row
  Each participant  identifier letters and digits; sessions.tsv (or
                    sub-<ID>_sessions.tsv) present, readable, session_id;
                    every ses- folder listed and every row a folder; acq_date
                    a real YYYY-MM-DD date; visit labels letters and digits,
                    no two differing only by case, in project.json "visits";
                    every image: type (start of the name) agreed, really a PNG,
                    not empty, not a duplicate of another; numbers padded;
                    subfolders and non-PNG files
  LORIS (read-only) the project and each site exist in LORIS; participants
                    already in LORIS still have the same dob and sex
                    (ingestion never changes LORIS)
  Summary           what ingestion will use: project, sites, cohorts, visits,
                    scan types found, one line per participant with its LORIS
                    status (new / PSCID / differs)
  Environment       the project lock works on this mount; output folders
                    writable; LORIS reachable (for the ARCHIMEDES team)

Exit code: 0 no errors, 1 errors found, 2 could not run.

Examples:
  php scripts/test/validate_png.php --collection=archimedes --project=ARCHI-TEST --save
  php scripts/test/validate_png.php --path=/data/archimedes/NewStudy --offline
  php scripts/test/validate_png.php --path=/data/archimedes/NewStudy --png-dir=/incoming/png --report-dir=/tmp/reports

HELP;
    exit(0);
}

$runAt  = date('Y-m-d H:i:s');
$stamp  = date('Y-m-d_His');
$errors = 0;

/**
 * @return array{0: ?array, 1: string} [decoded project.json or null, note]
 */
function loadProjectJson(string $file): array
{
    if (!file_exists($file)) {
        return [null, "not found: {$file}"];
    }
    $data = json_decode((string)file_get_contents($file), true);
    if (!is_array($data)) {
        return [null, "invalid JSON in {$file}: " . json_last_error_msg()];
    }
    return [$data, ''];
}

function report(array $sections, string $name, string $title, string $runAt, string $stamp, ?string $reportDir): int
{
    echo PngValidator::renderText($sections, $title, $runAt);

    if ($reportDir !== null) {
        if (!is_dir($reportDir) && !@mkdir($reportDir, 0775, true) && !is_dir($reportDir)) {
            fwrite(STDERR, "Cannot create report directory: {$reportDir}\n");
        } else {
            $base = rtrim($reportDir, '/') . '/' . preg_replace('/[^A-Za-z0-9._-]+/', '_', $name)
                . "_{$stamp}";
            $okTxt = @file_put_contents("{$base}.txt", PngValidator::renderText($sections, $title, $runAt)) !== false;
            $okCsv = PngValidator::writeCsv($sections, "{$base}.csv");
            if ($okTxt && $okCsv) {
                echo "Report saved: {$base}.txt and .csv\n\n";
            } else {
                fwrite(STDERR, "Cannot write report files in {$reportDir} (check permissions)\n");
            }
        }
    }
    return PngValidator::counts($sections)[PngValidator::ERROR];
}

$reportDir = $opts['report-dir'] ?? null;
$save      = isset($opts['save']) || $reportDir !== null;

/** Default report folder inside a project. */
const PROJECT_REPORT_DIR = 'logs/test/png_validation';
$validator = new PngValidator();

/**
 * loris_client_config.json, or null when it cannot be read.
 */
function readConfig(string $file): ?array
{
    $data = is_readable($file) ? json_decode((string) file_get_contents($file), true) : null;
    return is_array($data) ? $data : null;
}

$configFile = $opts['config'] ?? ARCHIMEDES_ROOT . '/config/loris_client_config.json';

try {
    $targets = [];   // [name, path]

    if (isset($opts['path'])) {
        $path = rtrim($opts['path'], '/');
        if (!is_dir($path)) {
            throw new RuntimeException("Project folder not found: {$path}");
        }
        $targets[] = [basename($path), $path];
    } else {
        if (isset($opts['png-dir'])) {
            throw new RuntimeException("--png-dir goes with --path (one project)");
        }
        if (isset($opts['project']) && !isset($opts['collection'])) {
            throw new RuntimeException("--project requires --collection");
        }
        if (!isset($opts['all']) && !isset($opts['collection'])) {
            throw new RuntimeException("Specify --all, --collection or --path (see --help)");
        }
        if (!is_readable($configFile)) {
            throw new RuntimeException("Configuration file not found: {$configFile}");
        }
        $config = json_decode((string)file_get_contents($configFile), true);
        if (!is_array($config)) {
            throw new RuntimeException("Invalid JSON in {$configFile}: " . json_last_error_msg());
        }
        // Same selection as the pipeline: enabled collections and
        // projects only; paths from collections[].base_path.
        foreach ($config['collections'] ?? [] as $coll) {
            if (!($coll['enabled'] ?? true)) {
                continue;
            }
            if (isset($opts['collection']) && $coll['name'] !== $opts['collection']) {
                continue;
            }
            foreach ($coll['projects'] ?? [] as $pc) {
                if (!($pc['enabled'] ?? true)) {
                    continue;
                }
                if (isset($opts['project']) && $pc['name'] !== $opts['project']) {
                    continue;
                }
                $targets[] = [$pc['name'], rtrim($coll['base_path'], '/') . '/' . $pc['name']];
            }
        }
        if ($targets === []) {
            throw new RuntimeException("No enabled project matched in {$configFile}");
        }
    }

    // LORIS is read unless --offline; without a readable config it is reported
    // (Environment) and the rest of the checks still run.
    $lorisConfig = null;
    if (!isset($opts['offline'])) {
        $lorisConfig = readConfig($configFile);
        if ($lorisConfig === null) {
            fwrite(STDERR, "WARNING cannot read {$configFile} - LORIS not checked (use --config=FILE or --offline)\n");
        }
    }

    foreach ($targets as [$name, $path]) {
        if (!is_dir($path)) {
            fwrite(STDERR, "Project folder not found or not mounted: {$path}\n");
            $errors++;
            continue;
        }
        [$project, $note] = loadProjectJson("{$path}/project.json");
        $sections = $validator->validateProjectFolder($path, $project, $note, $opts['png-dir'] ?? null, $lorisConfig);
        $errors  += report($sections, $project['project_common_name'] ?? $name,
            "Project: " . ($project['project_common_name'] ?? $name) . "  ({$path})",
            $runAt, $stamp, $save ? ($reportDir ?? "{$path}/" . PROJECT_REPORT_DIR) : null);
    }

    exit($errors > 0 ? 1 : 0);

} catch (Throwable $e) {
    fwrite(STDERR, "ERROR: {$e->getMessage()}\n");
    exit(2);
}
