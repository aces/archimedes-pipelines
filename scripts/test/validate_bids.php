#!/usr/bin/env php
<?php
/**
 * ARCHIMEDES BIDS validation - the BIDS pre-check, run when a site shares a
 * dataset.
 *
 * Cross-checks what a site shared - deidentified-raw/bids/ - against the BIDS
 * imaging user guide, the project's project.json, the BIDS validator
 * Loris-MRI uses, and LORIS, with the rules the BIDS pipeline applies, BEFORE
 * anything is ingested, and lists everything that needs fixing.
 *
 * READ-ONLY. LORIS is only read (login, GET /projects, /sites, /candidates,
 * the ExternalID mapper lookup): nothing is created or changed. --offline
 * skips LORIS. The report is printed on screen; with --save it is also written
 * as .txt and .csv to <project>/logs/test/bids_validation/.
 *
 * Counterparts: scripts/test/validate_png.php, scripts/dicom/run_dicom_validate.php.
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

use LORIS\Validation\BidsValidator;

$opts = getopt('', [
    'collection:', 'project:', 'all', 'path:', 'bids-dir:', 'python:',
    'offline', 'config:', 'save', 'report-dir:', 'help',
]);

if (isset($opts['help']) || $opts === []) {
    echo <<<'HELP'

ARCHIMEDES BIDS validation

Cross-checks the BIDS dataset a site shared against the BIDS user guide, the
project settings, the BIDS validator Loris-MRI uses and LORIS, using the rules
the BIDS pipeline applies, and lists everything to fix.
Read-only: nothing is ingested, created in LORIS, uploaded or emailed.
The report is printed on screen.

Usage:
  php scripts/test/validate_bids.php [WHAT] [--offline] [--python=BIN] [--save] [--report-dir=DIR]

WHAT (one of):
  --all                          every enabled project in loris_client_config.json
  --collection=NAME              every enabled project in one collection
  --collection=NAME --project=P  one configured project
  --path=/data/coll/project      any project folder (project.json read if present)
      [--bids-dir=DIR]           the shared dataset, if not
                                 <project>/deidentified-raw/bids

Options:
  --offline          no LORIS calls
  --python=BIN       Python with the bids_validator package (default:
                     imaging.python_bin, else the Loris-MRI virtualenv under
                     imaging.mri_code_path, else python3); not found = the
                     validator part is skipped, with a note
  --config=FILE      LORIS connection (default config/loris_client_config.json)
  --save             also save the report as .txt and .csv (one row per
                     problem, to send to the site) in
                     <project>/logs/test/bids_validation/<project>_<YYYY-MM-DD>_<HHMMSS>
  --report-dir=DIR   save the report files to DIR instead (implies --save)
  --help             show this help

What is checked:
  Project settings  modalities (Imaging); candidate_defaults project, site,
                    cohort; visits (optional)
  Folder            the dataset exists; dataset_description.json (Name,
                    BIDSVersion); folders at the top that are not BIDS; a sub-
                    folder inside another; files named for another participant
  participants.tsv  tab-separated (not CSV, not Excel), not empty,
                    participant_id, duplicates; sex and dob usable (as the
                    pipeline normalises them); site, cohort, project vs
                    candidate_defaults (the configured value is used); rows
                    without a folder, folders without a row; ID patterns
  Visits            each ses- folder exactly a LORIS visit ("visits", when
                    declared); files labelled with another ses-; a mismatch is
                    held back, the rest goes in (no sessions.tsv needed)
  BIDS validator    every file checked with the bids_validator package
                    Loris-MRI uses: a file it rejects is skipped by the import
  LORIS (read-only) the project and sites exist; participants already in
                    another LORIS project; participants already in LORIS still
                    have the same dob and sex
  Summary           project, site, cohort, visits per participant
  Environment       project lock; output folders writable

Exit code: 0 no errors, 1 errors found, 2 could not run.

Examples:
  php scripts/test/validate_bids.php --collection=archimedes --project=CNMDP --save
  php scripts/test/validate_bids.php --path=/data/archimedes/NewStudy --offline
  php scripts/test/validate_bids.php --path=/data/archimedes/NewStudy --bids-dir=/incoming/bids --python=/opt/loris-mri/.venv/bin/python

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
    echo BidsValidator::renderText($sections, $title, $runAt);

    if ($reportDir !== null) {
        if (!is_dir($reportDir) && !@mkdir($reportDir, 0775, true) && !is_dir($reportDir)) {
            fwrite(STDERR, "Cannot create report directory: {$reportDir}\n");
        } else {
            $base = rtrim($reportDir, '/') . '/' . preg_replace('/[^A-Za-z0-9._-]+/', '_', $name)
                . "_{$stamp}";
            $okTxt = @file_put_contents("{$base}.txt", BidsValidator::renderText($sections, $title, $runAt)) !== false;
            $okCsv = BidsValidator::writeCsv($sections, "{$base}.csv");
            if ($okTxt && $okCsv) {
                echo "Report saved: {$base}.txt and .csv\n\n";
            } else {
                fwrite(STDERR, "Cannot write report files in {$reportDir} (check permissions)\n");
            }
        }
    }
    return BidsValidator::counts($sections)[BidsValidator::ERROR];
}

$reportDir = $opts['report-dir'] ?? null;
$save      = isset($opts['save']) || $reportDir !== null;

/** Default report folder inside a project. */
const PROJECT_REPORT_DIR = 'logs/test/bids_validation';
$validator = new BidsValidator();

/**
 * loris_client_config.json, or null when it cannot be read.
 */
function readConfig(string $file): ?array
{
    $data = is_readable($file) ? json_decode((string) file_get_contents($file), true) : null;
    return is_array($data) ? $data : null;
}

$configFile = $opts['config'] ?? ARCHIMEDES_ROOT . '/config/loris_client_config.json';

/**
 * The Python to run the BIDS validator with: --python, imaging.python_bin, the
 * Loris-MRI virtualenv under imaging.mri_code_path, else python3 if present.
 */
function pythonBin(?string $given, ?array $config): ?string
{
    $candidates = array_filter([
        $given,
        $config['imaging']['python_bin'] ?? null,
        isset($config['imaging']['mri_code_path'])
            ? rtrim($config['imaging']['mri_code_path'], '/') . '/.venv/loris-mri-python/bin/python' : null,
        trim((string) shell_exec('command -v python3 2>/dev/null')) ?: null,
    ]);
    foreach ($candidates as $bin) {
        if (is_executable($bin)) {
            return $bin;
        }
    }
    return null;
}

try {
    $targets = [];   // [name, path]

    if (isset($opts['path'])) {
        $path = rtrim($opts['path'], '/');
        if (!is_dir($path)) {
            throw new RuntimeException("Project folder not found: {$path}");
        }
        $targets[] = [basename($path), $path];
    } else {
        if (isset($opts['bids-dir'])) {
            throw new RuntimeException("--bids-dir goes with --path (one project)");
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
        $sections = $validator->validateProjectFolder($path, $project, $note, $opts['bids-dir'] ?? null,
            $lorisConfig, pythonBin($opts['python'] ?? null, readConfig($configFile)));
        $errors  += report($sections, $project['project_common_name'] ?? $name,
            "Project: " . ($project['project_common_name'] ?? $name) . "  ({$path})",
            $runAt, $stamp, $save ? ($reportDir ?? "{$path}/" . PROJECT_REPORT_DIR) : null);
    }

    exit($errors > 0 ? 1 : 0);

} catch (Throwable $e) {
    fwrite(STDERR, "ERROR: {$e->getMessage()}\n");
    exit(2);
}
