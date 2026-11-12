#!/usr/bin/env php
<?php
/**
 * ARCHIMEDES REDCap pre-check.
 *
 * Checks a project's REDCap data dictionary, REDCap data exports and
 * project.json settings BEFORE the clinical pipeline runs, and lists
 * everything that needs fixing, following the rules the CBIGR instrument
 * manager applies to REDCap files. No LORIS, EviData or email calls.
 * The report is printed on screen; with --save it is also written as
 * .txt and .csv to <project>/logs/test/redcap_validation/.
 *
 * Run with --help for usage.
 */

if (is_readable(__DIR__ . '/../../vendor/autoload.php')) {
    require __DIR__ . '/../../vendor/autoload.php';
} else {
    // Lets the check run on a machine without `composer install`;
    // it only needs classes from src/.
    spl_autoload_register(function (string $class): void {
        if (str_starts_with($class, 'LORIS\\')) {
            $file = __DIR__ . '/../../src/' . str_replace('\\', '/', substr($class, 6)) . '.php';
            if (is_readable($file)) {
                require $file;
            }
        }
    });
}

use LORIS\Validation\RedcapValidator;

$opts = getopt('', [
    'collection:', 'project:', 'all', 'path:',
    'dd:', 'data:', 'project-json:',
    'save', 'report-dir:', 'help',
]);

if (isset($opts['help']) || $opts === []) {
    echo <<<'HELP'

ARCHIMEDES REDCap pre-check

Checks the REDCap data dictionary, the REDCap data exports and the
project settings before ingestion, using the rules the CBIGR instrument
manager applies to REDCap files, and lists everything to fix. Nothing is
uploaded or emailed. The report is printed on screen.

Usage:
  php scripts/test/validate_redcap.php [WHAT] [--save] [--report-dir=DIR]

WHAT (one of):
  --all                          every enabled project in loris_client_config.json
  --collection=NAME              every enabled project in one collection
  --collection=NAME --project=P  one configured project
  --path=/data/coll/project      any project folder (project.json read if present)
  --dd=FILE --data=FILE          individual files; repeat either option as needed
      [--project-json=FILE]      settings to apply to individual files

Options:
  --save             also save the report as .txt and .csv (one row per
                     problem, to send to the data provider) in
                     <project>/logs/test/redcap_validation/<project>_<YYYY-MM-DD>_<HHMMSS>
  --report-dir=DIR   save the report files to DIR instead (implies --save;
                     needed to save a --dd/--data run)
  --help             show this help

What is checked (data dictionary and data follow the instrument manager):
  Folder structure  project.json present; documentation/data_dictionary/
                    and deidentified-raw/clinical|preclinical/ exist (and
                    are lowercase); files the pipeline will not read (Excel,
                    other types, subfolders, files outside these folders);
                    a dictionary saved among the data or data among the
                    dictionaries (--path, --collection and --all only)
  Project settings  project_full_name; candidate_defaults (sex, dob,
                    project, site, cohort); sex_mappings; visit_mappings;
                    date_input_format; unfilled template values
  Data dictionary   REDCap's 18 columns in REDCap's order (read by position);
                    every row 18 values; supported field types (not
                    truefalse); choice codes kept, not renumbered; no field
                    named like a LORIS standard field; names; duplicates
  Data files        study_id, dob, sex, project, site, redcap_event_name,
                    cohort named exactly so (lowercase), or added from
                    candidate_defaults; every row as long as the header;
                    ID column matches the dictionary's record ID field;
                    values as the pipeline writes them (sex mapped,
                    defaults filled, visits mapped); visits, sites and
                    cohorts in project.json; columns against the
                    dictionary; checkbox columns (not stored); duplicate
                    participant + event rows (only first saved); values
                    against their field (warnings); duplicate file names

Exit code: 0 no errors, 1 errors found, 2 could not run.

Examples:
  php scripts/test/validate_redcap.php --collection=archimedes --project=FDG-PET
  php scripts/test/validate_redcap.php --path=/data/archimedes/NewStudy --save
  php scripts/test/validate_redcap.php --dd=dictionary.csv --data=export.csv --report-dir=/tmp/reports
  php scripts/test/validate_redcap.php --dd=dictionary.csv --data=export.csv --project-json=project.json

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
    echo RedcapValidator::renderText($sections, $title, $runAt);

    if ($reportDir !== null) {
        if (!is_dir($reportDir) && !@mkdir($reportDir, 0775, true) && !is_dir($reportDir)) {
            fwrite(STDERR, "Cannot create report directory: {$reportDir}\n");
        } else {
            $base = rtrim($reportDir, '/') . '/' . preg_replace('/[^A-Za-z0-9._-]+/', '_', $name)
                . "_{$stamp}";
            $okTxt = @file_put_contents("{$base}.txt", RedcapValidator::renderText($sections, $title, $runAt)) !== false;
            $okCsv = RedcapValidator::writeCsv($sections, "{$base}.csv");
            if ($okTxt && $okCsv) {
                echo "Report saved: {$base}.txt and .csv\n\n";
            } else {
                fwrite(STDERR, "Cannot write report files in {$reportDir} (check permissions)\n");
            }
        }
    }
    return RedcapValidator::counts($sections)[RedcapValidator::ERROR];
}

$reportDir = $opts['report-dir'] ?? null;
$save      = isset($opts['save']) || $reportDir !== null;

/** Default report folder inside a project. */
const PROJECT_REPORT_DIR = 'logs/test/redcap_validation';
$validator = new RedcapValidator();

try {
    // ── Individual files ────────────────────────────────────────────
    if (isset($opts['dd']) || isset($opts['data'])) {
        $dd   = array_values((array)($opts['dd'] ?? []));
        $data = array_map(fn($p) => ['path' => $p, 'label' => basename($p)],
            array_values((array)($opts['data'] ?? [])));
        foreach (array_merge($dd, array_column($data, 'path')) as $p) {
            if (!is_file($p)) {
                throw new RuntimeException("File not found: {$p}");
            }
        }
        [$project, $note] = isset($opts['project-json'])
            ? loadProjectJson($opts['project-json'])
            : [null, 'no --project-json given'];

        if (isset($opts['save']) && $reportDir === null) {
            throw new RuntimeException("--save with --dd/--data needs --report-dir=DIR (there is no project folder)");
        }
        $sections = $validator->validate($dd, $data, $project, $note);
        $name     = $project['project_common_name'] ?? 'files';
        $errors  += report($sections, $name, "Files: " . implode(', ', array_merge(
            array_map('basename', $dd), array_column($data, 'label'))), $runAt, $stamp, $reportDir);
        exit($errors > 0 ? 1 : 0);
    }

    // ── Project folders ─────────────────────────────────────────────
    $targets = [];   // [name, path]

    if (isset($opts['path'])) {
        $path = rtrim($opts['path'], '/');
        if (!is_dir($path)) {
            throw new RuntimeException("Project folder not found: {$path}");
        }
        $targets[] = [basename($path), $path];
    } else {
        if (isset($opts['project']) && !isset($opts['collection'])) {
            throw new RuntimeException("--project requires --collection");
        }
        if (!isset($opts['all']) && !isset($opts['collection'])) {
            throw new RuntimeException("Specify --all, --collection, --path or --dd/--data (see --help)");
        }
        $configFile = __DIR__ . '/../../config/loris_client_config.json';
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

    foreach ($targets as [$name, $path]) {
        if (!is_dir($path)) {
            fwrite(STDERR, "Project folder not found or not mounted: {$path}\n");
            $errors++;
            continue;
        }
        [$project, $note] = loadProjectJson("{$path}/project.json");
        $sections = $validator->validateProjectFolder($path, $project, $note);
        $errors  += report($sections, $project['project_common_name'] ?? $name,
            "Project: " . ($project['project_common_name'] ?? $name) . "  ({$path})",
            $runAt, $stamp, $save ? ($reportDir ?? "{$path}/" . PROJECT_REPORT_DIR) : null);
    }

    exit($errors > 0 ? 1 : 0);

} catch (Throwable $e) {
    fwrite(STDERR, "ERROR: {$e->getMessage()}\n");
    exit(2);
}
