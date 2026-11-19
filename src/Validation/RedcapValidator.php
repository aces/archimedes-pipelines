<?php
declare(strict_types=1);

namespace LORIS\Validation;

use LORIS\Pipelines\ClinicalPipeline;
use LORIS\Utils\Dob;

/**
 * Stand-alone REDCap pre-check, run BEFORE the clinical pipeline.
 *
 * Reads only. Never calls LORIS, EviData or the mail server, and writes
 * nothing itself (the CLI script saves the report). It answers one question: "if this
 * project were ingested now, what in the REDCap data dictionary, the
 * REDCap data export or project.json would need fixing first?" and
 * lists every problem at once, with the line numbers and the fix.
 *
 * The data dictionary and data checks follow what the CBIGR instrument
 * manager does with REDCap files (RedcapCSVParser, InstrumentDataParser,
 * RedcapDictionaryRecord), with the data first normalised the way the
 * clinical pipeline does it (candidate_defaults filled, sex mapped,
 * visits mapped, project name corrected).
 *
 *   Folder structure (folder mode only)
 *      project.json, documentation/data_dictionary/ and the data folders
 *      present and lowercase; files the pipeline will not read.
 *
 *   Project settings (project.json)
 *      project_full_name; candidate_defaults values; sex_mappings and
 *      visit_mappings targets; date_input_format; template values.
 *
 *   Data dictionary (instrument manager DD install)
 *      Columns are read BY POSITION into REDCap's 18 standard columns,
 *      so the header order must be REDCap's; a row with the wrong number
 *      of cells or an unsupported field type stops the install; choice
 *      codes outside [A-Za-z0-9._] are renumbered; a field may not reuse
 *      a name LORIS adds to every instrument.
 *
 *   Data file (instrument manager REDCAP_CSV upload, CREATE_SESSIONS,
 *   strict=false - as the pipeline sends it)
 *      study_id, dob, sex, project, site, redcap_event_name and cohort
 *      must be present with exactly these lowercase names (after the
 *      pipeline adds missing ones from candidate_defaults); every row
 *      must have as many values as the header; dob valid, sex mappable;
 *      visits/sites/cohorts known. ONE rejected row stops the whole
 *      file. Checkbox field___code columns are not stored; only the
 *      first row per participant and event is saved.
 *
 *   Only what the instrument manager (or the pipeline's own preflight)
 *   rejects is an ERROR. Values are not checked against the dictionary
 *   (choice codes, validation types, required fields): the instrument
 *   manager stores REDCap values without checking them. The dictionary
 *   does not need the ID/demographic fields (study_id, dob, sex, ...).
 *   Everything is judged on the processed copy the instrument manager
 *   receives, so what the pipeline fixes is a NOTE, not a problem.
 *
 * <form>_complete columns are allowed but never required.
 *
 * Severity:
 *   ERROR   - the instrument manager or the pipeline would reject it.
 *             Must be fixed before ingestion.
 *   WARNING - ingestion can go ahead, but data may be lost or not what
 *             was intended. Review.
 *   NOTE    - information only.
 *
 * Folder names, structural columns, sex vocabulary and excluded forms
 * are read from ClinicalPipeline (by reflection; the pipeline is not
 * changed), and dates from Dob/DateNormalizer,
 * so this check and the pipeline cannot drift apart.
 */
final class RedcapValidator
{
    public const ERROR   = 'ERROR';
    public const WARNING = 'WARNING';
    public const NOTE    = 'NOTE';

    /** Examples listed per issue before "... and N more". */
    private const MAX_EXAMPLES = 10;

    public const DD_DIR = 'documentation/data_dictionary';

    private const SEX_VALUES = ['Male', 'Female', 'Other'];

    /** Participant identifier columns LORIS accepts, in priority order. */
    private const ID_COLUMNS = ['study_id', 'studyid', 'pscid', 'candid'];

    private const EVENT_COLUMN = 'redcap_event_name';

    /** Values read once from ClinicalPipeline, see pipelineConst(). */
    private static array $pipelineConsts = [];

    /**
     * Field types RedcapDictionaryRecord::toLINST() converts. Any other
     * type (including REDCap's truefalse) throws "Unhandled REDCap type"
     * and stops the whole dictionary install.
     */
    private const FIELD_TYPES = [
        'text', 'notes', 'dropdown', 'radio', 'checkbox', 'yesno',
        'calc', 'file', 'slider', 'descriptive', 'sql',
    ];

    /**
     * Choice code the instrument manager keeps (RedcapDictionaryRecord::
     * _optionsToLINST). A choice not matching is renumbered 1, 2, ...
     */
    private const CHOICE_PATTERN = '/^\s*([[:alnum:]._]+)\s*,(.*)$/';

    /**
     * Columns the instrument manager requires in a REDCAP_CSV upload
     * with CREATE_SESSIONS (RedcapCSVParser::getEssentialHeaders), matched
     * case-sensitively. A missing one rejects the whole file.
     */
    private const REDCAP_ESSENTIAL = [
        'study_id', 'dob', 'sex', 'project', 'site', 'redcap_event_name', 'cohort',
    ];

    /**
     * The 18 columns the instrument manager reads a dictionary into,
     * BY POSITION (RedcapDictionaryRecord::getHeaders), with the header
     * REDCap writes for each.
     */
    private const DD_COLUMNS = [
        'field'      => 'Variable / Field Name',
        'form'       => 'Form Name',
        'section'    => 'Section Header',
        'type'       => 'Field Type',
        'label'      => 'Field Label',
        'choices'    => 'Choices, Calculations, OR Slider Labels',
        'note'       => 'Field Note',
        'validation' => 'Text Validation Type OR Show Slider Number',
        'min'        => 'Text Validation Min',
        'max'        => 'Text Validation Max',
        'identifier' => 'Identifier?',
        'branching'  => 'Branching Logic (Show field only if...)',
        'required'   => 'Required Field?',
        'alignment'  => 'Custom Alignment',
        'question'   => 'Question Number (surveys only)',
        'matrix'     => 'Matrix Group Name',
        'ranking'    => 'Matrix Ranking?',
        'annotation' => 'Field Annotation',
    ];

    private const CHOICE_TYPES = ['dropdown', 'radio', 'checkbox'];

    /** REDCap DD header (lowercased) => canonical key. */
    private const DD_HEADERS = [
        'variable / field name'                      => 'field',
        'field_name'                                 => 'field',
        'form name'                                  => 'form',
        'form_name'                                  => 'form',
        'field type'                                 => 'type',
        'field_type'                                 => 'type',
        'field label'                                => 'label',
        'field_label'                                => 'label',
        'choices, calculations, or slider labels'    => 'choices',
        'select_choices_or_calculations'             => 'choices',
        'text validation type or show slider number' => 'validation',
        'text_validation_type_or_show_slider_number' => 'validation',
        'text validation min'                        => 'min',
        'text_validation_min'                        => 'min',
        'text validation max'                        => 'max',
        'text_validation_max'                        => 'max',
        'branching logic (show field only if...)'    => 'branching',
        'branching_logic'                            => 'branching',
        'required field?'                            => 'required',
        'required_field'                             => 'required',
        'section header'                             => 'section',
        'section_header'                             => 'section',
        'field note'                                 => 'note',
        'field_note'                                 => 'note',
        'identifier?'                                => 'identifier',
        'identifier'                                 => 'identifier',
        'custom alignment'                           => 'alignment',
        'custom_alignment'                           => 'alignment',
        'question number (surveys only)'             => 'question',
        'question_number'                            => 'question',
        'matrix group name'                          => 'matrix',
        'matrix_group_name'                          => 'matrix',
        'matrix ranking?'                            => 'ranking',
        'matrix_ranking'                             => 'ranking',
        'field annotation'                           => 'annotation',
        'field_annotation'                           => 'annotation',
    ];

    private const DD_REQUIRED = ['field', 'form', 'type', 'label'];

    /** @var array<int, array{scope:string, file:string, summary:string, issues:array}> */
    private array $sections = [];

    /** Index into $sections of the section issues are added to. */
    private int $current = -1;

    /**
     * All dictionary fields across the project: name => field record.
     * @var array<string, array>
     */
    private array $fields = [];

    /** Data column name (lowercased) => [field name, checkbox code|null]. */
    private array $columns = [];

    /** Forms defined by the dictionaries (lowercased). */
    private array $forms = [];

    private array $project = [];

    /** Header row of the last file read, before trimming. */
    private array $lastRawHeaders = [];

    /** True when checking a project folder (structure already reported). */
    private bool $folderMode = false;

    // ══════════════════════════════════════════════════════════════════
    //  Entry points
    // ══════════════════════════════════════════════════════════════════

    /**
     * Validate a project folder laid out as the pipeline expects.
     *
     * @param array|null $project decoded project.json; null = not found
     */
    public function validateProjectFolder(string $projectPath, ?array $project, string $projectJsonNote = ''): array
    {
        $projectPath = rtrim($projectPath, '/');
        [$structure, $misplaced] = $this->checkFolderStructure($projectPath, $project, $projectJsonNote);

        $ddFiles = array_values(array_diff(glob("{$projectPath}/" . self::DD_DIR . '/*.csv') ?: [], $misplaced));
        sort($ddFiles);

        // Same selection as ClinicalPipeline::discoverDataFiles(): folders
        // in order (clinical, preclinical, phenotype), the first file to
        // claim a basename wins, a later duplicate is rejected.
        // exclude_data_files are skipped, as in uploadFromDirectory().
        $exclude   = (array)($project['exclude_data_files'] ?? []);
        $dataFiles = [];
        $seen      = [];
        $skipped   = [];
        $sources   = [];
        foreach (self::pipelineConst('TABULAR_DATA_DIRS') as $label => $rel) {
            $sources[] = [$label, rtrim($projectPath, '/') . "/{$rel}", ['csv', 'tsv'], true];
        }
        $sources[] = ['phenotype', rtrim($projectPath, '/') . '/' . self::pipelineConst('PHENOTYPE_DIR'), ['tsv'], false];

        foreach ($sources as [$label, $dir, $exts, $check]) {
            $paths = [];
            foreach ($exts as $ext) {
                $paths = array_merge($paths, glob("{$dir}/*.{$ext}") ?: []);
            }
            sort($paths);
            foreach ($paths as $p) {
                $name = basename($p);
                if (isset($seen[$name])) {
                    $skipped[] = [self::ERROR, "{$label}/{$name}",
                        "A file with the same name is already in {$seen[$name]}/. The pipeline"
                        . " ingests only the first one and rejects this one.",
                        "Rename one of the files; names must be unique across folders."];
                    continue;
                }
                $seen[$name] = $label;
                if (in_array($name, $exclude, true)) {
                    $skipped[] = [self::NOTE, "{$label}/{$name}",
                        "Listed in project.json exclude_data_files, so it is not ingested or checked.", ""];
                    continue;
                }
                if ($check && !in_array($p, $misplaced, true)) {
                    $dataFiles[] = ['path' => $p, 'label' => "{$label}/{$name}"];
                }
            }
        }

        $this->folderMode = true;
        try {
            $this->validate($ddFiles, $dataFiles, $project, $projectJsonNote);
        } finally {
            $this->folderMode = false;
        }

        foreach ($skipped as [$sev, $label, $msg, $fix]) {
            $this->section('Data file', $label, 'not checked');
            $this->add($sev, $msg, $fix);
        }

        // Folder structure goes first in the report.
        array_unshift($this->sections, $structure);
        $this->current = count($this->sections) - 1;
        return $this->sections;
    }

    /**
     * Is everything where the pipeline looks for it?
     *
     *   <project>/project.json
     *   <project>/documentation/data_dictionary/   .csv (REDCap) .linst .json
     *   <project>/deidentified-raw/clinical/       .csv .tsv
     *   <project>/deidentified-raw/preclinical/    .csv .tsv
     *   <project>/deidentified-raw/bids/phenotype/ .tsv
     *
     * Reports missing or wrongly-cased folders, files the pipeline will
     * not read (wrong type, in a subfolder, in the wrong folder), and a
     * dictionary saved among the data or data saved among the
     * dictionaries. Returns the section and the misplaced file paths,
     * which are then left out of the dictionary/data checks.
     *
     * @return array{0: array, 1: string[]}
     */
    private function checkFolderStructure(string $root, ?array $project, string $note): array
    {
        $issues    = [];
        $misplaced = [];
        $add = function (string $sev, string $msg, string $fix) use (&$issues): void {
            $issues[] = ['severity' => $sev, 'message' => $msg, 'fix' => $fix];
        };

        // project.json: without it the pipeline skips the project.
        if ($project === null) {
            $add(self::ERROR,
                file_exists("{$root}/project.json")
                    ? "project.json cannot be read ({$note}). The pipeline skips this project."
                    : "No project.json in the project folder. The pipeline skips this project.",
                "The ARCHIMEDES team creates project.json from config/project.example.json.");
        }

        // A folder that exists only with different capitals is not found
        // (paths are case-sensitive on the server).
        // Returns 1 found, 0 missing, -1 wrong capitals (already reported).
        $resolve = function (string $rel) use ($root, $add): int {
            if (is_dir("{$root}/{$rel}")) {
                return 1;
            }
            // Walk the path; report the first level that exists only
            // with different capitals.
            $dir  = $root;
            $done = [];
            foreach (explode('/', $rel) as $part) {
                if (is_dir("{$dir}/{$part}")) {
                    $dir    = "{$dir}/{$part}";
                    $done[] = $part;
                    continue;
                }
                foreach (scandir($dir) ?: [] as $e) {
                    if ($e !== $part && strcasecmp($e, $part) === 0 && is_dir("{$dir}/{$e}")) {
                        $prefix = $done === [] ? '' : implode('/', $done) . '/';
                        $add(self::ERROR,
                            "Folder '{$prefix}{$e}' must be named '{$prefix}{$part}' (lowercase as shown).",
                            "Rename the folder; folder names are case-sensitive.");
                        return -1;
                    }
                }
                return 0;
            }
            return 0;
        };

        $ddDir   = self::DD_DIR;
        $ddExts  = array_keys(self::pipelineConst('DD_EXTENSIONS'));
        $datExts = array_keys(self::pipelineConst('DATA_EXTENSIONS'));
        $phExts  = array_keys(self::pipelineConst('PHENOTYPE_DATA_EXTENSIONS'));
        $tabular = array_values(self::pipelineConst('TABULAR_DATA_DIRS'));
        $pheno   = self::pipelineConst('PHENOTYPE_DIR');

        // Data dictionary folder.
        $ddFound = 0;
        $ddState = $resolve($ddDir);
        if ($ddState === 1) {
            foreach ($this->listFolder("{$root}/{$ddDir}") as [$name, $isDir]) {
                $path = "{$root}/{$ddDir}/{$name}";
                $ext  = strtolower(pathinfo($name, PATHINFO_EXTENSION));
                if ($isDir) {
                    $add(self::WARNING, "{$ddDir}/{$name}/ is a subfolder; files inside it are not read.",
                        "Move the dictionary files directly into {$ddDir}/.");
                } elseif (!in_array($ext, $ddExts, true)) {
                    $add(self::WARNING, "{$ddDir}/{$name} is not read (only ." . implode(', .', $ddExts) . " files are).",
                        $this->saveAsHint($ext, 'the REDCap data dictionary as CSV'));
                } elseif ($ext === 'csv' && $this->headerHas($path, ',', 'redcap_event_name')) {
                    $misplaced[] = $path;
                    $add(self::ERROR, "{$ddDir}/{$name} is a data export, not a data dictionary.",
                        "Move it to deidentified-raw/clinical/ (or preclinical/).");
                } else {
                    $ddFound++;
                }
            }
            if ($ddFound === 0) {
                $add(self::NOTE, "{$ddDir}/ has no data dictionary files.",
                    "Add the REDCap data dictionary (.csv) unless its instruments are already in ARCHIMEDES.");
            }
        } elseif ($ddState === 0) {
            $add(self::ERROR, "No {$ddDir}/ folder.",
                "Create it and put the REDCap data dictionary (.csv) in it.");
        }

        // Data folders.
        $dataFound = 0;
        $anyDir    = false;
        foreach (array_merge(array_map(fn($d) => [$d, $datExts], $tabular), [[$pheno, $phExts]]) as [$rel, $exts]) {
            $state = $resolve($rel);
            if ($state === -1) {
                $anyDir = true;   // reported as wrongly named, not missing
            }
            if ($state !== 1) {
                continue;
            }
            $anyDir = true;
            foreach ($this->listFolder("{$root}/{$rel}") as [$name, $isDir]) {
                $path = "{$root}/{$rel}/{$name}";
                $ext  = strtolower(pathinfo($name, PATHINFO_EXTENSION));
                if ($isDir) {
                    $add(self::WARNING, "{$rel}/{$name}/ is a subfolder; files inside it are not read.",
                        "Move the data files directly into {$rel}/.");
                } elseif (!in_array($ext, $exts, true)) {
                    $add(self::WARNING, "{$rel}/{$name} is not read (only ." . implode(', .', $exts) . " files are).",
                        $this->saveAsHint($ext, 'the REDCap data export as CSV'));
                } elseif ($this->headerHas($path, $ext === 'tsv' ? "\t" : ',', 'variable / field name')) {
                    $misplaced[] = $path;
                    $add(self::ERROR, "{$rel}/{$name} is a data dictionary, not data.",
                        "Move it to {$ddDir}/.");
                } else {
                    $dataFound++;
                }
            }
        }
        if (!$anyDir) {
            $add(self::ERROR, "No data folder: expected " . implode('/, ', $tabular) . "/ or {$pheno}/.",
                "Put the REDCap data export in deidentified-raw/clinical/ (or preclinical/).");
        } elseif ($dataFound === 0) {
            $add(self::ERROR, "No data files to ingest in " . implode('/, ', $tabular) . "/ or {$pheno}/.",
                "Put the REDCap data export (.csv) in deidentified-raw/clinical/ (or preclinical/).");
        }

        // Tabular files left where nothing reads them.
        foreach (['', 'deidentified-raw', 'documentation'] as $rel) {
            $dir = $rel === '' ? $root : "{$root}/{$rel}";
            if (!is_dir($dir)) {
                continue;
            }
            foreach ($this->listFolder($dir) as [$name, $isDir]) {
                $ext = strtolower(pathinfo($name, PATHINFO_EXTENSION));
                if ($isDir || !in_array($ext, ['csv', 'tsv', 'xlsx', 'xls', 'linst'], true)) {
                    continue;
                }
                $where = ($rel === '' ? '' : "{$rel}/") . $name;
                $add(self::WARNING, "{$where} is not in a folder the pipeline reads, so it is ignored.",
                    "Data goes in deidentified-raw/clinical/ (or preclinical/); dictionaries in {$ddDir}/.");
            }
        }

        $section = [
            'scope'   => 'Folder structure',
            'file'    => $root,
            'summary' => "{$ddFound} dictionary file(s), {$dataFound} data file(s)",
            'issues'  => $issues,
        ];
        return [$section, $misplaced];
    }

    /**
     * Entries of a folder as [name, isDir], skipping hidden files and
     * Office lock files (~$...).
     *
     * @return array<array{0:string,1:bool}>
     */
    private function listFolder(string $dir): array
    {
        $out = [];
        foreach (scandir($dir) ?: [] as $e) {
            if ($e === '.' || $e === '..' || $e[0] === '.' || str_starts_with($e, '~$')) {
                continue;
            }
            $out[] = [$e, is_dir("{$dir}/{$e}")];
        }
        sort($out);
        return $out;
    }

    /** Does the file's header row contain this column (case-insensitive)? */
    private function headerHas(string $path, string $delim, string $column): bool
    {
        $fh = @fopen($path, 'r');
        if ($fh === false) {
            return false;
        }
        if (fread($fh, 3) !== "\xEF\xBB\xBF") {
            rewind($fh);
        }
        $h = fgetcsv($fh, 0, $delim, '"', '\\');
        fclose($fh);
        return is_array($h) && in_array($column, array_map(fn($x) => strtolower(trim((string)$x)), $h), true);
    }

    private function saveAsHint(string $ext, string $what): string
    {
        return in_array($ext, ['xlsx', 'xls'], true)
            ? "Excel files are not read: save {$what} (CSV UTF-8)."
            : "Remove it, or save {$what}.";
    }

    /**
     * Validate explicit files.
     *
     * @param string[]                             $ddFiles
     * @param array<array{path:string,label:string}> $dataFiles
     * @param array|null                           $project decoded project.json
     */
    public function validate(array $ddFiles, array $dataFiles, ?array $project, string $projectJsonNote = ''): array
    {
        $this->sections = [];
        $this->fields   = [];
        $this->columns  = [];
        $this->forms    = [];
        $this->project  = $project ?? [];

        $this->checkProjectSettings($project, $projectJsonNote);

        $ddByFile = [];
        foreach ($ddFiles as $path) {
            $ddByFile[$path] = $this->checkDictionary($path);
        }
        $this->checkCrossDictionary($ddByFile);

        $redcapData = 0;
        foreach ($dataFiles as $f) {
            if ($this->checkDataFile($f['path'], $f['label'])) {
                $redcapData++;
            }
        }

        if ($redcapData > 0 && $this->fields === []) {
            $this->section('Data dictionary', self::DD_DIR, 'none found');
            $this->add(self::NOTE,
                "There is REDCap data but no REDCap data dictionary, so the columns could not be"
                . " checked against instrument fields. Fine if the instruments are already installed.",
                "Export the data dictionary from REDCap (Project Setup > Data"
                . " Dictionary > Download) and add the .csv to " . self::DD_DIR . "/.");
        }

        return $this->sections;
    }

    // ══════════════════════════════════════════════════════════════════
    //  1. Project settings
    // ══════════════════════════════════════════════════════════════════

    private function checkProjectSettings(?array $p, string $note): void
    {
        $this->section('Project settings', 'project.json', 'checked by the ARCHIMEDES team');

        if ($p === null && $this->folderMode) {
            $this->add(self::NOTE,
                "Not checked: there is no readable project.json (see Folder structure).", "");
            return;
        }
        if ($p === null) {
            $this->add(self::WARNING,
                "No project.json was given" . ($note !== '' ? " ({$note})" : '')
                . ". Values that would normally come from candidate_defaults,"
                . " visit_mappings and sex_mappings are treated as not set.",
                "Run with the project's project.json so its defaults are taken into account.");
            return;
        }

        $full = trim((string)($p['project_full_name'] ?? ''));
        if ($full === '' || $this->isPlaceholder($full)) {
            $this->add(self::WARNING,
                "project_full_name is not set, so a wrong project name in the data is not corrected.",
                "Set it to the LORIS project name, exactly as spelled in LORIS (case-sensitive).");
        }

        $order = $p[Dob::ORDER_KEY] ?? null;
        if ($order !== null && !in_array(strtolower(trim((string)$order)), ['mdy', 'dmy'], true)) {
            $this->add(self::WARNING,
                "date_input_format is '{$order}', which the pipeline ignores (it uses 'mdy').",
                "Use 'mdy' (month/day/year, the REDCap default) or 'dmy'.");
        }

        $cd = $p['candidate_defaults'] ?? [];
        if (!is_array($cd)) {
            $this->add(self::ERROR, "candidate_defaults is not an object.",
                "Write it as {\"sex\": ..., \"dob\": ..., \"site\": ..., ...}.");
            $cd = [];
        }

        foreach ($cd as $k => $v) {
            if (is_string($v) && $this->isPlaceholder($v)) {
                $this->add(self::ERROR,
                    "candidate_defaults.{$k} still holds the template value '{$v}'.",
                    "Replace it with a real value, or remove the key.");
            }
        }

        // The pipeline fills blank sex cells with this value and THEN
        // maps it (case-insensitively), so 'male' or a sex_mappings code
        // still ends up valid.
        $sex = $this->cdValue('sex');
        if ($sex !== '') {
            $mapped = $this->mapSex($sex);
            if ($mapped === null) {
                $this->add(self::ERROR,
                    "candidate_defaults.sex is '{$sex}', which cannot be read as Male, Female or Other.",
                    "Use Male, Female or Other.");
            } elseif ($mapped !== $sex) {
                $this->add(self::NOTE,
                    "candidate_defaults.sex '{$sex}' is stored as '{$mapped}'.",
                    "Write it as '{$mapped}' to avoid the conversion.");
            }
        }

        $dob = $this->cdValue('dob');
        if ($dob !== '' && !Dob::isValid(Dob::normalize($dob, Dob::dateOrder($p)))) {
            $this->add(self::ERROR,
                "candidate_defaults.dob is '{$dob}', which is not a real date.",
                "Use YYYY-MM-DD, YYYY-MM or YYYY.");
        }

        $proj = $this->cdValue('project');
        if ($proj !== '' && $full !== '' && !$this->isPlaceholder($full) && $proj !== $full) {
            $this->add(self::ERROR,
                "candidate_defaults.project ('{$proj}') does not match project_full_name ('{$full}').",
                "Both must be the LORIS project name; make them identical.");
        }

        foreach (['site' => 'sites', 'cohort' => 'cohorts'] as $key => $listKey) {
            $val  = $this->cdValue($key);
            $list = $this->filledList($listKey);
            if ($val !== '' && $list !== [] && !$this->inListCi($val, $list)) {
                $this->add(self::WARNING,
                    "candidate_defaults.{$key} ('{$val}') is not in the {$listKey} list (" . implode(', ', $list) . ").",
                    "Check the spelling; it must match the {$key} name in LORIS exactly.");
            }
        }

        $sexMap = $p['sex_mappings'] ?? [];
        if (is_array($sexMap)) {
            foreach ($sexMap as $from => $to) {
                if (!in_array($to, self::SEX_VALUES, true)) {
                    $this->add(self::ERROR,
                        "sex_mappings maps '{$from}' to '{$to}'.",
                        "Each value must be exactly Male, Female or Other.");
                }
            }
        }

        $visits = $this->filledList('visits');
        $vmap   = $p['visit_mappings'] ?? [];
        if (is_array($vmap)) {
            foreach ($vmap as $from => $to) {
                $to = trim((string)$to);
                if ($this->isPlaceholder((string)$from) || $this->isPlaceholder($to)) {
                    $this->add(self::ERROR,
                        "visit_mappings still holds the template entry '{$from}' => '{$to}'.",
                        "Replace it with real values, or remove the block.");
                } elseif ($to === '') {
                    $this->add(self::ERROR,
                        "visit_mappings maps '{$from}' to an empty visit.",
                        "Give the LORIS visit label it should become.");
                } elseif ($visits !== [] && !$this->inListCi($to, $visits)) {
                    $this->add(self::WARNING,
                        "visit_mappings target '{$to}' is not in the visits list (" . implode(', ', $visits) . ").",
                        "Add it to visits, or correct the mapping. The visit must exist in LORIS.");
                }
            }
        }

        if ($visits === []) {
            $this->add(self::NOTE,
                "The visits list is empty, so visit names in the data cannot be checked here.",
                "List the LORIS visit labels in project.json visits to have them checked.");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  2. Data dictionaries
    // ══════════════════════════════════════════════════════════════════

    /**
     * @return array<string,int> field name => line, for cross-file checks
     */
    private function checkDictionary(string $path): array
    {
        $name = basename($path);
        $this->section('Data dictionary', $name, '');

        [$headers, $rows, $err] = $this->readCsv($path, ',');
        if ($err !== null) {
            $this->add(self::ERROR, $err, "Re-export the data dictionary from REDCap as CSV.");
            return [];
        }

        $map = [];
        foreach ($headers as $i => $h) {
            $key = self::DD_HEADERS[strtolower(trim($h))] ?? null;
            if ($key === null) {
                $lh = strtolower(trim($h));
                if (str_starts_with($lh, 'choices')) {
                    $key = 'choices';
                } elseif (str_starts_with($lh, 'branching logic')) {
                    $key = 'branching';
                }
            }
            if ($key !== null && !isset($map[$key])) {
                $map[$key] = $i;
            }
        }

        $missing = array_diff(self::DD_REQUIRED, array_keys($map));
        if ($missing !== []) {
            $labels = ['field' => 'Variable / Field Name', 'form' => 'Form Name',
                       'type' => 'Field Type', 'label' => 'Field Label'];
            $this->add(self::ERROR,
                "This is not a REDCap data dictionary: missing column(s) "
                . implode(', ', array_map(fn($k) => "'{$labels[$k]}'", $missing)) . ".",
                "Download the data dictionary from REDCap without editing its header row."
                . " (A LORIS .linst or BIDS .json dictionary is not checked by this script.)");
            return [];
        }

        // The instrument manager ignores the header names and reads each
        // row by position into REDCap's 18 columns, so a column out of
        // place is read as the wrong thing.
        $want     = array_keys(self::DD_COLUMNS);
        $misorder = [];
        foreach ($headers as $i => $h) {
            if ($i >= count($want)) {
                break;
            }
            $lh  = strtolower(trim($h));
            $key = self::DD_HEADERS[$lh] ?? (str_starts_with($lh, 'choices') ? 'choices'
                : (str_starts_with($lh, 'branching logic') ? 'branching' : null));
            if ($key !== $want[$i]) {
                $misorder[] = "column " . ($i + 1) . " is '{$h}', expected '" . self::DD_COLUMNS[$want[$i]] . "'";
            }
        }
        if (count($headers) > count($want)) {
            $this->add(self::ERROR,
                "The dictionary has " . count($headers) . " columns. The instrument manager reads"
                . " exactly REDCap's " . count($want) . " columns, so the install fails.",
                "Remove the extra columns (" . $this->examples(array_slice($headers, count($want))) . ")"
                . " or download the dictionary from REDCap again.");
        }
        if ($misorder !== []) {
            $this->add(self::ERROR,
                "Columns are not in REDCap's order. The instrument manager reads columns by"
                . " position, not by name, so these would be read as the wrong thing: "
                . $this->examples($misorder) . ".",
                "Download the data dictionary from REDCap again without moving, adding or"
                . " deleting columns.");
        }

        // Row length: fewer columns than 18 are padded; otherwise every
        // row must have exactly 18 values or the whole install fails.
        $badLen = [];
        foreach ($rows as [$line, $row]) {
            $n = count($row);
            if ($n > count($want) || (count($headers) >= count($want) && $n !== count($want))) {
                $badLen[] = "line {$line} ({$n} values)";
            }
        }
        if ($badLen !== []) {
            $this->add(self::ERROR,
                count($badLen) . " row(s) do not have " . count($want) . " values, so the"
                . " instrument manager stops the install: " . $this->examples($badLen) . ".",
                "Usually an unquoted comma or line break in a label or choice. Download the"
                . " dictionary from REDCap again.");
        }

        $get = fn(array $row, string $k): string =>
            isset($map[$k]) ? trim((string)($row[$map[$k]] ?? '')) : '';

        $seen     = [];
        $byIssue  = [];
        $formsHere = [];
        $fieldCount = 0;

        foreach ($rows as [$line, $row]) {
            $field = $get($row, 'field');
            $form  = $get($row, 'form');
            $type  = strtolower($get($row, 'type'));

            if ($field === '') {
                $byIssue['blank_field'][] = "line {$line}";
                continue;
            }
            $fieldCount++;

            $seen[$field] ??= $line;

            if ($form === '') {
                $byIssue['blank_form'][] = "line {$line} ('{$field}')";
            } elseif (!isset($formsHere[$form])) {
                $formsHere[$form] = $line;
            }

            if (!in_array($type, self::FIELD_TYPES, true)) {
                $byIssue['bad_type'][] = "line {$line} ('{$field}'): '" . $get($row, 'type') . "'";
            }

            $choices = [];
            if (in_array($type, self::CHOICE_TYPES, true)) {
                $c = $this->parseChoices($get($row, 'choices'));
                $choices = $c['choices'];
                if ($c['renumbered'] !== []) {
                    $byIssue['renumbered'][] = "line {$line} ('{$field}'): "
                        . implode(', ', array_map(fn($x) => "'{$x}'", $c['renumbered']));
                }
            }

            $validation = strtolower($get($row, 'validation'));

            if (in_array($form, self::pipelineConst('DEFAULT_EXCLUDE_FORMS'), true) || isset($this->fields[$field])) {
                continue;
            }

            $this->fields[$field] = [
                'name'       => $field,
                'form'       => $form,
                'type'       => $type,
                'choices'    => $choices,
                'validation' => $type === 'text' ? $validation : '',
                'dd'         => $name,
            ];
            if ($form !== '') {
                $this->forms[strtolower($form)] = true;
            }

            if ($type === 'checkbox') {
                foreach (array_keys($choices) as $code) {
                    $col = strtolower($field . '___' . str_replace('-', '_', (string)$code));
                    $this->columns[$col] = [$field, (string)$code];
                }
            } elseif ($type !== 'descriptive') {
                $this->columns[strtolower($field)] = [$field, null];
            }
        }

        $msgs = [
            'blank_field' => [self::ERROR,   "row(s) have no variable name, so the instrument cannot be built",
                              "Give every row a variable name, or delete the empty rows."],
            'blank_form'  => [self::ERROR,   "row(s) have no form name, so the field belongs to no instrument",
                              "Set the Form Name for every field."],
            'bad_type'    => [self::ERROR,   "field type(s) the instrument manager cannot convert; it stops"
                              . " the whole dictionary install",
                              "Use one of: " . implode(', ', self::FIELD_TYPES) . " (truefalse is not"
                              . " supported: use yesno or radio)."],
            'renumbered'  => [self::WARNING, "choice list(s) have options whose code the instrument manager"
                              . " does not accept; it installs them renumbered 1, 2, ..., so the codes in"
                              . " the data no longer mean the same answers",
                              "Write each option as 'code, label' with a code of letters, numbers, '.' or"
                              . " '_' only (no minus sign or spaces)."],
        ];
        $this->addGrouped($byIssue, $msgs);

        if ($formsHere !== []) {
            $this->add(self::NOTE,
                "Forms in this dictionary: " . $this->examples(array_keys($formsHere)) . ". If any of"
                . " them is already installed in ARCHIMEDES, the instrument manager installs none of"
                . " them (it refuses the whole dictionary).",
                "To add new forms to an installed project, put only the new forms in the dictionary file.");
        }

        $formCount = count($formsHere);
        $this->sections[$this->current]['summary'] = "{$fieldCount} field(s), {$formCount} form(s)";

        return $seen;
    }

    /**
     * Same variable name in two dictionary files: only the first is used.
     *
     * @param array<string, array<string,int>> $ddByFile
     */
    private function checkCrossDictionary(array $ddByFile): void
    {
        $owner = [];
        $dups  = [];
        foreach ($ddByFile as $path => $fields) {
            foreach ($fields as $field => $line) {
                if (isset($owner[$field])) {
                    $dups[] = "'{$field}' in {$owner[$field]} and " . basename($path);
                } else {
                    $owner[$field] = basename($path);
                }
            }
        }
        if ($dups !== []) {
            $this->section('Data dictionary', 'all dictionaries', 'compared');
            $this->add(self::WARNING,
                count($dups) . " variable name(s) appear in more than one dictionary file: "
                . $this->examples($dups) . ". Only the first definition is used for these checks.",
                "Keep each variable in one dictionary only.");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  3. Data files
    // ══════════════════════════════════════════════════════════════════

    /**
     * @return bool true when the file was checked as REDCap data
     */
    private function checkDataFile(string $path, string $label): bool
    {
        $delim = strtolower(pathinfo($path, PATHINFO_EXTENSION)) === 'tsv' ? "\t" : ',';
        $this->section('Data file', $label, '');

        [$headers, $rows, $err] = $this->readCsv($path, $delim);
        if ($err !== null) {
            $this->add(self::ERROR, $err, "Re-export the file from REDCap.");
            return true;
        }

        $lower = array_map(fn($h) => strtolower(trim($h)), $headers);
        $idx   = [];
        $dupCols = [];
        foreach ($lower as $i => $h) {
            if ($h === '') {
                $dupCols[] = 'column ' . ($i + 1) . ' has no header';
                continue;
            }
            if (isset($idx[$h])) {
                $dupCols[] = "'{$headers[$i]}'";
                continue;
            }
            $idx[$h] = $i;
        }

        $isRedcap = isset($idx[self::EVENT_COLUMN]);
        if (!$isRedcap) {
            // A REDCap export from a non-longitudinal project has no event
            // column. Recognise it by its columns matching the dictionary.
            $dataCols = array_filter(array_keys($idx), fn($c) => !in_array($c, self::structuralColumns(), true));
            $known    = array_filter($dataCols, fn($c) => isset($this->columns[$c]));
            if ($dataCols === [] || count($known) * 2 < count($dataCols)) {
                $this->sections[$this->current]['summary'] = count($rows) . ' row(s)';
                $this->add(self::NOTE,
                    "Not a REDCap export (no redcap_event_name column, and its columns are"
                    . " not in the REDCap dictionary), so it is not checked here.",
                    "");
                return false;
            }
            $this->add(self::ERROR,
                "No redcap_event_name column. LORIS needs the event (visit) of every row.",
                "Export from a longitudinal REDCap project, or add a redcap_event_name column"
                . " holding the visit of each row.");
        }

        $this->sections[$this->current]['summary'] = count($rows) . ' row(s), ' . count($headers) . ' column(s)';

        // The pipeline writes headers back exactly as they are, and the
        // instrument manager matches them exactly, so a space around a
        // header name makes it a different column.
        $spacedKey = [];
        $spaced    = [];
        foreach ($this->lastRawHeaders as $i => $raw) {
            if ($raw !== $headers[$i] && $headers[$i] !== '') {
                if (in_array($headers[$i], self::REDCAP_ESSENTIAL, true)) {
                    $spacedKey[] = "'{$raw}'";
                } else {
                    $spaced[] = "'{$raw}'";
                }
            }
        }
        if ($spacedKey !== []) {
            $this->add(self::ERROR,
                "Column name(s) with spaces around them: " . $this->examples($spacedKey) . ". The"
                . " instrument manager looks for the exact name and rejects the whole file.",
                "Remove the spaces from the header row.");
        }
        if ($spaced !== []) {
            $this->add(self::WARNING,
                "Column name(s) with spaces around them: " . $this->examples($spaced) . ". They will"
                . " not match the data dictionary field, so their values are not saved.",
                "Remove the spaces from the header row.");
        }

        // The pipeline recognises a REDCap file only by an exactly
        // lowercase redcap_event_name header (ClinicalClient::isRedcapCSV).
        if ($isRedcap && !in_array(self::EVENT_COLUMN, $headers, true)) {
            $this->add(self::ERROR,
                "The column '{$headers[$idx[self::EVENT_COLUMN]]}' must be written in lowercase as"
                . " redcap_event_name, or the file is not recognised as a REDCap export.",
                "Rename the header to redcap_event_name.");
        }

        if ($dupCols !== []) {
            $this->add(self::WARNING,
                "Repeated or blank column header(s): " . $this->examples($dupCols) . ". The"
                . " instrument manager keeps only the last column of each name.",
                "Every column needs a unique header.");
        }

        if ($rows === []) {
            $this->add(self::WARNING, "The file has a header row but no data rows; it will be skipped.",
                "Export the data again if rows were expected.");
            return true;
        }

        // The instrument manager pairs every row with the header
        // (array_combine); one row of a different length rejects the file.
        $badLen = [];
        foreach ($rows as [$line, $row]) {
            if (count($row) !== count($headers)) {
                $badLen[] = "line {$line} (" . count($row) . " values)";
            }
        }
        if ($badLen !== []) {
            $this->add(self::ERROR,
                count($badLen) . " row(s) do not have exactly " . count($headers) . " values (one per"
                . " column). The instrument manager rejects the whole file: " . $this->examples($badLen) . ".",
                "Usually an unquoted comma or line break inside a value. Export the data from REDCap again.");
        }

        $this->checkStructure($headers, $idx, $rows);
        $this->checkColumnsAgainstDictionary($headers, $idx);

        return true;
    }

    /**
     * Identifier, event, candidate fields, visits, duplicates.
     */
    private function checkStructure(array $headers, array $idx, array $rows): void
    {
        // Participant identifier: the instrument manager requires a column
        // named exactly study_id in a REDCap upload.
        $idCol = null;
        foreach (self::ID_COLUMNS as $c) {
            if (isset($idx[$c])) { $idCol = $idx[$c]; break; }
        }
        if (!in_array('study_id', $headers, true)) {
            if (isset($idx['study_id'])) {
                $this->add(self::ERROR,
                    "The column '{$headers[$idx['study_id']]}' must be named exactly study_id (lowercase)."
                    . " The instrument manager looks only for study_id and rejects the whole file.",
                    "Rename the header to study_id.");
            } elseif (isset($idx['record_id']) && !isset($idx['studyid']) && !isset($idx['pscid'])) {
                // The pipeline renames REDCap's record_id column to study_id.
                $idCol = $idx['record_id'];
                $this->add(self::NOTE,
                    "No study_id column: the pipeline renames the record_id column to study_id.",
                    "");
            } else {
                $this->add(self::ERROR,
                    "No study_id column. The instrument manager rejects the whole file.",
                    "Name the participant ID column study_id.");
            }
        }

        $eventCol = $idx[self::EVENT_COLUMN] ?? null;

        // Candidate fields. The instrument manager needs dob, sex,
        // project, site and cohort, named exactly so (lowercase). The
        // pipeline adds a missing column from candidate_defaults, but
        // when the file has the column under another name or case (DOB,
        // Sex, date_of_birth, Site) it fills THAT column and adds nothing,
        // so the exact name is still missing and the file is rejected.
        $candIdx = [];
        foreach (self::candidateFields() as $key => $names) {
            $exact = array_search($key, $headers, true);
            $default = $this->cdValue($key);
            if ($exact !== false) {
                $candIdx[$key] = [$exact, $default];
                continue;
            }
            $variant = null;
            foreach ($names as $n) {
                if (isset($idx[$n])) { $variant = $idx[$n]; break; }
            }
            if ($key === 'sex' && $variant !== null && strtolower($headers[$variant]) !== 'sex') {
                // A 'gender' column: the pipeline appends 'sex' from the
                // default and ignores gender; without a default, nothing.
                if ($default !== '') {
                    $this->add(self::WARNING,
                        "The column '{$headers[$variant]}' is not used: there is no sex column, so"
                        . " '{$default}' is taken from project.json (candidate_defaults.sex) for every row.",
                        "Rename '{$headers[$variant]}' to sex if it holds each participant's sex.");
                    $candIdx[$key] = [null, $default];
                } else {
                    $this->add(self::ERROR,
                        "No sex column ('{$headers[$variant]}' is not read as sex), and no"
                        . " candidate_defaults.sex in project.json. The instrument manager rejects the whole file.",
                        "Rename '{$headers[$variant]}' to sex.");
                    $candIdx[$key] = [null, ''];
                }
                continue;
            }
            if ($variant !== null) {
                $this->add(self::ERROR,
                    "The column '{$headers[$variant]}' must be named exactly {$key} (lowercase). The"
                    . " instrument manager looks only for {$key} and rejects the whole file.",
                    "Rename the header to {$key}.");
                $candIdx[$key] = [$variant, $default];
                continue;
            }
            if ($default === '') {
                $this->add(self::ERROR,
                    "No {$key} column, and no candidate_defaults.{$key} in project.json. The"
                    . " instrument manager rejects the whole file.",
                    "Add a {$key} column to the data, or ask the ARCHIMEDES team to set a default for the project.");
            } else {
                $this->add(self::NOTE,
                    "No {$key} column: '{$default}' will be taken from project.json"
                    . " (candidate_defaults.{$key}) for every row.",
                    "");
            }
            $candIdx[$key] = [null, $default];
        }

        $order   = Dob::dateOrder($this->project);
        $vmap    = [];
        foreach ((array)($this->project['visit_mappings'] ?? []) as $from => $to) {
            $from = strtolower(trim((string)$from));
            $to   = trim((string)$to);
            if ($from !== '' && $to !== '') {
                $vmap[$from] = $to;
            }
        }
        $visits  = $this->filledList('visits');
        $sites   = $this->filledList('sites');
        $cohorts = $this->filledList('cohorts');
        $full    = trim((string)($this->project['project_full_name'] ?? ''));

        $p = [];   // issue => examples
        $unknownVisits = [];
        $seenVisits    = [];
        $keys          = [];

        // DoB and sex once per participant, as the pipeline does: a blank
        // cell is filled from the same participant's other row before
        // candidate_defaults (ClinicalPipeline::scanParticipantDemographics).
        [$partValues, $partConflicts] = $this->participantDemographics(
            $rows, $idCol, ['dob' => $candIdx['dob'][0], 'sex' => $candIdx['sex'][0]]
        );

        foreach ($rows as [$line, $row]) {
            $cell = fn(?int $i): string => $i === null ? '' : trim((string)($row[$i] ?? ''));

            $id = $cell($idCol);
            if ($idCol !== null && $id === '') {
                $p['blank_id'][] = "line {$line}";
            }

            $event = $cell($eventCol);
            if ($eventCol !== null && $event === '') {
                $p['blank_event'][] = "line {$line}";
            }

            if ($event !== '') {
                $visit = $vmap[strtolower($event)] ?? $event;
                $seenVisits[$visit] = true;
                if ($visits !== [] && !$this->inListCi($visit, $visits)) {
                    $label = $visit === $event ? "'{$event}'" : "'{$event}' (mapped to '{$visit}')";
                    $unknownVisits[$label][] = $line;
                }
            }

            // Duplicate participant + event (+ repeat instrument/instance).
            if ($id !== '') {
                $key = $id . "\x1F" . $event;
                if (isset($keys[$key])) {
                    $p['dup_row'][] = "lines {$keys[$key]} and {$line} ({$id}"
                        . ($event !== '' ? ", {$event}" : '') . ")";
                } else {
                    $keys[$key] = $line;
                }
            }

            foreach ($candIdx as $key => [$col, $default]) {
                $v = $cell($col);
                $fromDefault = false;
                if ($v === '' && $col !== null && $id !== '' && isset($partValues[$id][$key])) {
                    $p["participant_{$key}"][] = "line {$line}";
                    continue;   // the participant's own value is checked on its own row
                }
                if ($v === '') {
                    if ($default === '') {
                        if ($col !== null) {
                            $p["blank_{$key}"][] = "line {$line}";
                        }
                        continue;
                    }
                    $v = $default;
                    $fromDefault = true;
                    if ($col !== null) {
                        $p["filled_{$key}"][] = "line {$line}";
                    }
                }
                if ($fromDefault) {
                    continue;   // defaults are checked once, in project settings
                }
                // project / site / cohort: a value in candidate_defaults is
                // authoritative; the pipeline replaces a different value.
                if (in_array($key, ['project', 'site', 'cohort'], true) && $default !== '' && $v !== $default) {
                    $p["override_{$key}"][$v][] = $line;
                    continue;
                }
                switch ($key) {
                    case 'dob':
                        if (!Dob::isValid(Dob::normalize($v, $order))) {
                            $p['bad_dob'][] = "line {$line}: '{$v}'";
                        }
                        break;
                    case 'sex':
                        if ($this->mapSex($v) === null) {
                            $p['bad_sex'][$v][] = $line;
                        }
                        break;
                    case 'project':
                        if ($full !== '' && !$this->isPlaceholder($full) && $v !== $full) {
                            $p['other_project'][$v][] = $line;
                        }
                        break;
                    case 'site':
                        if ($sites !== [] && !$this->inListCi($v, $sites)) {
                            $p['other_site'][$v][] = $line;
                        }
                        break;
                    case 'cohort':
                        if ($cohorts !== [] && !$this->inListCi($v, $cohorts)) {
                            $p['other_cohort'][$v][] = $line;
                        }
                        break;
                }
            }
        }

        $this->addLines($p, 'blank_id', self::WARNING, "row(s) have an empty study_id. The instrument"
            . " manager does not reject them: it creates a participant with an empty ID",
            "Fill in study_id on every row.");
        $this->addLines($p, 'blank_event', self::ERROR, "row(s) have an empty redcap_event_name",
            "Fill in the event on every row.");
        $this->addLines($p, 'dup_row', self::WARNING, "row(s) are for a participant and event that"
            . " already has a row; the instrument manager saves only the first and skips the rest"
            . " (repeating instruments included)",
            "Keep one row per participant per event.");
        foreach ($candIdx as $key => [$col, $default]) {
            $this->addLines($p, "participant_{$key}", self::NOTE,
                "row(s) have an empty {$key}; the pipeline takes it from the same participant's"
                . " other row", "");
            $this->addLines($p, "blank_{$key}", self::ERROR,
                "row(s) have an empty {$key}, it is not on any other row of that participant, and"
                . " there is no candidate_defaults.{$key} in project.json (one such row rejects the whole file)",
                "Fill in {$key} on at least one row of each participant, or ask the ARCHIMEDES team"
                . " to set candidate_defaults.{$key}.");
            $this->addLines($p, "filled_{$key}", self::NOTE,
                "row(s) have an empty {$key}; '{$default}' will be taken from project.json"
                . " (candidate_defaults.{$key})", "");
        }
        foreach ($partConflicts as $pid => $byKey) {
            foreach ($byKey as $k => $values) {
                $p['conflict'][] = "'{$pid}' {$k}: " . implode(' / ', array_map(fn($v) => "'{$v}'", $values));
            }
        }
        $this->addLines($p, 'conflict', self::WARNING, "participant(s) have different DoB or sex values"
            . " on different rows; blank cells and LORIS use the first",
            "Correct the source so each participant has one DoB and one sex.");
        $this->addLines($p, 'bad_dob', self::ERROR, "row(s) have a date of birth that is not a real date",
            "Use YYYY-MM-DD, YYYY-MM or YYYY.");
        $this->addValues($p, 'bad_sex', self::ERROR, "sex value(s) cannot be read as Male, Female or Other"
            . " (one such row rejects the whole file)",
            "Use Male, Female or Other, or tell the ARCHIMEDES team what each code means (sex_mappings).");
        foreach (['project', 'site', 'cohort'] as $k) {
            $this->addValues($p, "override_{$k}", self::NOTE,
                "{$k} value(s) differ from candidate_defaults.{$k} '" . $this->cdValue($k) . "'; the"
                . " pipeline replaces them with it", "");
        }
        $this->addValues($p, 'other_project', self::NOTE,
            "project value(s) differ from the LORIS project name '{$full}'; they will be replaced with it",
            "Check this file belongs to this project.");
        $this->addValues($p, 'other_site', self::WARNING,
            "site value(s) are not in the project's site list (" . implode(', ', $sites) . "). If a site"
            . " is not in LORIS, the instrument manager rejects the whole file",
            "Use the site name exactly as it is in LORIS.");
        $this->addValues($p, 'other_cohort', self::WARNING,
            "cohort value(s) are not in the project's cohort list (" . implode(', ', $cohorts) . "). If a"
            . " cohort is not in LORIS for this project, the instrument manager rejects the whole file",
            "Use the cohort name exactly as it is in LORIS.");

        if ($unknownVisits !== []) {
            $ex = [];
            foreach ($unknownVisits as $v => $lines) {
                $ex[] = "{$v} on " . count($lines) . " row(s), first line {$lines[0]}";
            }
            $this->add(self::ERROR,
                count($unknownVisits) . " event(s) are not LORIS visits of this project ("
                . implode(', ', $visits) . "): " . $this->examples($ex) . ". The instrument manager"
                . " rejects the whole file.",
                "Tell the ARCHIMEDES team which LORIS visit each event is, so it can be"
                . " added to visit_mappings, or ask for the visit to be created.");
        } elseif ($visits === [] && $seenVisits !== []) {
            $this->add(self::NOTE,
                "Events in this file: " . $this->examples(array_keys($seenVisits)) . ".",
                "Each must exist as a visit in LORIS (or be mapped in visit_mappings).");
        }
    }

    /**
     * Every column must be a dictionary field, a checkbox option column,
     * a structural column or a <form>_complete / <form>_timestamp column.
     */
    private function checkColumnsAgainstDictionary(array $headers, array $idx): void
    {
        if ($this->fields === []) {
            return;
        }
        $unknown   = [];
        $checkbox  = [];
        foreach ($idx as $col => $i) {
            if (isset($this->columns[$col]) && $this->columns[$col][1] !== null) {
                $checkbox[$this->columns[$col][0]] = true;
                continue;
            }
            if (isset($this->columns[$col]) || in_array($col, self::structuralColumns(), true)) {
                continue;
            }
            if (preg_match('/^(.+)_(complete|timestamp)$/', $col, $m) && isset($this->forms[$m[1]])) {
                continue;
            }
            $hint = '';
            if (preg_match('/^(.+)___(.+)$/', $col, $m) && isset($this->fields[$m[1]])) {
                $hint = $this->fields[$m[1]]['type'] === 'checkbox'
                    ? " (not an option of checkbox '{$m[1]}')"
                    : " ('{$m[1]}' is not a checkbox)";
            }
            $unknown[] = "'{$headers[$i]}'{$hint}";
        }
        if ($unknown !== []) {
            $this->add(self::WARNING,
                count($unknown) . " column(s) are not defined in the data dictionary, so they"
                . " will not be stored: "
                . $this->examples($unknown) . ".",
                "Add them to the data dictionary, or remove them from the export."
                . " Column names must match the variable names exactly.");
        }
        if ($checkbox !== []) {
            $this->add(self::WARNING,
                count($checkbox) . " checkbox field(s) are exported as one column per option"
                . " (field___code). The instrument manager stores a checkbox under its field name"
                . " only, so these answers are not saved: " . $this->examples(array_keys($checkbox)) . ".",
                "Tell the ARCHIMEDES team if these answers are needed.");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════════

    /**
     * Parse REDCap choices "1, Yes | 2, No" the way the instrument
     * manager does (RedcapDictionaryRecord::_optionsToLINST): ' | | ' is
     * collapsed, a leading '| ' dropped, and an option that is not
     * "code, label" with a code of [A-Za-z0-9._] is renumbered.
     *
     * @return array{choices: array<string,string>, renumbered: string[]}
     */
    private function parseChoices(string $raw): array
    {
        $out = ['choices' => [], 'renumbered' => []];
        if (trim($raw) === '') {
            return $out;
        }
        $raw = str_replace(' | | ', ' | ', $raw);
        if (str_starts_with($raw, '| ')) {
            $raw = substr($raw, 2);
        }
        foreach (explode('|', $raw) as $part) {
            $part = trim($part);
            if (preg_match(self::CHOICE_PATTERN, $part, $m) !== 1) {
                $out['renumbered'][] = $part;
                continue;
            }
            $code = $m[1];
            $out['choices'][$code] = trim($m[2]);
        }
        return $out;
    }

    /**
     * Read a delimited file.
     *
     * @return array{0: string[], 1: array<array{0:int,1:array}>, 2: ?string}
     *         [headers, [[line, row], ...], error]
     */
    private function readCsv(string $path, string $delim): array
    {
        if (!is_readable($path)) {
            return [[], [], "File cannot be read: {$path}"];
        }
        $fh = @fopen($path, 'r');
        if ($fh === false) {
            return [[], [], "File cannot be opened: {$path}"];
        }
        // Skip a UTF-8 byte-order mark (Excel adds one) so the first
        // header is read correctly even when it is quoted.
        if (fread($fh, 3) !== "\xEF\xBB\xBF") {
            rewind($fh);
        }
        $headers = fgetcsv($fh, 0, $delim, '"', '\\');
        if (!is_array($headers) || $headers === [null]) {
            fclose($fh);
            return [[], [], "The file is empty (no header row)."];
        }
        $rawHeaders = array_map(fn($h) => (string)$h, $headers);
        $headers    = array_map(fn($h) => trim((string)$h), $headers);

        if (count($headers) === 1 && $delim === ',' && str_contains($headers[0], "\t")) {
            fclose($fh);
            return [[], [], "The file is tab-separated but named .csv."];
        }
        if (count($headers) === 1 && $delim === "\t" && str_contains($headers[0], ',')) {
            fclose($fh);
            return [[], [], "The file is comma-separated but named .tsv."];
        }

        // Empty lines are skipped, as the instrument manager does
        // (SplFileObject::SKIP_EMPTY).
        $rows = [];
        $line = 1;
        while (($row = fgetcsv($fh, 0, $delim, '"', '\\')) !== false) {
            $line++;
            if ($row === [null] || (count($row) === 1 && trim((string)$row[0]) === '')) {
                continue;
            }
            $rows[] = [$line, $row];
        }
        fclose($fh);
        $this->lastRawHeaders = $rawHeaders;

        return [$headers, $rows, null];
    }

    /**
     * A constant of ClinicalPipeline, read by reflection so this check
     * uses the pipeline's own values (folders, structural columns, sex
     * vocabulary, excluded forms) without changing the pipeline, where
     * they are private.
     */
    private static function pipelineConst(string $name): mixed
    {
        if (!array_key_exists($name, self::$pipelineConsts)) {
            $ref = new \ReflectionClass(ClinicalPipeline::class);
            if (!$ref->hasConstant($name)) {
                throw new \LogicException("ClinicalPipeline::{$name} not found - the validator needs updating");
            }
            self::$pipelineConsts[$name] = $ref->getConstant($name);
        }
        return self::$pipelineConsts[$name];
    }

    /**
     * Columns that are never instrument data: the pipeline's structural
     * columns, every DoB/DoD and sex column name, plus REDCap's survey
     * identifier.
     *
     * @return string[]
     */
    private static function structuralColumns(): array
    {
        return array_merge(
            self::pipelineConst('STRUCTURAL_COLUMNS'),
            self::pipelineConst('DATE_COLUMN_NAMES'),
            self::pipelineConst('SEX_COLUMN_NAMES'),
            ['studyid', 'redcap_survey_identifier']
        );
    }

    /** @return array<string, string[]> key => accepted column names */
    private static function candidateFields(): array
    {
        return [
            'dob'     => Dob::COLUMN_NAMES,
            'sex'     => self::pipelineConst('SEX_COLUMN_NAMES'),
            'project' => ['project'],
            'site'    => ['site'],
            'cohort'  => ['cohort'],
        ];
    }

    /**
     * Sex value as the pipeline writes it to the processed copy:
     * SEX_VALUE_MAP + project sex_mappings, matched case-insensitively.
     * Null when the result is not one LORIS accepts.
     */
    private function mapSex(string $v): ?string
    {
        // '+' keeps numeric codes ("1", "2") as keys; array_merge would renumber them.
        $map = array_change_key_case(
            (is_array($this->project['sex_mappings'] ?? null) ? $this->project['sex_mappings'] : [])
            + self::pipelineConst('SEX_VALUE_MAP'),
            CASE_LOWER
        );
        $out = $map[strtolower(trim($v))] ?? trim($v);
        return in_array($out, self::SEX_VALUES, true) ? $out : null;
    }

    /**
     * Each participant's first non-blank DoB / sex in the file, and any
     * different later values. Mirrors
     * ClinicalPipeline::scanParticipantDemographics().
     *
     * @param array<string,?int> $cols key => column index
     * @return array{0: array<string,array<string,string>>, 1: array<string,array<string,array<string>>>}
     */
    private function participantDemographics(array $rows, ?int $idCol, array $cols): array
    {
        $values    = [];
        $conflicts = [];
        if ($idCol === null) {
            return [$values, $conflicts];
        }
        foreach ($rows as [, $row]) {
            $pid = trim((string)($row[$idCol] ?? ''));
            if ($pid === '') {
                continue;
            }
            foreach ($cols as $k => $ci) {
                if ($ci === null) {
                    continue;
                }
                $v = trim((string)($row[$ci] ?? ''));
                if ($v === '') {
                    continue;
                }
                if (!isset($values[$pid][$k])) {
                    $values[$pid][$k] = $v;
                } elseif ($values[$pid][$k] !== $v) {
                    $seen = $conflicts[$pid][$k] ?? [$values[$pid][$k]];
                    if (!in_array($v, $seen, true)) {
                        $seen[] = $v;
                    }
                    $conflicts[$pid][$k] = $seen;
                }
            }
        }
        return [$values, $conflicts];
    }

    /**
     * Is $v in $list, ignoring case? Visits, sites and cohorts are looked
     * up by the instrument manager in MySQL, whose default collation
     * ignores case.
     */
    private function inListCi(string $v, array $list): bool
    {
        foreach ($list as $x) {
            if (strcasecmp($v, (string)$x) === 0) {
                return true;
            }
        }
        return false;
    }

    private function cdValue(string $key): string
    {
        $v = trim((string)($this->project['candidate_defaults'][$key] ?? ''));
        return $this->isPlaceholder($v) ? '' : $v;
    }

    /** A project.json list with template "<...>" entries removed. */
    private function filledList(string $key): array
    {
        $list = $this->project[$key] ?? [];
        if (!is_array($list)) {
            return [];
        }
        return array_values(array_filter(
            array_map(fn($v) => trim((string)$v), $list),
            fn($v) => $v !== '' && !$this->isPlaceholder($v)
        ));
    }

    private function isPlaceholder(string $v): bool
    {
        return (bool)preg_match('/^<.*>$/', trim($v));
    }

    private function section(string $scope, string $file, string $summary): void
    {
        $this->sections[] = ['scope' => $scope, 'file' => $file, 'summary' => $summary, 'issues' => []];
        $this->current = count($this->sections) - 1;
    }

    private function add(string $severity, string $message, string $fix): void
    {
        $this->sections[$this->current]['issues'][] = [
            'severity' => $severity, 'message' => $message, 'fix' => $fix,
        ];
    }

    /** @param array<string, string[]> $byIssue */
    private function addGrouped(array $byIssue, array $msgs): void
    {
        foreach ($msgs as $key => [$sev, $what, $fix]) {
            if (!empty($byIssue[$key])) {
                $this->add($sev, count($byIssue[$key]) . " {$what}: " . $this->examples($byIssue[$key]) . ".", $fix);
            }
        }
    }

    private function addLines(array $p, string $key, string $sev, string $what, string $fix): void
    {
        if (!empty($p[$key])) {
            $this->add($sev, count($p[$key]) . " {$what}: " . $this->examples($p[$key]) . ".", $fix);
        }
    }

    /** $p[$key] = [value => [lines]] */
    private function addValues(array $p, string $key, string $sev, string $what, string $fix): void
    {
        if (empty($p[$key])) {
            return;
        }
        $ex = [];
        foreach ($p[$key] as $v => $lines) {
            $ex[] = "'{$v}' on " . count($lines) . " row(s), first line {$lines[0]}";
        }
        $this->add($sev, count($p[$key]) . " {$what}: " . $this->examples($ex) . ".", $fix);
    }

    private function examples(array $items): string
    {
        $items = array_values($items);
        $shown = array_slice($items, 0, self::MAX_EXAMPLES);
        $more  = count($items) - count($shown);
        return implode('; ', $shown) . ($more > 0 ? "; ... and {$more} more" : '');
    }

    // ══════════════════════════════════════════════════════════════════
    //  Reporting
    // ══════════════════════════════════════════════════════════════════

    /** @return array{ERROR:int, WARNING:int, NOTE:int} */
    public static function counts(array $sections): array
    {
        return ValidationReport::counts($sections);
    }

    public static function renderText(array $sections, string $title, string $runAt): string
    {
        return ValidationReport::renderText($sections, "REDCap validation report", $title, $runAt, [
            "ERROR   the instrument manager or the pipeline would reject it; fix before",
            "        ingestion. In a data file, one rejected row stops the whole file.",
            "WARNING ingestion can go ahead, but data may be lost or not as intended",
            "NOTE    information only",
        ]);
    }

    public static function writeCsv(array $sections, string $path): bool
    {
        return ValidationReport::writeCsv($sections, $path);
    }
}
