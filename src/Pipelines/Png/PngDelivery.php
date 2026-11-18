<?php

/**
 * PngDelivery
 *
 * Reads a PNG delivery laid out as the user guide asks
 * (ARCHIMEDES_PNG_User_Guide.docx) and says, for every visit and every image,
 * whether it can go further - without writing anything.
 *
 *   deidentified-raw/imaging/png/
 *     participants.tsv                      participant_id dob sex [project site cohort]
 *     sub-<ID>/sessions.tsv                 session_id [acq_date]   (or sub-<ID>_sessions.tsv)
 *     sub-<ID>/ses-<visit>/<type>_<NNN>.png
 *
 * The unit is the VISIT (sub-<ID>/ses-<visit>), as the study is for DICOM: a
 * PNG has no header and no StudyInstanceUID, so the folder names are the only
 * identity it has, and the file name is the only description (its scan type).
 *
 * Each visit is:
 *   linked    its sub- folder has a participants.tsv row and the visit is
 *             listed in the participant's sessions file - it can be synced,
 *             reidentified and ingested
 *   unlinked  anything else - reported with the reason, never processed
 *
 * Each image of a linked visit is accepted, or SET ASIDE with the reason
 * (guide section 2: "a file whose name we do not recognise is set aside and
 * reported back to you rather than loaded"). Set-aside images never block the
 * rest of the visit:
 *   - scan type (start of the file name) not letters and digits, or not one of
 *     project.json png.scan_types (when the project declares them)
 *   - empty, unreadable, or not really a PNG (signature check, the same as the
 *     LORIS script), so a renamed JPEG or a truncated transfer is caught here
 *   - byte-identical to an image already seen in this delivery (LORIS would
 *     keep only the first; the second is named so the site can check it)
 *
 * Problems that block more than one image are issues with a scope, used by
 * PngPreflight exactly as DicomPreflight uses organize's problems:
 *   project  participants.tsv unusable (comma-separated, .xlsx, empty, no
 *            participant_id, duplicate rows ...)
 *   subject  participant folder name not letters and digits, two folders that
 *            differ only by case, sessions file unreadable
 *   visit    visit folder name not letters and digits, two visit folders that
 *            differ only by case, acq_date not a real YYYY-MM-DD date,
 *            conflicting rows for one session_id
 *
 * Hashes: every accepted image is hashed (md5, the value LORIS stores as the
 * md5hash parameter of the file), and each visit gets a fingerprint over its
 * accepted images' names and hashes plus its acq_date. The reidentifier and
 * the import compare fingerprints, so a visit is redone only when what would
 * go into LORIS changed - including an image edited in place with the same
 * name and size (an md5 per image, not only sizes as DICOM; cached by
 * path+size+mtime+ctime so unchanged images are not read again on every run).
 *
 * Reads only. PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Png;

use LORIS\Pipelines\Shared\ParticipantsTsv;
use LORIS\Pipelines\Shared\ParticipantSync;
use LORIS\Pipelines\Shared\SessionsTsv;
use LORIS\Pipelines\Shared\SystemFiles;

final class PngDelivery
{
    /** First eight bytes of any PNG file (same check as import_png_dataset.py). */
    public const PNG_SIGNATURE = "\x89PNG\r\n\x1a\n";

    /** Identifier after sub- / ses-, matched in full (as the LORIS script does). */
    public const LABEL_REGEX = '/^[A-Za-z0-9]+$/';

    // Link reasons (unlinked visits).
    public const NO_PARTICIPANTS_TSV        = 'no_participants_tsv';
    public const PARTICIPANTS_TSV_UNUSABLE  = 'participants_tsv_unusable';
    public const NOT_IN_PARTICIPANTS_TSV    = 'not_in_participants_tsv';
    public const NO_SESSIONS_TSV            = 'no_sessions_tsv';
    public const NOT_IN_SESSIONS_TSV        = 'not_in_sessions_tsv';

    /** @var array<int, array<string, mixed>> */
    private array $problems = [];

    /** @var array<string, string> md5 => where it was first seen */
    private array $hashSeen = [];

    /**
     * md5 by "path|size|mtime|ctime|inode", kept between runs so an unchanged image is not
     * read again by every step of every run. Any change of size or mtime (a
     * re-delivered or edited file) means the bytes are read again.
     *
     * @var array<string, string>
     */
    private array $hashCache = [];
    private array $hashCacheUsed = [];
    private bool $hashCacheDirty = false;

    private function __construct(
        private string $deliveryDir,
        private array $projectConfig
    ) {
    }

    /**
     * Scan the delivery.
     *
     * @param string      $projectPath   Project root.
     * @param array       $projectConfig Decoded project.json.
     * @param string|null $deliveryDir   Override (tests); default deidentified-raw/imaging/png.
     *
     * @return array{
     *   generated_at: string, delivery_dir: string, participants_tsv: ?string,
     *   scan_types: string[], visits: array<int, array<string, mixed>>,
     *   problems: array<int, array<string, mixed>>, counts: array<string, int>
     * }
     */
    public static function scan(string $projectPath, array $projectConfig, ?string $deliveryDir = null): array
    {
        $dir  = rtrim($deliveryDir ?? PngPaths::of($projectPath, PngPaths::RAW_SUBDIR), '/');
        $self = new self($dir, $projectConfig);
        $cachePath = PngPaths::of($projectPath, PngPaths::HASH_CACHE);
        $self->hashCache = (array) (PngPaths::readJson($cachePath)['md5'] ?? []);

        $result = $self->_scan($projectPath);

        // Keep only entries still in the delivery; a cache write failure only
        // costs speed.
        if ($self->hashCacheDirty || count($self->hashCacheUsed) !== count($self->hashCache)) {
            try {
                PngPaths::writeJson($cachePath, ['updated_at' => date('c'), 'md5' => $self->hashCacheUsed]);
            } catch (\Throwable $e) {
                // ignored
            }
        }

        return $result;
    }

    /**
     * Scan types the project agreed during onboarding (project.json
     * png.scan_types). Empty = not declared: any letters-and-digits type is
     * accepted here and LORIS checks it against mri_scan_type at import.
     *
     * @return string[]
     */
    public static function scanTypes(array $projectConfig): array
    {
        return array_values(array_filter(array_map(
            static fn ($t): string => trim((string) $t),
            (array) ($projectConfig['png']['scan_types'] ?? [])
        ), static fn (string $t): bool => $t !== ''));
    }

    /**
     * Visit fingerprint: accepted images (name + md5) and the acq_date. Equal
     * fingerprints mean the same thing would be sent to LORIS.
     *
     * @param array<int, array{name: string, md5: string}> $images
     */
    public static function fingerprint(array $images, ?string $acqDate): string
    {
        $lines = array_map(static fn (array $i): string => $i['name'] . "\0" . $i['md5'], $images);
        sort($lines, SORT_STRING);
        return hash('sha256', implode("\n", $lines) . "\nacq_date=" . ($acqDate ?? ''));
    }

    // =========================================================================

    private function _scan(string $projectPath): array
    {
        $visits    = [];
        $scanTypes = self::scanTypes($this->projectConfig);

        if (!is_dir($this->deliveryDir)) {
            return $this->_result(null, $scanTypes, [], 'no delivery folder');
        }

        // --- participants.tsv --------------------------------------------------
        $tsvPath = ParticipantsTsv::locate($projectPath, PngPaths::MODALITY);
        if ($tsvPath === null && is_file($this->deliveryDir . '/participants.tsv')) {
            $tsvPath = $this->deliveryDir . '/participants.tsv';    // deliveryDir override
        }
        [$roster, $rosterError] = $this->_readParticipants($tsvPath);

        if ($scanTypes === []) {
            $this->_problem('warning', 'project', 'project.json',
                'png.scan_types is not set - any letters-and-digits image type is accepted here; '
                . 'LORIS checks it against mri_scan_type only at import',
                'add "png": {"scan_types": ["echo", "ecg", ...]} with the types agreed at onboarding');
        }

        // --- top level ------------------------------------------------------------
        $subjectDirs = [];
        $strays      = [];
        foreach ($this->_list($this->deliveryDir) as $name) {
            $path = $this->deliveryDir . '/' . $name;
            if ($name === 'participants.tsv') {
                continue;
            }
            if (is_dir($path) && str_starts_with($name, 'sub-')) {
                $subjectDirs[$name] = $path;
                continue;
            }
            $strays[] = $name;
        }
        if ($strays !== []) {
            $this->_problem('warning', 'project', '(top level)',
                count($strays) . ' entr' . (count($strays) === 1 ? 'y' : 'ies')
                . ' outside the sub-<ID>/ layout ignored: ' . $this->_few($strays),
                'participant folders must be named sub-<ID> (lower-case "sub-"); move or remove the rest');
        }
        if ($subjectDirs === []) {
            $this->_problem('error', 'project', '(top level)', 'no sub-<ID> participant folder found',
                'deliver one folder per participant, sub-<ID>/ses-<visit>/ (guide section 1)');
        }
        foreach (ParticipantSync::nestedSubjectDirs($this->deliveryDir) as $nested) {
            $this->_problem('warning', 'project', $nested,
                "a sub- folder inside another participant's folder - it is not read",
                'move it to the top level (copied one level too deep?)');
        }

        // Two folders the mapper (case-insensitive) would treat as one participant.
        $byLower = [];
        foreach (array_keys($subjectDirs) as $name) {
            $byLower[strtolower($name)][] = $name;
        }
        $caseClash = [];
        foreach ($byLower as $names) {
            if (count($names) > 1) {
                foreach ($names as $n) {
                    $caseClash[$n] = $names;
                }
            }
        }

        // --- each participant -------------------------------------------------------
        ksort($subjectDirs, SORT_STRING);
        foreach ($subjectDirs as $folder => $subjectPath) {
            $subject = substr($folder, 4);

            if (!preg_match(self::LABEL_REGEX, $subject)) {
                $this->_problem('error', 'subject', $folder,
                    'participant identifier must be letters and digits only after sub- '
                    . '(no underscores, dashes, spaces) - nothing in this folder is read',
                    'rename the folder (and its participants.tsv row), e.g. sub-ARCHI0001', $subject);
                continue;
            }
            if (isset($caseClash[$folder])) {
                $this->_problem('error', 'subject', $folder,
                    'folders ' . implode(' and ', $caseClash[$folder]) . ' differ only by upper/lower case '
                    . 'and would be the same participant in LORIS',
                    'keep one folder per participant, spelled the same way everywhere', $subject);
                continue;
            }

            $row = null;
            if ($roster !== null) {
                $row = $roster->get('sub-' . $subject) ?? $roster->get($subject);
            }
            $subjectLink = match (true) {
                $tsvPath === null       => self::NO_PARTICIPANTS_TSV,
                $roster === null        => self::PARTICIPANTS_TSV_UNUSABLE,
                $row === null           => self::NOT_IN_PARTICIPANTS_TSV,
                default                 => null,
            };

            foreach ($this->_readSubject($subject, $subjectPath, $subjectLink, $row, $scanTypes) as $visit) {
                $visits[] = $visit;
            }
        }

        // Rows in participants.tsv with no folder.
        if ($roster !== null) {
            $orphans = ParticipantSync::orphans($roster, $this->deliveryDir);
            foreach ($orphans['rows'] as $id) {
                $this->_problem('warning', 'project', 'participants.tsv',
                    "row sub-{$id} has no sub-{$id}/ folder - no images delivered for it",
                    'deliver the folder, or remove the row');
            }
        }

        return $this->_result($tsvPath, $scanTypes, $visits, $rosterError);
    }

    /**
     * participants.tsv, with the common save mistakes named.
     *
     * @return array{0: ?ParticipantsTsv, 1: ?string}
     */
    private function _readParticipants(?string $tsvPath): array
    {
        if ($tsvPath === null) {
            $this->_problem('warning', 'project', 'participants.tsv',
                'no participants.tsv in the delivery - every visit is UNLINKED (reported, nothing ingested)',
                'add participants.tsv at the top level with participant_id, dob, sex (guide section 3)');
            return [null, 'missing'];
        }

        $problem = ParticipantsTsv::formatProblem($tsvPath);
        $message = $problem === null ? null : 'the file ' . $problem;

        if ($message === null) {
            try {
                return [ParticipantsTsv::load(
                    $tsvPath,
                    $this->projectConfig['candidate_defaults'] ?? [],
                    $this->projectConfig
                ), null];
            } catch (\Throwable $e) {
                $message = $e->getMessage();
            }
        }

        $this->_problem('error', 'project', 'participants.tsv', $message,
            'save it as Tab Delimited Text (Excel) or Tab-separated values (Sheets), with a '
            . 'participant_id column and one row per participant (guide section 3)');

        return [null, $message];
    }

    /**
     * One participant's visits.
     *
     * @return array<int, array<string, mixed>>
     */
    private function _readSubject(
        string $subject,
        string $subjectPath,
        ?string $subjectLink,
        ?array $row,
        array $scanTypes
    ): array {
        $where = "sub-{$subject}";

        // Visit folders.
        $sessionDirs = [];
        foreach ($this->_list($subjectPath) as $name) {
            $path = $subjectPath . '/' . $name;
            if (!is_dir($path)) {
                if (!preg_match('/sessions\.tsv$/i', $name)) {
                    $this->_problem('warning', 'subject', "{$where}/{$name}",
                        'file outside a ses- folder ignored', 'images go in sub-<ID>/ses-<visit>/', $subject);
                }
                continue;
            }
            if (!str_starts_with($name, 'ses-')) {
                if (!str_starts_with($name, 'sub-')) {      // nested sub- already reported
                    $this->_problem('warning', 'subject', "{$where}/{$name}",
                        'folder is not a ses-<visit> folder - not read',
                        'put the images in sub-<ID>/ses-<visit>/', $subject);
                }
                continue;
            }
            $sessionDirs[$name] = $path;
        }

        if ($sessionDirs === []) {
            $this->_problem('warning', 'subject', $where, 'no ses-<visit> folder - nothing to ingest for this participant',
                'put the images in sub-<ID>/ses-<visit>/', $subject);
            return [];
        }

        // Sessions file (only consulted for a participant with a row).
        $sessions   = SessionsTsv::read($this->deliveryDir, $subject);
        $listed     = SessionsTsv::listed($this->deliveryDir, $subject);
        $sessFile   = basename(SessionsTsv::path($this->deliveryDir, $subject));
        $sessIssues = [];
        if ($subjectLink === null) {
            if ($sessions['error'] !== null) {
                $this->_problem('error', 'subject', "{$where}/{$sessFile}",
                    'could not be read: ' . $sessions['error'],
                    'save it as tab-separated text with a session_id column', $subject);
                return [];
            }
            if (SessionsTsv::both($this->deliveryDir, $subject)) {
                $this->_problem('warning', 'subject', $where,
                    "both sessions.tsv and sub-{$subject}_sessions.tsv - only sessions.tsv is read",
                    'keep one sessions file', $subject);
            }
            foreach ($this->_list($subjectPath) as $name) {
                if (preg_match('/^sub-(.+)_sessions\.tsv$/i', $name, $m) && $m[1] !== $subject) {
                    $this->_problem('warning', 'subject', "{$where}/{$name}",
                        "this sessions file is named for sub-{$m[1]}, not this participant - ignored",
                        "rename it to sessions.tsv if it is {$where}'s, or move it", $subject);
                }
            }
            $folderVisits = array_map(static fn ($n) => substr($n, 4), array_keys($sessionDirs));
            foreach (SessionsTsv::check($this->deliveryDir, $subject, $folderVisits) as $p) {
                if ($p['kind'] === SessionsTsv::LISTED_NO_FOLDER) {
                    $this->_problem('warning', 'subject', "{$where}/{$sessFile}", $p['message'], $p['fix'], $subject);
                }
                $sessIssues[$p['kind']] = $p;
            }
        }

        // Acquisition dates, with the checks the LORIS script would make.
        $acqDates = [];
        foreach ($sessions['rows'] as $r) {
            $v = $r['visit'];
            if ($v === '') {
                if ($subjectLink === null) {
                    $this->_problem('warning', 'subject', "{$where}/{$sessFile}",
                        "line {$r['line']}: empty session_id - row ignored", 'fill it in, e.g. ses-V01', $subject);
                }
                continue;
            }
            if (array_key_exists($v, $acqDates) && $acqDates[$v] !== $r['acq_date']) {
                $acqDates[$v] = ['conflict' => true];
                continue;
            }
            $acqDates[$v] ??= $r['acq_date'];
        }

        // Visit folders that differ only by case would be one LORIS visit.
        $lower = [];
        foreach (array_keys($sessionDirs) as $n) {
            $lower[strtolower($n)][] = $n;
        }

        $visits = [];
        ksort($sessionDirs, SORT_STRING);
        foreach ($sessionDirs as $folder => $visitPath) {
            $session  = substr($folder, 4);
            $vwhere   = "{$where}/{$folder}";
            $base     = [
                'key'         => "{$subject}/{$session}",
                'subject'     => $subject,
                'session'     => $session,
                'directory'   => $visitPath,
                'external_id' => $row !== null ? ParticipantSync::externalId($row, $subject) : $subject,
                'acq_date'    => null,
                'images'      => [],
                'set_aside'   => [],
                'fingerprint' => null,
            ];

            if (!preg_match(self::LABEL_REGEX, $session)) {
                $this->_problem('error', 'visit', $vwhere,
                    'visit label must be letters and digits only after ses- (no underscores, dashes, spaces)',
                    'rename the folder and its sessions.tsv row, e.g. ses-V01; ask the ARCHIMEDES team if '
                    . 'the LORIS visit label itself has other characters', $subject, $session);
                $visits[] = $base + ['link_status' => 'blocked', 'link_reason' => 'bad_visit_label'];
                continue;
            }
            if (count($lower[strtolower($folder)]) > 1) {
                $this->_problem('error', 'visit', $vwhere,
                    'folders ' . implode(' and ', $lower[strtolower($folder)]) . ' differ only by case and '
                    . 'would be the same LORIS visit', 'merge them into one ses- folder', $subject, $session);
                $visits[] = $base + ['link_status' => 'blocked', 'link_reason' => 'visit_case_clash'];
                continue;
            }

            // Unlinked: reported, nothing more is read (no hashing of images that
            // cannot go anywhere).
            $reason = $subjectLink;
            if ($reason === null && isset($sessIssues[SessionsTsv::MISSING])) {
                $reason = self::NO_SESSIONS_TSV;
            } elseif ($reason === null && isset($sessIssues[SessionsTsv::NO_COLUMN])) {
                $reason = self::NO_SESSIONS_TSV;
            } elseif ($reason === null && ($listed === null || !in_array($session, $listed, true))) {
                $reason = self::NOT_IN_SESSIONS_TSV;
            }
            if ($reason !== null) {
                $count = count(array_filter($this->_list($visitPath), static fn ($n) => (bool) preg_match('/\.png$/i', $n)));
                $visits[] = $base + ['link_status' => 'unlinked', 'link_reason' => $reason, 'png_files' => $count];
                continue;
            }

            // acq_date (optional; YYYY-MM-DD and a real date when given).
            $acq = $acqDates[$session] ?? '';
            if (is_array($acq)) {
                $this->_problem('error', 'visit', "{$where}/{$sessFile}",
                    "ses-{$session} is listed more than once with different acq_date values",
                    'keep one row per visit', $subject, $session);
                $visits[] = $base + ['link_status' => 'blocked', 'link_reason' => 'acq_date_conflict'];
                continue;
            }
            if ($acq !== '') {
                $problem = self::acqDateProblem($acq);
                if ($problem !== null) {
                    $this->_problem('error', 'visit', "{$where}/{$sessFile}",
                        "acq_date '{$acq}' for ses-{$session} {$problem}",
                        'use YYYY-MM-DD (e.g. 2024-11-19), or leave it empty', $subject, $session);
                    $visits[] = $base + ['link_status' => 'blocked', 'link_reason' => 'bad_acq_date'];
                    continue;
                }
                if ($acq > date('Y-m-d')) {
                    $this->_problem('warning', 'visit', "{$where}/{$sessFile}",
                        "acq_date '{$acq}' for ses-{$session} is in the future", 'check the date', $subject, $session);
                }
            }

            $visit = $base;
            $visit['acq_date'] = $acq !== '' ? $acq : null;
            [$visit['images'], $visit['set_aside']] = $this->_readImages($visitPath, $vwhere, $subject, $session, $scanTypes);
            $visit['fingerprint'] = self::fingerprint($visit['images'], $visit['acq_date']);
            $visit['link_status'] = 'linked';
            $visit['link_reason'] = null;

            if ($visit['images'] === []) {
                $this->_problem('warning', 'visit', $vwhere,
                    $visit['set_aside'] === [] ? 'no PNG image in this visit folder - nothing to ingest'
                        : 'every image in this visit was set aside - nothing to ingest',
                    'see the images listed for this visit', $subject, $session);
            }
            $visits[] = $visit;
        }

        return $visits;
    }

    /**
     * The images of one linked visit: accepted, or set aside with the reason.
     *
     * @return array{0: array<int, array<string, mixed>>, 1: array<int, array<string, mixed>>}
     */
    private function _readImages(string $visitPath, string $vwhere, string $subject, string $session, array $scanTypes): array
    {
        $accepted = [];
        $aside    = [];
        $others   = [];

        foreach ($this->_list($visitPath) as $name) {
            $path = $visitPath . '/' . $name;
            if (is_dir($path)) {
                $this->_problem('warning', 'visit', "{$vwhere}/{$name}/",
                    'subfolder inside a visit folder - not searched; its images are NOT ingested',
                    'put the images directly in the visit folder', $subject, $session);
                continue;
            }
            if (!preg_match('/\.png$/i', $name)) {
                $others[] = $name;
                continue;
            }

            $reason = $this->_imageProblem($path, $name, $scanTypes, $scanType, $fix);
            $md5    = null;
            if ($reason === null) {
                $md5 = $this->_md5($path);
                if (isset($this->hashSeen[$md5])) {
                    $reason = 'is byte-identical to ' . $this->hashSeen[$md5]
                        . ' - LORIS keeps one copy of an image, so only the first is ingested';
                    $fix    = 'remove the duplicate, or replace it with the image that was meant';
                } else {
                    $this->hashSeen[$md5] = "{$vwhere}/{$name}";
                }
            }

            if ($reason !== null) {
                $aside[] = ['name' => $name, 'path' => $path, 'reason' => $reason];
                $this->_problem('warning', 'image', "{$vwhere}/{$name}", 'SET ASIDE - ' . $reason, $fix,
                    $subject, $session, $name);
                continue;
            }

            $rest = (string) substr(pathinfo($name, PATHINFO_FILENAME), strlen($scanType));
            $rest = ltrim($rest, '_');
            if ($rest !== '' && !preg_match('/^[A-Za-z0-9_]+$/', $rest)) {
                $this->_problem('warning', 'image', "{$vwhere}/{$name}",
                    'file name has characters other than letters, digits and underscores (ingested anyway)',
                    "rename it like {$scanType}_001.png next time", $subject, $session, $name);
            } elseif (preg_match('/^\d{1,2}$/', $rest)) {
                $this->_problem('warning', 'image', "{$vwhere}/{$name}",
                    'number is not padded to three digits (ingested anyway)',
                    sprintf('name it %s_%03d.png so files sort correctly', $scanType, (int) $rest),
                    $subject, $session, $name);
            }

            $accepted[] = [
                'name'      => $name,
                'path'      => $path,
                'scan_type' => $scanType,
                'md5'       => $md5,
                'size'      => (int) filesize($path),
            ];
        }

        if ($others !== []) {
            $this->_problem('warning', 'visit', $vwhere,
                count($others) . ' non-PNG file(s) ignored: ' . $this->_few($others),
                'only .png images are ingested', $subject, $session);
        }

        return [$accepted, $aside];
    }

    /**
     * Why an image cannot be ingested, or null. Sets $scanType and $fix.
     */
    private function _imageProblem(string $path, string $name, array $scanTypes, ?string &$scanType, ?string &$fix): ?string
    {
        $stem     = (string) pathinfo($name, PATHINFO_FILENAME);
        $scanType = explode('_', $stem)[0];
        $fix      = null;

        if ($scanType === '') {
            $fix = 'name it <type>_<NNN>.png, e.g. echo_001.png';
            return 'the name starts with an underscore, so no image type can be read from it';
        }
        if (!preg_match('/^[A-Za-z0-9]+$/', $scanType)) {
            $fix = 'name it <type>_<NNN>.png with an agreed type, e.g. echo_001.png';
            return "image type '{$scanType}' (start of the name) must be letters and digits only";
        }
        if ($scanTypes !== [] && !in_array($scanType, $scanTypes, true)) {
            foreach ($scanTypes as $t) {
                if (strcasecmp($t, $scanType) === 0) {
                    $fix = "rename it to start with '{$t}' (same letters, agreed spelling)";
                    return "image type '{$scanType}' is not spelled as agreed ('{$t}')";
                }
            }
            $fix = 'use one of the agreed types (' . implode(', ', $scanTypes) . '), or ask the ARCHIMEDES '
                . 'team to add this type';
            return "image type '{$scanType}' is not one of the types agreed for this project";
        }
        if (!is_readable($path)) {
            $fix = 'fix the file permissions on the share';
            return 'the pipeline user cannot read the file';
        }
        $size = @filesize($path);
        if ($size === 0 || $size === false) {
            $fix = 'send the image again';
            return 'the file is empty';
        }
        $head = @file_get_contents($path, false, null, 0, 8);
        if ($head !== self::PNG_SIGNATURE) {
            $fix = 'export it as PNG (not renamed from .jpg/.bmp), and send it again if the transfer was cut';
            return 'is not a PNG image (wrong format renamed to .png, or a truncated transfer)';
        }
        return null;
    }

    private function _md5(string $path): string
    {
        clearstatcache(true, $path);
        // ctime (inode change time) cannot be set by touch or rsync -t and
        // changes on every write, so an edit that keeps size and mtime is
        // still re-read.
        $st  = @stat($path) ?: ['size' => 0, 'mtime' => 0, 'ctime' => 0, 'ino' => 0];
        $key = $path . '|' . $st['size'] . '|' . $st['mtime'] . '|' . $st['ctime'] . '|' . $st['ino'];
        if (!isset($this->hashCache[$key])) {
            $this->hashCache[$key] = (string) md5_file($path);
            $this->hashCacheDirty  = true;
        }
        return $this->hashCacheUsed[$key] = $this->hashCache[$key];
    }

    /**
     * Problem with an acq_date value, or null when it is a real YYYY-MM-DD date.
     */
    public static function acqDateProblem(string $value): ?string
    {
        if (!preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $value, $m)) {
            return 'is not in YYYY-MM-DD format';
        }
        if (!checkdate((int) $m[2], (int) $m[3], (int) $m[1])) {
            return 'is not a real date';
        }
        return null;
    }

    /**
     * Folder entries, without hidden files and system files (.DS_Store,
     * ._ AppleDouble companions, Thumbs.db ...), sorted.
     *
     * @return string[]
     */
    private function _list(string $dir): array
    {
        $names = @scandir($dir);
        if ($names === false) {
            $this->_problem('error', 'project', $dir, 'folder cannot be read by the pipeline user',
                'fix the permissions on the share');
            return [];
        }
        $out = [];
        foreach ($names as $n) {
            if ($n === '.' || $n === '..' || str_starts_with($n, '.') || SystemFiles::isIgnored($dir . '/' . $n)) {
                continue;
            }
            $out[] = $n;
        }
        sort($out, SORT_STRING);
        return $out;
    }

    private function _problem(
        string $level,
        string $scope,
        string $where,
        string $message,
        ?string $fix = null,
        ?string $subject = null,
        ?string $session = null,
        ?string $file = null
    ): void {
        $this->problems[] = array_filter([
            'level'   => $level,
            'scope'   => $scope,
            'where'   => $where,
            'message' => $message,
            'fix'     => $fix,
            'subject' => $subject,
            'session' => $session,
            'file'    => $file,
        ], static fn ($v) => $v !== null);
    }

    private function _few(array $names, int $n = 5): string
    {
        return implode(', ', array_slice($names, 0, $n)) . (count($names) > $n ? ', ...' : '');
    }

    private function _result(?string $tsvPath, array $scanTypes, array $visits, ?string $rosterError): array
    {
        if (!is_dir($this->deliveryDir)) {
            $this->_problem('error', 'project', $this->deliveryDir, 'the PNG delivery folder does not exist',
                'the site shares its images in <project>/deidentified-raw/imaging/png/');
        }

        $counts = ['visits' => count($visits), 'linked' => 0, 'unlinked' => 0, 'blocked' => 0,
            'images' => 0, 'set_aside' => 0];
        foreach ($visits as $v) {
            $counts[$v['link_status']] = ($counts[$v['link_status']] ?? 0) + 1;
            $counts['images']    += count($v['images']);
            $counts['set_aside'] += count($v['set_aside']);
        }

        return [
            'generated_at'     => date('c'),
            'delivery_dir'     => $this->deliveryDir,
            'participants_tsv' => $tsvPath,
            'participants_tsv_error' => $rosterError,
            'scan_types'       => $scanTypes,
            'visits'           => $visits,
            'problems'         => $this->problems,
            'counts'           => $counts,
        ];
    }

    /**
     * Advice for an unlinked visit, by reason (same wording style as DICOM).
     */
    public static function unlinkedAdvice(?string $reason, string $subject, string $session): string
    {
        $sub = "sub-{$subject}";
        return match ($reason) {
            self::NO_PARTICIPANTS_TSV       => "no participants.tsv in the delivery - add one with a row for {$sub}",
            self::PARTICIPANTS_TSV_UNUSABLE => 'participants.tsv cannot be used - see the error about it',
            self::NOT_IN_PARTICIPANTS_TSV   => "no row for {$sub} in participants.tsv - add it (participant_id, dob, sex)",
            self::NO_SESSIONS_TSV           => "no usable sessions file in {$sub}/ - add {$sub}/sessions.tsv with a "
                . "session_id column, one row per visit (e.g. ses-{$session})",
            self::NOT_IN_SESSIONS_TSV       => "ses-{$session} is not listed in {$sub}'s sessions.tsv - add a row "
                . "with session_id ses-{$session}",
            default                         => 'not matched to a participant and visit',
        } . '; until then it is reported only, nothing is ingested';
    }
}
