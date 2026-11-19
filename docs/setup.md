# Setup and configuration

Installation, the two configuration files (every key, including the mappings), the project
folder layout, permissions, logs, email, and the diagnostic scripts. How to run each
pipeline is in its own guide: [clinical](clinical.md), [DICOM](dicom.md), [PNG](png.md),
[BIDS](bids.md), [EviData](evidata.md).

---

## Contents

1. [Install](#1-install)
2. [`config/loris_client_config.json`](#2-configloris_client_configjson)
3. [Collections: which projects exist](#3-collections-which-projects-exist)
4. [`project.json`](#4-projectjson)
5. [Mappings and defaults: how participant values are read](#5-mappings-and-defaults-how-participant-values-are-read)
6. [Project folder layout](#6-project-folder-layout)
7. [Permissions](#7-permissions)
8. [Logs](#8-logs)
9. [Email](#9-email)
10. [Checks and diagnostics](#10-checks-and-diagnostics)
11. [Conventions shared by every pipeline](#11-conventions-shared-by-every-pipeline)

---

## 1. Install

```bash
cd /opt
git clone https://github.com/aces/archimedes-pipelines.git
cd archimedes-pipelines
composer install
cp config/loris_client_config.example.json config/loris_client_config.json && chmod 600 config/loris_client_config.json
php scripts/test/preflight_check.php        # server, install, config, LORIS, project paths: PASS / WARN / FAIL with fixes
```

Requirements: PHP ≥ 8.1 with `curl`, `json`, `mbstring`, `pdo`, `posix`; Composer;
MySQL/MariaDB client access (database fallback operations); a mail
transfer agent (postfix/sendmail) for email; `dcmtk` (`dcmdump`, `dcmodify`) for DICOM;
`mupdf-tools` for EviData report compression. Composer installs `aces/loris-php-api-client`,
Guzzle, Monolog and PHPMailer.

Run the pipelines as the pipeline user (e.g. `lorisadmin`), not root.

---

## 2. `config/loris_client_config.json`

One file per host. Copy the `.example`, `chmod 600` (it holds the API password).

| Key | Required | Meaning |
|---|---|---|
| `api.base_url`, `api.username`, `api.password` | yes | the LORIS instance and the pipeline API user |
| `api.api_version` | no (`v0.0.4-dev`) | LORIS API version |
| `api.token_expiry_minutes`, `api.timeout_seconds` | no (55, 600) | token refresh, request timeout |
| `verify_ssl` | yes | `true` in production |
| `timeout` | no (1800) | long-running script calls |
| `notification_defaults.default_on_success` / `default_on_error` / `default_on_evidata_failed` | yes | recipients when a project gives none (section 9) |
| `loris_profile` | no (`database_config.py`) | Loris-MRI profile name on the LORIS host (BIDS import) |
| `imaging.header_reader` | no (`dcmdump`) | DICOM header reader: `dcmdump`, `get_dicom_info` or `pydicom` |
| `imaging.dcmdump_path`, `imaging.dcmodify_path` | no (`/usr/bin/…`) | DCMTK tools |
| `imaging.python_bin`, `imaging.mri_code_path` | no | Loris-MRI Python / code, for `pydicom`, `get_dicom_info` and the BIDS pre-check's validator |
| `imaging.delivery_quiet_minutes` | no (5) | a DICOM or PNG delivery changed more recently than this is left for the next run (`0` = off) |
| `collections` | yes | section 3 |

---

## 3. Collections: which projects exist

A collection is a group of projects under one parent folder. **Only listed projects are
processed** — folders are never scanned — and each project is `<base_path>/<name>`.

```json
"collections": [
  { "name": "sftp", "base_path": "/data/A", "enabled": true,
    "projects": [ { "name": "project_x", "enabled": true } ] },
  { "name": "bic",  "base_path": "/data/A/B", "enabled": true,
    "projects": [ { "name": "cbig_project_1", "enabled": true },
                  { "name": "cbig_project_2", "enabled": false } ] }
]
```

| Key | Meaning |
|---|---|
| `name` | used in logs and `--collection=NAME` |
| `base_path` | parent folder of the project folders |
| `enabled` | `false` skips the whole collection |
| `projects[].name` / `.enabled` | a project folder under `base_path`; `false` skips it in `--all` / `--collection` runs |

Nested layouts work by declaring one collection per folder: `sftp` above does not pick up
`/data/A/B` even though it sits inside `/data/A`. A collection is also the way to run a
chosen set of projects together (`--collection=NAME`): list them in one.

---

## 4. `project.json`

One per project, at the project folder's root (copy `config/project.example.json`). Keys
starting with `_comment` are ignored.

| Key | Used by | Meaning |
|---|---|---|
| `project_full_name` | clinical | the LORIS project name, exact; corrects a mistyped project column; email subject |
| `project_common_name` | all | short name for logs and emails |
| `modalities` | DICOM, PNG, BIDS | must contain `"Imaging"` for the imaging pipelines to run the project (clinical ignores it) |
| `candidate_defaults` | all | values for missing columns (section 5) |
| `sex_mappings` | all | extra sex encodings (section 5) |
| `visit_mappings` | clinical | data visit value → LORIS visit (section 5) |
| `date_input_format` | all | how an ambiguous date like `03/04/1950` is read (section 5) |
| `visits` | DICOM, PNG, BIDS, pre-checks | the LORIS visit labels; optional (section 5) |
| `sites`, `cohorts` | pre-checks, PNG summary | expected values, reported when the data differs |
| `project_mappings` | BIDS, DICOM, PNG | LORIS project name → ProjectExternalID, for the legacy candidate path (section 5) |
| `row_tracking` | clinical | row identity columns (section 5) |
| `png.*` | PNG | scan types, preview pics, profile, polling — [png.md §7](png.md#7-settings) |
| `participant_metadata` | metadata pipeline | [clinical.md §10](clinical.md#10-participant-metadata-date-of-death) |
| `evidata` | EviData | [evidata.md](evidata.md) |
| `notification_emails` | all | section 9 |
| `bids_path` | BIDS import | a non-standard BIDS folder |
| `pi`, `tier`, `number_of_sites`, `guids`, `imaging_modalities`, `clinical_instruments`, … | — | informational only; not read |

---

## 5. Mappings and defaults: how participant values are read

The same rules apply in DICOM, PNG and BIDS (shared code); clinical applies the same
`candidate_defaults`, sex and visit mapping to its data files.

**Column names** in `participants.tsv` are read in any case and with these aliases:

| Read as | Accepted headers |
|---|---|
| `participant_id` | participant_id |
| `external_id` | external_id, externalid, extstudyid, ext_study_id, study_id, studyid |
| `dob` | dob, date_of_birth, birth_date |
| `sex` | sex, gender |
| `site` | site |
| `cohort` | cohort, group |
| `project` | project |

**`candidate_defaults`** — the configured value for a column:

```json
"candidate_defaults": { "project": "CNMDP", "site": "UOHI", "cohort": "Affected",
                        "sex": "Female", "dob": "1970-01-01" }
```

| Column | No column / blank cell | Same value, other case | Different value |
|---|---|---|---|
| `site`, `cohort`, `project` (imaging) | the default | the default's spelling | **replaced by the default**, with a note to correct the file |
| `site`, `cohort`, `project` (clinical) | the default | as written | site, cohort kept as written; project replaced by `project_full_name` (noted) |
| `sex`, `dob` | the default (keep `dob` a conservative placeholder, never a real date) | — | the file's value |

**`sex_mappings`** — built in: `m`, `male` → Male; `f`, `female` → Female; `o`, `other` →
Other (any case). Anything else must be mapped, or the participant is reported:

```json
"sex_mappings": { "1": "Male", "2": "Female", "u": "Other" }
```

`u`, `n/a`, `unknown` are **not** read as Other unless listed here.

**Dates** — `dob` is accepted as `YYYY-MM-DD`, `YYYY-MM`, `YYYY`, `M/D/YYYY`, `D/M/YYYY`.
`date_input_format` decides an ambiguous one:

```json
"date_input_format": "D/M/YYYY"
```

**Visits (imaging)** — the `ses-` folder name is the LORIS visit, used exactly:

| | Rule |
|---|---|
| `"visits": ["V01", "V02"]` (optional) | every `ses-` folder must be one of them exactly (case too) |
| no `"visits"` | the folder name is used as is; LORIS checks it when the session is created |
| DICOM, PNG | the folder must also be listed, exactly, in the participant's `sessions.tsv` (`session_id ses-V01`) |
| BIDS | no `sessions.tsv`; files carry the visit in their names and must match the folder |
| a mismatch | `VISIT MISMATCH`, with the rename to make; that visit is held back, the rest goes in |

`visit_mappings` is **not** applied to imaging visits (nothing is renamed on the way in);
it only suggests the rename (`ses-baseline` → "rename it ses-V01" when `baseline` maps to
`V01`).

**`visit_mappings` (clinical)** — the visit value in a data file (`redcap_event_name` or
`Visit_label`) → the LORIS visit; keys matched in any case; values not listed pass
unchanged:

```json
"visit_mappings": { "baseline_arm_1": "V01", "month_6_arm_1": "V02" }
```

**`project_mappings`** — LORIS project name → ProjectExternalID, used only when a candidate
has to be created through the older candidate endpoint (CandidatesPlus unavailable):

```json
"project_mappings": { "CNMDP": "7" }
```

The same value can be given as `candidate_defaults.project_external_id`.

**`row_tracking` (clinical)** — which columns identify a row, for row-level change
tracking. Defaults: identifiers `PSCID, CandID, participant_id, study_id, StudyID,
record_id`; visits `Visit_label, session_id, redcap_event_name`:

```json
"row_tracking": { "identifier_columns": ["subject_code"], "visit_columns": ["timepoint"] }
```

---

## 6. Project folder layout

```
{base_path}/{project}/
├── project.json
├── deidentified-raw/                 what the site delivers (read-only for the pipelines)
│   ├── clinical/  preclinical/       clinical CSV/TSV
│   ├── bids/                         BIDS dataset (phenotype/ → clinical)
│   ├── imaging/dicoms/               DICOM delivery
│   └── imaging/png/                  PNG delivery
├── documentation/data_dictionary/    instrument dictionaries (.linst, REDCap .csv, BIDS .json)
├── processed/                        pipeline working copies, manifests, tracking
├── deidentified-lorisid/             data relabelled with LORIS IDs (the only imaging source ingested)
│   ├── imaging/dicoms/  imaging/png/
│   └── bids/
└── logs/                             clinical/ preclinical/ dicom/ png/ bids/ evidata/ participant_metadata/ test/
```

`coded-raw/` and `coded-lorisid/` hold coded (not de-identified) copies kept by the data
team; the pipelines do not read them.

---

## 7. Permissions

Source files are never moved, renamed or deleted.

| Read-only for the pipelines | Read-write for the pipeline user |
|---|---|
| `deidentified-raw/` (all of it), `documentation/data_dictionary/` | `processed/`, `deidentified-lorisid/`, `logs/`, the project folder itself (lock files `.<modality>_pipeline.lock`) |

```bash
sudo chown -R <pipeline_user>:<group> PROJECT/processed PROJECT/deidentified-lorisid PROJECT/logs
sudo chmod -R 2775 PROJECT/processed PROJECT/deidentified-lorisid PROJECT/logs
```

A missing write bit fails quietly (tracking not saved, so work is redone every run):
check with `ls -ld` that the owner has `rwx`. On NFS, the export itself must be read-write
for the pipeline account (`root_squash` maps `sudo` to `nobody`). The imaging pipelines
check the mount and the project lock before each real run and stop when locks are not
enforced.

---

## 8. Logs

Per project, in `logs/<modality>/`:

```bash
tail -f PROJECT/logs/clinical/clinical_run_*.log
tail -f PROJECT/logs/dicom/dicom_bundle_*.log            # every step the DICOM bundle ran
cat     PROJECT/logs/dicom/dicom_import_errors_*.log     # only when there were errors
tail -f PROJECT/logs/png/png_bundle_*.log
tail -f PROJECT/logs/bids/bids_import_run_*.log
grep -r "ERROR" PROJECT/logs/
```

Pre-check reports: `logs/test/<redcap|png|bids>_validation/`, DICOM
`logs/dicom/dicom_validation_<date>.txt`.

---

## 9. Email

Per project in `project.json`, one block per pipeline:

```json
"notification_emails": {
  "clinical":             { "enabled": true, "on_success": ["team@x"], "on_error": ["admin@x"] },
  "dicom":                { "enabled": true, "on_success": ["team@x"], "on_error": ["admin@x"] },
  "png":                  { "enabled": true, "on_success": ["team@x"], "on_error": ["admin@x"] },
  "bids":                 { "enabled": true, "on_success": ["team@x"], "on_error": ["admin@x"] },
  "participant_metadata": { "enabled": true, "on_success": ["team@x"], "on_error": ["admin@x"] },
  "evidata":              { "enabled": true, "on_check_failed": ["privacy@x"] }
}
```

DICOM, PNG, BIDS and participant metadata send only when `enabled` is true and fall back
to `notification_defaults` for a missing list. Clinical ignores `enabled` and has no
fallback: a non-empty list sends. `evidata` has a single `on_check_failed` list.

---

## 10. Checks and diagnostics

All read-only.

| Script | Checks |
|---|---|
| `scripts/test/preflight_check.php` | server, install, config files and their values, EviData, LORIS login, LORIS folders, project paths — PASS / WARN / FAIL with the fix |
| `scripts/test/test_authentication.php` | LORIS API login with the configured user |
| `scripts/test/test_evidata_connection.php` | EviData token and API ([evidata.md](evidata.md)) |
| `scripts/test/test_mount_check.php <path>` | a mount answers in time |
| `scripts/test/test_cbigr_mapper.php` | the CBIGR ExternalID mapper |
| `scripts/test/validate_redcap.php` | clinical data before ingestion ([clinical.md §3](clinical.md#3-before-ingestion-the-pre-check)) |
| `scripts/dicom/run_dicom_validate.php` | a DICOM delivery ([dicom.md §2a](dicom.md#2a-checking-an-upload-before-ingestion)) |
| `scripts/test/validate_png.php` | a PNG delivery ([png.md §2a](png.md#2a-validating-a-share--scriptstestvalidate_pngphp)) |
| `scripts/test/validate_bids.php` | a BIDS dataset ([bids.md §3](bids.md#3-before-ingestion-the-pre-check)) |

---

## 11. Conventions shared by every pipeline

| | Clinical, participant metadata | DICOM, PNG, BIDS |
|---|---|---|
| default | **live**; `--dry-run` to preview | **dry run**; `--confirm` to execute (`--dry-run` wins) |
| selection | `--all` · `--collection=X` · `--collection=X --project=Y` | same |
| names | exact spelling | exact spelling (DICOM and PNG also accept another case) |
| several specific projects | one command per project, or list them in a collection | same |
| one run per project | yes (lock per modality) | yes (lock per modality) |
| unknown option | — | stops with exit `2` |
| re-run | only new or changed data is sent | same |
| after a crash | run the same command again | same |
| exit codes | `0` success, `1` errors | `0` success, `1` errors, `2` usage, `3` aborted (DICOM/PNG bundle data issues) |
