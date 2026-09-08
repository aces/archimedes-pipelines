# LORIS-ARCHIMEDES Pipelines

Automated data ingestion pipelines for ARCHIMEDES study using the [loris-php-api-client](https://github.com/aces/loris-php-api-client) library.

This repository provides production-ready pipelines for clinical and imaging data ingestion, with features like bulk CSV upload, automated candidate creation, email notifications, and comprehensive logging.

The pipelines are expected to be installed on the predefined data mount for the collection of projects, which follows a fixed directory structure, and each project must include its own `project.json` file.

---

## Features

- **Clinical Data Ingestion** - Automated CSV processing and upload
- **Clinical Instrument Install** - Install LINST, REDCap or BIDS instruments
- **Bulk Operations** - Process multiple files and projects
- **Email Notifications** - Success/failure reports via email
- **Comprehensive Logging** - Detailed execution logs with rotation
- **Dry Run Mode** - Test without making actual changes
- **Imaging Data Ingestion** - BIDS dataset ingestion
- **DICOM Import** - Archive and insert DICOM studies into LORIS tarchive tables
- **Participant Metadata Sync** - Update candidate demographic fields (Date of Death, Sex, DoB, etc.) in ARCHIMEDES
- **Multi-Project Support** - Handle multiple projects and collections

---

## Requirements

- PHP >= 8.1
- Composer
- [loris-php-api-client](https://github.com/aces/loris-php-api-client) (installed automatically)
- MySQL/MariaDB (for database fallback operations)
- Extensions: `curl`, `json`, `pdo`, `mbstring`

---

## Installation

```bash
cd /opt
git clone https://github.com/aces/archimedes-pipelines.git
cd archimedes-pipelines
composer install
```

This will automatically install:
- `aces/loris-php-api-client` - Auto-generated LORIS API client
- `guzzlehttp/guzzle` - HTTP client
- `monolog/monolog` - Logging
- `phpmailer/phpmailer` - Email notifications

---

## Configuration

### Main Configuration

Copy the example config file and edit your ARCHIMEDES credentials and collections:

```bash
cp config/loris_client_config.example.json config/loris_client_config.json
nano config/loris_client_config.json
```

### Project Configuration

Each project requires a `project.json` file at its root. See `config/project.example.json` for reference.

### EviData Configuration

EviData settings are split across two files: service/connection settings go in the **global** `config/evidata_config.json` (shared by every project on the host), and per-project policy goes in each **`project.json`**. The pipeline merges both, with project values taking precedence.

**Global — `config/evidata_config.json`** (the service connection, the same for all projects):

```json
"evidata": {
  "enabled": true,
  "api_base_url": "https://cbigr-docker.loris.ca/api",
  "token_url": "https://<keycloak-host>/.../token",
  "client_id": "<keycloak-client-id>",
  "client_secret_env": "EVIDATA_CLIENT_SECRET",
  "username_env": "EVIDATA_USERNAME",
  "password_env": "EVIDATA_PASSWORD",
  "population_size": 500000,
  "pdf_compression": "200",
  "mta_message_size_limit_mb": 10
}
```

**Per-project — `project.json`** (QI policy and who gets notified for this project):

```json
"evidata": {
  "qis": [],
  "exclude_qis": []
},
"notification_emails": {
  "evidata": {
    "enabled": true,
    "on_check_failed": ["team@example.com"]
  }
}
```

A few things worth knowing:

- Set `population_size` explicitly — the default does not reflect the true study population and skews the risk result.
- `mta_message_size_limit_mb` should match the mail host's limit (`postconf message_size_limit`, in MB) and is per-host. It controls when failure-report attachments get compressed before emailing.
- Leave `qis`/`exclude_qis` empty to use all columns as quasi-identifiers (the all-headers default), or list specific columns to override.
- Failure-report recipients use the `on_check_failed` key (not `on_success`/`on_error` like the other channels).

**Credentials (environment file).** Credentials are never stored in the JSON config. The config holds only the *names* of the environment variables (`client_secret_env`, `username_env`, `password_env`); the actual values come from an env file that the clinical runner loads automatically at startup — so cron does not need to `source` anything and a forgotten `source` cannot break a run.

Create the env file once (default location `/home/lorisadmin/evidata/evidata.env`):

```bash
# /home/lorisadmin/evidata/evidata.env
EVIDATA_CLIENT_SECRET=...
EVIDATA_USERNAME=...
EVIDATA_PASSWORD=...
```

The runner resolves the file path in this order: the `EVIDATA_ENV_FILE` environment variable, then an `env_file` key in `evidata_config.json`, then the default above. A real environment variable, if already set in the shell, always takes precedence over the file. If a variable is missing entirely the pipeline fails fast and names what it expected; values are never written to logs.

See `config/evidata_config.example.json` for the complete set of available keys and defaults.

---

## Clinical Ingestion Workflow

The clinical pipeline follows this process:

```
1. Load Collections from Config
   └── Read collections array from loris_client_config.json
       ├── Collection A
       │   ├── Project 1 (enabled)
       │   └── Project 2 (disabled)
       └── Collection B
           └── Project 1 (enabled)

2. For each enabled Collection:
   └── For each enabled Project:
       ├── Load project.json configuration
       ├── Check if modality (clinical) is enabled
       └── Continue to instrument processing

3. For each Instrument:
   ├── Check if instrument is installed in ARCHIMEDES
   ├── If NOT installed:
   │   ├── Look for Data Dictionary in documentation/data_dictionary/
   │   ├── Find .linst file OR REDCap data dictionary CSV
   │   └── Install instrument via API
   └── If installed:
       └── Continue to data ingestion

4. Data Ingestion:
   ├── Read CSV/TSV from deidentified-raw/clinical/ and bids/phenotype/
   ├── Compare the ORIGINAL file hash against .clinical_tracking.json
   │   └── Unchanged since last success → SKIPPED, no API call
   ├── Write an enriched copy to processed/clinical/
   │   └── Project / Cohort / Site stamped from project.json
   └── Upload the enriched copy (CREATE_SESSIONS)
       └── ARCHIMEDES resolves candidate + session; visit labels must pre-exist

5. Post-Processing (only on success):
   ├── Record the ORIGINAL file hash in .clinical_tracking.json
   ├── Copy the original to processed/clinical/YYYY-MM-DD/
   ├── Log results
   └── Send email notification (if enabled)
```

### Processed Copies and Change Detection

**The original is never modified, renamed, moved or deleted.** Everything the
pipeline writes is a copy under `processed/clinical/`.

| Copy | Path | Written | Notes |
|------|------|---------|-------|
| Enriched | `processed/clinical/<file>` | Before upload | What ARCHIMEDES actually receives. Overwritten each run. Not created if the file already carries Project/Cohort/Site — the original is then uploaded as-is. |
| Snapshot | `processed/clinical/YYYY-MM-DD/<file>` | After success | Dated copy of the original. Timestamp prefix on name clash. |

Tracking lives in `processed/clinical/.clinical_tracking.json`, keyed by filename.
The stored hash is MD5 of the **original**, never the enriched copy — so stamping
Project/Cohort/Site cannot make a file look changed.

| Situation | Result |
|-----------|--------|
| Hash matches last successful run | `SKIPPED - no changes`, no API call |
| Hash differs, or no entry yet | Ingested; ARCHIMEDES skips rows already present |
| Upload failed | No hash written → retried next run |
| Failed EviData check | Skipped before hashing → retried next run |
| `--force` | Hash check bypassed, everything re-uploaded |

**Candidates and visits are not created by the pipeline.** It makes no candidate
or visit API calls. The enriched copy goes to the instrument endpoint in
`CREATE_SESSIONS` mode and ARCHIMEDES resolves candidate and session server-side.
**Visit labels must already exist in ARCHIMEDES** — rows carrying a label that is not
configured for the project fail at upload.

### Collections Configuration

A **Collection** is a config-only grouping of projects that share one parent
folder, defined in `loris_client_config.json`.

| Key | Meaning |
|-----|---------|
| `name` | Label for logs and `--collection=NAME` |
| `base_path` | Parent folder holding the project directories |
| `enabled` | `false` skips the whole collection |
| `projects[]` | Projects to process; each found at `base_path/name` |

**Only the projects you list are processed.** There is no directory scanning, so
an unlisted folder under `base_path` is never touched — which means nested
layouts work by declaring one collection per folder:

```
/path/A/         → "sftp"   projects arriving via SFTP
/path/A/B/       → "bic"    projects from the BIC (mostly CBIG)
/path/A/ARCHI/   → "uohi"   projects directly from UOHI
```

```json
"collections": [
  { "name": "sftp", "base_path": "/path/A",       "enabled": true,
    "projects": [ {"name": "project_x", "enabled": true} ] },

  { "name": "bic",  "base_path": "/path/A/B",     "enabled": true,
    "projects": [ {"name": "cbig_project_1", "enabled": true},
                  {"name": "cbig_project_2", "enabled": false} ] },

  { "name": "uohi", "base_path": "/path/A/ARCHI", "enabled": true,
    "projects": [ {"name": "archi_project_1", "enabled": true} ] }
]
```

`sftp` does not pick up `/path/A/B` or `/path/A/ARCHI` even though they sit
inside `/path/A` — it only processes `project_x`, the one project it lists.

### Instrument Data Dictionary Location

All data dictionaries go in the project's `documentation/data_dictionary/`
folder — including the BIDS `.json` for a phenotype file. The format is detected
from the file extension alone; file contents are never inspected.

| Extension | Type sent as `instrument_type` |
|-----------|--------------------------------|
| `.linst` | `linst` |
| `.csv` | `redcap` |
| `.json` | `bids` |

A dictionary saved with the wrong extension is installed under the wrong type,
with no warning.

### BIDS Phenotype Data

Phenotype data is tabular clinical data, so the **clinical** pipeline ingests it,
not the BIDS imaging pipeline. Two locations are read:

| Location | Contents |
|----------|----------|
| `deidentified-raw/clinical/` | Clinical `.csv` / `.tsv` |
| `deidentified-raw/bids/phenotype/` | Phenotype `.tsv` data files |

**Data dictionaries are not read from `phenotype/`.** Every dictionary lives in
`documentation/data_dictionary/`, including the BIDS `.json` for a phenotype
file. A `.json` left beside the `.tsv` in `phenotype/` is ignored.

```
deidentified-raw/bids/phenotype/
└── moca.tsv                  # phenotype data

documentation/data_dictionary/
└── moca.json                 # its data dictionary
```

Filenames must be unique across `clinical/` and `bids/phenotype/` — tracking,
privacy artifacts and the processed copy are all keyed by filename.

### EviData Privacy-Risk Validation

EviData (by Woodway Assurance) provides automated privacy-risk validation for tabular clinical data. It applies only to the clinical pipeline; imaging, DICOM, and BIDS pipelines are not affected.

**Setup**

EviData runs as a separate service that the clinical pipeline calls over HTTP. Before running the pipeline against EviData, ensure:

- The EviData service is reachable at the host/port configured in your config file. Do not point production runs at a local install or personal dev VM; use the shared, provisioned EviData host.
- The EviData connection settings (base URL, company tag, population size, QI configuration) are set in config. Nothing is hardcoded — the population size in particular must be set explicitly in config, as the client default does not match study requirements.
- For emailed failure reports, set `mta_message_size_limit_mb` in the EviData config to match the mail host's `postconf message_size_limit` (value in MB). This is per-host: the dev VM and production targets may differ. When a failed-report attachment batch would exceed this limit, the pipeline compresses the report PDFs with `mutool` before sending; install it with `sudo apt install mupdf-tools`.

**Usage**

When EviData is enabled, each clinical file passes through the validation lifecycle (upload → validate → generate → poll → results → download report) as part of normal ingestion. Files that pass are ingested; files that fail the privacy check are skipped (not ingested) and retried on the next run. For failed files, a privacy-risk report is emailed to the configured recipients, with all report artifacts also preserved under the project's `logs/evidata/` directory.

When the combined report attachments exceed the configured mail size limit, the pipeline first compresses the whole batch, then falls back to shorter summary PDFs, and finally to a contact-the-team note if even those do not fit. If `mutool` is not installed, compression is skipped and the pipeline says so in the log before falling back.

QI (quasi-identifier) resolution follows this precedence: project `project.json` → global `evidata_config.json` → all-headers default.

---

## Running the Clinical Pipeline

### Dry Run Mode (Recommended First)

```bash
php scripts/run_clinical_pipeline.php --all --dry-run --verbose
```

### Process All Projects

```bash
php scripts/run_clinical_pipeline.php --all
```

### Process Specific Project

```bash
php scripts/run_clinical_pipeline.php --collection=COLLECTION_NAME --project=PROJECT_NAME
```

### Process Specific Instrument

```bash
php scripts/run_clinical_pipeline.php --collection=COLLECTION_NAME --project=PROJECT_NAME --instrument=INSTRUMENT_NAME
```

---

## Command-Line Options

| Option | Description |
|--------|-------------|
| `--all` | Process all projects |
| `--collection=NAME` | Specific collection |
| `--project=NAME` | Specific project |
| `--instrument=NAME` | Specific instrument |
| `--dry-run` | Test without changes |
| `--force` | Bypass hash check — re-upload all files even if unchanged |
| `--verbose` | Detailed output |
| `--help` | Show help |

---

## BIDS/Imaging Ingestion Workflow

The BIDS pipeline automates candidate creation, reidentification, and imaging import in three distinct steps:

```
1. Load Collections from Config
   └── Read collections array from loris_client_config.json
       ├── Collection A
       │   ├── Project 1 (enabled)
       │   └── Project 2 (disabled)
       └── Collection B
           └── Project 1 (enabled)

2. For each enabled Collection:
   └── For each enabled Project:
       ├── Load project.json configuration
       ├── Check if modality (imaging) is enabled
       ├── Setup logging to project logs/bids_*_YYYY-MM-DD.log
       └── Continue to BIDS processing

3. STEP 1: Participant Sync (Create ARCHIMEDES Candidates)
   ├── Script: run_bids_participant_sync.php
   ├── Read deidentified-raw/bids/participants.tsv
   ├── Validate BIDS structure (orphan/missing directories)
   ├── Check if candidate exists (CBIGR mapper)
   ├── Create candidate (ARCHIMEDES API)
   ├── Link ExternalID to candidate 
   └── Log to logs/bids_participant_sync_YYYY-MM-DD.log

4. STEP 2: BIDS Reidentification (Map ExternalID → PSCID)
   ├── Script: run_bids_reidentifier.php
   ├── Extract ID pattern from participants.tsv
   ├── Execute CBIGR Script API: bidsreidentifier
   ├── Rename: sub-{ExternalID} → sub-{PSCID}
   ├── Copy to deidentified-lorisid/bids/
   └── Log to logs/bids_reidentifier_YYYY-MM-DD.log

5. STEP 3: BIDS Import (Ingest into LORIS-MRI)
   ├── Script: run_imaging_pipeline.php
   ├── Scan deidentified-lorisid/bids/ for new sessions
   ├── Execute CBIGR Script API: bidsimport
   ├── Mark sessions as processed
   ├── Log to logs/imaging_YYYY-MM-DD.log
   └── Send email notification (if enabled)
```

### Required participants.tsv Format

Participant metadata must be in BIDS `participants.tsv` file with required columns:

```tsv
participant_id	age	sex	group	external_id	site	dob
sub-EXTERNAL001	28	Female	control	EXTERNAL-001	UOHI	1995-01-15
sub-EXTERNAL002	34	Male	patient	EXTERNAL-002	UOHI	1989-06-20
```

**Required columns:**
- `participant_id` - BIDS subject ID (e.g., sub-EXTERNAL001)
- `external_id` - External study identifier
- `sex` - Male/Female (required by ARCHIMEDES)
- `site` - ARCHIMEDES site name (must match database)
- `dob` - Date of birth in YYYY-MM-DD format
- `project` - ARCHIMEDES project name (optional if in project.json)

---

## Running the BIDS Pipeline

### Step 1: Participant Sync

Create ARCHIMEDES candidates and link ExternalIDs.
Source/target directories are resolved automatically from `project.json → data_access.mount_path`.

```bash
# Dry run (recommended first)
php scripts/run_bids_participant_sync.php \
    --collection=COLLECTION --project=PROJECT --dry-run -v

# Live run
php scripts/run_bids_participant_sync.php \
    --collection=COLLECTION --project=PROJECT --confirm

# All projects
php scripts/run_bids_participant_sync.php --all --confirm
```

### Step 2: BIDS Reidentification

Map ExternalIDs to PSCIDs and copy to `deidentified-lorisid/bids/`.
Source/target directories are resolved automatically from `project.json → data_access.mount_path`.

```bash
# Dry run (recommended first)
php scripts/run_bids_reidentifier.php \
    --collection=COLLECTION --project=PROJECT --dry-run -v

# Live run
php scripts/run_bids_reidentifier.php \
    --collection=COLLECTION --project=PROJECT --confirm

# Force overwrite existing target directory
php scripts/run_bids_reidentifier.php \
    --collection=COLLECTION --project=PROJECT --confirm --force

# All projects
php scripts/run_bids_reidentifier.php --all --confirm
```

### Step 3: BIDS Import

Import BIDS imaging data into LORIS-MRI.
Source directory is resolved automatically from `project.json → data_access.mount_path`.
Skips automatically if already successfully imported (tracked per project).


```bash
# Dry run (recommended first)
php scripts/run_bids_import_pipeline.php \
    --collection=COLLECTION --project=PROJECT --dry-run -v

# Live run
php scripts/run_bids_import_pipeline.php \
    --collection=COLLECTION --project=PROJECT --confirm

# All projects
php scripts/run_bids_import_pipeline.php --all --confirm
```

---

## Command-Line Options (BIDS)

### Participant Sync (Step 1)

| Option | Description |
|--------|-------------|
| `--all` | Process all projects |
| `--collection=NAME` | Specific collection |
| `--project=NAME` | Specific project (requires `--collection`) |
| `--confirm` | Execute live run (default is dry run) |
| `--dry-run` | Test without changes |
| `-v, --verbose` | Detailed output |
| `--help` | Show help |

### BIDS Reidentifier (Step 2)

| Option | Description |
|--------|-------------|
| `--all` | Process all projects |
| `--collection=NAME` | Specific collection |
| `--project=NAME` | Specific project (requires `--collection`) |
| `--confirm` | Execute live run (default is dry run) |
| `--dry-run` | Test without execution |
| `--force` | Delete and overwrite existing target directory |
| `-v, --verbose` | Detailed output |
| `--help` | Show help |

### BIDS Import (Step 3)

| Option | Description |
|--------|-------------|
| `--all` | Process all projects |
| `--collection=NAME` | Specific collection |
| `--project=NAME` | Specific project (requires `--collection`) |
| `--confirm` | Execute live run (default is dry run) |
| `--dry-run` | Test without changes |
| `--no-validate` | Skip BIDS validation |
| `-v, --verbose` | Detailed output |
| `--help` | Show help |

---

## DICOM Ingestion Workflow

Takes a DICOM delivery, regroups it into studies, matches those studies to LORIS
candidates, and hands the result to the importer. Four scripts and a bundle that
runs them in order.

```
run_all_dicom_scripts_bundle.php     bundle — runs the four below
  ├─ run_dicom_organize.php          EviData check + group by StudyInstanceUID
  ├─ run_dicom_participant_sync.php  create candidates
  ├─ run_dicom_reidentifier.php      LORIS IDs onto folders and headers
  └─ run_dicom_import.php            archive into LORIS   ← only write to LORIS
```

| Step | Writes to | Talks to LORIS |
|---|---|---|
| organise | `processed/imaging/` | no |
| participant sync | nothing on disk | yes — creates candidates |
| relabel | `deidentified-lorisid/` | yes — reads and creates sessions |
| import | LORIS imaging tables | **yes — the import** |

The first three are reversible: delete their output and start again, nothing has
reached the imaging tables. The import is the point of no return, and the bundle
asks before it.

Dry run is the default everywhere. Every dry run prints the exact `--confirm`
command to copy.

**A delivery does not have to be well-formed.** Grouping comes from the headers,
so it works with no `sub-`/`ses-` folders and no `participants.tsv`. What is
missing only affects whether a study can be matched to a person:

| Delivery | Result |
|---|---|
| Organised, `participants.tsv` present | `linked` — candidates created, folders and headers relabelled |
| No `participants.tsv`, or subject not in it | `unlinked` — organised and importable, warning per study, no LORIS ID |
| Not organised into `sub-`/`ses-` folders | `unlinked` — grouped by header UID, folder name generated |

Nothing is dropped. Unlinked studies still archive, with a NULL `SessionID` and
the site's identifier left in `PatientName`. The participant sync and relabel
steps are skipped by name when nothing can be linked, and the bundle stops and
asks before importing them:

```
NOT REIDENTIFIED: 3 of 3 study/studies
  3 unlinked - no LORIS ID could be resolved
Ingest them anyway, with no linked LORIS ID? [y/N]:
```

| Section | |
|---|---|
| [What one run does](#what-one-run-does) | The four steps, start to finish |
| [Why regroup](#why-regroup-a-delivery) · [Organise](#organise) · [Relabel](#relabel) | The idea behind each |
| [Where a study travels](#where-a-study-travels) | Which directory the import reads |
| [Link status](#link-status) · [EviData](#evidata-check-optional) | linked/unlinked, privacy check |
| [Hashing and tracking](#hashing-and-tracking) | Why a repeat run is cheap, and what state is kept |
| [Scripts and usage](#scripts-and-usage) | Every script, every flag, examples |
| [Layout](#layout) · [Config](#config) | Paths and settings |

---

### How it works

#### What one run does

```bash
php scripts/run_all_dicom_scripts_bundle.php \
    --collection=archimedes --project=FDG-PET --confirm -v
```

**Before anything.** The delivery is stat-ed — sizes and paths, nothing opened —
and compared to the last successful run. Unchanged means the project is skipped
in about a second.

**Step 1 — organise.** Every file's headers are read, files grouped by
`StudyInstanceUID` then `SeriesInstanceUID`, one folder per study written to
`processed/imaging/dicoms/`. Grouping comes from headers, so it works whatever
the site sent. Each study is marked `linked`, `unlinked` or `phantom`. Nothing
is dropped. (If EviData is enabled it runs first, on the raw delivery,
read-only.)

**Step 2 — candidates.** Only for `linked` studies. Reads `participants.tsv`,
creates missing candidates, links their ExternalIDs. Skipped entirely when
nothing is linked.

**Step 3 — relabel.** Also only for `linked` studies. Copies to
`deidentified-lorisid/imaging/dicoms/` as `PSCID_CandID_Visit_MODALITIES` and
rewrites `PatientName`, moving the site's identifier to `OtherPatientNames`.

**Then it asks.** If anything is unlinked, the bundle stops before the import:

```
NOT REIDENTIFIED: 3 of 3 study/studies
Ingest them anyway, with no linked LORIS ID? [y/N]:
```

Everything above is reversible — delete the output and start again. The import
is not.

**Step 4 — import.** Reads from wherever each study ended up:
`deidentified-lorisid/` for relabelled studies, `processed/` for unlinked ones.
Runs once per directory that has studies.

| Delivery | What happens |
|---|---|
| With `participants.tsv` | All four steps; import from `deidentified-lorisid/` |
| Without it | Organise, skip 2 and 3, ask, import from `processed/` |
| Some subjects missing from it | All four steps; import runs twice, one directory each |

---

#### Why regroup a delivery

A DICOM study is identified by `StudyInstanceUID` inside every file's header —
not by the folder a site put it in. The two do not have to agree, and often
don't:

| What arrives | What it actually is |
|---|---|
| One folder, several UIDs | Several studies in one folder |
| Several folders, one UID | One study split up |
| A flat pile of files | No grouping at all |
| One folder, one UID | Already correct — most common |

`import_dicom_study.py` takes **one directory = one study**. Give it a folder
holding two UIDs and the archive is wrong; split one UID across two folders and
you import the same study twice.

A PET/CT is the case that makes this concrete. The PET series and the CT used to
correct it share **one** `StudyInstanceUID`. They are one study, archived as one
unit, and must end up in one folder even though the scanner may have written
them separately.

So every file's headers are read, files are grouped by UID, and one folder per
study is written. When a delivery was already correct this is just a copy — the
check is cheap and the alternative is trusting folder layout you did not create.

---

#### Organise

Headers are read with `dcmdump`, grouped by `StudyInstanceUID`, then by
`SeriesInstanceUID`. Modality is recorded, never used to decide grouping.

The delivered folder name is kept when a study came from a single folder — that
name is the site's identifier for it. Names are generated only for flat dumps
or studies split across folders.

```
deidentified-raw/imaging/dicoms/TST02_ROM_00000001_02_SE01_MR/
processed/imaging/dicoms/TST02_ROM_00000001_02_SE01_MR/
    ├── series-0001_MR_T1w-MPRAGE/
    ├── series-0002_PT_AC-3D/
    └── series-0003_CT_LowDoseCT/
```

`sub-`/`ses-` folders are read for subject and visit, because the visit label
exists only in the path — never in the headers.

A study that gains a series between deliveries changes folder name
(`..._PT` → `..._CT-PT`). The old folder is not removed.

---

#### Relabel

Copies to `deidentified-lorisid/` as `PSCID_CandID_Visit_MODALITIES`, then
rewrites with `dcmodify`:

| Tag | Before | After |
|---|---|---|
| `(0010,0010)` PatientName | `ARCHI0001` | `QPN0000474_718905_V01` |
| `(0010,1001)` OtherPatientNames | — | `ARCHI0001` |

Numeric tags, not keywords. `-nb` always, or `dcmodify` leaves a `.bak` beside
every file and they end up in the tarchive. Originals are never modified.

---

#### Where a study travels

```
deidentified-raw/imaging/dicoms/     as delivered
        │  organise: regroup by StudyInstanceUID
        ▼
processed/imaging/dicoms/            one folder per study
        │
        ├── can be linked? ──yes──►  relabel: LORIS IDs on folder + headers
        │                                    │
        │                                    ▼
        │                            deidentified-lorisid/imaging/dicoms/
        │                                    │
        └── no ──────────────────────────────┤
                                             ▼
                                        import → tarchive
```

So the import reads from `deidentified-lorisid/` when a study was relabelled,
and from `processed/` when it could not be. Never from `deidentified-raw/`:

| Study | Import from |
|---|---|
| `linked` | `deidentified-lorisid/imaging/dicoms/` — relabelled folder and headers |
| `unlinked` or `phantom` | `processed/imaging/dicoms/` — grouped, not relabelled |

Importing from `deidentified-raw/` would archive the original files with the
site identifier still in `PatientName`, silently discarding the relabelling —
and the files are still in whatever grouping the site sent, so a folder holding
two `StudyInstanceUID`s archives as one wrong study.

**The bundle handles this.** After relabelling it reads the manifest and invokes
the importer once per source that has studies, passing `--source-subdir`:

```
importing 2 relabelled from deidentified-lorisid/imaging/dicoms
importing 1 unlinked/phantom from processed/imaging/dicoms
```

A delivery where nothing could be linked imports once, from `processed/`.

Running the importer by hand needs the flag given explicitly — it still defaults
to `deidentified-raw/imaging/dicoms` so existing callers are unaffected:

```bash
php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET \
    --source-subdir=deidentified-lorisid/imaging/dicoms --confirm -v
```

---

#### Link status

| State | | In LORIS |
|---|---|---|
| `linked` | Matched to a candidate and visit | Normal session |
| `unlinked` | Nobody to match it to | Archived, no session |
| `phantom` | Test object, not a person | Attached to the scanner |

Phantom sets a LORIS flag meant for test objects — wrong, and awkward to undo,
on a patient scan. Unlinked just means we do not know whose it is.

Unlinked studies are still organised and importable. Reason recorded as
`not_organised`, `no_session_folder`, `no_participants_tsv`,
`not_in_participants_tsv`, `visit_not_configured` or `declared`.

Status is **not** in the folder name when the delivered name is preserved. Read
it from the manifest or the per-study `.provenance.json`:

```bash
jq -r '.studies[] | "\(.link_status)\t\(.link_reason // "-")\t\(.directory_name)"' \
  PROJECT/processed/imaging/dicom_studies.json
```

---

#### EviData check (optional)

Estimates how identifiable a dataset is. Reads headers, writes a CSV — one row
per study, one column per header field. Changes nothing.

Off unless all three are true:

| Setting | Where |
|---|---|
| `evidata.enabled` | `config/evidata_config.json` |
| `evidata.enabled` not false | `project.json` |
| `evidata.imaging.enabled` true | `project.json` |

Enabling it for clinical does not enable it for DICOM. When on it is a gate: if
the check cannot be produced, the project is skipped and nothing imported.

---

#### Hashing and tracking

Three files record what has been done. They answer different questions and are
written at different points, so they can disagree — and when they do, the
disagreement is the useful information.

| File | Answers | Written by |
|---|---|---|
| `processed/imaging/.dicom_delivery_state.json` | Has this delivery changed since we last organised it? | organise, on success |
| `processed/imaging/.dicom_organize_tracking.json` | Which studies are organised, and did their bytes change? | organise, per study |
| `processed/imaging/.dicom_import_processed.json` | Which studies reached LORIS? | import |

A study organised but not imported shows in the second and not the third. That
is a normal state after answering `n` at the prompt, or after an import failure.

##### What is hashed, exactly

A DICOM study is thousands of files, so hashing all of them on every run is not
viable. Hence two tiers, and what each covers:

| | Manifest hash | Content hash |
|---|---|---|
| Input | relative path + byte size of every file | the bytes of every file |
| Method | sorted list, SHA-256 of the list | SHA-256 per file, digests sorted, SHA-256 of those |
| Files opened | none | all |
| Detects | files added, removed, resized | any byte change |
| Misses | a file edited in place at the same size | nothing |
| Run | every time | only when the manifest differs |

Sorting the per-file digests rather than the paths is what makes a rename
invisible: the same bytes under a different filename produce the same content
hash, so a re-copied study is not reprocessed.

Paths are stored relative to the delivery root, so moving the whole delivery
does not invalidate anything.

Note the consequence of the delivery check being size-based: a file edited in
place at the same size stops the run before the per-study content hash ever
runs, so it is not caught at either level. DICOM files are not normally edited
in place.

##### Two levels of checking

**Whole delivery, first.** The source tree is manifest-hashed and compared to
the last successful run. Unchanged skips the project entirely — before the
EviData scan, before any header is read:

```
UNCHANGED since 2026-09-08T16:30:06+00:00 - 6540 file(s), nothing to re-ingest
Pass --force to re-organise anyway.
```

**Per study**, only if the delivery changed:

| Comparison | Action |
|---|---|
| Manifest matches | Skip |
| Manifest differs, content matches | Skip — renamed or re-copied; refresh the manifest hash |
| Content differs | Re-organise |
| No stored hash | Record a baseline, skip |

The baseline row matters once: the first run after this ships finds no hashes on
studies already imported, and records them rather than reprocessing everything.

##### Rules

Hashes are written **only after a successful run**, so a stored fingerprint
always means "this was organised", never "we looked at it". A dry run, or a run
with errors, leaves the state alone and the next run repeats the work.

`--force` bypasses both levels. Deleting the state files has the same effect and
is the way to start genuinely clean:

```bash
rm PROJECT/processed/imaging/.dicom_delivery_state.json
rm PROJECT/processed/imaging/.dicom_organize_tracking.json
```

The import keeps its own tracking, so clearing the two above re-organises but
does not re-import. Clearing the LORIS database means clearing
`.dicom_import_processed.json` too, or nothing re-ingests.

---

### Scripts and usage

#### Options common to every script

| Option | |
|---|---|
| `--all` | All enabled collections and projects |
| `--collection=NAME` | One collection |
| `--project=NAME` | One project (needs `--collection`) |
| `--confirm` | Execute |
| `--dry-run` | Explicit dry run; wins if both given |
| `--force` | Ignore tracking and hashes |
| `--config=FILE` | Default `config/loris_client_config.json` |
| `-v` | Verbose |
| `--help` | Usage |

Dry run is the default everywhere; each one prints the exact `--confirm`
command to copy. Names match case-insensitively; a name matching nothing lists
what exists.

Exit: `0` ok · `1` usage · `2` failure · `3` nothing found or declined ·
`4` finished with failures.

#### run_all_dicom_scripts_bundle.php — the bundle

| Option | |
|---|---|
| `--yes` | Auto-answer the pre-import prompt. Required off a terminal. |
| `--no-unlinked` | Refuse unrelabelled studies instead of asking |
| `--steps=LIST` | Only these: `organize,sync,reidentify,import` |
| `--skip=LIST` | All except these |
| `--no-organize` … `--no-import` | Shorthand for one `--skip` |
| `--stop-after=STEP` | `organize` \| `sync` \| `reidentify` |

```bash
B="php scripts/run_all_dicom_scripts_bundle.php --collection=archimedes --project=FDG-PET"

$B -v                          # preview
$B --confirm -v                # run
$B --confirm --no-import       # prepare, import nothing
$B --confirm --skip=organize   # reuse existing manifest
$B --confirm --force -v        # redo everything

php scripts/run_all_dicom_scripts_bundle.php --all --confirm --yes   # cron
```

Sequences the four steps, stops a project at the first failure, keeps a master
log at `logs/dicom/dicom_bundle_<date>.log` with a copy of every step's output.
Steps are subprocesses, so each stays independently runnable.

#### run_dicom_organize.php

Writes `processed/imaging/`. No LORIS calls.

| Option | |
|---|---|
| `--move` | Move instead of copy (default: copy) |
| `--phantom` | Treat the whole run as test-object scans |
| `--no-unlinked` | Skip unlinkable studies instead of carrying them forward |
| `--evidata` / `--no-evidata` | Force the privacy check on / off |
| `--strict` | Stop at the first problem |

```bash
O="php scripts/run_dicom_organize.php --collection=archimedes --project=FDG-PET"

$O -v                    # preview
$O --confirm -v          # organise
$O --evidata -v          # privacy check only, writes nothing else
$O --confirm --force     # ignore hashes
$O --confirm --phantom   # test scans
```

Exit `3` = no readable DICOMs. Exit `4` = studies failed a check, including
"most of the delivery was unreadable", which fails rather than reporting
success on incomplete studies.

#### run_dicom_participant_sync.php

Creates candidates. Writes nothing to disk.

| Option | |
|---|---|
| `--all-participants` | Every TSV row, not just subjects with imaging |

```bash
S="php scripts/run_dicom_participant_sync.php --collection=archimedes --project=FDG-PET"

$S -v          # who exists already
$S --confirm
```

Reports folders with no `participants.tsv` row and rows with no folder, before
creating anything. Neither is fatal.

#### run_dicom_reidentifier.php

Writes `deidentified-lorisid/`. Rewrites headers.

| Option | |
|---|---|
| `--create-candidates` | Create unresolved candidates here (off by default — sync owns it) |

```bash
R="php scripts/run_dicom_reidentifier.php --collection=archimedes --project=FDG-PET"

$R -v
$R --confirm -v
```

#### run_dicom_import.php

The only step that writes to LORIS. Scans a directory for study folders and
calls `POST /cbigr_api/script/importdicomstudy` for each one, which archives the
DICOMs into a `.tar.gz`, computes MD5 checksums and inserts or updates the
tarchive record.

Each study is classified `SUCCESS`, `ALREADY_EXISTS` (not an error) or `FAILED`,
and recorded in `.dicom_import_processed.json` so it is not re-imported.
Previously failed studies are retried automatically; successful ones need
`--force`.

| Option | |
|---|---|
| `--update` | `--update` instead of `--insert` |
| `--session` | Associate with a LORIS session |
| `--overwrite` | Overwrite existing archive files |
| `--source-subdir=PATH` | Directory to scan, relative to the project root. Default `deidentified-raw/imaging/dicoms`. |
| `--profile=NAME` | Python config file. Default `database_config.py`. |

```bash
I="php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET"

$I --confirm -v                                                # default source
$I --confirm --source-subdir=deidentified-lorisid/imaging/dicoms -v
$I --confirm --update --session --overwrite -v
$I --confirm --force -v                                        # re-import
```

Run through the bundle and `--source-subdir` is set for you. Run it alone and it
defaults to the raw delivery, which is only correct if the delivery was already
one folder per study.

---

### Layout

```
PROJECT/
├── deidentified-raw/imaging/dicoms/   as delivered — never modified
├── processed/imaging/                 organise output + state + manifest
├── deidentified-lorisid/imaging/      relabel output + provenance
└── logs/dicom/                        per-step logs + bundle master log
```

```bash
M=PROJECT/processed/imaging/dicom_studies.json

jq -r '.studies[].series[].modality' $M | sort | uniq -c   # modalities present
jq -r '.problems[] | "\(.level)\t\(.message)"' $M          # warnings and errors
tail -f PROJECT/logs/dicom/dicom_bundle_$(date +%F).log    # whole run
```

---

### Config

`loris_client_config.json`:

```json
"imaging": {
    "header_reader": "dcmdump",
    "dcmdump_path": "/usr/bin/dcmdump",
    "dcmodify_path": "/usr/bin/dcmodify"
}
```

`project.json`:

```json
"candidate_defaults": {
    "site": "University of Ottawa Heart Institute (UOHI)",
    "cohort": "Control",
    "project": "FDG PET",
},
"evidata": { "enabled": true, "imaging": { "enabled": true } }
```

`candidate_defaults.project` must match the LORIS project name exactly —
`"FDG PET"`, not the folder name `"FDG-PET"`.

`participants.tsv`: sites send `participant_id`, `dob`, `sex`. The rest comes
from `candidate_defaults`; `external_id` defaults to `participant_id` minus
`sub-`.

Requires PHP >= 8.1 and DCMTK (`apt install dcmtk`).

---

## Participant Metadata Workflow

The participant metadata pipeline updates candidate demographic fields in ARCHIMEDES (Date of Death today; extensible to Sex, DoB, and other fields) from BIDS, clinical, and custom-configured source files. Each source's MD5 is tracked so unchanged files are skipped on subsequent runs.

```
1. Load Collections from Config
   └── Read collections array from loris_client_config.json
       ├── Collection A
       │   ├── Project 1 (enabled)
       │   └── Project 2 (disabled)
       └── Collection B
           └── Project 1 (enabled)

2. For each enabled Collection:
   └── For each enabled Project:
       ├── Load project.json configuration
       ├── Check if participant_metadata is enabled
       ├── Load tracking file (.participant_metadata_tracking.json)
       └── Continue to source processing

3. Build Candidate Index (one GET per project):
   ├── GET /cbigr_api/candidatesPlus
   ├── Index candidates by PSCID
   └── Index candidates by every registered ExtStudyID

4. Per-Source Processing:
   ├── Default: deidentified-raw/bids/participants.tsv
   ├── Default: every *.csv under deidentified-raw/clinical/
   ├── Custom: each entry in participant_metadata.sources
   ├── Hash check (md5_file) - skip if unchanged (unless --force)
   ├── For each row:
   │   ├── Resolve identifier to CandID via lookup index
   │   ├── Extract configured fields (DoD today, etc.)
   │   ├── Skip duplicates and idempotent values
   │   └── PUT /cbigr_api/candidatesPlus?CandID=X with changed fields
   └── Update tracking on clean source runs

5. Post-Processing:
   ├── Write project summary to run log
   ├── Persist tracking file with new hashes
   ├── Send email notification (if enabled)
   └── Return exit code (0 = success, 1 = any unresolved or failures)
```

### project.json Configuration

Each source declares which ARCHIMEDES fields to update via a `fields` map (ARCHIMEDES field name → list of source column names; first non-empty wins):

```json
"participant_metadata": {
  "enabled": true,
  "defaults": {
    "bids": {
      "identifier_field": "participant_id",
      "identifier_type": "PSCID",
      "identifier_strip_prefix": "sub-",
      "fields": { "DoD": ["date_of_death", "dod"] }
    },
    "clinical": {
      "identifier_field": "external_id",
      "identifier_type": "ExtStudyID",
      "fields": { "DoD": ["DoD", "DateOfDeath", "date_of_death"] }
    }
  },
  "sources": [
    {
      "path": "deidentified-raw/registry/weekly_deaths.csv",
      "identifier_field": "external_id",
      "identifier_type": "ExtStudyID",
      "fields": { "DoD": ["dod"] }
    }
  ]
}
```

Future fields (Sex, DoB, etc.) are added by extending the endpoint and adding keys to `fields` - no pipeline code change required.

### Tracking File

The pipeline maintains a `.participant_metadata_tracking.json` file under `processed/participant_metadata/` to track source-file hashes. Sources unchanged since the last run are skipped. Use `--force` to override and reprocess all sources.

---

## Running the Participant Metadata Pipeline

### Dry Run Mode (Recommended First)

```bash
# All enabled projects
php scripts/run_participant_metadata_pipeline.php --all --dry-run --verbose

# All projects in a collection
php scripts/run_participant_metadata_pipeline.php --collection=archimedes --dry-run --verbose

# Single project
php scripts/run_participant_metadata_pipeline.php --collection=archimedes --project=FDG-PET --dry-run --verbose
```

### Execute (Live Run)

```bash
# All projects
php scripts/run_participant_metadata_pipeline.php --all

# Single collection
php scripts/run_participant_metadata_pipeline.php --collection=archimedes

# Single project
php scripts/run_participant_metadata_pipeline.php --collection=archimedes --project=FDG-PET
```

### Force Reprocess

```bash
# Reprocess all sources (ignore hash tracking)
php scripts/run_participant_metadata_pipeline.php --collection=archimedes --project=FDG-PET --force

# Force across all projects
php scripts/run_participant_metadata_pipeline.php --all --force
```

---

## Command-Line Options (Participant Metadata)

| Option | Description |
|--------|-------------|
| `--all` | Process all enabled collections & projects |
| `--collection=NAME` | Process all enabled projects in a collection |
| `--project=NAME` | Process a specific project (requires `--collection`) |
| `--dry-run` | Test without making PUT calls |
| `--force` | Bypass hash check - reprocess every source file |
| `--verbose` | Per-row diagnostics |
| `--help` | Show help |

---

## Directory Structure

```
{collection_base_path}/{ProjectName}/
├── project.json                          # Project configuration
│
├── coded-raw/                            # coded raw participant data
│   ├── clinical/                         # coded raw Patient records & clinical assessment in csv/tsv
│   ├── imaging/
│   │   └── dicoms/                       # coded raw DICOM studies (one folder per study)
│   ├── bids/                             # coded raw MRI and EEG Data (ExternalIDs)
│   └── genomics/
│
├──coded-lorisid/                         # LORIS-relabelled coded data
│   ├── clinical/                         #  coded Patient records & clinical assessment in csv/tsv
│   ├── imaging/
│   │   └── dicoms/                       # coded  DICOM studies (one folder per study)
│   ├── bids/                             # coded MRI and EEG Data (ExternalIDs)
│   └── genomics/
│
├── deidentified-raw/                     # De-identified participant data
│   ├── clinical/                         # Raw Patient records & clinical assessment in csv/tsv
│   ├── imaging/
│   │   └── dicoms/                       # Raw DICOM studies (one folder per study)
│   ├── bids/                             # Deidentified MRI and EEG Data (ExternalIDs)
│   │   └── phenotype/                    # BIDS phenotype .tsv (read by clinical pipeline)
│   └── genomics/
│
├── deidentified-lorisid/                 # LORIS-relabelled deidentified data
│   ├── imaging/
│   │   └── dicoms/                       # DICOM data with LORIS IDs
│   ├── bids/                             # Reidentified MRI and EEG Data with LORIS IDs
│   └── genomics/
│
├── processed/                            # Data after CBIG/LORIS processing
│   ├── clinical/
│   ├── imaging/
│   ├── bids/         
│   │   └── derivatives/
│   ├── participant_metadata/             # Participant metadata hash tracking
│   └── freesurfer-output/                # Converted & cleaned data (NIfTI, MINC)
│
├── logs/                                 # Execution logs
│   ├── clinical/                         # Clinical pipeline logs
│   ├── dicom/                            # DICOM import pipeline logs
│   └── participant_metadata/             # Participant metadata pipeline logs
│
└── documentation/
    ├── data_dictionary/                  # Instrument Data Dictionary (.linst, REDCap CSV)
    └── readme.txt
```

### Directory Permissions

Source files are never moved, renamed or deleted — what lands in `processed/` is always a copy. The pipeline treats the de-identified input directories as **read-only** and writes only to its own output and log trees. Getting these permissions right matters: a missing write bit causes silent failures (tracking not persisted, snapshots not archived, EviData artifacts not saved) while reads still succeed, so the run appears to work but loses state between runs.

**Read-only (the pipeline never writes here):**
- `deidentified-raw/` and its subdirectories (`clinical/`, `bids/`, `bids/phenotype/`, `imaging/`, `genomics/`)
- `documentation/data_dictionary/`

**Read-write (the pipeline must be able to create and write files here):**
- `processed/` and all its subdirectories (`clinical/`, `imaging/`, `bids/`, `participant_metadata/`, etc.)
- `logs/` and all its subdirectories (`clinical/`, `dicom/`, `evidata/`, `participant_metadata/`)
- In-place tracking files: `.clinical_tracking.json` under `processed/clinical/`, `.dicom_import_processed.json` under `deidentified-raw/imaging/dicoms/`, `.participant_metadata_tracking.json` under `processed/participant_metadata/`

```bash
# Owner + group read/write/execute, others read/execute; setgid for inheritance.
sudo chmod -R 2775 PROJECT/processed PROJECT/logs
sudo chown -R <pipeline_user>:<group> PROJECT/processed PROJECT/logs
```

The owner bits matter most: a directory owned by the pipeline user but with the owner bits cleared (e.g. `d---rwxrwx`) locks the owner out entirely, because Linux applies the owner class first and does not fall through to group permissions. Verify with `ls -ld` — you want the owner to show `rwx` (e.g. `drwxrwsr-x`).

On NFS-mounted data, the mount must export these paths read-write and the mount owner/permissions must match the pipeline account; a read-only or mismatched-ownership mount (e.g. NFS `root_squash` mapping `sudo` to `nobody`) will cause write failures even though reads succeed. In that case the export itself, not just the local permissions, must be corrected on the NFS server.

---

## Logging

Logs are stored in each project's `logs/` directory.

```bash
# View today's clinical run log
tail -f PROJECT/logs/clinical/clinical_run_*.log

# View today's DICOM run log
tail -f PROJECT/logs/dicom/dicom_run_*.log

# View DICOM error log (only exists if errors occurred)
cat PROJECT/logs/dicom/dicom_errors_*.log

# View today's imaging log
tail -f PROJECT/logs/imaging_$(date +%Y-%m-%d).log

# View today's participant metadata log
tail -f PROJECT/logs/participant_metadata/participant_metadata_run_*.log

# Search for errors across all logs
grep "ERROR" PROJECT/logs/**/*.log
```

---

## Email Notifications

Per-project in `project.json`:

```json
{
    "notification_emails": {
        "clinical": {
            "enabled": true,
            "on_success": ["team@example.com"],
            "on_error": ["admin@example.com"]
        },
        "dicom": {
            "enabled": true,
            "on_success": ["team@example.com"],
            "on_error": ["admin@example.com"]
        },
        "bids": {
            "enabled": true,
            "on_success": ["imaging@example.com"],
            "on_error": ["admin@example.com"]
        },
        "participant_metadata": {
            "enabled": true,
            "on_success": ["team@example.com"],
            "on_error": ["admin@example.com"]
        },
        "evidata": {
            "enabled": true,
            "on_check_failed": ["team@example.com"]
        }
    }
}
```

The `evidata` channel differs from the others: it has a single `on_check_failed` list (privacy-check failures) instead of `on_success`/`on_error`.

---