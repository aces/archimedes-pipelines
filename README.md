# LORIS-ARCHIMEDES Pipelines

Automated data ingestion pipelines for ARCHIMEDES study using the [loris-php-api-client](https://github.com/aces/loris-php-api-client) library.

This repository provides production-ready pipelines for clinical and imaging data ingestion, with features like bulk CSV upload, automated candidate creation, email notifications, and comprehensive logging.

The pipelines are expected to be installed on the predefined data mount for the collection of projects, which follows a fixed directory structure, and each project must include its own `project.json` file.

---

## Features

- **Clinical Data Ingestion** - Automated CSV processing and upload
- **Clinical Instrument Install** - Install REDCap or LINST instruments
- **Bulk Operations** - Process multiple files and projects
- **Email Notifications** - Success/failure reports via email
- **Comprehensive Logging** - Detailed execution logs with rotation
- **Dry Run Mode** - Test without making actual changes
- **Imaging Data Ingestion** - BIDS dataset ingestion
- **DICOM Import** - Archive and insert DICOM studies into LORIS tarchive tables
- **Participant Metadata Sync** - Update candidate demographic fields (Date of Death, Sex, DoB, etc.) in LORIS
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

Copy the example config file and edit your LORIS credentials and collections:

```bash
cp config/loris_client_config.json.example config/loris_client_config.json
nano config/loris_client_config.json
```

### Project Configuration

Each project requires a `project.json` file at its root. See `config/project.json.example` for reference.

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

See `config/evidata_config.json.example` for the complete set of available keys and defaults.

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
   ├── Check if instrument is installed in LORIS
   ├── If NOT installed:
   │   ├── Look for Data Dictionary in documentation/data_dictionary/
   │   ├── Find .linst file OR REDCap data dictionary CSV
   │   └── Install instrument via API
   └── If installed:
       └── Continue to data ingestion

4. Data Ingestion:
   ├── Read CSV from deidentified-raw/clinical/
   ├── Validate data against instrument schema
   ├── Create candidates (if not exist)
   ├── Create visits (if not exist)
   └── Upload instrument data via API

5. Post-Processing:
   ├── Move processed files to processed/clinical/
   ├── Log results
   └── Send email notification (if enabled)
```

### Collections Configuration

Collections and projects are defined in `loris_client_config.json`. Each collection has a base path and a list of projects that can be individually enabled or disabled. See `config/loris_client_config.json.example` for reference.

### Instrument Data Dictionary Location

Instrument Data Dictionary should be placed in the project's `documentation/data_dictionary/` folder. The pipeline automatically detects the format (LINST or REDCap CSV) and installs accordingly.

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

3. STEP 1: Participant Sync (Create LORIS Candidates)
   ├── Script: run_bids_participant_sync.php
   ├── Read deidentified-raw/bids/participants.tsv
   ├── Validate BIDS structure (orphan/missing directories)
   ├── Check if candidate exists (CBIGR mapper)
   ├── Create candidate (LORIS API)
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

### Collections Configuration

Collections and projects are defined in `loris_client_config.json`. Each collection has a base path and a list of projects that can be individually enabled or disabled. See `config/loris_client_config.json.example` for reference.

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
- `sex` - Male/Female (required by LORIS)
- `site` - LORIS site name (must match database)
- `dob` - Date of birth in YYYY-MM-DD format
- `project` - LORIS project name (optional if in project.json)

---

## Running the BIDS Pipeline

### Step 1: Participant Sync

Create LORIS candidates and link ExternalIDs.
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

## DICOM Ingestion Workflow (Convert to tarchive)

The DICOM import pipeline scans each project's `deidentified-raw/imaging/dicoms/` directory for study folders, archives them into LORIS tarchive format, and inserts or updates the records in the LORIS database via the `cbigr_api` script endpoint.

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
       ├── Authenticate with LORIS API
       └── Continue to DICOM processing

3. STEP 1: Scan DICOM Directories
   ├── Scan deidentified-raw/imaging/dicoms/
   ├── Discover study directories (one per DICOM study)
   ├── Load tracking file (.dicom_import_processed.json)
   └── Skip already-processed studies (unless --force)

4. STEP 2: Import Studies
   ├── For each study directory:
   │   ├── Call importdicom study script Endpoint from LORIS/CBIG
   │   ├── Script archives DICOMs into .tar.gz
   │   ├── Calculates MD5 checksums
   │   ├── Inserts/updates tarchive record in database
   │   └── Associates with LORIS session (if --session flag)
   │
   ├── Classify results:
   │   ├── SUCCESS → mark as processed
   │   ├── ALREADY_EXISTS → mark as already_exists (not an error)
   │   └── FAILED → log error details
   │
   └── Update tracking file after each study

5. Post-Processing:
   ├── Write project summary to run log
   ├── Write errors to error log (if any)
   ├── Send email notification (from project.json config)
   └── Return exit code (0 = success, 1 = any failures)
```

### DICOM Study Directory Structure

Each study should be a subdirectory under `deidentified-raw/imaging/dicoms/` containing the DICOM files:

### Tracking File

The pipeline maintains a `.dicom_import_processed.json` file in the dicoms directory to track which studies have been processed. This prevents re-importing studies on subsequent runs. Use `--force` to override and reprocess all studies.

### Logging

Logs are stored in each project's `logs/dicom/` directory:

---

## Running the DICOM Import Pipeline

### Dry Run Mode (Recommended First)

```bash
# All enabled projects
php scripts/run_dicom_import.php --all --verbose

# All projects in a collection
php scripts/run_dicom_import.php --collection=archimedes --verbose

# Single project
php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET --verbose
```

### Execute (Live Run)

```bash
# All projects
php scripts/run_dicom_import.php --all --confirm --verbose

# Single collection
php scripts/run_dicom_import.php --collection=archimedes --confirm --verbose

# Single project
php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET --confirm --verbose
```

### Force Reprocess

```bash
# Reprocess all studies (ignore tracking file)
php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET --confirm --force --verbose

# Force reprocess across all projects
php scripts/run_dicom_import.php --all --confirm --force --verbose
```

### Update Mode

```bash
# Update existing studies instead of insert
php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET --confirm --update --verbose

# Update with session association and overwrite
php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET --confirm --update --session --overwrite --verbose
```

---

## Command-Line Options (DICOM Import)

| Option | Description |
|--------|-------------|
| `--all` | Process all enabled collections & projects |
| `--collection=NAME` | Process all enabled projects in a collection |
| `--project=NAME` | Process a specific project (requires `--collection`) |
| `--confirm` | Execute (default is dry run) |
| `--force` | Reprocess already-processed studies |
| `--update` | Use `--update` flag instead of `--insert` |
| `--session` | Associate study with LORIS session |
| `--overwrite` | Overwrite existing archive files |
| `--profile=NAME` | Python config file (default: `database_config.py`) |
| `--config=FILE` | Config file path (default: `config/loris_client_config.json`) |
| `--verbose` | Detailed output |
| `--help` | Show help |

---

## Participant Metadata Workflow

The participant metadata pipeline updates candidate demographic fields in LORIS (Date of Death today; extensible to Sex, DoB, and other fields) from BIDS, clinical, and custom-configured source files. Each source's MD5 is tracked so unchanged files are skipped on subsequent runs.

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

Each source declares which LORIS fields to update via a `fields` map (LORIS field name → list of source column names; first non-empty wins):

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

The pipeline treats the de-identified input directories as **read-only** and writes only to its own output and log trees. Getting these permissions right matters: a missing write bit causes silent failures (tracking not persisted, snapshots not archived, EviData artifacts not saved) while reads still succeed, so the run appears to work but loses state between runs.

**Read-only (the pipeline never writes here):**
- `deidentified-raw/` and its subdirectories (`clinical/`, `bids/`, `imaging/`, `genomics/`)
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