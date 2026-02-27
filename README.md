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

```bash
# Dry run (recommended first)
php scripts/run_bids_participant_sync.php \
  {collection_base_path}/{ProjectName}/deidentified-raw/bids \
  --project="PROJECT" \
  --dry-run -v

# Live run
php scripts/run_bids_participant_sync.php \
  {collection_base_path}/{ProjectName}/deidentified-raw/bids \
  --project="PROJECT"
```

### Step 2: BIDS Reidentification

Map ExternalIDs to PSCIDs and copy to lorisid directory.

```bash
# Dry run (recommended first)
php scripts/run_bids_reidentifier.php \
  {collection_base_path}/{ProjectName}/deidentified-raw/bids \
  {collection_base_path}/{ProjectName}/deidentified-lorisid/bids \
  --dry-run -v

# Live run 
php scripts/run_bids_reidentifier.php \
  {collection_base_path}/{ProjectName}/deidentified-raw/bids \
  {collection_base_path}/{ProjectName}/deidentified-lorisid/bids

# Or with explicit project name
php scripts/run_bids_reidentifier.php \
  {collection_base_path}/{ProjectName}/deidentified-raw/bids \
  {collection_base_path}/{ProjectName}/deidentified-lorisid/bids \
  --project="PROJECT"
```

### Step 3: BIDS Import

Import imaging data into LORIS-MRI.

```bash
# Dry run (recommended first)
php scripts/run_imaging_pipeline.php \
  --collection={collection} \
  --project={project} \
  --dry-run --verbose

# Process specific project
php scripts/run_imaging_pipeline.php \
  --collection={collection} \
  --project={project} \

# Process all projects
php scripts/run_imaging_pipeline.php --all
```

---

## Command-Line Options (BIDS)

### Participant Sync (Step 1)

| Option | Description |
|--------|-------------|
| `<bids_directory>` | Path to BIDS root (required) |
| `--project=NAME` | Override project name from project.json |
| `--dry-run` | Test without changes |
| `-v, --verbose` | Detailed output |
| `--help` | Show help |

### BIDS Reidentifier (Step 2)

| Option | Description |
|--------|-------------|
| `<source_dir>` | deidentified-raw/bids (required) |
| `<target_dir>` | deidentified-lorisid/bids (required) |
| `--project=NAME` | Project name (overrides project.json) |
| `--dry-run` | Test without execution |
| `--force` | Overwrite target if exists |
| `-v, --verbose` | Detailed output |
| `-h, --help` | Show help |

### Imaging Pipeline (Step 3)

| Option | Description |
|--------|-------------|
| `--all` | Process all projects |
| `--collection=NAME` | Specific collection |
| `--project=NAME` | Specific project |
| `--force` | Reprocess all scans |
| `--async` | Run asynchronously |
| `--no-validate` | Skip BIDS validation |
| `--dry-run` | Test without changes |
| `--verbose` | Detailed output |
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
   │   ├── POST /cbigr_api/script/importdicomstudy
   │   │   ├── args: { source: "/path/to/study", profile: "database_config.py" }
   │   │   └── flags: ["insert", "verbose"]
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

```
{collection_base_path}/{ProjectName}/
└── deidentified-raw/
    └── imaging/
        └── dicoms/
            ├── TST02_ROM_00000001_02_SE01_MR/
            │   ├── 1.3.12.2.1107.5.2.38.51068.xxx.dcm
            │   ├── 1.3.12.2.1107.5.2.38.51068.yyy.dcm
            │   └── ...
            ├── TST02_ROM_00000002_02_SE01_MR/
            │   └── ...
            └── .dicom_import_processed.json   # Tracking file (auto-generated)
```

### Tracking File

The pipeline maintains a `.dicom_import_processed.json` file in the dicoms directory to track which studies have been processed. This prevents re-importing studies on subsequent runs. Use `--force` to override and reprocess all studies.

```json
{
  "TST02_ROM_00000001_02_SE01_MR": {
    "status": "success",
    "detail": "",
    "timestamp": "2026-02-27T15:53:19+00:00"
  }
}
```

### Logging

Logs are stored in each project's `logs/dicom/` directory:

```
{ProjectName}/logs/dicom/
├── dicom_run_2026-02-27_18-36-05.log        # Full run log (always created)
└── dicom_errors_2026-02-27_18-36-05.log     # Error log (only if errors occur)
```

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

### Custom Profile

```bash
# Use a different Python configuration file
php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET --confirm --profile=custom_config.py --verbose
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

## Directory Structure

```
{collection_base_path}/{ProjectName}/
├── project.json                          # Project configuration
│
├── deidentified-raw/                     # De-identified participant data
│   ├── clinical/                         # Raw Patient records & clinical assessment in csv/tsv
│   ├── imaging/
│   │   └── dicoms/                       # Raw DICOM studies (one folder per study)
│   ├── bids/                             # Deidentified MRI and EEG Data (ExternalIDs)
│   └── genomics/
│
├── deidentified-lorisid/                 # LORIS-relabelled data
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
│   └── freesurfer-output/                # Converted & cleaned data (NIfTI, MINC)
│
├── logs/                                 # Execution logs
│   ├── clinical/                         # Clinical pipeline logs
│   └── dicom/                            # DICOM import pipeline logs
│
└── documentation/
    ├── data_dictionary/                  # Instrument Data Dictionary (.linst, REDCap CSV)
    └── readme.txt
```

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
        "imaging": {
            "enabled": true,
            "on_success": ["imaging@example.com"],
            "on_error": ["admin@example.com"]
        }
    }
}
```


---
