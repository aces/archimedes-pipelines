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
   │   ├── Look for definition in documentation/data_dictionary/
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

### Instrument Definition Location

Instrument definitions should be placed in the project's `documentation/data_dictionary/` folder. The pipeline automatically detects the format (LINST or REDCap CSV) and installs accordingly.

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
  /data/archimedes/FDG-PET/deidentified-raw/bids \
  --project="PROJECT" \
  --dry-run -v

# Live run
php scripts/run_bids_participant_sync.php \
  /data/archimedes/FDG-PET/deidentified-raw/bids \
  --project="PROJECT"
```

### Step 2: BIDS Reidentification

Map ExternalIDs to PSCIDs and copy to lorisid directory.

```bash
# Dry run (recommended first)
php scripts/run_bids_reidentifier.php \
  {collection_base_path}/{ProjectName}/deidentified-raw/bids \
  {collection_base_path}/{ProjectName}//deidentified-lorisid/bids \
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

## Command-Line Options

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


## Directory Structure

```
{collection_base_path}/{ProjectName}/
├── project.json                          # Project configuration
│
├── deidentified-raw/                     # De-identified participant data
│   ├── clinical/                         # Clinical instrument CSVs
│   ├── imaging/
│   │   └── dicoms/
│   ├── bids/                             # MRI and EEG Data (Raw)
│   └── genomics/
│
├── deidentified-lorisid/                 # LORIS-relabelled data
│   ├── clinical/
│   ├── imaging/
│   ├── bids/                             #  Reidentified MRI and EEG Data
│   └── genomics/
│
├── processed/                            # Pipeline outputs
│   ├── clinical/
│   ├── imaging/
│   ├── bids/
│   │   └── derivatives/
│   └── freesurfer-output/
│
├── logs/                                 # Execution logs
│
└── documentation/
    ├── data_dictionary/                  # Instrument definitions (.linst, REDCap CSV)
    └── readme.txt
```

---

## Logging

Logs are stored in each project's `logs/` directory.

```bash
# View today's clinical log
tail -f PROJECT/logs/clinical_$(date +%Y-%m-%d).log

# View today's imaging log
tail -f PROJECT/logs/imaging_$(date +%Y-%m-%d).log

# Search for errors
grep "ERROR" PROJECT/logs/*.log
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
        "imaging": {
            "enabled": true,
            "on_success": ["imaging@example.com"],
            "on_error": ["admin@example.com"]
        }
    }
}
```

---