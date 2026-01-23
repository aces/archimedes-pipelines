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

## Imaging Pipeline

Processes BIDS imaging data from `deidentified-lorisid/bids/` using ScriptApi `bidsimport` endpoint.

### Usage

```bash
# Process all projects
php scripts/run_imaging_pipeline.php --all

# Process specific project
php scripts/run_imaging_pipeline.php --collection=COLLECTION --project=PROJECT

# Dry run (recommended first)
php scripts/run_imaging_pipeline.php --all --dry-run --verbose
```

### Options

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

### Workflow

```
┌─────────────────────────────────────────────────────────────┐
│  1. Load config → Check imaging.enabled                     │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  2. For each project:                                       │
│     • Load project.json                                     │
│     • Find BIDS directory (deidentified-lorisid/bids/)      │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  3. Scan for new data:                                      │
│     • Find sub-*/ses-*/ directories                         │
│     • Check processed/.imaging_processed.json               │
│     • Skip already processed (unless --force)               │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  4. BIDS import:                                            │
│     • POST /cbigr_api/script/bidsimport                     │
│     • Auto-create candidates/sessions                       │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  5. Post-processing:                                        │
│     • Write record to processed/imaging/{scan}.json         │
│     • Update processed/.imaging_processed.json              │
│     • Log to logs/imaging_YYYY-MM-DD.log                    │
│     • Send email notification                               │
└─────────────────────────────────────────────────────────────┘
```

---


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
│   ├── bids/
│   └── genomics/
│
├── deidentified-lorisid/                 # LORIS-relabelled data
│   ├── clinical/
│   ├── imaging/
│   ├── bids/
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