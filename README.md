# LORIS-ARCHIMEDES Pipelines

Automated data ingestion pipelines for ARCHIMEDES study using the [loris-php-api-client](https://github.com/aces/loris-php-api-client) library.

This repository provides production-ready pipelines for clinical and imaging data ingestion, with features like bulk CSV upload, automated candidate creation, email notifications, and comprehensive logging.

The pipelines are expected to be installed on the predefined data mount for the collection of projects, which follows a fixed directory structure, and each project must include its own `project.json` file.

---

## Features

-  **Clinical Data Ingestion** - Automated CSV processing and upload
-  **Candidate Management** - Automatic candidate and visit creation
-  **Bulk Operations** - Process multiple files and projects
-  **Email Notifications** - Success/failure reports via email
-  **Comprehensive Logging** - Detailed execution logs with rotation
-  **Dry Run Mode** - Test without making actual changes
-  **Imaging Data Ingestion** - BIDS data set Ingestion
-  **Multi-Project Support** - Handle multiple projects and collections

---

## Requirements

- PHP >= 8.1
- Composer
-  [loris-php-api-client](https://github.com/aces/loris-php-api-client) (installed automatically)
- MySQL/MariaDB (for database fallback operations)
- Extensions: `curl`, `json`, `pdo`, `mbstring`

---

## Installation

```bash
# Clone repository
cd /opt
git clone https://github.com/aces/archimedes-pipelines.git
cd loris-pipelines

# Install dependencies (including loris-php-client)
composer install
```

This will automatically install:
- `aces/loris-php-client` - Auto-generated LORIS API client
- `guzzlehttp/guzzle` - HTTP client
- `monolog/monolog` - Logging
- `phpmailer/phpmailer` - Email notifications

---

## Configuration

### Main Configuration

Copy the example config file and edit your LORIS credentials:

```bash
cp config/loris_client_config.json.example config/loris_client_config.json
nano config/loris_client_config.json
```

### Run Clinical Pipeline

#### Dry Run Mode (Recommended First)

Test the pipeline without making any changes:

```bash
# Dry run for all projects
php examples/run_clinical_pipeline.php --all --dry-run --verbose

# Dry run for specific project
php examples/run_clinical_pipeline.php --collection=example_collection --project=PROJECT_A --dry-run
```

#### Process All Data

Run the pipeline for all configured projects:

```bash
php examples/run_clinical_pipeline.php --all
```

#### Process Specific Project

Run the pipeline for a single project:

```bash
php examples/run_clinical_pipeline.php --collection=example_collection --project=PROJECT_A
```

#### Process Specific Instrument

Run the pipeline for a specific instrument:

```bash
php examples/run_clinical_pipeline.php --collection=example_collection --project=PROJECT_A --instrument=demographics
```

#### Verbose Output

Enable detailed logging output:

```bash
php examples/run_clinical_pipeline.php --all --verbose
```

---

### Command-Line Options

| Option | Description | Example                          |
|--------|-------------|----------------------------------|
| `--all` | Process all projects | `--all`                          |
| `--collection=NAME` | Specific collection | `--collection=example_collection` |
| `--project=NAME` | Specific project | `--project=PROJECT_A`            |
| `--instrument=NAME` | Specific instrument | `--instrument=instr`             |
| `--dry-run` | Test without changes | `--dry-run`                      |
| `--verbose` | Detailed output | `--verbose`                      |
| `--help` | Show help | `--help`                         |

---

## Pipeline Architecture

```
┌─────────────────────────────────────┐
│     loris-pipelines                 │
│  ┌───────────────────────────────┐  │
│  │  ClinicalPipeline.php         │  │
│  │  ├─ Read CSV files            │  │
│  │  ├─ Validate data             │  │
│  │  ├─ Create candidates         │  │
│  │  ├─ Create visits             │  │
│  │  └─ Upload instrument data    │  │
│  └───────────┬───────────────────┘  │
│              │ uses                 │
│              ▼                       │
│  ┌───────────────────────────────┐  │
│  │  Email notifications          │  │
│  │  Logging infrastructure       │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │ depends on
               ▼
┌──────────────────────────────────────┐
│     aces/loris-php-client            │
│  ┌────────────────────────────────┐  │
│  │  AuthenticationApi             │  │
│  │  ClinicalApi                   │  │
│  │  Auto-generated from OpenAPI   │  │
│  └────────────────────────────────┘  │
└──────────────┬───────────────────────┘
               │ HTTP calls
               ▼
┌──────────────────────────────────────┐
│      LORIS API Server                │
│   https://your-loris.org/api/v0.0.4  │
└──────────────────────────────────────┘
```

---

## Logging

All pipeline executions are logged automatically:

```bash
# View today's log
tail -f logs/clinical_$(date +%Y-%m-%d).log

# View specific date
tail -f logs/clinical_2024-11-18.log

# Search for errors
grep "ERROR" logs/clinical_2024-11-18.log

```