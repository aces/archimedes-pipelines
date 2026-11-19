# LORIS-ARCHIMEDES Pipelines

Pipelines that load ARCHIMEDES study data into LORIS — clinical tables, DICOM, PNG and BIDS
imaging, and participant metadata — from each project's folder on the data mount, using the
[loris-php-api-client](https://github.com/aces/loris-php-api-client) and the CBIGR script API.

Each project folder has a fixed layout and its own `project.json`; the projects to process
are listed in `config/loris_client_config.json`.

---

## Features

- **Clinical** — CSV/TSV (clinical, preclinical, BIDS phenotype) into LORIS instruments;
  installs missing instruments (LINST, REDCap, BIDS dictionaries)
- **DICOM** — checks a delivery, groups it into examinations, creates candidates and
  sessions, relabels with LORIS IDs, archives in LORIS
- **PNG** — images (echo, ECG …) matched to participants and visits, inserted into LORIS
- **BIDS** — MRI/EEG datasets: candidates, relabelling, Loris-MRI import
- **Participant metadata** — date of death (and other candidate fields) from data files
- **Pre-checks for sites** — read-only reports of everything to fix before ingestion
  (REDCap, DICOM, PNG, BIDS)
- **EviData privacy-risk check** for clinical files (and a DICOM header extract)
- **Safe to re-run** — only new or changed data is sent; nothing is duplicated; after a
  crash, the same command picks up where it stopped
- **One run per project at a time**, email per project run, logs per project

---

## Install

```bash
git clone https://github.com/aces/archimedes-pipelines.git && cd archimedes-pipelines
composer install
cp config/loris_client_config.example.json config/loris_client_config.json   # then edit, chmod 600
php scripts/test/preflight_check.php
```

Configuration, every `project.json` key and mapping, folder layout, permissions, logs and
email: **[docs/setup.md](docs/setup.md)**.

---

## How every pipeline is run

| | Clinical, participant metadata | DICOM, PNG, BIDS |
|---|---|---|
| default | **live** — add `--dry-run` to preview | **preview** — add `--confirm` to execute |
| one project | `--collection=NAME --project=NAME` | same |
| a collection / everything | `--collection=NAME` / `--all` | same |
| re-run after new data or a crash | the same command | the same command |

Use the exact collection and project names from `config/loris_client_config.json`.

---

## Workflows

### Clinical — [docs/clinical.md](docs/clinical.md)

```bash
php scripts/test/validate_redcap.php    --collection=archimedes --project=FDG-PET --save   # pre-check (read-only)
php scripts/run_clinical_pipeline.php   --collection=archimedes --project=FDG-PET --dry-run
php scripts/run_clinical_pipeline.php   --collection=archimedes --project=FDG-PET
```

Files in `deidentified-raw/clinical/`, `preclinical/` and `bids/phenotype/` → LORIS
instruments. Unchanged files are skipped; `--force` re-sends them.
Participant metadata (date of death): `php scripts/run_participant_metadata_pipeline.php`
— same guide, section 10.

### DICOM — [docs/dicom.md](docs/dicom.md)

```bash
php scripts/dicom/run_dicom_validate.php --collection=archimedes --project=FDG-PET            # pre-check
php scripts/dicom/run_dicom_bundle.php   --collection=archimedes --project=FDG-PET            # preview
php scripts/dicom/run_dicom_bundle.php   --collection=archimedes --project=FDG-PET --confirm
```

organize → participant sync → reidentify → import, from `deidentified-raw/imaging/dicoms/`.
The bundle runs all four; each step can also run on its own.

### PNG — [docs/png.md](docs/png.md)

```bash
php scripts/test/validate_png.php      --collection=archimedes --project=ARCHI-TEST --save   # pre-check
php scripts/png/run_png_bundle.php     --collection=archimedes --project=ARCHI-TEST           # preview
php scripts/png/run_png_bundle.php     --collection=archimedes --project=ARCHI-TEST --confirm
```

participant sync → reidentify → import, from `deidentified-raw/imaging/png/`.

### BIDS — [docs/bids.md](docs/bids.md)

```bash
php scripts/test/validate_bids.php                --collection=archimedes --project=CNMDP --save   # pre-check
C="--collection=archimedes --project=CNMDP"
php scripts/bids/run_bids_participant_sync.php $C --confirm \
 && php scripts/bids/run_bids_reidentifier.php $C --confirm \
 && php scripts/bids/run_bids_import.php       $C --confirm
```

Three steps in order (no bundle), from `deidentified-raw/bids/`.

### EviData — [docs/evidata.md](docs/evidata.md)

Privacy-risk check run inside the clinical pipeline (and a header extract in DICOM
organize): settings, credentials, quasi-identifiers, how to turn it on and test it.

---

## Documentation

| Guide | Covers |
|---|---|
| [docs/setup.md](docs/setup.md) | install, configuration, `project.json`, mappings and defaults, folders, permissions, logs, email, diagnostics |
| [docs/clinical.md](docs/clinical.md) | clinical and participant metadata: every run case, `--force`, crash recovery |
| [docs/dicom.md](docs/dicom.md) | DICOM: steps, bundle, every run case, `--force`, crash recovery, messages |
| [docs/png.md](docs/png.md) | PNG: steps, bundle, every run case, `--force`, crash recovery, messages |
| [docs/bids.md](docs/bids.md) | BIDS: steps, every run case, visits, `--force`, crash recovery, Loris-MRI requirements |
| [docs/evidata.md](docs/evidata.md) | EviData privacy check |
