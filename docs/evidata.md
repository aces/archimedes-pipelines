# EviData privacy-risk check — how it works and how to run it

EviData (Woodway Assurance) scores how easily the records in a table could be
re-identified. ARCHIMEDES uses it in two places:

| Where | What is checked | Result |
|---|---|---|
| **Clinical** (`run_clinical_pipeline.php`) | every clinical, preclinical and BIDS phenotype CSV/TSV, file by file, **sent to the EviData service** | a file that passes is ingested; a file that fails is **not** ingested, its report is emailed, and it is checked again next run |
| **DICOM** (`run_dicom_organize.php`, also in the bundle) | a table of DICOM header values built from the delivery (one row per study, one column per tag), **written as a CSV**, not sent | written to `logs/evidata/evidata_imaging_<project>_<ts>.csv` for review; if the table cannot be built, the project is **not** organized or ingested |

PNG and BIDS have no EviData step.

---

## Contents

1. [Settings](#1-settings)
2. [Credentials](#2-credentials)
3. [Which columns are quasi-identifiers](#3-which-columns-are-quasi-identifiers)
4. [Turning it on and off](#4-turning-it-on-and-off)
5. [How to run it](#5-how-to-run-it)
6. [What a failure looks like](#6-what-a-failure-looks-like)
7. [After a crash or an interruption](#7-after-a-crash-or-an-interruption)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Settings

Two files. The **service** is configured once per host in `config/evidata_config.json`
(copy `config/evidata_config.example.json`); the **policy** is per project in
`project.json`. Project values win where both set the same key (`exclude_qis` is the
exception: the two lists are combined).

**Global — `config/evidata_config.json`**

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | master switch for the service (no file = off) |
| `api_base_url` | — | `https://<evidata-host>/api` (behind a TLS proxy; the API container is on `:8000` otherwise) |
| `token_url` | — | Keycloak token URL, `https://<evidata-host>/auth/realms/<realm>/protocol/openid-connect/token` |
| `client_id` | `evidata-api` | the Keycloak **programmatic** client (not the browser client) |
| `scope` | `openid profile email` | must include `openid`, or the API answers 401 |
| `client_secret_env`, `username_env`, `password_env` | `EVIDATA_CLIENT_SECRET` … | **names** of environment variables, never the values (section 2) |
| `env_file` | `/home/lorisadmin/evidata/evidata.env` | where those variables are read from |
| `population_size` | `50000` | reference population; must be ≥ rows + 1. **Set it explicitly**: the default skews the risk result |
| `qis`, `exclude_qis` | `[]` | global quasi-identifier policy (section 3) |
| `poll_interval_seconds`, `poll_timeout_seconds` | `10`, `600` | report generation polling |
| `analysis_type`, `client_name`, `recipient_name` | — | labels printed on the report |
| `mta_message_size_limit_mb` | — | the mail host's limit (`postconf message_size_limit`, in MB); larger report batches are compressed |
| `pdf_compression` | — | compression level for report PDFs |
| `from_address` | `archimedes-pipeline@<host>` | sender of failure emails |

**Per project — `project.json`**

```json
"evidata": {
  "enabled": true,
  "imaging":  { "enabled": true, "scan_mode": "full" },
  "qis": [],
  "exclude_qis": ["SeriesDescription", "Modality"]
},
"notification_emails": {
  "evidata": { "enabled": true, "on_check_failed": ["privacy-team@example.org"] }
}
```

| Key | Meaning |
|---|---|
| `enabled` | `false` switches the DICOM header extract off for this project (see section 4 for clinical) |
| `imaging.enabled` | `true` turns on the DICOM header extract; absent = off |
| `imaging.scan_mode` | `full` reads every DICOM file; `sample` one file per folder (faster, may miss values) |
| `qis`, `exclude_qis` | this project's quasi-identifier policy (section 3) |
| `notification_emails.evidata.on_check_failed` | who receives failure reports (falls back to `notification_defaults.default_on_evidata_failed` in `loris_client_config.json`) |

---

## 2. Credentials

Never in the JSON files. The config holds only variable **names**; the values sit in an env
file that the clinical runner and the connection test load by themselves, so cron needs no
`source`:

```bash
# /home/lorisadmin/evidata/evidata.env   (chmod 600, never committed)
EVIDATA_CLIENT_SECRET=...
EVIDATA_USERNAME=...
EVIDATA_PASSWORD=...
```

The file is found via the `EVIDATA_ENV_FILE` environment variable, then `env_file` in
`evidata_config.json`, then the default above. A variable already set in the shell wins over
the file. A missing value stops the run with the name it expected; values are never logged.

---

## 3. Which columns are quasi-identifiers

A quasi-identifier (QI) is a column someone could combine with others to recognise a
person (date of birth, site, sex, …). EviData's risk depends entirely on which columns are
QIs.

**Which list is used** (first match wins):

| # | Source | Used when |
|---|---|---|
| 1 | `project.json` → `evidata.qis` | not empty — the project owns its policy |
| 2 | `evidata_config.json` → `qis` | the project has none |
| 3 | **all headers** of each file | both are empty or absent |

**Form of a `qis` list:**

```json
"qis": ["dob", "sex", "site"]                       // the same columns for every file

"qis": {                                            // per file
  "_default": ["dob", "sex"],                       // files not listed below
  "demographics.csv": ["dob", "sex", "postal_code"],
  "visits.csv": "*"                                 // every header of this file
}
```

Names in `qis` are **case-sensitive** (EviData checks them against the CSV header as
written). Keep the global `qis` a flat list: a per-file map there would apply to every
project that falls back to it.

**Excluding columns (`exclude_qis`).** Only used in all-headers mode (row 3, or `"*"` for a
file). The global and project lists are **combined**, matched **case-insensitively**, and
those columns are left out of the QI set. Nothing else is removed: identifiers that are
unique on every row stay unless you exclude them. If excluding leaves no column, the file
fails with "All headers excluded … loosen exclude_qis or define an explicit qis list".

```json
"exclude_qis": ["study_id", "redcap_event_name", "instrument_complete"]
```

The run log shows, per file, which columns were used and which were excluded.

The DICOM extract uses the same precedence; a few header columns are never QIs there.

---

## 4. Turning it on and off

| Check | On when | Off when |
|---|---|---|
| clinical | `evidata_config.json` exists and `enabled: true` — then **every** project's clinical files are checked | the file is missing or `enabled: false` (the pipeline then runs exactly as without EviData) |
| DICOM extract | global `enabled: true`, project `evidata.enabled` not `false`, **and** `evidata.imaging.enabled: true` | any of those not met; or `--no-evidata` for one run |
| DICOM extract, one run only | `--evidata` on the organize step or the bundle (runs even when the project has not enabled it) | `--no-evidata` |

The clinical check has no per-project switch: the per-project `evidata.clinical.enabled`
in the template is not read. To exempt a project from the clinical check today, the
service must be off for the host.

Every DICOM run prints the rule that decided (`evidata: SKIPPED (global evidata.enabled is
false)`, `… project evidata.imaging.enabled …`).

---

## 5. How to run it

There is no separate EviData command for ingestion: it runs inside the pipelines.

```bash
# test the connection and credentials (token, API) — run this first on a new host
php scripts/test/test_evidata_connection.php
php scripts/test/test_evidata_connection.php --config /path/to/evidata_config.json

# clinical: the check runs for every file before ingestion
php scripts/run_clinical_pipeline.php --collection=archimedes --project=FDG-PET --dry-run   # check, ingest nothing
php scripts/run_clinical_pipeline.php --collection=archimedes --project=FDG-PET             # check, then ingest what passed

# DICOM: the header extract runs at the start of organize
php scripts/dicom/run_dicom_bundle.php --collection=archimedes --project=FDG-PET --confirm
php scripts/dicom/run_dicom_bundle.php --collection=archimedes --project=FDG-PET --confirm --evidata     # force it this once
php scripts/dicom/run_dicom_bundle.php --collection=archimedes --project=FDG-PET --confirm --no-evidata  # skip it this once
php scripts/dicom/run_dicom_organize.php --collection=archimedes --project=FDG-PET --evidata             # extract only (organize dry run)
```

A clinical **dry run still sends the files to EviData** (that is the check); it ingests
nothing.

---

## 6. What a failure looks like

**Clinical, per file:**

| Outcome | What happens |
|---|---|
| passes | the file is ingested as usual |
| fails the risk threshold | not ingested; not recorded, so it is checked again next run; report PDFs emailed to `on_check_failed` and kept in `logs/evidata/` |
| EviData unreachable or errors | not ingested (treated as not passed); retried next run |
| QI settings invalid for the project | every file of the project fails; the message names the setting |

The whole project is never stopped by one file: files that pass go in.

**Email size.** When the reports exceed `mta_message_size_limit_mb`, the batch is compressed
with `mutool` (`sudo apt install mupdf-tools`), then replaced by shorter summary PDFs, then
by a note to contact the team. Without `mutool` the compression step is skipped (logged).

**DICOM:** the extract cannot be built (unreadable delivery, `dcmdump` missing) → that
project is skipped: nothing organized, nothing ingested. The CSV is listed in the import
email for review.

---

## 7. After a crash or an interruption

Run the same command again. A clinical file is recorded only after it was checked **and**
ingested, so an interrupted check is simply done again. The DICOM extract is rebuilt from
the delivery on the next organize.

---

## 8. Troubleshooting

| Message | Cause | Fix |
|---|---|---|
| `EviData: not enabled in evidata_config.json — skipping pre-flight` | service off | set `enabled: true` if intended |
| 401 `Failed to authenticate user` | token without `openid` scope, or wrong client | `scope` includes `openid`; `client_id` is the programmatic client |
| missing environment variable (named) | env file not found or incomplete | check `EVIDATA_ENV_FILE` / `env_file`, `chmod 600`, the three names |
| QI name rejected by EviData | `qis` case differs from the CSV header | write `qis` exactly as the header |
| `All headers excluded …` | `exclude_qis` removed every column | loosen `exclude_qis`, or give `qis` |
| population size error | `population_size` < rows + 1 | raise `population_size` |
| reports not received | attachment too large for the mail host | set `mta_message_size_limit_mb`; install `mupdf-tools` |
