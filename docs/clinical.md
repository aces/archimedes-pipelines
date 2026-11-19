# Clinical ingestion — how to run it

Clinical, preclinical and BIDS phenotype tables (CSV/TSV) go into LORIS instruments
through the ARCHIMEDES API. A second, smaller pipeline updates candidate fields such as
date of death (section 10).

```
deidentified-raw/clinical/*.csv ─┐
deidentified-raw/preclinical/    ├─ (EviData privacy gate) ─► processed/clinical/<file> ─► LORIS instrument
deidentified-raw/bids/phenotype/ ┘     optional                 copy with Project/Site/Cohort    (CREATE_SESSIONS)
```

| Script | Does | Writes to LORIS |
|---|---|---|
| `scripts/test/validate_redcap.php` | **Pre-check**: data dictionaries, data files and project settings against the CBIGR instrument manager's rules; fix-it report | no (reads only) |
| `scripts/run_clinical_pipeline.php` | installs missing instruments, then uploads every new or changed file | instruments, rows |
| `scripts/run_participant_metadata_pipeline.php` | date of death (and other candidate fields) from `participants.tsv`, clinical CSVs or custom sources | candidate fields |

> **Clinical runs LIVE by default.** Add `--dry-run` to preview. This is the opposite of
> the imaging pipelines (DICOM, PNG, BIDS), which preview unless you add `--confirm`.

---

## Contents

1. [Which projects run](#1-which-projects-run)
2. [What the site delivers](#2-what-the-site-delivers)
3. [Before ingestion: the pre-check](#3-before-ingestion-the-pre-check)
4. [Running it — every case](#4-running-it--every-case)
5. [What a run does](#5-what-a-run-does)
6. [Change detection and when to use `--force`](#6-change-detection-and-when-to-use---force)
7. [After a crash or an interruption](#7-after-a-crash-or-an-interruption)
8. [EviData privacy-risk validation](#8-evidata-privacy-risk-validation)
9. [Logs, email, tracking](#9-logs-email-tracking)
10. [Participant metadata (date of death)](#10-participant-metadata-date-of-death)

---

## 1. Which projects run

| Rule | Where |
|---|---|
| Collection and project `"enabled": true` | `config/loris_client_config.json` |
| `project.json` exists and is valid JSON (otherwise a warning and the project is skipped) | project root |
| the collection's mount answers (checked once per collection before any project) | `base_path` |

Clinical does not read `"modalities"`: every enabled project runs. A project with nothing in
`clinical/`, `bids/phenotype/` or `preclinical/` simply has nothing to send.

`--collection` and `--project` must be spelt **exactly** as in `loris_client_config.json`
(the imaging scripts accept any case; clinical does not).

---

## 2. What the site delivers

```
<project>/
├── project.json
├── deidentified-raw/
│   ├── clinical/                 ← CSV/TSV data files
│   ├── preclinical/              ← same rules, its own pass, outputs and email
│   └── bids/phenotype/           ← BIDS phenotype .tsv (clinical ingests these, not BIDS)
└── documentation/
    └── data_dictionary/          ← EVERY dictionary, including a phenotype file's .json
```

**Data dictionaries** — all in `documentation/data_dictionary/`. The type is taken from
the extension only (contents are never inspected):

| Extension | Installed as |
|---|---|
| `.linst` | `linst` |
| `.csv` | `redcap` |
| `.json` | `bids` |

A dictionary saved with the wrong extension is installed under the wrong type, with no
warning. A `.json` left beside a `.tsv` in `phenotype/` is ignored.

**File names** must be unique across `clinical/` and `bids/phenotype/`: tracking, privacy
artifacts and the processed copy are keyed by file name. A file reusing a name is reported
and not ingested.

**Candidates and visits are not created by this pipeline.** The file goes to the instrument
endpoint in `CREATE_SESSIONS` mode and ARCHIMEDES resolves candidate and session server-side.
Visit labels must already exist for the project; a row with an unknown visit fails at upload.

---

## 3. Before ingestion: the pre-check

```bash
php scripts/test/validate_redcap.php --collection=archimedes --project=FDG-PET          # one project
php scripts/test/validate_redcap.php --collection=archimedes                           # a collection
php scripts/test/validate_redcap.php --all --save                                      # everything, saved
php scripts/test/validate_redcap.php --path=/data/archimedes/NewStudy --save           # a folder not configured yet
php scripts/test/validate_redcap.php --dd=dictionary.csv --data=export.csv --report-dir=/tmp/reports
```

Read-only: nothing is uploaded or emailed. It checks folder structure, project settings
(`candidate_defaults`, `sex_mappings`, `visit_mappings`, `date_input_format`), every data
dictionary (REDCap's 18 columns, field types, choice codes, names) and every data file
(required columns, values as the pipeline would write them, visits, sites, cohorts,
duplicates). `--save` writes a `.txt` and a `.csv` (one row per problem, to send to the
data provider) to `<project>/logs/test/redcap_validation/`. Exit `0` no errors, `1` errors,
`2` could not run.

---

## 4. Running it — every case

`P="php scripts/run_clinical_pipeline.php"`

| I want to… | Command |
|---|---|
| Preview one project (nothing sent) | `$P --collection=archimedes --project=FDG-PET --dry-run --verbose` |
| Ingest one project | `$P --collection=archimedes --project=FDG-PET` |
| Ingest one instrument of a project | `$P --collection=archimedes --project=FDG-PET --instrument=moca` |
| Ingest every enabled project of a collection | `$P --collection=archimedes` |
| Ingest every enabled project | `$P --all` |
| Preview everything | `$P --all --dry-run` |
| Two specific projects | one command each — see below |
| Re-send a file that has not changed | `$P --collection=archimedes --project=FDG-PET --force` (section 6) |
| Cron, nightly | `0 2 * * * cd /opt/archimedes-pipelines && php scripts/run_clinical_pipeline.php --all` |

**Two (or more) specific projects.** `--project` takes one name. Run once per project:

```bash
for p in FDG-PET CNMDP; do
  php scripts/run_clinical_pipeline.php --collection=archimedes --project=$p || echo "FAILED: $p"
done
```

Projects in different collections: give each its own `--collection`. To process the same
set regularly, list them together in a collection of their own in
`loris_client_config.json` and run `--collection=NAME`.

**At the same time.** One clinical run per project at a time (project lock,
`.clinical_pipeline.lock` in the project folder). A second run of the same project is
skipped with a message naming the run that holds it; different projects can run in
parallel.

| Option | Effect |
|---|---|
| `--all` | every enabled project of every enabled collection |
| `--collection=NAME` | every enabled project of one collection |
| `--project=NAME` | one project (needs `--collection`) |
| `--instrument=NAME` | one instrument (needs `--collection` and `--project`) |
| `--dry-run` | preview: no instrument install, no upload, no tracking update |
| `--force` | ignore the change detection: re-send every file (section 6) |
| `--verbose` | debug output |
| `--help` | usage |

---

## 5. What a run does

```
1. Projects: enabled collections → enabled projects → project.json (modality on?)
2. EviData privacy gate, when enabled (section 8): a failed file is not ingested
3. Each instrument:
   ├── not installed? → install it from documentation/data_dictionary/ (.linst / .csv / .json)
   └── installed      → go on
4. Each data file (clinical/, preclinical/, bids/phenotype/):
   ├── unchanged since the last successful upload (MD5 of the ORIGINAL)? → SKIPPED, no API call
   ├── write processed/clinical/<file>: Project / Cohort / Site from candidate_defaults,
   │   sex mapped to Male/Female/Other, visits through visit_mappings,
   │   a wrong project name replaced by project_full_name (noted)
   └── upload it (CREATE_SESSIONS); ARCHIMEDES keeps only rows it does not have yet
5. Only after a successful upload:
   ├── record the MD5 in .clinical_tracking.json
   ├── copy the original to processed/clinical/YYYY-MM-DD/
   └── email (when enabled)
```

**The original is never modified, renamed, moved or deleted.**

| Copy | Path | Written | Notes |
|---|---|---|---|
| Enriched | `processed/clinical/<file>` | before upload | what ARCHIMEDES receives; overwritten each run; not created when the file already carries Project/Cohort/Site |
| Snapshot | `processed/clinical/YYYY-MM-DD/<file>` | after success | dated copy of the original |

`preclinical/` runs as its own pass: `processed/preclinical/`, `logs/preclinical/`, a
`"<STATUS>: <project> Preclinical Ingestion"` email to the clinical recipients.

---

## 6. Change detection and when to use `--force`

The MD5 of each **original** file is stored in `processed/clinical/.clinical_tracking.json`
(`processed/preclinical/.preclinical_tracking.json`) after a successful upload.

| Situation | Result |
|---|---|
| no entry yet | first upload — every row sent |
| MD5 differs | whole file sent again; ARCHIMEDES saves only the new rows |
| MD5 matches | `SKIPPED - no changes`, no API call |
| upload failed / EviData check failed | nothing recorded → retried next run |
| `--force` | sent again even if unchanged; ARCHIMEDES still skips rows it has |

**Use `--force` only when** the file did not change but LORIS should receive it again:
- an instrument was reinstalled or fixed in LORIS and rows were rejected before;
- rows were deleted in LORIS and must be loaded again;
- `project.json` changed in a way that changes the enriched copy (a `visit_mappings`,
  `sex_mappings` or `candidate_defaults` value) and already-sent rows were rejected.

**Not needed** for a new or edited file (detected), a failed upload (retried), or after a
crash (section 7). To re-send one file only, delete its key from the tracking file instead
of forcing the whole project.

---

## 7. After a crash or an interruption

**Run the same command again.** Nothing else is needed:

| Interrupted during | On the next run |
|---|---|
| the EviData check | that file was not recorded → checked and sent again |
| an instrument install | the install is retried (an instrument that did install is detected as installed) |
| an upload (part of the rows reached ARCHIMEDES) | the file was not recorded → sent again in full; ARCHIMEDES keeps only the rows it does not have, so **no duplicate rows** |
| after the upload, before the tracking update | sent again; ARCHIMEDES skips every row it already has |
| anything, with the project lock held | the lock is released when the process ends (it is an OS file lock); the next run is not blocked |

Tracking is written only after success, so an interrupted run never marks a file as done.
Do **not** use `--force` to recover: it is never needed for that.

---

## 8. EviData privacy-risk validation

EviData (Woodway Assurance) checks each clinical file for privacy risk before it is
ingested. It applies to clinical files (the DICOM organize step has its own optional
header extract, see [dicom.md](dicom.md)).

**Settings** are split: the service connection in the global `config/evidata_config.json`,
the per-project policy and recipients in `project.json` (project values win).

```json
// config/evidata_config.json
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

// project.json
"evidata": { "qis": [], "exclude_qis": [] },
"notification_emails": { "evidata": { "enabled": true, "on_check_failed": ["team@example.com"] } }
```

- Set `population_size` explicitly; the default skews the risk result.
- `mta_message_size_limit_mb` must match the mail host (`postconf message_size_limit`).
  Larger report batches are compressed with `mutool` (`sudo apt install mupdf-tools`), then
  replaced by summary PDFs, then by a contact-the-team note.
- Empty `qis` / `exclude_qis` = every column is a quasi-identifier. Precedence: project
  `project.json` → global `evidata_config.json` → all headers.
- Failure reports go to `on_check_failed` (not `on_success` / `on_error`).

**Credentials** are never in the JSON, only the names of environment variables. The values
come from an env file the runner loads by itself (cron needs no `source`):

```bash
# /home/lorisadmin/evidata/evidata.env
EVIDATA_CLIENT_SECRET=...
EVIDATA_USERNAME=...
EVIDATA_PASSWORD=...
```

The file is found via `EVIDATA_ENV_FILE`, then `env_file` in `evidata_config.json`, then the
default above; a variable already set in the shell wins. Test the connection with
`php scripts/test/test_evidata_connection.php`.

**At run time** each file goes through upload → validate → generate → poll → results →
report. A file that passes is ingested; one that fails is **not** ingested, its report is
emailed and kept under `logs/evidata/`, and it is checked again on the next run. With the
gate off (no config file, or `enabled: false`) the pipeline runs exactly as without it.

---

## 9. Logs, email, tracking

| What | Where |
|---|---|
| run log | `<project>/logs/clinical/clinical_run_<timestamp>.log` |
| preclinical | `<project>/logs/preclinical/` |
| EviData artifacts | `<project>/logs/evidata/` |
| tracking | `processed/clinical/.clinical_tracking.json`, `processed/preclinical/.preclinical_tracking.json` |
| email | `notification_emails.clinical` (`enabled`, `on_success`, `on_error`) in `project.json` |

---

## 10. Participant metadata (date of death)

Updates candidate fields in ARCHIMEDES — date of death today; other fields are added by
configuration — from `deidentified-raw/bids/participants.tsv`, every
`deidentified-raw/clinical/*.csv`, and any source listed in `project.json`. Only values
that differ from LORIS are sent (`PUT /cbigr_api/candidatesPlus`).

> Like clinical, it runs **live by default**; add `--dry-run` to preview.

```
1. Projects with participant_metadata.enabled
2. One GET of all candidates → index by PSCID and by every ExtStudyID
3. Each source (unchanged since last run → skipped, unless --force):
   each row → resolve the identifier → take the configured fields → PUT only what differs
4. Tracking updated for sources that ran cleanly; email; exit 0 (1 = unresolved rows or failures)
```

```json
"participant_metadata": {
  "enabled": true,
  "defaults": {
    "bids":     { "identifier_field": "participant_id", "identifier_type": "PSCID",
                  "identifier_strip_prefix": "sub-", "fields": { "DoD": ["date_of_death", "dod"] } },
    "clinical": { "identifier_field": "external_id", "identifier_type": "ExtStudyID",
                  "fields": { "DoD": ["DoD", "DateOfDeath", "date_of_death"] } }
  },
  "sources": [
    { "path": "deidentified-raw/registry/weekly_deaths.csv", "identifier_field": "external_id",
      "identifier_type": "ExtStudyID", "fields": { "DoD": ["dod"] } }
  ]
}
```

`M="php scripts/run_participant_metadata_pipeline.php"`

| I want to… | Command |
|---|---|
| Preview everything | `$M --all --dry-run --verbose` |
| Preview one collection | `$M --collection=archimedes --dry-run --verbose` |
| Run one project | `$M --collection=archimedes --project=FDG-PET` |
| Run a collection / everything | `$M --collection=archimedes` · `$M --all` |
| Reprocess unchanged sources (e.g. after a LORIS-side fix) | `$M --collection=archimedes --project=FDG-PET --force` |
| Cron, weekly | `0 4 * * 1 cd /opt/archimedes-pipelines && php scripts/run_participant_metadata_pipeline.php --all` |

Options: `--all`, `--collection=NAME`, `--project=NAME` (needs `--collection`), `--dry-run`,
`--force`, `--verbose`, `--help`.

**After a crash: run it again.** Each PUT is idempotent (a value LORIS already has is not
sent), and a source is recorded only after it ran cleanly. Tracking:
`processed/participant_metadata/.participant_metadata_tracking.json` (delete it to
reprocess every source). Logs: `logs/participant_metadata/`.
