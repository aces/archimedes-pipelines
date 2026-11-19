# BIDS ingestion — how to run it

A site's BIDS dataset (MRI, EEG) goes into LORIS in three steps, run in this order:

```
deidentified-raw/bids/ ──sync──► LORIS candidates ──reidentify──► deidentified-lorisid/bids/ ──import──► LORIS
  sub-<ExternalID>        from participants.tsv     sub-<ExternalID> → sub-<PSCID>          Loris-MRI bids_import
                          (+ candidate_defaults)    (CBIGR bidsreidentifier)                (CBIGR bidsimport)
```

| # | Script | Does | Writes to LORIS |
|---|---|---|---|
| 0 | `scripts/test/validate_bids.php` | **Pre-check** against the user guide, `project.json`, the BIDS validator Loris-MRI uses, and LORIS; fix-it report | no (reads only) |
| 1 | `scripts/bids/run_bids_participant_sync.php` | checks the whole dataset, then creates missing candidates and links their ExternalIDs | candidates |
| 2 | `scripts/bids/run_bids_reidentifier.php` | copies the dataset to `deidentified-lorisid/bids/` with LORIS IDs | no (files only) |
| 3 | `scripts/bids/run_bids_import.php` | imports the reidentified dataset (sessions and files) | sessions, files |

There is no BIDS bundle: run the three in order (section 4 has the one-line form).
**Everything is a dry run unless you add `--confirm`.**

Phenotype tables (`deidentified-raw/bids/phenotype/*.tsv`) are clinical data: the clinical
pipeline ingests them ([clinical.md](clinical.md)), not this one.

---

## Contents

1. [Which projects run](#1-which-projects-run)
2. [What the site delivers](#2-what-the-site-delivers)
3. [Before ingestion: the pre-check](#3-before-ingestion-the-pre-check)
4. [Running it — every case](#4-running-it--every-case)
5. [What each step checks and does](#5-what-each-step-checks-and-does)
6. [Visits](#6-visits)
7. [Re-running: what is picked up](#7-re-running-what-is-picked-up)
8. [When to use `--force`](#8-when-to-use---force)
9. [After a crash or an interruption](#9-after-a-crash-or-an-interruption)
10. [What Loris-MRI requires, and how the pipeline meets it](#10-what-loris-mri-requires-and-how-the-pipeline-meets-it)
11. [Logs, email, tracking, exit codes](#11-logs-email-tracking-exit-codes)
12. [On the LORIS server](#12-on-the-loris-server)

---

## 1. Which projects run

| Rule | Where |
|---|---|
| Collection and project `"enabled": true` | `config/loris_client_config.json` |
| `"modalities"` contains `"Imaging"` | the project's `project.json` |
| a BIDS folder exists: `deidentified-raw/bids/` (or `deidentified-raw/imaging/bids/`) | project folder |

`--all` and `--collection` pick up only enabled projects; a project with no BIDS folder is
skipped with the folders it looked in. Spell `--collection` and `--project` exactly as in
`loris_client_config.json` (the BIDS scripts compare them exactly).

---

## 2. What the site delivers

```
<project>/deidentified-raw/bids/
├── dataset_description.json        required ("Name", "BIDSVersion")
├── participants.tsv                one row per participant
├── sub-ARCHI0001/
│   ├── ses-V01/anat/sub-ARCHI0001_ses-V01_T1w.nii.gz (+ .json)
│   ├── ses-V01/eeg/sub-ARCHI0001_ses-V01_task-rest_eeg.edf ...
│   └── ses-V02/anat/...
└── sub-ARCHI0002/ses-V01/...
```

- The `ses-` folder name is the LORIS visit, exactly (section 6). **No `sessions.tsv` is
  needed**: the visits are the folder names.
- File names carry the participant and the visit (`sub-ARCHI0001_ses-V01_...`) and must
  match the folders they sit in.

**participants.tsv**

```tsv
participant_id	dob	sex	cohort	site	project
sub-ARCHI0001	1948-03-22	Male	Affected	UOHI	CNMDP
sub-ARCHI0002	1955-01-01	Female	Affected	UOHI	CNMDP
```

| Column | Required | Notes |
|---|---|---|
| `participant_id` | yes | `sub-<ID>`, matching the `sub-` folder |
| `sex` (or `gender`) | yes | `Male` / `Female` / `Other`, also `M` / `F` / `O`, any case, plus `project.json` `sex_mappings`; else `candidate_defaults.sex` |
| `dob` | yes | `YYYY-MM-DD` preferred (`YYYY-MM`, `YYYY`, `M/D/YYYY`, `D/M/YYYY` normalised; `date_input_format` decides M/D vs D/M); else `candidate_defaults.dob` |
| `site`, `cohort` (or `group`), `project` | from `candidate_defaults` | **when `candidate_defaults` sets one, that value is used**: blank or missing → the default; same value in another case → the default's spelling; a different value → replaced, with a note to correct the file |
| `external_id` | no | defaults to `participant_id` without `sub-`; if given it must equal it (BIDS labels cannot hold `-`) |

Header names are read in any case and with their aliases (`Participant_ID`, `DoB`, `Gender`,
`Group`, `ExtStudyID`). Minimal file, with site, cohort and project in `candidate_defaults`:

```tsv
participant_id	sex	dob
sub-ARCHI0001	Male	1948-03-22
```

---

## 3. Before ingestion: the pre-check

```bash
V="php scripts/test/validate_bids.php"
$V --collection=archimedes --project=CNMDP                 # one project, on screen
$V --collection=archimedes --project=CNMDP --save          # also .txt + .csv for the site
$V --collection=archimedes                                 # every enabled project of a collection
$V --all --save                                            # everything
$V --path=/data/archimedes/NewStudy --offline              # a folder not configured yet, no LORIS
$V --path=/data/archimedes/NewStudy --bids-dir=/incoming/bids --python=/opt/loris-mri/.venv/bin/python
```

Read-only. LORIS is read over the API only (login, projects, sites, candidate lookups);
there is no database connection. It reports, in one list:

| Section | Checks |
|---|---|
| Project settings | `modalities` has Imaging; `candidate_defaults` project / site / cohort; `visits` (optional) |
| Folder | the dataset exists; `dataset_description.json` (valid, `Name`, `BIDSVersion`); folders at the top that are not BIDS; a `sub-` folder inside another; files named for another participant |
| participants.tsv | tab-separated (not CSV, not Excel); missing values (dob, sex); site / cohort / project different from `candidate_defaults`; folders with no row; rows with no folder; ID patterns |
| Participants | `VISIT MISMATCH` (folder not exactly a LORIS visit; files labelled with another `ses-`); data with no `ses-` folder; no cohort (Loris-MRI needs one to create a session) |
| BIDS validator | every file through the `bids_validator` package Loris-MRI uses: a file it rejects is **silently skipped by the import**, so it is listed here |
| LORIS | project and sites exist; participants already in **another** LORIS project; participants already in LORIS whose dob / sex differ |

The BIDS validator runs with `--python=`, else `imaging.python_bin`, else the Loris-MRI
virtualenv under `imaging.mri_code_path`, else `python3`; when none has `bids_validator`
the report says so and the rest still runs. Reports go to
`<project>/logs/test/bids_validation/`. Exit `0` ready, `1` errors, `2` could not run.

---

## 4. Running it — every case

```bash
S="php scripts/bids/run_bids_participant_sync.php"
R="php scripts/bids/run_bids_reidentifier.php"
I="php scripts/bids/run_bids_import.php"
C="--collection=archimedes --project=CNMDP"
```

| I want to… | Command |
|---|---|
| Preview the three steps (nothing changed) | `$S $C -v && $R $C -v && $I $C -v` |
| Ingest one project | `$S $C --confirm && $R $C --confirm && $I $C --confirm` |
| Every enabled project of a collection | `for s in "$S" "$R" "$I"; do $s --collection=archimedes --confirm \|\| break; done` |
| Every enabled project | `$S --all --confirm && $R --all --confirm && $I --all --confirm` |
| Two specific projects | one chain each — see below |
| Only create the candidates | `$S $C --confirm` |
| Prepare everything, import nothing | `$S $C --confirm && $R $C --confirm` |
| Import what is already reidentified | `$I $C --confirm` |
| Rebuild the reidentified tree | `$R $C --confirm --force`, then `$I $C --confirm` (section 8) |
| Re-import an unchanged dataset | delete `processed/bids/.bids_import_tracking.json`, then `$I $C --confirm` |
| Import without Loris-MRI's BIDS validation | `$I $C --confirm --no-validate` (files the validator rejects are then imported too; use only for a known false rejection) |
| Cron, nightly | `30 2 * * * cd /opt/archimedes-pipelines && php scripts/bids/run_bids_participant_sync.php --all --confirm && php scripts/bids/run_bids_reidentifier.php --all --confirm && php scripts/bids/run_bids_import.php --all --confirm` |

`&&` stops at the first failing step: with `--all` or `--collection`, a step that fails for
**any** project stops the chain for all of them (exit `1`). To let the other projects go on,
run the three steps one after another without `&&` — that is safe: a participant the sync
did not create is never reidentified (`NOT_SYNCED`), and the import refuses an unfinished
reidentification, so a stopped project gets nothing.

**Two (or more) specific projects** — `--project` takes one name. One chain per project:

```bash
for p in CNMDP FDG-PET; do
  C="--collection=archimedes --project=$p"
  $S $C --confirm && $R $C --confirm && $I $C --confirm || echo "STOPPED: $p"
done
```

Projects in different collections: give each its own `--collection`. For a set you run
often, list them in a collection of their own in `loris_client_config.json`.

**At the same time.** One BIDS run per project (`.bids_pipeline.lock`, shared by the three
scripts): a second one is skipped with a message naming the run that holds it. Different
projects, and DICOM or PNG runs of the same project, are not blocked.

**Options** (all three): `--all` · `--collection=NAME` · `--project=NAME` (needs
`--collection`) · `--confirm` · `--dry-run` (default; wins over `--confirm`) ·
`--config=FILE` · `-v` · `--help`. Reidentifier: `--force`. Import: `--no-validate`.
An unknown option stops the script (exit `2`); it is never ignored.

---

## 5. What each step checks and does

### 1 — participant sync

Reads `participants.tsv` through the shared roster (same rules as DICOM and PNG), then runs
**one check of the whole dataset before anything is created**:

| Blocks the dataset (nothing created) | Flagged, the rest goes in |
|---|---|
| `participants.tsv` missing, CSV, Excel, unreadable | `VISIT MISMATCH` (section 6) |
| bad `participant_id`, sex, dob (shared `ParticipantsCheck`) | site / cohort / project different from `candidate_defaults` (note) |
| a `sub-` folder with no `participants.tsv` row | participant IDs following different patterns |
| files named for another participant; a `sub-` folder inside another | rows with no folder |
| no `dataset_description.json` | |
| an ID already a candidate of **another** LORIS project | |

Then, for each row: find the candidate by ExternalID (CBIGR mapper), or create it
(candidatesPlus, legacy fallback) and link the ExternalID. Existing candidates are matched,
never modified.

### 2 — reidentify

Calls the CBIGR `bidsreidentifier` script (`sub-<ExternalID>` → `sub-<PSCID>` in folder and
file names and in the TSVs). Only new or changed participants are sent (section 7), staged
through symlinks; held-back visits are left out. A participant with no candidate of this
project behind its ExternalID (the sync stopped or has not run) is **not** sent:
`NOT_SYNCED`, exit `1` — it would otherwise be copied with its original ID and the import
would create a candidate the sync held back. Output is written to a hidden work folder
next to the target and moved into `deidentified-lorisid/bids/` **only when the run
succeeded** (section 9).

### 3 — import

Refuses to run while the last reidentification failed, or while the tree holds participant
folders the reidentifier did not finish. Then, when the dataset changed since the last
successful import (fingerprint), it calls the CBIGR `bidsimport` script
(`createcandidate`, `createsession`) on the reidentified tree, with a corrected
`participants.tsv` (section 10), and polls the job. An unchanged dataset is skipped.

---

## 6. Visits

The `ses-` folder name is the LORIS visit, used **exactly** (case too). With
`project.json` `"visits"` (optional) it must be one of them. Nothing is renamed on the way
in: BIDS file names carry the visit, so a renamed folder would no longer match its files.

| Folder | `"visits": ["V01","V02"]` | no `"visits"` |
|---|---|---|
| `ses-V01` | goes in as V01 | goes in as V01 |
| `ses-v01` | **VISIT MISMATCH** — "rename it ses-V01 (the folder and the ses-v01 in its file names)" | goes in as `v01` (LORIS checks it at session creation) |
| `ses-V07` | **VISIT MISMATCH** — not in `"visits"` | goes in as V07 |
| `ses-V01` holding `sub-X_ses-1_T1w.nii.gz` | **VISIT MISMATCH** — file labelled `ses-1` | same |

A mismatched visit is **held back**: flagged by the sync, the pre-check and the
reidentifier (also in dry runs), left out of the reidentified tree, and **the rest of the
dataset goes in**. Fix it and re-run the three steps: the participant is redone with the
fixed visit. Why it matters: LORIS compares visit labels without case, so Loris-MRI itself
would accept `ses-v01` and create a session spelt `v01`.

---

## 7. Re-running: what is picked up

Re-run the same three commands after every delivery.

| Delivery | Result |
|---|---|
| nothing changed | sync: all matched · reidentify: `all already reidentified — skipping` · import: `Already imported successfully — dataset unchanged` |
| new participant (folder + row) | candidate created · that participant reidentified · dataset imported (files already in LORIS are not duplicated) |
| **new visit for a participant already in** | the participant is redone (`changed since it was reidentified`) · the new visit imported |
| a file added, replaced (different size) or removed | the participant is redone and replaced as a whole in `deidentified-lorisid/` · imported |
| a held-back visit fixed | the participant is redone · the visit imported |
| `participants.tsv` edited | new rows: candidates created; an existing candidate whose dob / sex differ is reported, LORIS is not changed |
| a participant removed from the delivery | reported (`subjects in tracking are missing from source`); its reidentified copy stays until `--force` (section 8); **LORIS is not changed** |

---

## 8. When to use `--force`

Only the reidentifier has `--force`: it rebuilds the whole `deidentified-lorisid/bids/`
tree from the delivery. The previous tree stays in place until the rebuild has succeeded,
then is swapped as a whole.

| Use `--force` when | Why |
|---|---|
| a participant was removed from the delivery and its reidentified copy must go | a normal run only adds and replaces participants |
| a candidate's PSCID changed in LORIS | reidentified names must follow |
| the reidentified tree was edited or damaged by hand | rebuilt from the delivery |
| a file changed without changing its size | the change check compares paths and sizes |

Not needed for new participants, new visits, changed files, fixed visits, or after a
crash. After `--force`, run the import as usual.

The import has no `--force`: it re-imports whenever the dataset changed. To re-import an
unchanged dataset (e.g. after deleting data in LORIS), delete
`processed/bids/.bids_import_tracking.json`. The sync never needs forcing: it only matches
or creates.

---

## 9. After a crash or an interruption

**Run the same three steps again.** Nothing needs cleaning by hand.

| Interrupted during | State left | On the next run |
|---|---|---|
| sync | some candidates created | matched (not created twice); the rest created |
| sync stopped by a blocking problem, then reidentify and import run anyway | nothing created | reidentify: `NOT_SYNCED`, nothing copied · import: refused · fix the problem, re-run the three |
| reidentify (the pipeline killed, or the LORIS job failed) | output only in the hidden `.bids.reidentify-tmp-<run>` folder; `deidentified-lorisid/bids/` unchanged; nothing recorded | the leftover folder is removed, the participants are reidentified again |
| reidentify, before anything was ever finished | the import finds no scans and stops | — |
| import (the pipeline killed; the LORIS job keeps going) | the LORIS job finishes on its own | the dataset is imported again; files already in LORIS are skipped, **no duplicates** |
| import refused (`the last reidentifier run did not finish`) | nothing imported | run the reidentifier, then the import |

Tested: kill during reidentify → target untouched, import refused, rerun completes; kill
during import → rerun leaves the same files and sessions.

---

## 10. What Loris-MRI requires, and how the pipeline meets it

| Loris-MRI `bids_import` (backport 26.0-CBIG-29.0) requires | The pipeline |
|---|---|
| `participants.tsv` columns by exact lowercase name: `participant_id`, `cohort` (or `subproject`), `site`, `project`, `sex`, `dob` | the import passes a corrected copy (kept in `processed/bids/`): header names normalised, `cohort` / `site` / `project` from `candidate_defaults`, sex normalised |
| a `cohort` that exists in LORIS, to create a session | taken from `candidate_defaults.cohort`; the pre-check flags participants with none |
| sub- label = the candidate's PSCID (or CandID) | the reidentifier renames `sub-<ExternalID>` → `sub-<PSCID>` |
| visit = the `ses-` label, or the LORIS setting `default_bids_vl` | exact-visit rule (section 6); data with no `ses-` folder flagged by the pre-check |
| `dataset_description.json` | blocking check in the sync; pre-check |
| files the `bids_validator` accepts (others are not indexed → not imported) | the pre-check runs the same validator and lists them |

---

## 11. Logs, email, tracking, exit codes

| What | Where |
|---|---|
| run logs | `<project>/logs/bids/bids_<step>_run_<timestamp>.log` (+ `_errors_` when there are errors) |
| pre-check reports | `<project>/logs/test/bids_validation/` |
| reidentify record | `processed/bids/.bids_reidentify_tracking.json` (per participant: when, signature, visits held back; last run) |
| import record | `processed/bids/.bids_import_tracking.json` |
| corrected participants.tsv | `processed/bids/` (audit copy of what Loris-MRI received) |
| email | `notification_emails.bids` (`enabled`, `on_success`, `on_error`) in `project.json` |

Exit codes: `0` success, `1` errors, `2` usage error. Older record names
(`.bids_reidentifier_processed.json`, `.bids_import_processed.json`) are still read.

---

## 12. On the LORIS server

- CBIGR script API: `bidsreidentifier` and `bidsimport`, and the pipeline API user's
  permission to run scripts.
- `tools/OPEN/bids_reidentifier.php`: datasets with EEG need the one-line `handleEEG()` fix
  (`CBIGR-bids_reidentifier-eeg-fix.patch`); without it every dataset with an `eeg/`
  folder fails at reidentify.
- The Loris-MRI Python environment with `bids_validator` (comes with pybids), for the
  pre-check.
