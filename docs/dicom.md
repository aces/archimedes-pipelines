# DICOM ingestion — how to run it

Four scripts and one bundle that runs them in order:

```
deidentified-raw/  ──organize──►  processed/  ──sync──►  (candidates)  ──reidentify──►  deidentified-lorisid/  ──import──►  LORIS
  as delivered        one folder        participants.tsv     sessions + PSCID_CandID_Visit      the ONLY source
                      per study         → candidates         headers reidentified                 that is ingested
```

| # | Script | Does | Writes to LORIS |
|---|---|---|---|
| 0 | `run_dicom_validate.php` | **Checks the upload against the user guide** and writes a fix-it report for the site. Read-only | no (reads only) |
| 1 | `run_dicom_organize.php` | Optional EviData check, then groups files by StudyInstanceUID into `processed/` | no |
| 2 | `run_dicom_participant_sync.php` | **Pre-flight check of every mismatch**, then creates missing candidates from `participants.tsv` (never changes existing ones) | candidates |
| 3 | `run_dicom_reidentifier.php` | Creates each visit's session; copies studies to `deidentified-lorisid/` as `PSCID_CandID_Visit_MOD` | sessions |
| 4 | `run_dicom_import.php` | Archives each study through `POST /cbigr_api/script/importdicomstudy` | the import |
| — | `run_dicom_bundle.php` | Runs 1→4; ingests only if 1–3 all succeeded | — |

**Everything is a dry run unless you add `--confirm`.**

---

## 1. Which projects run

A project is processed only when **all** of these hold:

| Rule | Where |
|---|---|
| Collection `enabled: true` and project `enabled: true` | `config/loris_client_config.json` |
| `"modalities"` contains `"Imaging"` | the project's `project.json` |
| `project.json` exists and is valid JSON | project root |

`--all` and `--collection=X` only pick up enabled projects. Naming a disabled
project with `--project` runs it anyway, with a warning. A project without
`Imaging` is always skipped, with the reason printed. Every script applies the
same rule.

---

## 2. What the site delivers (user guide)

```
<project>/deidentified-raw/imaging/dicoms/
  participants.tsv                 participant_id  dob  sex
  sub-ARCHI0001/
    sessions.tsv                   session_id  (acq_date)   - or sub-ARCHI0001_sessions.tsv
    ses-V01/MR/   ses-V01/PET/     any layout below the visit folder
    ses-V02/MR/
```

`ses-` labels must be visits configured in LORIS, and each `ses-` folder must be
listed in the participant's `sessions.tsv` (preferred name; `sub-<ID>_sessions.tsv`
is accepted too). A visit that isn't listed isn't ingested. PatientName should be the
participant ID without `sub-`.

---

## 2a. Checking an upload before ingestion

```
php scripts/dicom/run_dicom_validate.php --collection=archimedes --project=ARCHI-TEST
php scripts/dicom/run_dicom_validate.php --collection=archimedes --project=ARCHI-TEST --offline   # no LORIS calls
```

Run it whenever a site uploads, before the bundle. It is read-only: the
organizer runs as a dry run into a working folder under `logs/dicom/` (removed
afterwards), and LORIS is only read. The upload, `processed/` and
`deidentified-lorisid/` are never touched.
It catches everything that would stop sync, reidentification or import, checked
against the user guide, and writes a report the site can fix their data from:
`logs/dicom/dicom_validation_<date>.txt` (or `--report=FILE`).

| Exit | Result | Meaning |
|---|---|---|
| 0 | READY | every examination can be ingested |
| 1 | NOT READY | something must be fixed, something will not be ingested, or there is nothing to ingest |
| 3 | NO PROBLEMS FOUND, BUT NOT EVERYTHING COULD BE CHECKED | LORIS could not be read (down, wrong credentials, or no `loris_api` permission). The data is not blocked for this; the report has a COULD NOT CHECK section for the ARCHIMEDES team |
| 2 | — | usage error |

| Guide § | Checked |
|---|---|
| 1 Folders | `participants.tsv` at the top; `sub-<ID>/ses-<visit>/`; folders or files outside that shape; empty visits; a `sub-` folder inside another |
| 2 Naming | IDs and visit labels letters and digits only; visit configured in LORIS for the project |
| 3 Spreadsheets | tab-separated (not CSV or `.xlsx`); not empty, header-only or unreadable; `participant_id`, `dob`, `sex` present and valid (with `candidate_defaults` and `sex_mappings`); duplicate or empty IDs; an `external_id`/`ExtStudyID` that differs from the folder ID (reidentification would not find the participant); **orphans both ways** (a folder with no row, a row with no folder); the sessions file present (`sub-<ID>/sessions.tsv`, preferred, or `sub-<ID>/sub-<ID>_sessions.tsv`; with both, `sessions.tsv` is read and both are reported), its `session_id` matching the `ses-` folders, `acq_date` as YYYY-MM-DD |
| 4 Headers | **PatientName = `participant_id` without `sub-`**, and which participant it belongs to when it doesn't match; one PatientName per study; no `\ ^ =` in it (reidentification can't write those); Modality and StudyDate present; SeriesDescription kept; text that is not UTF-8; identifying fields filled in (AccessionNumber, StudyDescription, InstitutionName, physician and operator names, PatientComments …; one file per folder is sampled) |
| Files | files or folders the pipeline user cannot read (the organizer would skip them and lose those images) |
| 5 Examinations | unreadable files, an examination in two folders, conflicting copies (the same checks as the real run) |
| LORIS | project (`candidate_defaults` / `participants.tsv`) exists; cohort is one of the project's cohorts; site is a LORIS site name the pipeline user can see; **a participant already in LORIS still has the same `dob` and `sex` in `participants.tsv`** (ingestion never changes LORIS, so a difference would stay) (LORIS 26 API: `/projects`, `/projects/{name}`, `/sites`, `/candidates/{CandID}`) |

A missing sessions file (`sessions.tsv` or `sub-<ID>_sessions.tsv` in the participant
folder), or a `ses-` folder not listed in it, is a
MUST FIX: the bundle does not ingest that visit until it is listed.

The report is in these parts:
- **MUST FIX**: blocks ingestion of the affected data.
- **NOT INGESTED**: cannot be matched to a participant or visit.
- **PLEASE CHECK**: warnings.
- **COULD NOT CHECK**: for the ARCHIMEDES team, not the site.
- **EXAMINATIONS**: each one READY or BLOCKED with the reason, including any the organizer rejected.

Each item says where the problem is, what is wrong, and how to fix it:

```
 RESULT: NOT READY - 10 problem(s) to fix; 1 examination(s) ready, 8 blocked

MUST FIX - blocks ingestion of the affected data
   6. [files] sub-ARCHI0002/ses-V02
      PATIENTNAME MISMATCH - PatientName "ARCHI0001" does not match the folder sub-ARCHI0002; ...
      -> PatientName 'ARCHI0001' is participant sub-ARCHI0001 in participants.tsv - if these
         files are sub-ARCHI0001's, move them to sub-ARCHI0001/; otherwise set PatientName to ARCHI0002
  11. [LORIS] participants.tsv (sub-ARCHI0002)
      site 'UOH' is not a LORIS site name the pipeline user can see
      -> use the site name 'University of Ottawa Heart Institute (UOHI)'

NOT INGESTED - cannot be matched to a participant or visit
   3. [participants.tsv] sub-ARCHI0009/
      no row in participants.tsv - nothing in this folder will be synced, reidentified or ingested
      -> add a row for sub-ARCHI0009 (participant_id, dob, sex), or remove the folder

EXAMINATIONS
  READY    sub-ARCHI0001/ses-V02  MR        2 series     8 files
  BLOCKED  sub-ARCHI0002/ses-V01  MR        3 series    12 files  - see MUST FIX
  BLOCKED  sub-ARCHI0002/ses-V02                     (not organized)  - see MUST FIX
```

Not checked here (the pipeline handles these at run time): a StudyInstanceUID
already in LORIS from another project (import refuses it); whether the pipeline
user is affiliated with the project (the API does not show it, see §11); the
project's external name for `candidatesPlus`; private vendor tags; and anything
else that needs the LORIS database. The check reads every file once, as the
real organize step does, so a large upload takes about as long as organizing it.

---

## 3. Quick start

```bash
cd archimedes-pipelines
B="php scripts/dicom/run_dicom_bundle.php"

$B --collection=archimedes --project=FDG-PET -v             # 1. preview (dry run)
$B --collection=archimedes --project=FDG-PET --confirm -v   # 2. run it
$B --all --confirm                                          # 3. cron: every enabled imaging project
```

Re-running the same command after a new deposit processes only what is new or
changed. Nothing already in LORIS is duplicated.

---

## 3a. Worked example — the workflow in order

Output from a real run (mock LORIS, the user-guide mock data: participants
ARCHI0001 and ARCHI0002, 4 studies). Lines are shortened.

**Order:**

| # | Script | Does | LORIS |
|---|---|---|---|
| 1 | `run_dicom_organize.php` | checks the files, one folder per study in `processed/` | no |
| 2 | `run_dicom_participant_sync.php` | pre-flight checks, then creates or links candidates | candidates |
| 3 | `run_dicom_reidentifier.php` | creates sessions, reidentifies to `PSCID_CandID_Visit` in `deidentified-lorisid/` | sessions |
| 4 | `run_dicom_import.php` | ingests from `deidentified-lorisid/` | the import |
| — | `run_dicom_bundle.php` | runs 1 → 4; never ingests unless 1–3 succeeded | — |

### Each script on its own

```bash
C="--collection=archimedes --project=ARCHI-TEST"
```

**1. Organize**
```
$ php scripts/dicom/run_dicom_organize.php $C --confirm
=== archimedes/ARCHI-TEST ===
  evidata: SKIPPED (global evidata.enabled is false)
  4 study/studies, 4 matched to a participant, 0 unlinked (no matching participant)
  modalities: MR:6  PT:1
```
*Study* = one examination (all files with one StudyInstanceUID). *Matched to a
participant* = its `sub-` folder has a row in `participants.tsv`, so it can go
on to sync, reidentify and import. *Unlinked* = no matching participant, which
is only reported.

**2. Participant sync** — pre-flight first, then candidates
```
$ php scripts/dicom/run_dicom_participant_sync.php $C --confirm
  2 participant row(s) in participants.tsv
  enriched from candidate_defaults: site, cohort, project, external_id
  PRE-FLIGHT (before any candidate is created): 0 error(s), 0 warning(s)
    ARCHI0001        created ARCHI0001 (300001)
    ARCHI0002        created ARCHI0002 (300002)
  0 matched, 2 created, 0 skipped, 0 failed
```

**3. Reidentify**
```
$ php scripts/dicom/run_dicom_reidentifier.php $C --confirm
  4/4 mapped, 4 matched, 0 created, 0 unlinked (not processed)
  28 file(s) had headers rewritten
```
`deidentified-lorisid/imaging/dicoms/` now holds `ARCHI0001_300001_V01_MR`,
`ARCHI0001_300001_V01_PT`, `ARCHI0001_300001_V02_MR` and `ARCHI0002_300002_V01_MR`,
with PatientName = `PSCID_CandID_Visit` in every file.

**4. Import**
```
$ php scripts/dicom/run_dicom_import.php $C --confirm
  Source: …/ARCHI-TEST/deidentified-lorisid/imaging/dicoms
  Found 4 studies
    Launched — job_id=1 … → SUCCESS        (×4, each --insert --session)
```

The four in a row, stopping at the first failure:
```bash
php scripts/dicom/run_dicom_organize.php $C --confirm \
 && php scripts/dicom/run_dicom_participant_sync.php $C --confirm \
 && php scripts/dicom/run_dicom_reidentifier.php $C --confirm \
 && php scripts/dicom/run_dicom_import.php $C --confirm
```

### The bundle (the normal way)

```bash
B="php scripts/dicom/run_dicom_bundle.php --collection=archimedes --project=ARCHI-TEST"
```

**Dry run** — `$B` — every step previews, nothing is created:
```
--- run_dicom_organize.php           Dry run complete. No studies were written.
--- run_dicom_participant_sync.php   PRE-FLIGHT … 0 error(s) · ARCHI0001 would create · ARCHI0002 would create
--- run_dicom_reidentifier.php       DRY RUN - no files will be written
--- run_dicom_import.php             DRY RUN …
```

**Real run** — `$B --confirm`:
```
--- run_dicom_organize.php           4 study/studies: 4 matched to a participant, 0 unlinked
--- run_dicom_participant_sync.php   PRE-FLIGHT 0 error(s) · 2 created
--- run_dicom_reidentifier.php       4/4 mapped
--- run_dicom_import.php             SUCCESS ×4 · Result: COMPLETED SUCCESSFULLY
```

**Next deposit** — a new `ses-V02` for ARCHI0002 — same command, only the new study goes in:
```
organize    5 study/studies: 5 matched to a participant
sync        2 matched, 0 created
reidentify  1/5 mapped                     (the new visit only)
import      4 × SKIPPED — manifest unchanged · ARCHI0002_300002_V02_MR ingested
```

**Re-run with nothing changed:**
```
organize    UNCHANGED since … nothing to re-ingest
import      5 × SKIPPED
```

**When there are data issues** — every issue first, then you choose
(section 4):
```
DATA ISSUES - found before any candidate is created (4):
  sub-ARCHI0002/ses-V01   - PATIENTNAME MISMATCH …
  sub-ARCHI0003_ses-V01_MR - sex 'X' cannot be mapped …
  …
GOOD: 2 participant study/studies with no issue
Continue with the 2 good study/studies? … [y/N]
```

Unattended: `$B --all --confirm` (aborts a project with data issues), or
`$B --all --confirm --continue-good` (goes ahead with the good studies).

---

## 4. The bundle

```bash
php scripts/dicom/run_dicom_bundle.php <selection> [options]
```

| Selection | |
|---|---|
| `--all` | every enabled collection and project (with Imaging) |
| `--collection=NAME` | every enabled project in one collection |
| `--collection=NAME --project=NAME` | one project |

| Option | Effect |
|---|---|
| `--confirm` | execute (default: dry run of every step) |
| `--dry-run` | explicit dry run; wins over `--confirm` |
| `-v` | verbose, passed to every step |
| `--no-unlinked` | stop the project if any study is unlinked (they are never ingested either way) |
| `--continue-good` | when data issues are found, continue with the good studies without asking (unattended runs); without it the bundle asks, and with no answer it aborts |
| `--force` | ignore hashes: re-organize, re-map, re-import (`--update --overwrite`) |
| `--steps=organize,sync,reidentify,import` | run only these |
| `--skip=LIST` | run all but these (`--skip=import` = prepare everything, ingest nothing) |
| `--evidata` / `--no-evidata` | force the EviData check on / off for this run |
| `--config=FILE` | default `config/loris_client_config.json` |

The bundle hands each script its own options: selection, `--config`,
`--confirm` and `-v` go to all four. `--force` goes to organize, reidentify and
import. `--evidata` goes to organize. The import also gets `--source-subdir`.
Every command it ran is in `logs/dicom/dicom_bundle_<date>.log`.

**Data issues: report everything, then you decide.** After organize and
before any candidate is created, the bundle collects **every** issue (organize
plus the pre-flight below) and prints one report:

```
DATA ISSUES - found before any candidate is created (4):
  sub-ARCHI0001_ses-V02_MR
    - organize: sub-ARCHI0001/ses-V02: 1 file(s) look like DICOM but could not be read ...: broken.dcm
  sub-ARCHI0002/ses-V01
    - organize: PATIENTNAME MISMATCH - PatientName "ARCHI0009" does not match the folder sub-ARCHI0002 ...
  sub-ARCHI0003_ses-V01_MR
    - sub-ARCHI0003: sex 'X' cannot be mapped to Male, Female or Other ...
  sub-ARCHI0001_ses-V08_MR
    - sub-ARCHI0001/ses-V08: visit 'V08' is not configured in LORIS ...
GOOD: 2 participant study/studies with no issue: sub-ARCHI0001_ses-V01_MR, sub-ARCHI0001_ses-V01_PT

Continue with the 2 good study/studies? The 4 with issues are NOT created,
reidentified or ingested in this run; fix them and re-run to add them. [y/N]
```

| Answer | Result |
|---|---|
| **yes** (or `--continue-good`) | only the good studies go on. Nothing is created, reidentified or ingested for the ones with issues, and the email names them. Re-run after fixing to add them. |
| **no**, or no answer (unattended without `--continue-good`) | **ABORTED**: nothing created or ingested for the project. Exit `3`, `FAILED` email. |
| issue affecting the **whole project** (`participants.tsv` unreadable or missing a column, LORIS visits unreadable) | **aborts without asking**, even with `--continue-good` |

An issue excludes exactly what it affects:
- a participant issue (`sex`, `dob`, identifier) → all of that participant's studies
- a file issue → the studies in that `sub-`/`ses-` folder
- a study issue (PatientName mismatch, unconfigured visit, …) → that study

A participant left with no good study isn't created. In a dry run you get the
same report, and the preview continues without the bad studies.

**Stop rule.** A LORIS error while creating a candidate or session still stops
the project **before the import**, with nothing ingested. The run exits `1`
and sends a `FAILED … (stopped at <step>)` email.

**Pre-flight — the checks.** Participant sync runs them too, before it creates
anything, so a stand-alone sync is protected the same way. Run on its own it
doesn't ask: any issue and it creates nothing. Without `--confirm` it's the
stand-alone check.

| Checked | Error when |
|---|---|
| `participants.tsv` | unreadable, no `participant_id`, duplicate rows, a needed column missing and not in `candidate_defaults` |
| `sex` | cannot be mapped to Male / Female / Other — `M`, `male`, `f`, `u`, `n/a` … are accepted, plus `project.json` `sex_mappings` (e.g. `{"1": "Male", "2": "Female"}`), as in clinical |
| `dob` | not a date LORIS accepts after normalisation (`YYYY-MM-DD`, `YYYY-MM`, `YYYY`, `M/D/YYYY`, `D/M/YYYY`; `date_input_format` decides M/D vs D/M) |
| identifiers | `sub-` or `ses-` label not letters and digits only |
| visits | a participant study in a `ses-` folder that is not a visit configured in LORIS |

Missing `site`, `cohort`, `project`, `sex`, `dob` are taken from
`project.json` `candidate_defaults`, the same as clinical. Warnings, which don't
stop the run, cover unlinked studies (section 7), rows with no
imaging, and empty PatientName.

Organize checks the files themselves first (section 9). Its issues join the
same report.

---

## 5. Each script on its own

All four take `--all | --collection=X [--project=Y]`, `--config=FILE`,
`--confirm`, `--dry-run`, `-v`, `--help`. Without `--confirm` they only preview.
Run them in order; only the bundle enforces the stop rule.

Every DICOM and BIDS script stops with exit `2` on an option it does not know
(`Unknown option: --no-import`), instead of ignoring it and running with the
default. Removed in v15: bundle `--stop-after` (use `--steps`), `--no-organize`
`--no-sync` `--no-reidentify` `--no-import` (use `--skip`), `--yes` and
`--no-session`; import `--session`, `--session-linked`, `--no-session`;
organize `--no-api`.

```bash
C="--collection=archimedes --project=FDG-PET"
php scripts/dicom/run_dicom_organize.php         $C --confirm \
 && php scripts/dicom/run_dicom_participant_sync.php $C --confirm \
 && php scripts/dicom/run_dicom_reidentifier.php     $C --confirm \
 && php scripts/dicom/run_dicom_import.php           $C --confirm
```

| Script | Extra options |
|---|---|
| `run_dicom_organize.php` | `--force` (ignore hashes and the upload quiet period) · `--no-unlinked` (skip studies with no participant) · `--evidata` / `--no-evidata` · `--move` (move, don't copy) · `--dry-run` |
| `run_dicom_participant_sync.php` | `--all-participants` (every row, not just subjects with imaging) · `--dry-run` (= the pre-flight check alone) |
| `run_dicom_reidentifier.php` | `--force` (re-map every study; also re-assigns moved studies) · `--create-candidates` (create here instead of in sync) · `--dry-run` |
| `run_dicom_import.php` | `--force` (re-import done studies) · `--update` · `--overwrite` · `--source-subdir=PATH` (default `deidentified-lorisid/imaging/dicoms`; anything else is warned) · `--profile=NAME` · `--dry-run`. Reidentified participant studies always get `--session`; unlinked studies are never imported |

---

## 6. Recipes

| I want to… | Command |
|---|---|
| See what would happen | `$B $C -v` |
| Ingest a new or updated deposit | `$B $C --confirm -v` |
| Prepare everything but ingest nothing | `$B $C --confirm --skip=import` |
| Ingest what is already prepared | `$B $C --confirm --steps=import` |
| Only organize (check the grouping) | `$B $C --confirm --steps=organize` |
| Retry after a failure | fix the cause, re-run `$B $C --confirm` (failed studies are retried automatically) |
| Redo everything | `$B $C --confirm --force` |
| Skip EviData this once | `$B $C --confirm --no-evidata` |
| Every enabled imaging project, unattended | `$B --all --confirm` (aborts a project that has data issues) |
| Same, but go ahead with the good studies when some have issues | `$B --all --confirm --continue-good` |
| Stop the project if any study is unlinked | `$B $C --confirm --no-unlinked` |

`B="php scripts/dicom/run_dicom_bundle.php"` and `C="--collection=archimedes --project=FDG-PET"`.

---

## 7. Sessions and unlinked studies

| Study | What happens |
|---|---|
| **matched** — its `sub-` folder has a matching row in `participants.tsv`, its `ses-` folder is listed in the participant's `sessions.tsv` (or `sub-<ID>_sessions.tsv`) and is a visit configured in LORIS | candidate created or matched, session created, reidentified, ingested with `--insert --session` |
| **unlinked** — no matching participant (anything else) | **nothing**: not synced, not reidentified, not ingested. Reported as `UNLINKED`, with the reason and what to do |

There is no phantom handling for now, because we can't tell a phantom from a
participant scan. A `sub-phantomACR` folder is just another participant
folder: with a `participants.tsv` row it's processed like any participant,
without one it's unlinked.

The run prints, and the email repeats:

```
UNLINKED 2 study/studies - no matching participant: not synced, not reidentified, NOT ingested:
    sub-ARCHI0003/ses-V01  [MR]
      no matching participant in participants.tsv - add sub-ARCHI0003 to participants.tsv (its ses- folder
      must be a visit configured in LORIS) and re-run; otherwise it stays reported, nothing is done with it
    scannerQA_20250802  [MR]
      not inside a sub-<ID>/ses-<visit>/ folder - move it into one, add the participant to participants.tsv
      and re-run; otherwise it stays reported, nothing is done with it
```

| Reason | What to do |
|---|---|
| no matching participant in `participants.tsv` | add the participant's row; its `ses-` folder must be a configured visit |
| no `participants.tsv` in the delivery | add the file with a row for the participant |
| not inside a `sub-<ID>/ses-<visit>/` folder | move it into one, and add the participant to `participants.tsv` |
| no `ses-<visit>` folder | move it into `sub-<ID>/ses-<visit>/` (a configured visit) |
| no sessions file in `sub-<ID>/` | add `sub-<ID>/sessions.tsv` (or `sub-<ID>_sessions.tsv`), one row per visit (`session_id`, e.g. `ses-V01`) |
| visit not listed in the sessions file | add a row for it (`session_id ses-<visit>`) |

Once it's fixed, the next run processes the study like any other. Until then
it stays in the raw delivery, untouched, and is reported on every run.

**No session, no import (for now).** As in BIDS, where every scan sits under a
`sub-`/`ses-` folder, every DICOM study is ingested attached to its LORIS
session. Right before a study would be imported, the import checks it has one:

| Check | When it fails |
|---|---|
| the reidentifier's `<study>.provenance.json` says *linked*, with a CandID and a visit | not reidentified, or no CandID/visit recorded |
| that session exists in LORIS (`GET candidates/{CandID}/{Visit}`, real runs only, once per session) | `404` from LORIS |

Then the study is **not imported**: `WARNING NO SESSION - <reason> - not
ingested`. It's listed in the run summary and the email (`SUCCESS (warnings)`),
and it isn't recorded, so the next run imports it once the session exists.
A session deleted in LORIS is **not** recreated by a normal re-run (the
reidentifier skips studies it already did): create the visit in LORIS, or run
`run_dicom_reidentifier.php --force` (it recreates the session), then re-run
the bundle. A LORIS answer
other than 200/404 (slow, 5xx) doesn't block; the import then reports its own
error if the session really is missing. A dry run doesn't log in, so it checks
the sidecar only.

---

## 8. Incremental deposits — re-run, it catches up

| Deposit | Result on re-run |
|---|---|
| nothing changed | project skipped in seconds; nothing sent |
| new visit for an existing participant | session created; that study ingested |
| new participant (folder + `participants.tsv` row) | candidate + session created; ingested |
| participant folder delivered before its `participants.tsv` row | reported `UNLINKED`, nothing done; once the row arrives: candidate + session created, ingested |
| scanner dump without `sub-`/`ses-` folders | reported `UNLINKED`, never ingested; move it under `sub-<ID>/ses-<visit>/` and add the participant to ingest it |
| new series / extra files in a study | re-archived in place (`--update --overwrite`), same name |
| a file removed from a study | re-archived; LORIS file list replaced |
| `participants.tsv` edited (even same size) | detected. New rows: candidate created and its studies ingested. For a participant already in LORIS, a different `dob`/`sex` is a **warning** (run, email by name, validator MUST FIX); LORIS is **not** changed and the imaging still goes in |
| new `ses-` folder **and** its `sessions.tsv` row | session created, ingested |
| new `ses-` folder, **no** `sessions.tsv` row yet | `UNLINKED` (visit not listed), nothing created or ingested; once the row is added: session created, ingested |
| `sessions.tsv` edited only (e.g. `acq_date`) | detected; nothing re-imported |
| a whole study / participant removed | working copies removed; **LORIS is not changed**; reported every run as "still in LORIS" |
| removed study re-deposited unchanged | recognised; nothing re-imported |
| `participants.tsv` row removed for an ingested study | kept as matched, not re-imported, warning |
| study moved to another visit / participant folder | kept as is in LORIS, warning; `--force` re-assigns |
| a study failed in LORIS | others ingested (PARTIAL); the failed one is retried next run |
| an import still running after the 4-hour watch (ingestion can take hours) | recorded as still running; next run asks LORIS — running: left alone, done: recorded, error: retried. **Never started twice** |
| a visit folder that is not configured in LORIS | pre-flight error, nothing created; add the visit in LORIS or rename the folder |
| a study's session was deleted in LORIS | `NO SESSION` warning, not imported (a normal re-run does not recreate it); `run_dicom_reidentifier.php --force` or create the visit, then re-run: imported |

To really delete a study from LORIS, use LORIS-MRI
`tools/delete_imaging_upload.pl` (it backs up what it deletes).

---

## 9. Safety rules (what is guaranteed)

- **Ingestion only from `deidentified-lorisid/`** — never from the raw delivery or `processed/`.
- **Only after organize, sync and reidentify all succeeded**, and only with `--confirm`.
- **One StudyInstanceUID = one LORIS archive.** "Already inserted" becomes an update **only** for a study this pipeline imported for this project. A UID that came from another project or a manual import is refused, never overwritten.
- **Never downgraded or moved silently.** A reidentified study keeps its PSCID and visit.
- **One run per project at a time** (lock). A second run is skipped.
- **Uploads in progress are left alone:** if anything in the delivery changed in the last `imaging.delivery_quiet_minutes` (default 5), the project waits for the next run. `--force` overrides this.
- **Long imports are never duplicated.** Ingestion runs under the LORIS server process manager and can take hours. If the pipeline stops watching a job (4-hour limit, or LORIS stops answering), the study is recorded as *still running*. The next run checks that job before doing anything with the study.
- **Data issues are reported together, before anything is created** (section 4). The project then aborts, or continues with the good studies only, if you answer yes or pass `--continue-good`. Data issues are:
  - a PatientName that doesn't match its `sub-` folder (case ignored), or one study with two PatientNames — **PATIENTNAME MISMATCH**. An empty PatientName is only a warning, since it can't be checked
  - any pre-flight check (section 4)
  - a study in two participant or visit folders
  - a file that looks like DICOM but can't be read (truncated, empty, corrupt)
  - one SOPInstanceUID used by two different files
- **Unlinked studies are never ingested** — only a study matched to a `participants.tsv` row with a configured visit goes in.
- **Exact duplicate files** in the same visit are archived once, with a warning.
- **Non-DICOM files** (`README.txt`, `.DS_Store`, `DICOMDIR`) are ignored.

---

## 10. Email and logs

One email per project run (`project.json` → `notification_emails.dicom`):

| Subject | Meaning |
|---|---|
| `SUCCESS` | all that needed ingesting went in |
| `SUCCESS (warnings)` | same, but something needs a look (unlinked studies, excluded studies, removals, moves) |
| `PARTIAL` | some ingested, some failed — the failed studies are listed |
| `FAILED` | nothing ingested (stopped before ingestion, aborted, or all failed) |

The body has counts plus the failed studies only. EviData gets one line, and
only when it's on for the project's imaging. Details are in:

| File | |
|---|---|
| `logs/dicom/dicom_bundle_<date>.log` | the whole bundle run, every command and its output |
| `logs/dicom/dicom_import_run_<ts>.log` | the import, with the run summary (names behind every count) |
| `logs/dicom/dicom_import_errors_<ts>.log` | import errors, in full (see below) |
| `processed/imaging/dicom_studies.json` | organized studies, link status, problems |
| `deidentified-lorisid/imaging/dicom_studies_lorisid.json` | reidentified studies, problems |

Tracking files (what was already done; hashes decide what is redone) — one
name pattern, `.<modality>_<step>_tracking.json`, as in clinical:

| File | Was (before v15) |
|---|---|
| `processed/imaging/.dicom_organize_tracking.json` | (unchanged) |
| `deidentified-lorisid/imaging/.dicom_reidentify_tracking.json` | `.dicom_mapping_tracking.json` |
| `processed/imaging/.dicom_import_tracking.json` | `.dicom_import_processed.json` |
| `processed/bids/.bids_reidentify_tracking.json` | `.bids_reidentifier_processed.json` |
| `processed/bids/.bids_import_tracking.json` | `.bids_import_processed.json` |

Existing state carries over on its own: while only the old name exists it is
read; the next time that step records something (a real run with work to do)
it writes the new name and then removes the old file. A dry run reads it and
changes nothing. Deleting a tracking file still means
"redo everything" for that step.

For each failed study, `dicom_import_errors_<ts>.log` (and `ERROR-DETAIL` lines in
`dicom_import_run_<ts>.log`) has the full, untruncated detail from the script API:

- **What was sent:** `source` and `flags`.
- **What came back:** `http_status`, the endpoint's `errors` JSON (for a 400/403
  at launch), or the raw body if the server didn't return JSON.
- **For a job that ran:** `job_id`, `state`, `exit_code`, `end_time` and the
  complete `import_dicom_study.py` output (`error_detail`).
- **For an auto-retry** (already / not inserted): the first attempt's output,
  logged as `FIRST-ATTEMPT` in the run log.

Only the email and the console show a shortened message. The `job_id` is also
the row in LORIS `server_processes`, for anything more.

---

## 11. Settings

| Setting | Where | Default |
|---|---|---|
| `enabled` (collection, project) | `loris_client_config.json` | — (missing = off) |
| `imaging.dcmdump_path`, `imaging.dcmodify_path` | `loris_client_config.json` | `/usr/bin/…` |
| `imaging.delivery_quiet_minutes` | `loris_client_config.json` | `5` (`0` = off) |
| `modalities: ["Imaging", …]` | `project.json` | required |
| `candidate_defaults.project / site / cohort / sex / dob` | `project.json` | fill empty `participants.tsv` columns |
| `sex_mappings` (e.g. `{"1": "Male"}`) | `project.json` | added to the built-in M/F/Other mapping |
| `date_input_format` | `project.json` | how `dob` like `03/04/1950` is read |
| `evidata.enabled`, `evidata.imaging.enabled` | `project.json` (+ `config/evidata_config.json`) | off |
| `notification_emails.dicom.{enabled,on_success,on_error}` | `project.json` | off |

**On the LORIS server: what the pipeline API user (`api.username`) needs.**
The CBIGR override of the LORIS 26 `api` module answers **403 to every `/api/...`
call (except login) without `loris_api`**. The endpoints themselves are unchanged
LORIS 26 code.

| Used by | Call | Permission (or superuser) |
|---|---|---|
| sync, reidentify, validator | `/api/v0.0.4-dev/...` (projects, sites, candidates, visit sessions) | **`loris_api`** |
| sync | `POST /cbigr_api/candidatesPlus` | `access_all_profiles`, or `data_entry` at a study site; **and affiliated with the candidate's site and project** |
| sync | `POST /cbigr_api/externalToInternalIdMapper` | `data_entry` |
| import | `/cbigr_api/script/importdicomstudy`, `/cbigr_api/script/job/{id}` | current branch: `pipeline_imaging_scripts` (SQL patch `2026-10-16_pipeline_imaging_scripts_permission.sql`); older branch (backport): `imaging_uploader_allsites` |

Without these the call fails with HTTP 403, and the error log shows the response.
The validator reports a 403 with the permission to add. It checks that the site
is one the user is affiliated with (`/sites` lists only those), but it cannot see
the user's project affiliation, because `/projects` lists every project.
`deidentified-lorisid/` must also be readable by the web server
user at the same path on the LORIS host: the endpoint checks `source` there.
Both LORIS-MRI layouts work; the endpoint finds `import_dicom_study.py` itself.

---

## 12. Exit codes and common messages

`0` ok · `1` failure, or the run went ahead without the studies that had issues (or the import's usage error) · `2` usage error · `3` aborted — data issues found and not continued (bundle)

| Message | Do |
|---|---|
| `STOP <step> failed - INGESTION ABORTED` | read the error above it, fix, re-run |
| `DATA ISSUES … (N)` then `ABORTED` | fix every listed line and re-run, or answer yes / `--continue-good` to go ahead with the good studies |
| `PRE-FLIGHT … N error(s)` (stand-alone sync) | fix every listed line; nothing was created |
| `PATIENTNAME MISMATCH` | check the files really belong to that participant; fix PatientName or move them |
| `STILL RUNNING in LORIS - job N` | nothing to do; the next run picks up the result |
| `WARNING NO SESSION - … - not ingested` | not reidentified: re-run the bundle. Session missing in LORIS: create the visit, or `run_dicom_reidentifier.php --force`; the next run imports it |
| `UNLINKED N study/studies` | follow the advice under each one: add the `participants.tsv` row, or move it into `sub-<ID>/ses-<visit>/` |
| `WAIT … probably still uploading` | wait, or `--force` if the upload is complete |
| `SKIP …: project.json "modalities" does not include "Imaging"` | add `Imaging`, or it's not an imaging project |
| `REFUSED: StudyInstanceUID … already in LORIS but was not imported by this pipeline` | find where it came from; `--force` only if this project should own it |
| `moved in the delivery … kept, NOT moved in LORIS` | undo the move, or re-run with `--force` |
| `REMOVED from delivery … STILL IN LORIS` | intended? delete in LORIS with `delete_imaging_upload.pl` |
| `another bundle run is in progress` | wait for it to finish |

---

## 13. Reusing the participant step for other modalities

The participant logic is shared by DICOM, BIDS, PNG and any other modality with
`sub-`/`ses-` folders and a `participants.tsv`. Each modality reuses it rather
than copying it:

| Shared piece | Does | Used by |
|---|---|---|
| `ParticipantsTsv::locate()` | finds the file: `bids`, `dicom`, or any folder under `deidentified-raw/` (`png`, `imaging/pngs`, …) | all |
| `ParticipantsTsv::readRows()` | parses the file (tabs, CRLF, Excel BOM, short/long rows; an empty first cell never shifts the columns) | DICOM, BIDS |
| `ParticipantSync::load()` | rows filled from `candidate_defaults`, plus the LORIS columns still missing | DICOM |
| `ParticipantsCheck::check()` | pre-flight: columns, identifiers, sex, dob, visit labels, visits configured in LORIS | DICOM |
| `LorisApiClientAdapter::normaliseSexValue()` | sex → Male/Female/Other, plus `project.json` `sex_mappings` | DICOM, BIDS |
| `ParticipantSync::EXTERNAL_ID_COLUMNS` / `externalId()` | `external_id`, `ExtStudyID`, `study_id`, … | DICOM, BIDS |
| `ParticipantSync::orphans()` / `orphanFolders()` | `sub-` folders with no row, rows with no folder | DICOM, BIDS |
| `ParticipantSync::nestedSubjectDirs()` | a `sub-` folder inside another one | DICOM (BIDS has its own, with the file-name check) |
| `ParticipantSync::sync()` | find by external ID, create if missing (dry run: report only); warns when an existing candidate's dob/sex differ | DICOM |
| `ParticipantSync::lorisDifferences()` | `participants.tsv` dob/sex vs the candidate in LORIS, same normalisation as creation (read-only) | DICOM sync, validator |
| `SessionsTsv::listed()` / `check()` | `sub-<ID>/sessions.tsv` (preferred) or `sub-<ID>_sessions.tsv`: which visits are listed (only those are ingested), and every mismatch with the `ses-` folders (missing file, no `session_id`, visit not listed, row with no folder, bad `acq_date`) | DICOM organizer, validator (BIDS: ready to use, same file) |
| `LorisApiClientAdapter` (`src/Endpoints/`) | the LORIS calls (mapper, candidatesPlus, candidates, visits, sessions) | DICOM, BIDS |

DOB everywhere: `LORIS\Utils\Dob` (unchanged).

**BIDS** (`BidsParticipantSync`) uses the shared parser, sex mapping, external-ID
columns and orphan check. It keeps its own rules: PSCID = external ID,
CandidatesPlus with the legacy fallback, the LORIS project check, a warning for
each default used, the subject-containment check, and skipping a bad participant
while carrying on. Compared with before, a BIDS run differs only in two ways:

- a row with an empty `participant_id` is reported (`Line N: missing participant_id`)
  instead of being read with its columns shifted
- `sex_mappings` is honoured (`1`/`2` and so on), as in clinical and DICOM

**A new modality:**

```php
$tsv    = ParticipantsTsv::locate($projectPath, 'imaging/pngs');
$loaded = ParticipantSync::load($tsv, $projectConfig);          // stop if $loaded['missing']
$check  = ParticipantsCheck::check($tsv, $projectConfig, $sessions,   // ['ARCHI0001' => ['V01']]
            fn () => $api->getConfiguredVisitLabels(), null, 'PNG data');  // stop on $check['issues']
$orphans = ParticipantSync::orphans($loaded['roster'], $deliveryDir);
$n = ParticipantSync::sync($loaded['roster'], $api, $wanted, [], $confirm,
        fn ($line) => fwrite(STDERR, "$line\n"), $logger);
```

The modality supplies only `$sessions`, the participants and visits it found in
its own delivery. `scripts/dicom/run_dicom_participant_sync.php` is a full example.

---

## 14. Where the code is

BIDS, DICOM and the shared imaging code each have their own folder. Clinical
and participant metadata are where they always were.

```
src/Pipelines/
  Dicom/    LORIS\Pipelines\Dicom   DicomStudyOrganizer, DicomPreflight, DicomReidentifier,
                                    DicomHeaderWriter, DicomImportPipeline, DicomEviDataExtract
  Bids/     LORIS\Pipelines\Bids    BidsParticipantSync, BidsReidentifier, BidsImportPipeline
  Shared/   LORIS\Pipelines\Shared  ParticipantsTsv, ParticipantSync, ParticipantsCheck,
                                    SessionsTsv (sessions.tsv rules),
                                    StudyFingerprint, FingerprintTracker, TrackingFile,
                                    Cli (strict option check for every BIDS/DICOM script)
  ClinicalPipeline.php, ParticipantMetadataPipeline.php      (clinical - unchanged)
src/Endpoints/  LORIS\Endpoints     the API clients: LorisApiClientAdapter + LorisApiClientInterface
                                    (BIDS/DICOM), ClinicalClient, EviDataClient (unchanged)
src/Utils/, src/Database/                                    (shared with clinical - unchanged)

scripts/
  dicom/    run_dicom_validate, run_dicom_organize, run_dicom_participant_sync,
            run_dicom_reidentifier, run_dicom_import, run_dicom_bundle
  bids/     run_bids_participant_sync, run_bids_reidentifier, run_bids_import
  run_clinical_pipeline.php, run_participant_metadata_pipeline.php   (unchanged)
  test/     connection and setup checks (unchanged)

docs/dicom.md   this guide
```

A new modality (PNG …) gets `src/Pipelines/<Modality>/` and `scripts/<modality>/`
and builds on `Shared/` (section 13). A class moves to `Shared/` when a second
modality uses it.
