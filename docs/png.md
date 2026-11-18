# PNG ingestion — how to run it

PNG images (echocardiogram stills, ECG traces, …) shared by a site, matched to a
LORIS participant and visit, and inserted into LORIS through the CBIGR script API
(`POST /cbigr_api/script/importpngdataset` → C-BIG `import_png_dataset.py`).
Same shape as DICOM ([docs/dicom.md](dicom.md)), minus the organize step (a PNG
delivery is already one folder per visit): **validate → participant sync →
reidentify → import**, and one bundle that checks the delivery, then runs the
three scripts in order.

```
deidentified-raw/imaging/png/  ──validate──►  report for the site (read-only)
        │
        ├─(bundle)──►  processed/imaging/png_visits.json   scan: every visit, image, hash, problem
        ├─sync──────►  LORIS candidates                     participants.tsv → candidatesPlus (ExtStudyID)
        ├─reidentify►  deidentified-lorisid/imaging/png/    session created; sub-<PSCID>/ses-<Visit>/ copies
        └─import────►  LORIS files table (assembly_png/)    one async job per visit, polled
```

| # | Script (`scripts/png/`) | Does | Writes to LORIS |
|---|---|---|---|
| 0 | `scripts/test/validate_png.php` | Cross-checks the share against the user guide, project.json and LORIS; fix-it report (+ `.csv` for the site) | no (reads only) |
| 1 | `run_png_participant_sync.php` | Pre-flight of every issue, then creates missing candidates linked by ExternalID | candidates |
| 2 | `run_png_reidentifier.php` | Creates each visit's session; copies the images to `deidentified-lorisid/` | sessions |
| 3 | `run_png_import.php` | Sends each visit to `importpngdataset` (async, polled) | the images |
| — | `run_png_bundle.php` | Scans + checks the delivery (writes the scan manifest), then runs 1→3; ingests only if 1–2 succeeded | — |
| (optional) | `run_png_scan.php` | The bundle's scan on its own: reads and hashes every image, writes `processed/imaging/png_visits.json`. Not needed in normal runs | no |

**Everything is a dry run unless you add `--confirm`.**

---

## 1. Which projects run

The DICOM rule, unchanged: collection and project `enabled: true` in
`config/loris_client_config.json`, and `"Imaging"` in the project's `project.json`
`modalities`. A project with no `deidentified-raw/imaging/png/` folder is skipped
(`SKIP no PNG delivery`), so `--all` is safe on projects that only send DICOM.

---

## 2. What the site delivers (user guide: *ARCHIMEDES — Preparing PNG Images for Submission*)

```
<project>/deidentified-raw/imaging/png/
  participants.tsv                 participant_id  dob  sex  [project site cohort]
  sub-ARCHI0001/
    sessions.tsv                   session_id  [acq_date]      (or sub-ARCHI0001_sessions.tsv)
    ses-V01/  echo_001.png  echo_002.png  ecg_001.png
    ses-V02/  echo_001.png
  sub-ARCHI0002/ ...
```

- The **folder names** identify the participant and the visit; the **start of the
  file name** (up to the first `_`) is the image type (LORIS scan type).
- `sub-` and `ses-` labels: letters and digits only. A `ses-` label must be a visit
  configured in LORIS (and in `project.json` `visits`, when declared).
- Each visit must be listed in the participant's `sessions.tsv` (as for DICOM;
  a visit that is not listed is **unlinked**: reported, not ingested).
- Image types must be the ones agreed at onboarding: `project.json`
  `png.scan_types`. An image with another type is **set aside** and reported back;
  the rest of the visit goes in (guide §2).

---

## 2a. Validating a share — `scripts/test/validate_png.php`

The one PNG validation, the counterpart of `scripts/test/validate_redcap.php`: run it
**when a site shares its files**, before anything is ingested. Same options, same report
layout (ERROR / WARNING / NOTE, each with a *Fix:*), same `.txt` + `.csv` output (the `.csv`
has one row per problem: send it to the site). It runs the pipeline's own scan and
pre-flight, so the validation and the pipeline can never disagree.

**Read-only.** LORIS is only read — login, `GET /projects`, `GET /sites`, `GET /candidates`,
`GET /candidates/{CandID}`, and the ExternalID mapper lookup; nothing is created or changed.
`--offline` skips LORIS (then it also runs without `composer install`). No EviData or email
calls. It finds the repository root itself, so it works from `scripts/test/` or
wherever it is moved.

```bash
php scripts/test/validate_png.php --collection=archimedes --project=ARCHI-TEST --save   # → logs/test/png_validation/
php scripts/test/validate_png.php --path=/data/archimedes/NewStudy --offline             # no LORIS
php scripts/test/validate_png.php --path=/data/archimedes/NewStudy --png-dir=/incoming/png --report-dir=/tmp/r
```

| Section | Cross-checks |
|---|---|
| `[Project settings]` | `modalities` has Imaging; `candidate_defaults` project/site/cohort; `visits`, `cohorts`, `sites`, `png.scan_types` (no template values, types letters/digits) |
| `[Folder]` | the PNG folder exists; entries outside `sub-<ID>/ses-<visit>/`; nested `sub-` folders |
| `[participants.tsv]` | tab-separated (not CSV/Excel), rows, duplicate IDs; rows without a folder; site / cohort not in the project's lists (WARNING, as REDCap) |
| `[Participant] sub-<ID>` | one section per participant (visits, images per type): identifier; sessions file and `session_id`; folders vs rows both ways; `acq_date`; visit labels (case clash, in `visits`); sex/dob; every image (type agreed, real PNG, not empty, not duplicate); subfolders and other files; visits **NOT INGESTED** and why |
| LORIS (in the sections above) | `LORIS:` errors — the project or a site is not in LORIS (with the right name); a participant already in LORIS has a different dob/sex in participants.tsv |
| `[Summary]` | what ingestion will use: project, sites, cohorts, visits, scan types found, one line per participant (dob/sex normalised, site, cohort, external ID, LORIS new / PSCID / DIFFERS, visits) |
| `[Environment]` | project lock pre-flight on this mount; output folders writable; LORIS reachable |

ERROR = the pipeline would not ingest it (a participant, a visit, or the whole delivery);
WARNING = ingestion goes ahead but some images are not loaded / something is not as intended;
NOTE = information. Exit `0` no errors, `1` errors, `2` could not run.

---

## 3. Quick start

```bash
cd archimedes-pipelines
C="--collection=archimedes --project=ARCHI-TEST"
php scripts/test/validate_png.php $C --save   # 0. validate the share (read-only; .txt + .csv for the site)
php scripts/png/run_png_bundle.php   $C -v              # 1. preview (dry run)
php scripts/png/run_png_bundle.php   $C --confirm -v    # 2. run it
php scripts/png/run_png_bundle.php   --all --confirm    # 3. cron: every enabled imaging project
```

Re-running after a new deposit processes only what is new or changed. Nothing
already in LORIS is duplicated.

---

## 4. What each step checks and does

### 0 — validate, and the bundle's scan (read only)

Everything the user guide asks, plus what the LORIS script would reject later:

| Checked | Result when wrong |
|---|---|
| `participants.tsv` present, tab-separated (not CSV, not an `.xlsx` renamed), not empty, `participant_id` column, no duplicate rows | **project issue** (whole project stops) — missing file: every visit **unlinked** |
| `sub-` label letters/digits only; two folders differing only by case (`sub-A1` / `sub-a1`) | **participant issue** |
| `sex` maps to Male/Female/Other (`M`, `f`, `project.json` `sex_mappings`); `dob` normalises (`YYYY-MM-DD`, `YYYY-MM`, `YYYY`, …) | **participant issue** (shared `ParticipantsCheck`, same messages as DICOM) |
| `ses-` label letters/digits only; `ses-V01` and `ses-v01` both present; visit not in `project.json` `visits` | **visit issue** |
| `acq_date` (optional) is a real `YYYY-MM-DD` date; one `session_id` listed twice with different dates | **visit issue** (future date: warning) |
| participant has no `participants.tsv` row; no sessions file; visit not listed in it | **unlinked** (reported, nothing done) |
| image type (start of the name) not letters/digits, or not in `png.scan_types`, or wrong case (`ECG_001.png` when `ecg` was agreed) | image **set aside** |
| image empty, unreadable, or not a PNG (signature check — a renamed JPEG, a truncated transfer) | image **set aside** |
| image byte-identical to one already in the delivery | second copy **set aside** (LORIS keeps one copy of an image) |
| number not padded (`echo_1.png`), odd characters in the name | warning, ingested |
| subfolder inside a visit, non-PNG files, entries outside `sub-<ID>/` | warning (not read) |
| `participants.tsv` row with no folder | warning |

LORIS (read-only; `--offline` skips it): the project exists, each `site` is a
LORIS site the API user can see, and a participant already in LORIS still has the
same `dob`/`sex` (**ERROR** — ingestion never changes LORIS).

These checks are reported by `scripts/test/validate_png.php` (§2a): one
section per participant, then a **Summary** of everything ingestion will use:

| Summary part | Shows |
|---|---|
| files | delivery folder; participants.tsv format (tab-separated, BOM / Windows line ends, UTF-8), rows, columns, which LORIS columns come from `candidate_defaults`; sessions files present; images accepted / set aside / non-PNG ignored, total size |
| project | `modalities` has Imaging; each LORIS project value, where it comes from, exists in LORIS; the ExternalID project name candidatesPlus will use |
| sites | each site value, count, source; exists in LORIS; in `project.json` `sites` |
| cohorts | each cohort value, count, source; in `project.json` `cohorts`; LORIS checks it when the session is created |
| visits | `project.json` `visits`; each delivered visit: participants, images, unlinked / data issues; held (not declared); case differences mapped to the LORIS spelling; declared but not delivered |
| scan types | agreed types; each type found with image and visit counts; each set-aside type with its reason; agreed but not delivered. Whether a type exists in LORIS `mri_scan_type` is checked by `run_png_import.php --validate` / at import |
| participants | sex / dob summary; how many already in LORIS and how many new; one line per participant: dob and sex as normalised (and from what), site, cohort, external ID, LORIS status (new / PSCID / DIFFERS), each visit |

Then **Environment**: project lock pre-flight (see Concurrent runs), output folders
writable, LORIS reachable. Exit `0` no errors, `1` errors, `2` could not run.

The report always ends with the guide §4 reminder: identifying text burned into
an image cannot be detected automatically.

### 1 — participant sync

The shared `ParticipantSync`, exactly as DICOM: find each participant by external ID
(`/cbigr_api/externalToInternalIdMapper`); create it when missing via
`POST /cbigr_api/candidatesPlus` with `ExtStudyID` + `ProjectExternalName` (legacy
two-step fallback), so the site's ID is linked to the candidate. Existing
candidates are never changed; a different `dob`/`sex` is a warning. Only
participants with at least one linked visit are synced (`--all-participants`: every
row). Missing `site`, `cohort`, `project`, `sex`, `dob` come from `project.json`
`candidate_defaults`.

### 2 — reidentify

For each linked visit: candidate → `PUT /candidates/{CandID}/{Visit}` (session) →
copy the accepted images to

```
deidentified-lorisid/imaging/png/
  participants.tsv                 participant_id = sub-<PSCID>   (no dob/sex: identifier only)
  sub-<PSCID>/sessions.tsv         session_id = ses-<LORIS visit label>, acq_date
  sub-<PSCID>/ses-<Visit>/<the site's file name>.png
```

How PNG differs from DICOM here, on purpose:

- **Nothing inside the image is rewritten.** A PNG has no header; its identity is
  the folder names. "Reidentify" = rename `sub-<site ID>` → `sub-<PSCID>` and
  `ses-<folder>` → the LORIS visit spelling (`ses-v01` → `ses-V01`). The bytes
  are never altered (guide §4); every copy is verified against the md5 taken
  at scan time.
- **The site's file names are kept**: they carry the scan type, and LORIS renames
  its own copy (`<CandID>_<Visit>_<type>_<NNN>.png`) on insert.
- **Provenance** (guide §4 "we record the file name you gave it"):
  `deidentified-lorisid/imaging/png_visits_lorisid.json` and `png_provenance.tsv`
  (one line per image: LORIS-side copy ← original path, md5, scan type, external
  ID, PSCID, CandID, visit). The import then adds each image's LORIS **FileID**
  and path (`processed/imaging/.png_import_tracking.json`).
- **Identity is the image hash**: an image already reidentified for another
  visit/participant is set aside (LORIS would skip it as already registered and
  it would silently stay where it was).

Held back with a warning (the rest go in): visit not in `project.json` `visits`;
LORIS refuses the visit for the cohort. **Errors** (stop the bundle before the
import): candidate lookup/creation fails, no CandID, session creation fails, a
PSCID or LORIS visit label that is not letters/digits (the LORIS script reads
both from folder names and cannot accept anything else), two visits resolving to
the same PSCID/visit, a copy that does not match its original.

### 3 — import

One LORIS job per visit:
`POST /cbigr_api/script/importpngdataset` with
`{"args": {"directory": "<project>/deidentified-lorisid/imaging/png", "subject": "sub-<PSCID>", "visit": "ses-<Visit>", "profile": "database_config.py"}, "flags": ["create_pic", "verbose"], "async": true}`,
then `GET /cbigr_api/script/job/{id}` until it ends.

How PNG differs from the DICOM import, on purpose:

- **No insert/update/overwrite modes.** The LORIS script skips any image whose
  hash is already in `files` and gives each new image a free version number. So
  re-sending a visit is always safe: what is in LORIS is skipped, what is new goes
  in. A changed visit is simply sent again; a failed or partial one is retried as is.
- **Per-image result**: "inserted as FileID N" / "already registered as FileID N"
  are read from the script output and kept per image.
- **One visit per job**: the LORIS script validates everything it is given before
  inserting anything, so a problem in one visit never blocks another.
- **Environment problems stop the project after the first visit** instead of
  failing every one: no `png` row in `ImagingFileTypes`, data dir / `assembly_png/`
  / `pic/` not writable, `dataDirBasepath` unset, endpoint missing (404),
  permission missing (403), login failure, a Python crash (traceback).

Shared with DICOM: only from `deidentified-lorisid/`; a visit is sent only when its
session exists (`GET candidates/{CandID}/{Visit}`, else `NO SESSION` warning, not
recorded, retried next run); fingerprints decide what is redone (`--force`:
everything); a job the run stops watching (`png.poll_timeout_seconds`, default 3600)
is recorded as running and asked about next run — **never started twice**.

`--validate` asks LORIS to check every visit (`--validate-only`: candidate,
session, visit label, scan types in `mri_scan_type`, PNG signature) without
inserting anything — no `--confirm` needed.

---

## 5. The bundle

```bash
php scripts/png/run_png_bundle.php <selection> [options]
```

| Option | Effect |
|---|---|
| `--confirm` | execute (default: dry run of every step) |
| `--dry-run` | explicit dry run; wins over `--confirm` |
| `--continue-good` | on data issues, continue with the good visits without asking (unattended) |
| `--force` | ignore fingerprints and the upload quiet period: redo every visit (safe: nothing is duplicated) |
| `--steps=sync,reidentify,import` / `--skip=LIST` | run only / all but these (the scan and data-issue check always run) |
| `--no-pic` | no imaging-browser preview pics |
| `-v`, `--config=FILE` | as DICOM |

**Data issues: report everything, then you decide** — identical to DICOM §4,
with the visit as the unit: project issues abort; participant issues exclude
all of that participant's visits; visit issues exclude that visit. Without a
terminal and without `--continue-good` the project is **ABORTED** (exit `3`).
The exclusions go to `processed/imaging/.png_excluded.json` and every step
honours them. Set-aside images and unlinked visits are not issues: they are
reported and never block.

**Stop rule**: participant sync or reidentify fails → nothing is imported for the
project (FAILED email). One run per project (see *Concurrent runs* below). A delivery that
changed in the last `imaging.delivery_quiet_minutes` (default 5) is left for the
next run.

Each script also runs on its own (`--all | --collection=X [--project=Y]`,
`--confirm`, `--dry-run`, `-v`, `--config`, `--help`); an unknown option stops it
with exit `2`.

| Script | Extra options |
|---|---|
| `run_png_participant_sync.php` | `--all-participants`, `--exclude-file=FILE` |
| `run_png_reidentifier.php` | `--force`, `--create-candidates`, `--exclude-file=FILE` |
| `run_png_import.php` | `--validate`, `--force`, `--no-pic`, `--profile=NAME`, `--exclude-file=FILE`, `--no-email` |

### Concurrent runs (blocked per project folder)

One PNG run per project at a time, folder by folder, with the same shared lock as every
pipeline (`Shared\ProjectLock`: `.dicom_pipeline.lock`, `.bids_pipeline.lock`,
`.clinical_pipeline.lock`): every PNG script that writes - the bundle, participant sync,
reidentify, import (`--confirm`) - takes `<project>/.png_pipeline.lock` (non-blocking
`flock`) for as long as it works on that project. A second run on the same project is **skipped at once** (exit `1`), and says who
holds it:

```
SKIP another PNG run is in progress for this project (run_png_bundle.php, pid 237, host vm,
     since 2026-11-17T14:57:41+00:00) - skipped, nothing done; re-run when it has finished
```

- One lock for all the steps, not only the bundle: two overlapping runs would create the same
  candidate twice, copy into the same `deidentified-lorisid/` visit and overwrite each other's
  tracking files, and send the same visit to LORIS twice at the same moment.
- The bundle hands its lock to the three step scripts it starts (file descriptor 3, checked to be
  the project's own lock file), so they run under it; nothing else can borrow it.
- Other projects, and DICOM/BIDS/clinical runs on the same project, are not blocked (one lock file per modality).
- Never stale: the OS releases the lock when the process ends, even on `kill -9` or a crash; the
  text left in the file is only a note. A visit whose LORIS job was in flight is recorded as running
  and asked about on the next run - not sent again.
- Dry runs, `--validate`, `validate_png.php` and `run_png_scan.php` do not lock (they write
  nothing that a real run uses).
- **Lock pre-flight** (`ProjectLock::probe()`, run by the validator and by the bundle before every real
  run): (1) the lock file can be created in the project folder, (2) a lock can be taken (a mount
  without lock support fails here), (3) it is *enforced*: a second, separate handle on the same file
  must be refused (a mount that silently grants every lock fails here), and (4) what the mount is,
  from `/proc/mounts`:

  | Result | Meaning | Bundle |
  |---|---|---|
  | OK | local disk, or NFS without `nolock`/`local_lock` (locks go to the server and hold across hosts) | runs |
  | WARN | NFS `nolock` / `local_lock=all\|flock`, CIFS `nobrl`, FUSE (sshfs, rclone, ...): the lock works but only on this host | runs, prints the warning: keep every PNG run for the project on one host (one cron machine) |
  | FAIL | folder not writable, no lock support, or locks not enforced | **STOP** for that project, nothing done |

---

## 6. Incremental deposits — re-run, it catches up

| Deposit | Result on re-run |
|---|---|
| nothing changed | every visit SKIPPED (fingerprint unchanged); nothing sent |
| new participant / new visit (+ rows) | candidate / session created; ingested |
| new image in an ingested visit | visit re-sent; the new image inserted, the others reported "already in LORIS" |
| image replaced (same name, new content) | new image inserted; **STILL IN LORIS** warning names the old FileID |
| image removed from a visit | **STILL IN LORIS** warning with its FileID; LORIS not changed |
| visit / participant removed | working copy removed; **REMOVED … STILL IN LORIS (FileIDs …)** every run |
| `sessions.tsv` `acq_date` edited | visit re-sent; already-inserted images keep their old date (LORIS skips them) — fix in LORIS if needed |
| a visit failed in LORIS | the others go in (PARTIAL); the failed one is retried next run |
| image set aside, then fixed | ingested on the next run |
| `external_id` changed for a reidentified participant | kept, NOT moved, warning; `run_png_reidentifier.php --force` re-assigns |

Fingerprint of a visit = its accepted images' names + md5 + acq_date. Unlike
DICOM's size-based manifest hash, every image has an md5, so an image edited in
place with the same size is still detected. md5s are cached by path + size +
mtime + ctime (`processed/imaging/.png_hash_cache.json`; ctime cannot be preserved
by `touch`/`rsync -t`), so an unchanged image is read
once, not on every run; the reidentifier still re-reads and verifies every image
it copies.

Why PNG does not use the shared `FingerprintTracker` (DICOM, BIDS): its quick check is
file name + size, which would miss an image re-exported at the same size; it hashes every
file in the folder, so a set-aside image would trigger a re-import and an `acq_date`
correction in `sessions.tsv` would not; and PNG also tracks a running LORIS job per visit
and the FileID of each image. The tracking rules are the same: a fingerprint is stored only
after a success, a failed visit is always retried, files are written atomically.

To delete an image from LORIS: LORIS-side tooling (not done by this pipeline).

---

## 7. Settings

`project.json`:

```json
"modalities": ["Imaging"],
"visits": ["V01", "V02"],
"candidate_defaults": {"project": "ARCHI", "site": "UOHI", "cohort": "Control"},
"png": {
  "scan_types": ["echo", "ecg"],
  "create_pic": true,
  "profile": "database_config.py",
  "poll_interval_seconds": 5,
  "poll_timeout_seconds": 3600
},
"notification_emails": {"png": {"enabled": true, "on_success": ["..."], "on_error": ["..."]}}
```

| Setting | Default | Notes |
|---|---|---|
| `png.scan_types` | none (warning) | the image types agreed at onboarding; each must also exist in LORIS `mri_scan_type` |
| `png.create_pic` | `true` | preview pics for the imaging browser |
| `png.profile` | `database_config.py` | Loris-MRI config file name on the LORIS host |
| `png.poll_interval_seconds` / `poll_timeout_seconds` | 5 / 3600 | job polling |
| `notification_emails.png` | off | one email per project run: SUCCESS / SUCCESS (warnings) / PARTIAL / FAILED |
| `imaging.delivery_quiet_minutes` (`loris_client_config.json`) | 5 | shared with DICOM |

---

## 8. On the LORIS server (CBIGR + Loris-MRI)

The endpoint and SQL ship in the CBIGR change (see the CBIGR patch). Needed:

| What | Why |
|---|---|
| `modules/cbigr_api/php/endpoints/script/importpngdataset.class.inc` + the `importpngdataset` case in `script.class.inc` | the endpoint |
| `baserunner.class.inc`: the script's own package `src/` on `PYTHONPATH`; legacy path optional (generic, existing endpoints unchanged) | without it the source-script path fails with `ModuleNotFoundError` (verified) |
| SQL `2026-11-17_script_import_png_dataset.sql`: `script_import_png_dataset` in `server_processes.type` | async jobs are SPM processes; without the ENUM value the job cannot be launched or polled |
| `ImagingFileTypes` row `png` (same patch, `INSERT IGNORE`) | `files.FileType` foreign key |
| one `mri_scan_type` row per agreed type (`Scan_type` column on LORIS 26 C-BIG) | the script never creates scan types |
| Loris-MRI with the C-BIG `cbig` package (`import-png-dataset` installed in the venv, or the source tree) | the script |
| `{dataDirBasepath}/logs/` exists; `assembly_png/` and `pic/` writable by the web server user | the script runs as the web server user via SPM (a missing `logs/` crashes it — verified) |
| `deidentified-lorisid/` readable by the web server user **at the same path** on the LORIS host | the endpoint checks `directory` there |
| API user: `loris_api` (sync/reidentify), `data_entry`/`access_all_profiles` with site+project affiliation (candidatesPlus), `api_script_execution` (script endpoints) | 403 otherwise |
| PSCIDs and visit labels of letters and digits only | the LORIS script reads both from `sub-`/`ses-` folder names |

---

## 9. Email, logs, tracking

| File | |
|---|---|
| `logs/png/png_bundle_<date>.log` | the whole bundle run |
| `logs/png/png_import_run_<ts>.log` | the import, with the summary and everything also reported |
| `logs/png/png_import_errors_<ts>.log` | per failed visit: what was sent, `http_status`, `job_id`, `state`, `exit_code`, full stdout/stderr of `import_png_dataset.py` |
| `logs/png/png_validation_<date>.txt` | the site report |
| `logs/png/png_participant_sync.log` | candidate creation |
| `processed/imaging/png_visits.json` | scan manifest |
| `processed/imaging/.png_import_tracking.json` | per visit: status, fingerprint, job_id, per-image FileID/path, retired images |
| `deidentified-lorisid/imaging/.png_reidentify_tracking.json` | per visit: candidate, visit, copies, fingerprint |
| `deidentified-lorisid/imaging/png_visits_lorisid.json`, `png_provenance.tsv` | provenance |

Deleting a tracking file means "redo everything" for that step (safe: LORIS skips
what it already has).

Email subject: `SUCCESS` / `SUCCESS (warnings)` / `PARTIAL` / `FAILED`: `<project> PNG Import`.
The body lists failed visits, STILL IN LORIS images, set-aside images, unlinked
and excluded visits.

---

## 10. Exit codes and common messages

`0` ok · `1` failure · `2` usage · `3` aborted on data issues (bundle)

| Message | Do |
|---|---|
| `SET ASIDE …` | the site renames / re-exports the image; next run ingests it |
| `UNLINKED …` | add the `participants.tsv` row / the `sessions.tsv` row |
| `VISIT NOT IN project.json` / `VISIT NOT IN LORIS` | fix the `ses-` folder, or set the visit up (project.json `visits`, LORIS cohort) |
| `NO SESSION …` | create the visit in LORIS, or `run_png_reidentifier.php --force` |
| `scan type 'X' has no entry in the mri_scan_type table` | add `X` to `mri_scan_type` (LORIS), or remove it from `png.scan_types` |
| `STILL IN LORIS … FileID N` | intended? delete that file in LORIS |
| `STILL RUNNING in LORIS - job N` | nothing; the next run picks up the result |
| `STOPPED: …` (environment) | fix the LORIS-side cause named; re-run (nothing is sent twice) |
| `PSCID 'X' … is not letters and digits` | the project's PSCID format cannot be used by the LORIS PNG script — LORIS team |
| `WAIT … probably still uploading` | wait, or `--force` |

---

## 11. Where the code is

```
src/Pipelines/Png/   PngPaths (every path),
                     PngOverview (validator OVERVIEW), PngDelivery (scan + guide rules + hashes),
                     PngPreflight (issues, plan, exclusions), PngReidentifier,
                     PngImportPipeline, PngCli (script plumbing)
src/Validation/      PngValidator (the validation, next to RedcapValidator), ValidationReport (shared report)
scripts/test/        validate_png.php
scripts/png/         run_png_participant_sync, run_png_reidentifier,
                     run_png_import, run_png_bundle (+ run_png_scan, optional)
```

Shared with the other pipelines - nothing generic is kept in `Png/`, PNG uses no
DICOM class, and the shared classes never load a PNG class (checked in the tests). The
next modality, or BIDS, takes these as they are (docs/dicom.md §13 has the kit):

| Shared piece | Also used by |
|---|---|
| `Shared/ParticipantsTsv`, `ParticipantSync`, `ParticipantsCheck`, `SessionsTsv`, `SystemFiles`, `Cli` | DICOM, BIDS |
| `Endpoints/LorisApiClientAdapter` (mapper, candidatesPlus, candidates, sessions) | DICOM, BIDS |
| `Utils/Dob` | DICOM, BIDS, clinical |
| `Shared/ImagingProject` (project selection `targets()`, upload quiet period) | DICOM |
| `Shared/ProjectLock` (`enter()`: per-project lock, hand-over to steps, mount pre-flight) | DICOM, BIDS, clinical |
| `Shared/PipelineSetup` (options, config, `readJson()`, `logger()`, LORIS adapter, `--confirm` line); `PngCli` adds only the PNG row lookup | DICOM, BIDS |
| `Shared/BundleSupport` (step runner + abort detection + lock hand-over, stop rule + FAILED email, prompt, bundle log) | DICOM bundle |
| `Shared/ParticipantsTsv::formatProblem()` (Excel / comma-separated / empty `.tsv`) | DICOM validator |
| `Validation/ValidationReport` (report + `.csv` layout); `RedcapValidator` now uses it too (output unchanged, checked byte for byte) | REDCap pre-check |
