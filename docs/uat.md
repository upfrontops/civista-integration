# Civista to HubSpot Sync. User Acceptance Test (UAT).

**Project:** civista-integration
**Test environment:** Railway, sandbox HubSpot portal.
**Tester:** _______________________
**Date executed:** _______________________
**Build under test:** commit `_____________` (recorded by harness in §10).

---

## 1. Purpose

This document is the formal acceptance contract for the nightly Civista to HubSpot sync. It exists to prove three things, in order of importance:

1. Bytes in, bytes out at the database boundary. Every row that arrives via SFTP is persisted byte-for-byte in the staging tables, verified by SHA-256 hash.
2. Bytes in, bytes out at the HubSpot boundary. Every staging row that ships is read back from HubSpot and compared to the local payload, also verified by SHA-256 hash.
3. Every transformation is auditable. Wherever the middleware adapts a value to fit HubSpot's type expectations (for example, stripping the time portion of a datetime so HubSpot's `date` type accepts it), the original value is preserved and the transformation is logged on the row.

A failure on any of these, even with a 99% pass rate elsewhere, is a UAT fail. Banking data does not tolerate silent loss.

This document also covers operational acceptance: deployment health, authentication gating, loud failure surfacing, sandbox to prod cutover safety, feature flag behavior, and the operator UI.

## 2. Reference architecture

```
Silver Lake / Jack Henry
        │
        │ MOVEit pushes 5 CSVs nightly
        ▼
SFTP listener (Railway TCP Proxy on 2222)
        │
        ▼
    /incoming/*.csv
        │  parse, then HASH A (sha256 of verbatim CSV row)
        ▼
Postgres staging tables
        │  re-read raw_csv, then HASH B
        ▼
Diff engine (skip already-shipped, identical rows)
        │
        ▼
HubSpot batch upsert (100 per batch, or 1 per batch for suspect rows)
        │
        │  read records back, then HASH C
        ▼
shipped_records ledger
        │
        ▼
File archived to /archive/<YYYY-MM-DD>/  or  quarantined
```

## 3. Pre-conditions

The UAT cannot start until all of the following are true on Railway. The harness in `scripts/uat.js` checks these as Test 0 and refuses to proceed if any fail.

| # | Condition | How verified |
|---|---|---|
| P1 | Railway service `civista-integration` is deployed at the latest `origin/main` commit. | `RAILWAY_GIT_COMMIT_SHA` env var matches `git rev-parse origin/main`. |
| P2 | Postgres service is attached and reachable. | `DATABASE_URL` resolves; `SELECT 1` succeeds within 2s. |
| P3 | HubSpot API key is set and valid. | `GET /account-info/v3/details` returns 200 plus a `portalId`. |
| P4 | Required env vars present: `SFTP_HOST_KEY_PEM`, `SFTP_USER`, `SFTP_PASS`, `MANUAL_SYNC_TOKEN`, `HUBSPOT_API_KEY`. | All present (boot would refuse otherwise). |
| P5 | Feature flags set for UAT: `ENABLE_DEBUG_UI=1`, `ENABLE_WIRE_LOG=1`, `ENABLE_SCHEMA_CHECK=1`. | `GET /api/info` `features` block reflects them. |
| P6 | TCP Proxy enabled on port 2222; public host:port issued by Railway. | Provided to harness via `SFTP_HOST` and `SFTP_PORT` env. |
| P7 | HubSpot sandbox is empty (no leftover records in the 6 objects). | `scripts/wipe_sandbox.py` ran cleanly, or all 6 object counts equal 0. |
| P8 | Test CSVs (5 files, 50 or more rows each) available locally for upload. | Harness verifies presence and row counts. |

## 4. Test data

Five real Civista CSVs, sliced to the first 50 data rows each (header plus 50 lines of data, 51 lines total):

* `HubSpot_CIF.csv`. Combined contacts and companies.
* `HubSpot_DDA.csv`. Deposits.
* `HubSpot_Loan.csv`. Loans.
* `HubSpot_CD.csv`. Time deposits.
* `HubSpot_Debit_Card.csv`. Debit cards.

The sample is real Civista data, not synthetic. We use only 50 rows per file to bound test runtime and HubSpot sandbox quota; the same code paths handle the full 261k+ production scale.

## 5. Test cases

Each test has: ID, what, how, expected, actual (filled by harness), status (PASS or FAIL).

### T1. Deployment health

| Item | Detail |
|---|---|
| Why | Confirms the right code is running and the platform is healthy. |
| Steps | 1. `GET /health`. Expect 200, `database: "connected"`. 2. `GET /api/info`. Expect commit SHA equal to `git rev-parse origin/main`. 3. Pool timeout fast-fails. Verify `/health` returns within 2.5s or less. |
| Pass | All three conditions true. |

### T2. Schema alignment (Postgres vs HubSpot)

| Item | Detail |
|---|---|
| Why | If staging columns drifted from HubSpot property names, no record would round-trip cleanly. This catches schema drift before any data is shipped. |
| Steps | `GET /api/schema-check`. |
| Pass | For all 6 objects: `errors: []`, `missingInHs: 0` (every PG column matches a HubSpot property exactly). |

### T3. Portal cutover guard

| Item | Detail |
|---|---|
| Why | Sandbox is Civista's HubSpot child portal linked to their live account. When `HUBSPOT_API_KEY` flips from sandbox to prod, sandbox-portal `hubspot_id`s in `shipped_records` become invalid. The diff engine would silently skip records that need re-ship. The boot guard refuses to start unless `meta.last_portal_id` matches the current key's portal. |
| Steps | 1. `GET /api/info` returns 200 (boot completed, guard passed). 2. `SELECT value FROM meta WHERE key = 'last_portal_id'` returns the same portalId returned by `GET /account-info/v3/details`. |
| Pass | Both conditions true. |

### T4. SFTP ingest (auth and integrity)

| Item | Detail |
|---|---|
| Why | The SFTP path is how MOVEit will actually deliver files in production. Both the auth gate AND the byte integrity contract must hold. |
| Steps | 1. Auth, wrong password. SFTP connect with bogus pass. Expect "auth rejected" loud event in `sync_errors` (severity=warning, type=`sftp_auth_rejected`). 2. Auth, right password. Connects, no error. 3. Integrity. Upload one CSV, compare local md5 against `md5sum /app/incoming/<file>` over SSH. |
| Pass | (1) loud event row exists, (2) auth succeeds, (3) md5 match exact. |

### T5. CSV parse and lossless DB persistence (HASH A equals HASH B)

| Item | Detail |
|---|---|
| Why | The first half of the financial data audit guarantee. If any byte is lost between CSV and DB, this test fails. |
| Steps | 1. Upload all 5 CSVs (50 rows each, 250 total). 2. Trigger `/sync` with `X-Sync-Token`. 3. After parse completes, query each of the 6 staging tables: `SELECT COUNT(*) WHERE row_hash = db_persist_hash AND db_persist_hash IS NOT NULL`. |
| Pass | Returned count equals total staged rows for every table. Zero rows have `needs_review = true` on the parse side. |

### T6. HubSpot upsert and read-back verification (HASH B equals HASH C)

| Item | Detail |
|---|---|
| Why | The second half of the audit guarantee. If HubSpot stores anything different from what we sent (truncation, type coercion, etc.) this catches it. |
| Steps | 1. After T5's sync, query each staging table: `SELECT COUNT(*) FILTER (WHERE hubspot_persist_hash IS NOT NULL AND hubspot_verify_diff IS NULL) AS bc_ok, COUNT(*) FILTER (WHERE hubspot_verify_diff IS NOT NULL) AS bc_mismatch`. 2. Cross-check via HubSpot API: `POST /crm/v3/objects/<obj>/search {"limit":1}`. The `total` should equal the number of rows that passed HASH B for that object. |
| Pass | Per object: `bc_ok` equals staged count, `bc_mismatch` equals 0, HubSpot's reported `total` matches DB. |

### T7. Coercion audit (every transformation recorded)

| Item | Detail |
|---|---|
| Why | Per memory rule 3, derived columns are OK only if every coercion is auditable on the row. This proves the rule. |
| Steps | 1. After sync, query each staging table: `SELECT id, coercions FROM <table> WHERE jsonb_array_length(coercions) > 0 LIMIT 5`. 2. Each entry should be `{prop, csv, from, to, coerce}` with `coerce` in `{date_only, email_strict, yn_to_bool, deceased_flag_special, trim}`. 3. `GET /api/coercions` returns aggregates by coerce type and 25 or fewer sample entries. |
| Pass | At least one row in CIF staging has a `date_only` coercion (the source has timestamps in `LastLogin` and `EnrollmentDt`). All 4 custom-object tables have rows with `yn_to_bool` audit entries. `/api/coercions` aggregates match what's in staging. |

### T8. Loud failure surfacing

| Item | Detail |
|---|---|
| Why | Per memory rules 5 and 7, every failure must surface to the UI. No silent skipping. |
| Steps | 1. Inject a row with `email = "collect"` (real Civista row at CIF `\AA0011`). Already in the 50-row sample. 2. Inject DiscAcpt = "Y". Also real, in the sample. 3. After sync, query `sync_errors` for severity and type breakdown. |
| Pass | `email_suspect` warning exists for the collect row. `hubspot_record_rejected` exists for any DiscAcpt row that HubSpot refused. The bad-email row's neighbors STILL ship (per the 1-row batch isolation). `mapping_issues` populated for any HubSpot-rejected property. |

### T9. Manual sync auth gating

| Item | Detail |
|---|---|
| Why | `/sync` is a destructive operation in prod (writes to HubSpot). Requires header `X-Sync-Token`. |
| Steps | 1. `POST /sync` (no header). Expect 403. 2. `POST /sync -H "X-Sync-Token: bogus"`. Expect 403, plus `manual_sync_auth_rejected` warning in `sync_errors`. 3. `POST /sync -H "X-Sync-Token: <correct>"`. Expect 200. |
| Pass | All three behave as expected. |

### T10. Idempotence (diff engine correctness)

| Item | Detail |
|---|---|
| Why | The cron runs every night at 2 AM. If MOVEit drops the same file twice, we must not duplicate or re-write records to HubSpot. |
| Steps | 1. After T6, re-upload the same 5 CSVs. 2. Trigger `/sync` again. 3. Check `sync_log` for the second run. `records_skipped` should equal staged rows; `records_created` equals 0. |
| Pass | Second run ships 0 new records and skips about 250 unchanged rows. HubSpot record counts unchanged. |

### T11. Feature flag gating

| Item | Detail |
|---|---|
| Why | Prod will likely run with `ENABLE_DEBUG_UI=0`, `ENABLE_SCHEMA_CHECK=0`, `ENABLE_WIRE_LOG=0`. Verify each flag's off state. |
| Steps | (Two-pass test, requires env vars toggled between runs.) Pass A, flags on: `GET /` returns UI HTML; `GET /api/schema-check` returns 200; SSE `event: hubspot` records emitted. Pass B, flags off: same probes return 404, no hubspot events. |
| Pass | Both passes behave as expected. (Pass B can be deferred until after first prod sandbox cycle if env flipping is operationally costly.) |

### T12. Cron schedule registration (sanity, not execution)

| Item | Detail |
|---|---|
| Why | Production sync runs at 2 AM ET via cron. We don't wait for 2 AM in UAT, but we verify the schedule is registered and the handler honors `MANUAL_SYNC_TOKEN`-style gating and portal guard. |
| Steps | 1. Inspect Railway logs at boot for `Cron: starting nightly sync` skip messages on prior boots, OR 2. Verify `index.js` cron block is present in deployed code via SSH `cat /app/index.js | grep -A3 "cron.schedule"`. |
| Pass | Cron registered; handler refuses to run if `portalGuardOk === false`. |

### T14. UI basic auth enforced (automated)

| Item | Detail |
|---|---|
| Why | Bank-grade. Every UI and API route except `/health` requires HTTP Basic auth using `SFTP_USER` and `SFTP_PASS`. The password never appears in front-end code. The browser's native auth dialog handles the prompt. |
| Steps | 1. `GET /api/info` with no Authorization header. Expect HTTP 401 plus `WWW-Authenticate: Basic` response header. 2. `GET /api/info` with `Authorization: Basic <base64('wrong:wrong')>`. Expect 401. 3. `GET /health` with no Authorization header. Expect 200 (Railway's healthcheck must not be auth-gated). 4. (Implicit through every other test.) The harness's `http()` helper auto-attaches `Authorization: Basic <base64('SFTP_USER:SFTP_PASS')>` on every request, so all other tests passing implies correct credentials are accepted. |
| Pass | Steps 1, 2, 3 all behave as expected. Comparison is constant-time (SHA-256 digest plus `crypto.timingSafeEqual`), and failed attempts surface as `ui_basic_auth_rejected` warnings in `sync_errors`. |

### T13. Operator UI walkthrough (manual)

| Item | Detail |
|---|---|
| Why | The UI is the operator surface. Even when every API endpoint is correct, the UI binds them together. A broken UI is a broken handoff to Civista. |
| Pre | `ENABLE_DEBUG_UI=1`, `ENABLE_WIRE_LOG=1`, `ENABLE_SCHEMA_CHECK=1` on Railway. Browser open at the Railway public URL. |
| Steps | 1. Open `https://<railway-url>/`. UI loads, three left panels visible (Connect, Upload, Run). 2. SFTP Connection panel: enter the Railway TCP proxy host and port, username `civista`, password from Railway env. Click Connect. Expect "Authenticated to <host>:<port>". 3. Upload panel: select the 5 sample CSVs, click "Upload via SFTP". Each shows "delivered". 4. Run panel: click "Run Full Sync". Live Log panel begins streaming. 5. Watch the HubSpot Wire panel, every API call appears in real time with method, path, status, duration. 6. After sync settles, scroll to Hash Health pills. Both `A to B (CSV to DB)` and `B to C (DB to HubSpot)` show green with `ok` count equal to total staged. 7. Coercion Audit panel shows entries for `date_only`, `email_strict`, `yn_to_bool`, `trim`. 8. Data Issues panel shows the held mapping (DiscAcpt) and any per-record HubSpot rejections. 9. Click "Refresh Schema Check". Schema table renders, all 6 objects with 0 errors. |
| Pass | Every step above completes without console errors. The screenshots from steps 4, 6, 7 are captured for the sign-off file. |

## 6. Pass criteria

UAT passes if and only if every test T1 through T14 has status PASS. There are no soft fails.

A test marked DEFERRED (T11 Pass B is the only candidate) requires explicit operator sign-off in §8 with the reason and the planned re-test date.

## 7. Test execution

`scripts/uat.js` is the runnable harness for T1 through T11. It exercises tests over HTTPS plus a small set of SFTP calls (which require the Railway Workspace SSH key registered). T12 and T13 are manual.

```bash
# Required env (point at your Railway service)
export RAILWAY_URL=https://<civista-integration-domain>
export MANUAL_SYNC_TOKEN=<from Railway env>
export SFTP_HOST=<Railway TCP proxy host>     # for example, hopper.proxy.rlwy.net
export SFTP_PORT=<Railway TCP proxy port>     # for example, 34212
export SFTP_USER=civista
export SFTP_PASS=<from Railway env>
export HUBSPOT_API_KEY=<sandbox key, for verification reads only>
export UAT_SAMPLES_DIR=/abs/path/to/incoming  # contains the 5 sample CSVs

node scripts/uat.js
```

The harness writes its results to `uat-results.md` (in the working directory). Paste that file's contents into §10 of this document on completion.

## 8. Sign-off

| Role | Name | Status | Date |
|---|---|---|---|
| Test executor (PatchOps) | _______________ | PASS / FAIL | _______ |
| Civista IT acceptance | _______________ | PASS / FAIL | _______ |
| Civista business acceptance | _______________ | PASS / FAIL | _______ |
| Approval to enable nightly cron | _______________ | YES / NO | _______ |

**Deferred tests (if any):**

| Test ID | Reason for deferral | Re-test by |
|---|---|---|
| | | |

## 9. What to do if a test fails

| Test that fails | Likely cause | First action |
|---|---|---|
| T1 | Deploy didn't pick up latest commit, or DB unhealthy. | Check Railway logs; `git push origin main` if commit mismatch. |
| T2 | Schema drift after a HubSpot property change. | Re-pull HubSpot schema, update `src/transform/hubspot-mapping.js`. |
| T3 | Portal mismatch between API key and `meta.last_portal_id`. | Run `node scripts/cutover-portal.js` (see `docs/cutover.md`). |
| T4 | SFTP host key not registered or password rejected. | Check `SFTP_HOST_KEY_PEM` env contents; verify SFTP_USER and PASS. |
| T5 | `db_persist_hash` mismatch. | STOP. This is data corruption. DO NOT ship to prod. Investigate the parser to DB write path. |
| T6 | `hubspot_persist_hash` mismatch. | HubSpot is mutating values we send (for example, a property's allowed enum dropped a value we used). Inspect `hubspot_verify_diff` JSONB. |
| T7 | Missing coercion audit entries. | Code regression in `buildPayload`; check `src/sync/hubspot.js`. |
| T8 | Bad email row took down its batch instead of being isolated. | Check that `email` field has `coerce: 'email_strict'` in mapping; `syncRows` partitions suspect rows. |
| T9 | `/sync` accepts requests without token. | `MANUAL_SYNC_TOKEN` env var not set on Railway, OR auth bypass introduced. |
| T10 | Records re-shipped on second run. | Diff engine broken; check `shipped_records` table population. |
| T11 | Feature flag doesn't disable surface. | Code regression in `index.js` route guards. |
| T12 | Cron block not in deployed code. | Old commit deployed. |
| T13 | UI panel error or missing element. | Browser console error usually points at the failing fetch; cross-check the corresponding `/api/...` route. |
| T14 | Basic auth not enforced, or `/health` is gated. | Confirm `SFTP_USER` and `SFTP_PASS` env vars are set on Railway. Confirm the `app.use(basicAuth)` middleware is registered before the route handlers in `index.js`. `/health` short-circuit must remain before the auth check. |

## 10. Results (filled by harness)

```
<paste uat-results.md output here>
```

---

**Document version:** 1.1
**Last updated:** 2026-05-04
