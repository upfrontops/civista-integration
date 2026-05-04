# Civista ↔ HubSpot Sync — User Acceptance Test (UAT)

**Project:** civista-integration
**Test environment:** Railway (sandbox HubSpot portal)
**Tester:** _______________________
**Date executed:** _______________________
**Build under test:** commit `_____________` (recorded by harness in §10)

---

## 1. Purpose

This document is the formal acceptance contract for the nightly Civista → HubSpot sync. It exists to prove three things, in order of importance:

1. **Bytes-in-bytes-out (CSV → DB).** Every row that arrives via SFTP is persisted byte-for-byte in the staging tables, verified by SHA-256 hash.
2. **Bytes-in-bytes-out (DB → HubSpot).** Every staging row that ships is read back from HubSpot and compared to the local payload, also verified by SHA-256 hash.
3. **Every transformation is auditable.** Wherever the middleware adapts a value to fit HubSpot's type expectations (e.g., stripping the time portion of a datetime so HubSpot's `date` type accepts it), the original value is preserved and the transformation is logged on the row.

A failure on **any** of these — even with a 99% pass rate elsewhere — is a UAT fail. Banking data does not tolerate silent loss.

This document also covers operational acceptance: deployment health, authentication gating, loud-failure surfacing, sandbox→prod cutover safety, and feature flag behavior.

## 2. Reference architecture

```
Silver Lake / Jack Henry
        │
        │ MOVEit pushes 5 CSVs nightly
        ▼
SFTP listener (Railway TCP Proxy :2222)
        │
        ▼
    /incoming/*.csv
        │  parse  →  HASH A (sha256 of verbatim CSV row)
        ▼
Postgres staging tables
        │  re-read raw_csv  →  HASH B
        ▼
Diff engine (skip already-shipped, identical rows)
        │
        ▼
HubSpot batch upsert (100/batch; 1/batch for suspect rows)
        │
        │  read records back  →  HASH C
        ▼
shipped_records ledger
        │
        ▼
File archived to /archive/<YYYY-MM-DD>/  or  quarantined
```

## 3. Pre-conditions

The UAT cannot start until **all** of the following are true on Railway. The harness in `scripts/uat.js` checks these as Test 0 and refuses to proceed if any fail.

| # | Condition | How verified |
|---|---|---|
| P1 | Railway service `civista-integration` is deployed at the latest `origin/main` commit | `RAILWAY_GIT_COMMIT_SHA` env var matches `git rev-parse origin/main` |
| P2 | Postgres service is attached and reachable | `DATABASE_URL` resolves; `SELECT 1` succeeds within 2s |
| P3 | HubSpot API key is set and valid | `GET /account-info/v3/details` returns 200 + a `portalId` |
| P4 | Required env vars present: `SFTP_HOST_KEY_PEM`, `SFTP_USER`, `SFTP_PASS`, `MANUAL_SYNC_TOKEN`, `HUBSPOT_API_KEY` | All present (boot would refuse otherwise) |
| P5 | Feature flags set for UAT: `ENABLE_DEBUG_UI=1`, `ENABLE_WIRE_LOG=1`, `ENABLE_SCHEMA_CHECK=1` | `GET /api/info` `features` block reflects them |
| P6 | TCP Proxy enabled on port 2222; public host:port issued by Railway | Provided to harness via `SFTP_HOST` and `SFTP_PORT` env |
| P7 | HubSpot sandbox is empty (no leftover records in 6 objects) | `scripts/wipe_sandbox.py` ran cleanly OR all 6 object counts = 0 |
| P8 | Test CSVs (5 files, ≥50 rows each) available locally for upload | Harness verifies presence + row counts |

## 4. Test data

Five real Civista CSVs, sliced to the first 50 data rows each (header + 50 = 51 lines):

- `HubSpot_CIF.csv` — combined contacts + companies
- `HubSpot_DDA.csv` — deposits
- `HubSpot_Loan.csv` — loans
- `HubSpot_CD.csv` — time deposits
- `HubSpot_Debit_Card.csv` — debit cards

The sample is real Civista data, not synthetic. We use only 50 rows per file to bound test runtime and HubSpot sandbox quota; the same code paths handle the full 261k+ production scale.

## 5. Test cases

Each test has: **ID**, **what**, **how**, **expected**, **actual** (filled by harness), **status** (PASS / FAIL).

### T1 — Deployment health

| Item | Detail |
|---|---|
| Why | Confirms the right code is running and the platform is healthy. |
| Steps | 1. `GET /health` → expect 200, `database: "connected"`<br>2. `GET /api/info` → expect commit SHA == `git rev-parse origin/main`<br>3. Pool timeout fast-fails: simulate by checking `/health` returns within ≤2.5s |
| Pass | All three conditions true. |

### T2 — Schema alignment (Postgres ↔ HubSpot)

| Item | Detail |
|---|---|
| Why | If staging columns drifted from HubSpot property names, no record would round-trip cleanly. This catches schema drift before any data is shipped. |
| Steps | `GET /api/schema-check` |
| Pass | For all 6 objects: `errors: []`, `missingInHs: 0` (every PG column matches a HubSpot property exactly). |

### T3 — Portal cutover guard

| Item | Detail |
|---|---|
| Why | Sandbox is Civista's HubSpot child portal linked to their live account. When `HUBSPOT_API_KEY` flips from sandbox to prod, sandbox-portal `hubspot_id`s in `shipped_records` become invalid — diff engine would silently skip records that need re-ship. The boot guard refuses to start unless `meta.last_portal_id` matches the current key's portal. |
| Steps | 1. `GET /api/info` returns 200 (boot completed, guard passed).<br>2. `SELECT value FROM meta WHERE key = 'last_portal_id'` returns the same portalId returned by `GET /account-info/v3/details`. |
| Pass | Both conditions true. |

### T4 — SFTP ingest (auth + integrity)

| Item | Detail |
|---|---|
| Why | The SFTP path is how MOVEit will actually deliver files in production. Both the auth gate AND the byte-integrity contract must hold. |
| Steps | 1. **Auth — wrong password:** SFTP connect with bogus pass → expect "auth rejected" loud event in `sync_errors` (severity=warning, type=`sftp_auth_rejected`).<br>2. **Auth — right password:** connects, no error.<br>3. **Integrity:** upload one CSV; compare local md5 vs `md5sum /app/incoming/<file>` over SSH. |
| Pass | (1) loud event row exists, (2) auth succeeds, (3) md5 match exact. |

### T5 — CSV parse + lossless DB persistence (HASH A = HASH B)

| Item | Detail |
|---|---|
| Why | The first half of the financial-data audit guarantee. If any byte is lost between CSV and DB, this test fails. |
| Steps | 1. Upload all 5 CSVs (50 rows each = 250 total).<br>2. Trigger `/sync` with `X-Sync-Token`.<br>3. After parse completes, query each of the 6 staging tables:<br>&nbsp;&nbsp;`SELECT COUNT(*) WHERE row_hash = db_persist_hash AND db_persist_hash IS NOT NULL` |
| Pass | Returned count == total staged rows for every table. Zero rows have `needs_review = true` on the parse side. |

### T6 — HubSpot upsert + read-back verification (HASH B = HASH C)

| Item | Detail |
|---|---|
| Why | The second half of the audit guarantee. If HubSpot stores anything different from what we sent (truncation, type coercion, etc.) this catches it. |
| Steps | 1. After T5's sync, query each staging table:<br>&nbsp;&nbsp;`SELECT COUNT(*) FILTER (WHERE hubspot_persist_hash IS NOT NULL AND hubspot_verify_diff IS NULL) AS bc_ok, COUNT(*) FILTER (WHERE hubspot_verify_diff IS NOT NULL) AS bc_mismatch`<br>2. Cross-check via HubSpot API: `POST /crm/v3/objects/<obj>/search {"limit":1}` → `total` should equal the number of rows that passed HASH B for that object. |
| Pass | Per object: `bc_ok` == staged count, `bc_mismatch` == 0, HubSpot's reported `total` matches DB. |

### T7 — Coercion audit (every transformation recorded)

| Item | Detail |
|---|---|
| Why | Per memory rule 3: derived columns are OK only if every coercion is auditable on the row. This proves the rule. |
| Steps | 1. After sync, query each staging table:<br>&nbsp;&nbsp;`SELECT id, coercions FROM <table> WHERE jsonb_array_length(coercions) > 0 LIMIT 5`<br>2. Each entry should be `{prop, csv, from, to, coerce}` with `coerce ∈ {date_only, email_strict, yn_to_bool, deceased_flag_special, trim}`.<br>3. `GET /api/coercions` returns aggregates by coerce type and ≤25 sample entries. |
| Pass | At least one row in CIF staging has a `date_only` coercion (the source has timestamps in `LastLogin`/`EnrollmentDt`). All 4 custom-object tables have rows with `yn_to_bool` audit entries. `/api/coercions` aggregates match what's in staging. |

### T8 — Loud failure surfacing

| Item | Detail |
|---|---|
| Why | Per memory rules 5 & 7: every failure must surface to the UI. No silent skipping. |
| Steps | 1. Inject a row with `email = "collect"` (real Civista row at CIF `\AA0011`) — already in the 50-row sample.<br>2. Inject DiscAcpt = "Y" — also real, in the sample.<br>3. After sync, query `sync_errors` for severity / type breakdown. |
| Pass | `email_suspect` warning exists for the collect row. `hubspot_record_rejected` exists for any DiscAcpt row that HubSpot refused. The bad-email row's neighbors STILL ship (per the 1-row batch isolation). `mapping_issues` populated for any HubSpot-rejected property. |

### T9 — Manual sync auth gating

| Item | Detail |
|---|---|
| Why | `/sync` is a destructive operation in prod (writes to HubSpot). Requires header `X-Sync-Token`. |
| Steps | 1. `POST /sync` (no header) → expect 403<br>2. `POST /sync -H "X-Sync-Token: bogus"` → expect 403, plus `manual_sync_auth_rejected` warning in `sync_errors`<br>3. `POST /sync -H "X-Sync-Token: <correct>"` → expect 200 |
| Pass | All three behave as expected. |

### T10 — Idempotence (diff engine correctness)

| Item | Detail |
|---|---|
| Why | The cron runs every night at 2 AM. If MOVEit drops the same file twice, we must not duplicate or re-write records to HubSpot. |
| Steps | 1. After T6, re-upload the same 5 CSVs.<br>2. Trigger `/sync` again.<br>3. Check `sync_log` for the second run: `records_skipped` should equal staged rows; `records_created` == 0. |
| Pass | Second run ships 0 new records and skips ~250 unchanged rows. HubSpot record counts unchanged. |

### T11 — Feature flag gating

| Item | Detail |
|---|---|
| Why | Prod will likely run with `ENABLE_DEBUG_UI=0`, `ENABLE_SCHEMA_CHECK=0`, `ENABLE_WIRE_LOG=0`. Verify each flag's off-state. |
| Steps | (Two-pass test, requires env vars toggled between runs.)<br>**Pass A — flags on:** `GET /` → UI HTML; `GET /api/schema-check` → 200; SSE `event: hubspot` records emitted.<br>**Pass B — flags off:** Same probes → 404 / no hubspot events. |
| Pass | Both passes behave as expected. (Pass B can be deferred until after first prod sandbox cycle if env flipping is operationally costly.) |

### T12 — Cron schedule registration (sanity, not execution)

| Item | Detail |
|---|---|
| Why | Production sync runs at 2 AM ET via cron. We don't wait for 2 AM in UAT, but we verify the schedule is registered and the handler honors `MANUAL_SYNC_TOKEN`-style gating + portal guard. |
| Steps | 1. Inspect Railway logs at boot for `Cron: starting nightly sync` skip messages on prior boots, OR<br>2. Verify `index.js` cron block is present in deployed code via SSH `cat /app/index.js | grep -A3 "cron.schedule"`. |
| Pass | Cron registered; handler refuses to run if `portalGuardOk === false`. |

## 6. Pass criteria

UAT passes if and only if **every** test T1–T12 has status PASS. There are no soft fails.

A test marked **DEFERRED** (T11 Pass B is the only candidate) requires explicit operator sign-off in §8 with the reason and the planned re-test date.

## 7. Test execution

`scripts/uat.js` is the runnable harness. It exercises T1–T11 over HTTPS + a small set of SSH calls (which require the Railway Workspace SSH key registered). T12 is a manual code+log inspection.

```bash
# Required env (point at your Railway service)
export RAILWAY_URL=https://<civista-integration-domain>
export MANUAL_SYNC_TOKEN=<from Railway env>
export SFTP_HOST=<Railway TCP proxy host>     # e.g. hopper.proxy.rlwy.net
export SFTP_PORT=<Railway TCP proxy port>     # e.g. 34212
export SFTP_USER=civista
export SFTP_PASS=<from Railway env>
export HUBSPOT_API_KEY=<sandbox key, for verification reads only>

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
| T1 | Deploy didn't pick up latest commit; or DB unhealthy | Check Railway logs; `git push origin main` if commit mismatch |
| T2 | Schema drift after a HubSpot property change | Re-pull HubSpot schema, update `src/transform/hubspot-mapping.js` |
| T3 | Portal mismatch between API key and `meta.last_portal_id` | Run `node scripts/cutover-portal.js` (see `docs/cutover.md`) |
| T4 | SFTP host key not registered or password rejected | Check `SFTP_HOST_KEY_PEM` env contents; verify SFTP_USER/PASS |
| T5 | `db_persist_hash` mismatch | **STOP.** This is data corruption — DO NOT ship to prod. Investigate the parser → DB write path. |
| T6 | `hubspot_persist_hash` mismatch | HubSpot is mutating values we send (e.g., a property's allowed enum dropped a value we used). Inspect `hubspot_verify_diff` JSONB. |
| T7 | Missing coercion audit entries | Code regression in `buildPayload`; check `src/sync/hubspot.js` |
| T8 | Bad email row took down its batch instead of being isolated | Check that `email` field has `coerce: 'email_strict'` in mapping; `syncRows` partitions suspect rows. |
| T9 | `/sync` accepts requests without token | `MANUAL_SYNC_TOKEN` env var not set on Railway, OR auth bypass introduced |
| T10 | Records re-shipped on second run | Diff engine broken; check `shipped_records` table population |
| T11 | Feature flag doesn't disable surface | Code regression in `index.js` route guards |
| T12 | Cron block not in deployed code | Old commit deployed |

## 10. Results (filled by harness)

```
<paste uat-results.md output here>
```

---

**Document version:** 1.0
**Last updated:** 2026-05-04
