# Operations Runbook

Day-to-day operations guide for PatchOps team members managing the Civista sync.

---

## Checking System Health

**Endpoint:** `GET /health`

Returns JSON showing:
- Database connection status
- Last sync time per table
- Record counts (attempted, created, updated, failed, skipped)
- Any errors from the most recent run

```
healthy   = all tables synced without errors
unhealthy = at least one table had failures or errors
```

---

## Triggering a Manual Sync

**Endpoint:** `POST /sync`

Use when:
- Civista re-exports files after a failed nightly run
- You need to sync during the day for a specific reason
- Testing after a fix

**Steps:**
1. Place CSV files in the `/incoming/` directory (via SFTP or the upload endpoint)
2. Hit `POST /sync`
3. Response confirms sync started; check `/health` for results

**Important:** Only one sync runs at a time. If the nightly cron is already running, the manual trigger returns HTTP 409.

---

## Uploading Files Manually

**Endpoint:** `POST /upload` (multipart form)

Use this if SFTP isn't available. Accepts multiple CSV files in a single request.

```bash
curl -X POST https://{host}/upload \
  -F "files=@HubSpot_CIF.csv" \
  -F "files=@HubSpot_DDA.csv"
```

---

## Common Scenarios

### Scenario: Circuit Breaker Tripped

**Symptom:** Sync log shows "CIRCUIT BREAKER: Skipping {table}" with a percentage drop.

**What happened:** Tonight's file has 30%+ fewer records than last time.

**Action:**
1. Contact Civista IT — ask if their export job completed successfully
2. If they confirm it's fine and the lower count is expected (e.g., data cleanup): you'll need to run the sync with the understanding that this is intentional
3. If it was a partial export: ask them to re-run and drop the new file

### Scenario: HubSpot Rate Limited

**Symptom:** Logs show "Rate limited" messages with retry attempts.

**What happened:** HubSpot told us to slow down. The system retries automatically (up to 10 times with exponential backoff).

**Action:** Usually resolves itself. If you see "Rate limited after 10 retries" — HubSpot may be having issues or we're hitting a plan limit. Check HubSpot status page or API usage dashboard.

### Scenario: Batch Failed

**Symptom:** `records_failed > 0` in health endpoint, error_details has specifics.

**What happened:** A batch of 100 records couldn't be sent to HubSpot. Other batches likely succeeded.

**Action:**
1. Check error_details — usually a field validation error or missing required property
2. The failed records will be retried on the next sync (they remain "unsynced" in the diff engine)
3. If it's a schema mismatch, check that HubSpot property definitions haven't changed

### Scenario: No Files Found

**Symptom:** Sync log shows "No CSV files found in incoming directory."

**What happened:** The nightly SFTP delivery didn't arrive.

**Action:** Check with Civista IT about their export schedule. Verify SFTP connectivity.

### Scenario: Duplicate Records in HubSpot

**Symptom:** Multiple HubSpot records with the same CIF number.

**What happened:** The `cif_number` property wasn't set as a unique identifier on the HubSpot object.

**Action:** This must be configured in HubSpot via their schema API BEFORE the first sync. See [Deployment](./deployment.md) for first-run setup steps.

---

## Schedule

| Event | Time | Timezone |
|-------|------|----------|
| Nightly sync | 2:00 AM | Eastern (America/New_York) |

---

## Key Logs to Watch

All logging goes to stdout (Railway captures this automatically).

| Log Pattern | Meaning |
|-------------|---------|
| `Starting full sync` | Sync kicked off |
| `=== Syncing {table} ({n} rows) ===` | Processing a specific file |
| `CIRCUIT BREAKER: Skipping` | File failed safety check |
| `{table}: {n} to sync, {n} unchanged` | Diff engine results |
| `Batch upsert {type}: {n}/{total}` | Progress through batches |
| `Rate limited...retrying` | HubSpot told us to slow down |
| `Archived {file}` | File moved to archive |
| `Sync complete. {n} tables processed.` | Done |

---

## Archived Files

Processed CSVs are moved to `/archive/{YYYY-MM-DD}/`. These serve as a record of exactly what was processed on each date. They're useful for:
- Debugging "what did the data look like on date X?"
- Re-processing if needed (copy back to `/incoming/` and trigger sync)
