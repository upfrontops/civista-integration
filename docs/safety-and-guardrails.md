# Safety & Guardrails

This document explains how the system protects Civista's HubSpot CRM from data corruption. These protections are designed so that the system fails safe — if something goes wrong, it stops and waits for a human.

## Protection 1: Circuit Breaker (30% Drop Threshold)

**What it does:** If tonight's file has 30%+ fewer records than last night's, the sync halts for that file.

**Why it matters:** The most dangerous scenario isn't a crash — it's a partial export from Silver Lake that *looks* normal but is missing half the data. Without this check, the system might interpret "record is gone" as "delete it from HubSpot" in a future version.

**Example:**
- Yesterday's CIF: 261,000 records
- Today's CIF: 180,000 records (31% drop)
- Result: **Sync skipped.** Logged with reason.

**What to do:** Check with Civista's IT team. Usually means their export job failed partway through. Once they re-export, drop the file in `/incoming/` and trigger a manual sync.

## Protection 2: Upsert-Only (No Deletes)

**What it does:** The system only creates or updates records. It never deletes anything from HubSpot.

**Why it matters:** Even if a record disappears from a source file, it stays in HubSpot. Deletion is a manual, intentional operation — never automated.

## Protection 3: Diff Engine (Only Sync Changes)

**What it does:** Every row is hashed. If the hash matches what we sent last time, we skip it.

**Why it matters:**
- Reduces risk of accidental overwrites
- Keeps HubSpot audit logs clean (no phantom "updates" that change nothing)
- If something goes wrong, the blast radius is limited to records that actually changed

## Protection 4: Batch Size Limit (100 Records)

**What it does:** We send records to HubSpot 100 at a time, not all at once.

**Why it matters:** If a batch fails, we only lose 100 records from that run — not all 261,000. The rest continue processing. Failed batches get logged with the exact error.

## Protection 5: Exponential Backoff

**What it does:** If HubSpot says "slow down" (HTTP 429), we wait longer between retries — doubling the wait each time.

**Why it matters:** Prevents us from getting banned by HubSpot's API and ensures we're a good citizen in their rate-limit system. We retry up to 10 times before giving up on a batch.

## Protection 6: Sequential Processing Order

**What it does:** CIF (contacts/companies) always syncs first, then accounts.

**Why it matters:** Account records reference contacts via CIF number. Processing in order ensures the parent contact/company exists before the child account tries to reference it.

## Protection 7: Sync Locking

**What it does:** Only one sync can run at a time. If a manual sync is triggered while the nightly cron is running, it gets rejected.

**Why it matters:** Prevents race conditions where two syncs try to write the same records simultaneously.

## Protection 8: Full Sync Logging

**What it does:** Every sync run logs:
- Start/end time
- Row counts (attempted, created, updated, failed, skipped)
- File hash (proves which exact file was processed)
- Any errors with full details

**Why it matters:** Complete audit trail. If Civista asks "what happened to record X?", we can trace exactly when it was last synced, what changed, and whether any errors occurred.

## What the System Does NOT Do (Yet)

| Concern | Status |
|---------|--------|
| Deleting records from HubSpot | **Not implemented.** Records that disappear from CSVs are simply not updated. |
| Associations (linking accounts to contacts) | **Deferred.** Records exist independently for now. |
| Email notifications on failure | **Not yet.** Check the `/health` endpoint or sync logs. |
| Rollback on partial failure | **Not implemented.** Failed batches are logged; successful batches remain. |
