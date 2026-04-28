# Sandbox → Prod Portal Cutover

## Why this matters

The HubSpot "sandbox" we use is Civista's child portal linked to their live
account. Each portal has its own internal `hubspot_id` namespace. When the
`HUBSPOT_API_KEY` env var flips between portals (sandbox → prod, or prod →
sandbox), the `hubspot_id` values cached in our `shipped_records` and
`hubspot_id_map` tables stop being valid.

If we don't reset, two silent corruptions happen:

1. The diff engine's lookup `(source_table, source_key, row_hash)` matches a
   `shipped_records` row holding a sandbox `hubspot_id`. The orchestrator
   marks the row "already shipped" and skips it. **The row never gets
   written to the prod portal.**
2. If we ever did `PATCH` by id (we currently do `upsert` by `idProperty`,
   so this is theoretical), we'd be writing to a non-existent prod id and
   silently fail.

The boot guard in `index.js` refuses to start the service in this state.
The `/sync` route returns 503 and the cron emits a `cron_skip_portal_guard`
loud event.

## Before changing the API key

1. **Decide what to do with already-shipped records on the new portal.**
   - If you want a fresh sync (every row re-ships into the new portal),
     proceed below.
   - If the new portal already has matching records (e.g., you imported
     them out of band) and you want to upsert by `cif_number` /
     `primary_key` / `composite_key`, that's still fine — the cutover
     script clears OUR cache; HubSpot's own dedup-by-idProperty handles
     the merge.

2. **Run the cutover script** *before* changing the env var:
   ```bash
   railway run --service=civista-integration node scripts/cutover-portal.js
   ```
   While the OLD key is still in env, this records the OLD portal id into
   `meta.last_portal_id`. (No truncation in this case because portal ids
   match.)

3. **Update `HUBSPOT_API_KEY`** in Railway env vars to the new portal's
   token.

4. **Re-run the cutover script** with the new key:
   ```bash
   railway run --service=civista-integration node scripts/cutover-portal.js
   ```
   This time it sees the new portal id ≠ stored, TRUNCATEs the ledger and
   staging tables, and writes the new portal id.

5. **Restart the Railway service.** The boot guard will compare the now-
   matching meta value to the live portal id and start cleanly.

## After the cutover

- `shipped_records` is empty → the next sync treats every staging row as
  new and ships it.
- The 2 AM cron will pick up the next nightly drop normally.
- If you want a manual smoke test before waiting for cron:
  ```bash
  curl -X POST https://<railway-url>/sync \
    -H "X-Sync-Token: $MANUAL_SYNC_TOKEN"
  ```

## What the script does

`scripts/cutover-portal.js`:

1. Reads `HUBSPOT_API_KEY` from env, calls `GET /account-info/v3/details`
   to learn the portal id the key points at.
2. Reads `meta.last_portal_id` from Postgres.
3. If they match: exits without changes.
4. If they differ (or stored is empty): TRUNCATEs `sync_log`,
   `sync_errors`, `mapping_issues`, `hubspot_id_map`, `shipped_records`,
   and all 6 `stg_*` tables. Writes the new portal id to `meta`.
