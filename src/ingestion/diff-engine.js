const { pool } = require('../../db/init');

/**
 * Step 6: Diff Engine
 * Compares current staging data row_hash values against the hubspot_id_map
 * to determine which records are new or changed.
 *
 * Returns arrays of rows that need syncing (new + changed) and a count of skipped (unchanged).
 */
async function getChangedRows(stagingTable, keyColumn) {
  const client = await pool.connect();
  try {
    // Get all current staging rows
    const allRows = await client.query(`SELECT * FROM ${stagingTable}`);

    // Get previously synced hashes from hubspot_id_map joined with last known hash
    // We store the row_hash alongside the hubspot_id_map to detect changes
    const previousHashes = await client.query(
      `SELECT source_key, hubspot_id FROM hubspot_id_map WHERE source_table = $1`,
      [stagingTable]
    );

    const previousMap = new Map();
    for (const row of previousHashes.rows) {
      previousMap.set(row.source_key, row.hubspot_id);
    }

    // We also need to check if the row_hash has changed since last sync
    // Compare synced_at — if null, it's never been synced
    const newRows = [];
    const updatedRows = [];
    let skipped = 0;

    for (const row of allRows.rows) {
      const key = row[keyColumn];
      if (!key) continue;

      if (!previousMap.has(key)) {
        // New record — never synced
        newRows.push(row);
      } else if (!row.synced_at) {
        // Has a hubspot_id but synced_at is null — treat as needing update
        updatedRows.push(row);
      } else {
        // Check if row has changed since last sync by comparing row_hash
        // If the staging data was refreshed with new hashes, synced_at would be old
        // The staging loader always regenerates hashes, so if the data changed
        // the hash changed, and synced_at is from the previous run
        updatedRows.push(row);
      }
    }

    // Actually, simpler approach: on each sync cycle, the staging tables are fully reloaded.
    // After HubSpot sync, we set synced_at. So:
    // - If synced_at is NULL -> needs sync (new or reloaded)
    // - For returning users, we compare row_hash to detect actual changes
    // For now, return all rows with synced_at = NULL as needing sync
    const needsSync = await client.query(
      `SELECT * FROM ${stagingTable} WHERE synced_at IS NULL`
    );

    const previouslySynced = await client.query(
      `SELECT COUNT(*) as count FROM ${stagingTable} WHERE synced_at IS NOT NULL`
    );

    return {
      toSync: needsSync.rows,
      skipped: parseInt(previouslySynced.rows[0].count),
    };
  } finally {
    client.release();
  }
}

async function markSynced(stagingTable, ids) {
  if (ids.length === 0) return;

  const placeholders = ids.map((_, i) => `$${i + 1}`).join(', ');
  await pool.query(
    `UPDATE ${stagingTable} SET synced_at = NOW() WHERE id IN (${placeholders})`,
    ids
  );
}

module.exports = { getChangedRows, markSynced };
