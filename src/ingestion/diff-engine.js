const { pool } = require('../../db/init');

/**
 * Diff engine backed by the shipped_records ledger.
 *
 * For each row in staging, if (source_table, source_key, row_hash) already
 * exists in shipped_records, it has been sent and is unchanged — skip.
 * Otherwise, it's new or changed — send.
 *
 * This replaces the prior synced_at-column approach, which didn't work
 * because staging is fully rebuilt each run (DELETE + INSERT).
 */
async function getChangedRows(stagingTable, keyColumn) {
  const sql = `
    SELECT s.*
    FROM ${stagingTable} s
    WHERE s.${keyColumn} IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM shipped_records sr
        WHERE sr.source_table = $1
          AND sr.source_key = s.${keyColumn}
          AND sr.row_hash = s.row_hash
      )
  `;
  const toSync = await pool.query(sql, [stagingTable]);

  // Also flag any staging rows with NULL keys — those can't be tracked, they're errors.
  const nullKey = await pool.query(
    `SELECT * FROM ${stagingTable} WHERE ${keyColumn} IS NULL OR ${keyColumn} = ''`
  );

  // Count how many are unchanged vs total (for reconciliation).
  const total = await pool.query(`SELECT COUNT(*) as c FROM ${stagingTable}`);
  const totalCount = parseInt(total.rows[0].c);
  const skipped = totalCount - toSync.rows.length - nullKey.rows.length;

  return {
    toSync: toSync.rows,
    skipped,
    nullKeyRows: nullKey.rows,
    total: totalCount,
  };
}

/**
 * Record that a batch of rows was successfully shipped to HubSpot.
 * Takes succeeded entries from batchUpsert: [{ sourceKey, hubspotId, wasNew }]
 * and the matching source row data (for the row_hash).
 */
async function recordShipped(sourceTable, successes, sourceRowsByKey) {
  if (!successes || successes.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const { sourceKey, hubspotId } of successes) {
      const row = sourceRowsByKey.get(sourceKey);
      if (!row) continue;
      await client.query(
        `INSERT INTO shipped_records (source_table, source_key, row_hash, hubspot_id, shipped_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (source_table, source_key)
         DO UPDATE SET row_hash = EXCLUDED.row_hash,
                       hubspot_id = EXCLUDED.hubspot_id,
                       shipped_at = NOW()`,
        [sourceTable, sourceKey, row.row_hash, hubspotId]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { getChangedRows, recordShipped };
