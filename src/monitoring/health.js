const { pool } = require('../../db/init');

async function getHealthStatus() {
  const client = await pool.connect();
  try {
    // Last sync per table
    const lastSyncs = await client.query(`
      SELECT DISTINCT ON (table_name)
        table_name,
        started_at,
        completed_at,
        records_attempted,
        records_created,
        records_updated,
        records_failed,
        records_skipped,
        error_details
      FROM sync_log
      ORDER BY table_name, started_at DESC
    `);

    const tables = {};
    let hasErrors = false;

    for (const row of lastSyncs.rows) {
      tables[row.table_name] = {
        last_sync: row.completed_at || row.started_at,
        records_attempted: row.records_attempted,
        records_created: row.records_created,
        records_updated: row.records_updated,
        records_failed: row.records_failed,
        records_skipped: row.records_skipped,
        errors: row.error_details,
      };
      if (row.records_failed > 0 || row.error_details) {
        hasErrors = true;
      }
    }

    return {
      status: hasErrors ? 'unhealthy' : 'healthy',
      database: 'connected',
      tables,
    };
  } catch (err) {
    return {
      status: 'unhealthy',
      database: 'disconnected',
      error: err.message,
    };
  } finally {
    client.release();
  }
}

module.exports = { getHealthStatus };
