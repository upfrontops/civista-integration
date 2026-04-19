const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS sync_log (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(50),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        records_attempted INTEGER,
        records_created INTEGER,
        records_updated INTEGER,
        records_failed INTEGER,
        records_skipped INTEGER,
        error_details JSONB,
        file_hash VARCHAR(64),
        row_count INTEGER
      );
    `);
    console.log('Database initialized: sync_log table created');
  } finally {
    client.release();
  }
}

module.exports = { initDb, pool };
