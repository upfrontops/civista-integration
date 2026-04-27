/**
 * Error recorder. Writes every failure/quarantine/validation-miss to sync_errors.
 * Called from anywhere in the pipeline. Never silent.
 */

const { pool } = require('../../db/init');

const ERROR_TYPES = {
  CLASSIFICATION: 'classification',
  VALIDATION: 'validation',
  HUBSPOT_BATCH: 'hubspot_batch',
  HUBSPOT_RECORD: 'hubspot_record',
  PARSE: 'parse',
  INFRA: 'infra',
};

async function recordError({ runId, sourceTable, sourceKey, errorType, errorMessage, recordSnapshot }) {
  await pool.query(
    `INSERT INTO sync_errors (run_id, source_table, source_key, error_type, error_message, record_snapshot)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      runId || null,
      sourceTable || null,
      sourceKey || null,
      errorType,
      errorMessage,
      recordSnapshot ? JSON.stringify(recordSnapshot) : null,
    ]
  );
}

async function recordErrorBatch(errors) {
  if (!errors || errors.length === 0) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const e of errors) {
      await client.query(
        `INSERT INTO sync_errors (run_id, source_table, source_key, error_type, error_message, record_snapshot)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          e.runId || null,
          e.sourceTable || null,
          e.sourceKey || null,
          e.errorType,
          e.errorMessage,
          e.recordSnapshot ? JSON.stringify(e.recordSnapshot) : null,
        ]
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

async function countErrorsForRun(runId) {
  const result = await pool.query(
    `SELECT error_type, COUNT(*) as count FROM sync_errors WHERE run_id = $1 GROUP BY error_type`,
    [runId]
  );
  const counts = {};
  let total = 0;
  for (const row of result.rows) {
    counts[row.error_type] = parseInt(row.count);
    total += parseInt(row.count);
  }
  return { total, byType: counts };
}

// Some errors (notably pg AggregateError on ECONNREFUSED) come back with
// an empty `.message`. Falling back to .code / nested .errors keeps the
// failure visible — silent "" responses are a no-ship per CLAUDE.md.
function describeError(e) {
  if (!e) return 'unknown error';
  if (typeof e === 'string') return e;
  if (e.message) return e.message;
  if (e.code) return `error code ${e.code}`;
  if (Array.isArray(e.errors) && e.errors[0]) return describeError(e.errors[0]);
  try { return JSON.stringify(e); } catch { return String(e); }
}

module.exports = { recordError, recordErrorBatch, countErrorsForRun, describeError, ERROR_TYPES };
