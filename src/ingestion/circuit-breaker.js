const { pool } = require('../../db/init');

const DROP_THRESHOLD = 0.30; // 30% drop triggers circuit breaker

async function checkCircuitBreaker(tableName, currentRowCount) {
  const result = await pool.query(
    `SELECT row_count FROM sync_log
     WHERE table_name = $1 AND row_count IS NOT NULL
     ORDER BY started_at DESC LIMIT 1`,
    [tableName]
  );

  if (result.rows.length === 0) {
    // First sync ever — no previous data to compare
    return { safe: true, reason: 'first sync' };
  }

  const previousCount = result.rows[0].row_count;
  if (previousCount === 0) {
    return { safe: true, reason: 'previous count was zero' };
  }

  const dropPercent = (previousCount - currentRowCount) / previousCount;

  if (dropPercent > DROP_THRESHOLD) {
    return {
      safe: false,
      reason: `Row count dropped ${(dropPercent * 100).toFixed(1)}% (${previousCount} → ${currentRowCount}). Threshold is ${DROP_THRESHOLD * 100}%.`,
      previousCount,
      currentCount: currentRowCount,
    };
  }

  return { safe: true, reason: 'within threshold' };
}

module.exports = { checkCircuitBreaker, DROP_THRESHOLD };
