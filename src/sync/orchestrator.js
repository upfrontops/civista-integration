const fs = require('fs');
const path = require('path');
const { pool } = require('../../db/init');
const { parseAndStage, hashFile } = require('../ingestion/csv-parser');
const { checkCircuitBreaker } = require('../ingestion/circuit-breaker');
const { getChangedRows, recordShipped } = require('../ingestion/diff-engine');
const { classifyCifRecords } = require('../transform/classify');
const { recordErrorBatch, ERROR_TYPES } = require('../monitoring/errors');
const {
  syncCifContacts,
  syncCifCompanies,
  syncDda,
  syncLoans,
  syncCds,
  syncDebitCards,
} = require('./hubspot');

const FILE_TABLE_MAP = {
  'HubSpot_CIF.csv': 'cif',
  'HubSpot_DDA.csv': 'dda',
  'HubSpot_Loan.csv': 'loans',
  'HubSpot_CD.csv': 'cd',
  'HubSpot_Debit_Card.csv': 'debit_cards',
};

// Key column per staging table — used by diff engine and ledger.
const KEY_COLUMNS = {
  stg_cif: 'cif_number',
  stg_dda: 'primarykey',
  stg_loans: 'primarykey',
  stg_cd: 'primarykey',
  stg_debit_cards: 'composite_key',
};

async function createSyncLog(tableName, rowCount, fileHash) {
  const result = await pool.query(
    `INSERT INTO sync_log (table_name, started_at, row_count, file_hash)
     VALUES ($1, NOW(), $2, $3) RETURNING id`,
    [tableName, rowCount, fileHash]
  );
  return result.rows[0].id;
}

async function updateSyncLog(logId, updates) {
  const sets = [];
  const values = [logId];
  let paramIdx = 2;

  for (const [key, val] of Object.entries(updates)) {
    sets.push(`${key} = $${paramIdx}`);
    values.push(key === 'error_details' ? JSON.stringify(val) : val);
    paramIdx++;
  }

  sets.push(`completed_at = NOW()`);

  await pool.query(
    `UPDATE sync_log SET ${sets.join(', ')} WHERE id = $1`,
    values
  );
}

async function countCsvRows(filePath) {
  return new Promise((resolve, reject) => {
    let count = 0;
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    let remainder = '';

    stream.on('data', (chunk) => {
      const lines = (remainder + chunk).split('\n');
      remainder = lines.pop();
      count += lines.length;
    });

    stream.on('end', () => {
      if (remainder.trim()) count++;
      resolve(Math.max(0, count - 1));
    });

    stream.on('error', reject);
  });
}

/**
 * Sync one table end-to-end. Returns a reconciliation report:
 *   { tableName, sourceRowCount, shippedCount, errorCount, skippedUnchanged,
 *     quarantineCount, reconciled (boolean), skipped?, error? }
 */
async function syncTable(tableName, filePath) {
  const stagingTable = `stg_${tableName}`;
  const fileHash = hashFile(filePath);
  const rowCount = await countCsvRows(filePath);
  const runId = await createSyncLog(tableName, rowCount, fileHash);

  console.log(`\n=== Syncing ${tableName} (${rowCount} rows, run_id=${runId}) ===`);

  try {
    // Circuit breaker
    const cbResult = await checkCircuitBreaker(tableName, rowCount);
    if (!cbResult.safe) {
      console.warn(`CIRCUIT BREAKER: Skipping ${tableName} — ${cbResult.reason}`);
      await recordErrorBatch([{
        runId,
        sourceTable: stagingTable,
        errorType: ERROR_TYPES.INFRA,
        errorMessage: `Circuit breaker: ${cbResult.reason}`,
      }]);
      await updateSyncLog(runId, {
        records_attempted: 0,
        records_skipped: rowCount,
        error_details: { circuit_breaker: cbResult.reason },
      });
      return {
        tableName, runId, sourceRowCount: rowCount,
        shippedCount: 0, errorCount: 1, skippedUnchanged: 0, quarantineCount: rowCount,
        reconciled: false, skipped: true, reason: cbResult.reason,
      };
    }

    // Parse + stage
    await parseAndStage(filePath, tableName);

    // Classification (CIF only). Unclassified rows go to sync_errors and are not sent.
    let unclassifiedCount = 0;
    if (tableName === 'cif') {
      const { unclassified } = await classifyCifRecords();
      unclassifiedCount = unclassified.length;
      if (unclassified.length > 0) {
        await recordErrorBatch(unclassified.map(r => ({
          runId,
          sourceTable: stagingTable,
          sourceKey: r.cif_number || null,
          errorType: ERROR_TYPES.CLASSIFICATION,
          errorMessage: 'Could not classify as contact or company (likely NULL taxidtype or partial name data)',
          recordSnapshot: r,
        })));
      }
    }

    // Diff
    const keyColumn = KEY_COLUMNS[stagingTable];
    const { toSync, skipped, nullKeyRows } = await getChangedRows(stagingTable, keyColumn);
    console.log(`${tableName}: ${toSync.length} to sync, ${skipped} unchanged, ${nullKeyRows.length} null-key (quarantined)`);

    // Null-key rows cannot be sent (can't identify them in HubSpot). Quarantine.
    if (nullKeyRows.length > 0) {
      await recordErrorBatch(nullKeyRows.map(r => ({
        runId,
        sourceTable: stagingTable,
        sourceKey: null,
        errorType: ERROR_TYPES.VALIDATION,
        errorMessage: `Missing key column (${keyColumn}) — cannot upsert to HubSpot`,
        recordSnapshot: r,
      })));
    }

    // Build index of source rows by key for ledger writeback.
    const sourceByKey = new Map();
    for (const r of toSync) sourceByKey.set(r[keyColumn], r);

    let totalShipped = 0;
    let totalFailed = 0;
    let totalInvalid = 0;

    // Helper: run one sync, record shipped + failed.
    const runSegment = async (segmentRows, syncFn, segmentName) => {
      if (segmentRows.length === 0) return;
      // Build a by-key index for *this segment* so ledger writes use the right row_hash.
      const segmentByKey = new Map();
      for (const r of segmentRows) {
        const k = r[keyColumn === 'cif_number' ? 'cif_number' : keyColumn];
        if (k) segmentByKey.set(k, r);
      }

      const { succeeded, failed, invalidInputs } = await syncFn(segmentRows);

      await recordShipped(stagingTable, succeeded, segmentByKey);
      totalShipped += succeeded.length;

      if (invalidInputs.length > 0) {
        await recordErrorBatch(invalidInputs.map(i => ({
          runId,
          sourceTable: stagingTable,
          sourceKey: null,
          errorType: ERROR_TYPES.VALIDATION,
          errorMessage: `[${segmentName}] ${i.reason}`,
          recordSnapshot: i.input,
        })));
        totalInvalid += invalidInputs.length;
      }

      if (failed.length > 0) {
        await recordErrorBatch(failed.map(f => ({
          runId,
          sourceTable: stagingTable,
          sourceKey: f.sourceKey,
          errorType: ERROR_TYPES.HUBSPOT_RECORD,
          errorMessage: `[${segmentName}] ${f.reason}`,
        })));
        totalFailed += failed.length;
      }
    };

    // Dispatch to the appropriate HubSpot sync function(s).
    if (tableName === 'cif') {
      const contacts = toSync.filter(r => r.record_type === 'contact');
      const companies = toSync.filter(r => r.record_type === 'company');
      // Any 'unclassified' rows in toSync were already recorded above;
      // they are deliberately excluded from the HubSpot push.

      await runSegment(contacts, syncCifContacts, 'contacts');
      await runSegment(companies, syncCifCompanies, 'companies');
    } else {
      const syncFn = {
        dda: syncDda, loans: syncLoans, cd: syncCds, debit_cards: syncDebitCards,
      }[tableName];
      await runSegment(toSync, syncFn, tableName);
    }

    const quarantineCount = (nullKeyRows.length) + unclassifiedCount + totalInvalid + totalFailed;
    const reconciled = (totalShipped + skipped + quarantineCount) === rowCount;

    await updateSyncLog(runId, {
      records_attempted: toSync.length,
      records_created: totalShipped, // not split by created vs updated anymore; both count as shipped
      records_updated: 0,
      records_failed: totalFailed + totalInvalid,
      records_skipped: skipped,
      error_details: reconciled
        ? null
        : { reconciliation_mismatch: true, source: rowCount, shipped: totalShipped, skipped, quarantine: quarantineCount },
    });

    console.log(
      `${tableName}: source=${rowCount} shipped=${totalShipped} ` +
      `skipped=${skipped} quarantined=${quarantineCount} ` +
      `reconciled=${reconciled}`
    );

    return {
      tableName, runId,
      sourceRowCount: rowCount,
      shippedCount: totalShipped,
      errorCount: totalFailed + totalInvalid + nullKeyRows.length + unclassifiedCount,
      skippedUnchanged: skipped,
      quarantineCount,
      reconciled,
    };
  } catch (err) {
    // Any uncaught error = infra failure for this table. Record and re-raise to caller
    // so it can quarantine the file.
    console.error(`Error syncing ${tableName}: ${err.message}`);
    await recordErrorBatch([{
      runId,
      sourceTable: stagingTable,
      errorType: ERROR_TYPES.INFRA,
      errorMessage: err.message,
      recordSnapshot: { stack: err.stack },
    }]);
    await updateSyncLog(runId, {
      records_attempted: 0,
      records_failed: rowCount,
      error_details: { error: err.message },
    });
    return {
      tableName, runId,
      sourceRowCount: rowCount,
      shippedCount: 0,
      errorCount: 1,
      skippedUnchanged: 0,
      quarantineCount: 0,
      reconciled: false,
      error: err.message,
    };
  }
}

/**
 * Archive a successfully-processed file.
 * Only call when syncTable returned without error.
 */
async function archiveFile(filePath, archiveDir) {
  const date = new Date().toISOString().split('T')[0];
  const dest = path.join(archiveDir, date);
  fs.mkdirSync(dest, { recursive: true });
  const filename = path.basename(filePath);
  fs.renameSync(filePath, path.join(dest, filename));
  console.log(`Archived ${filename} → ${dest}/`);
}

/**
 * Move a failed file to quarantine with an error sidecar.
 * The next run will NOT retry automatically — operator must move it back
 * to /incoming/ after investigating.
 */
async function quarantineFile(filePath, quarantineDir, result) {
  const date = new Date().toISOString().split('T')[0];
  const dest = path.join(quarantineDir, date);
  fs.mkdirSync(dest, { recursive: true });
  const filename = path.basename(filePath);
  const destPath = path.join(dest, filename);
  fs.renameSync(filePath, destPath);
  fs.writeFileSync(
    `${destPath}.error.json`,
    JSON.stringify({
      quarantinedAt: new Date().toISOString(),
      result,
    }, null, 2)
  );
  console.warn(`QUARANTINED ${filename} → ${dest}/ (see .error.json)`);
}

async function runFullSync(incomingDir, archiveDir, quarantineDir) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Starting full sync at ${new Date().toISOString()}`);
  console.log(`${'='.repeat(60)}`);

  if (!quarantineDir) {
    quarantineDir = path.join(path.dirname(archiveDir), 'quarantine');
  }

  const results = [];

  if (!fs.existsSync(incomingDir)) {
    console.log(`No incoming directory found at ${incomingDir}`);
    return { runs: results, reconciled: true };
  }

  const files = fs.readdirSync(incomingDir).filter(f => f.endsWith('.csv'));
  if (files.length === 0) {
    console.log('No CSV files found in incoming directory');
    return { runs: results, reconciled: true };
  }

  const syncOrder = [
    'HubSpot_CIF.csv', 'HubSpot_DDA.csv', 'HubSpot_Loan.csv',
    'HubSpot_CD.csv', 'HubSpot_Debit_Card.csv',
  ];

  for (const filename of syncOrder) {
    if (!files.includes(filename)) {
      console.log(`Skipping ${filename} — not found in incoming`);
      continue;
    }

    const tableName = FILE_TABLE_MAP[filename];
    if (!tableName) continue;

    const filePath = path.join(incomingDir, filename);
    const result = await syncTable(tableName, filePath);
    results.push(result);

    if (result.error || result.skipped || !result.reconciled) {
      // Any non-clean run → quarantine the file, don't archive it.
      await quarantineFile(filePath, quarantineDir, result);
    } else {
      await archiveFile(filePath, archiveDir);
    }
  }

  const allReconciled = results.every(r => r.reconciled);
  console.log(`\nSync complete. ${results.length} tables processed. All reconciled: ${allReconciled}`);
  return { runs: results, reconciled: allReconciled };
}

module.exports = { runFullSync, syncTable, FILE_TABLE_MAP };
