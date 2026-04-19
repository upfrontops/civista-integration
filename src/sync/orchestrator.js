const fs = require('fs');
const path = require('path');
const { pool } = require('../../db/init');
const { parseAndStage, hashFile } = require('../ingestion/csv-parser');
const { checkCircuitBreaker } = require('../ingestion/circuit-breaker');
const { getChangedRows, markSynced } = require('../ingestion/diff-engine');
const { classifyCifRecords } = require('../transform/classify');
const {
  syncCifContacts,
  syncCifCompanies,
  syncDda,
  syncLoans,
  syncCds,
  syncDebitCards,
} = require('./hubspot');

// Map CSV filenames to table names
const FILE_TABLE_MAP = {
  'HubSpot_CIF.csv': 'cif',
  'HubSpot_DDA.csv': 'dda',
  'HubSpot_Loan.csv': 'loans',
  'HubSpot_CD.csv': 'cd',
  'HubSpot_Debit_Card.csv': 'debit_cards',
};

// Key column used for diff engine per staging table
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
      // Subtract 1 for header row
      resolve(Math.max(0, count - 1));
    });

    stream.on('error', reject);
  });
}

async function syncTable(tableName, filePath) {
  const stagingTable = `stg_${tableName}`;
  const fileHash = hashFile(filePath);
  const rowCount = await countCsvRows(filePath);
  const logId = await createSyncLog(tableName, rowCount, fileHash);

  console.log(`\n=== Syncing ${tableName} (${rowCount} rows) ===`);

  try {
    // Circuit breaker check
    const cbResult = await checkCircuitBreaker(tableName, rowCount);
    if (!cbResult.safe) {
      console.warn(`CIRCUIT BREAKER: Skipping ${tableName} — ${cbResult.reason}`);
      await updateSyncLog(logId, {
        records_attempted: 0,
        records_skipped: rowCount,
        error_details: { circuit_breaker: cbResult.reason },
      });
      return { tableName, skipped: true, reason: cbResult.reason };
    }

    // Parse and stage
    await parseAndStage(filePath, tableName);

    // Classification (CIF only)
    if (tableName === 'cif') {
      await classifyCifRecords();
    }

    // Diff engine — get rows that need syncing
    const keyColumn = KEY_COLUMNS[stagingTable];
    const { toSync, skipped } = await getChangedRows(stagingTable, keyColumn);

    console.log(`${tableName}: ${toSync.length} to sync, ${skipped} unchanged`);

    if (toSync.length === 0) {
      await updateSyncLog(logId, {
        records_attempted: 0,
        records_skipped: skipped,
      });
      return { tableName, synced: 0, skipped };
    }

    // Sync to HubSpot
    let results;
    switch (tableName) {
      case 'cif': {
        const contacts = toSync.filter(r => r.record_type === 'contact');
        const companies = toSync.filter(r => r.record_type === 'company');

        const contactResults = contacts.length > 0
          ? await syncCifContacts(contacts)
          : { created: 0, updated: 0, failed: 0, errors: [] };

        const companyResults = companies.length > 0
          ? await syncCifCompanies(companies)
          : { created: 0, updated: 0, failed: 0, errors: [] };

        results = {
          created: contactResults.created + companyResults.created,
          updated: contactResults.updated + companyResults.updated,
          failed: contactResults.failed + companyResults.failed,
          errors: [...contactResults.errors, ...companyResults.errors],
        };
        break;
      }
      case 'dda':
        results = await syncDda(toSync);
        break;
      case 'loans':
        results = await syncLoans(toSync);
        break;
      case 'cd':
        results = await syncCds(toSync);
        break;
      case 'debit_cards':
        results = await syncDebitCards(toSync);
        break;
    }

    // Mark synced rows
    const syncedIds = toSync.map(r => r.id);
    await markSynced(stagingTable, syncedIds);

    // Update sync log
    await updateSyncLog(logId, {
      records_attempted: toSync.length,
      records_created: results.created,
      records_updated: results.updated,
      records_failed: results.failed,
      records_skipped: skipped,
      error_details: results.errors.length > 0 ? results.errors : null,
    });

    console.log(`${tableName}: created=${results.created} updated=${results.updated} failed=${results.failed}`);
    return { tableName, ...results, skipped };
  } catch (err) {
    console.error(`Error syncing ${tableName}: ${err.message}`);
    await updateSyncLog(logId, {
      records_attempted: 0,
      records_failed: rowCount,
      error_details: { error: err.message, stack: err.stack },
    });
    return { tableName, error: err.message };
  }
}

async function archiveFile(filePath, archiveDir) {
  const date = new Date().toISOString().split('T')[0];
  const dest = path.join(archiveDir, date);
  fs.mkdirSync(dest, { recursive: true });
  const filename = path.basename(filePath);
  fs.renameSync(filePath, path.join(dest, filename));
  console.log(`Archived ${filename} → ${dest}/`);
}

async function runFullSync(incomingDir, archiveDir) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Starting full sync at ${new Date().toISOString()}`);
  console.log(`${'='.repeat(60)}`);

  const results = [];

  // Discover CSV files in incoming directory
  if (!fs.existsSync(incomingDir)) {
    console.log(`No incoming directory found at ${incomingDir}`);
    return results;
  }

  const files = fs.readdirSync(incomingDir).filter(f => f.endsWith('.csv'));
  if (files.length === 0) {
    console.log('No CSV files found in incoming directory');
    return results;
  }

  // Sync in order: CIF first (contacts/companies), then accounts
  const syncOrder = ['HubSpot_CIF.csv', 'HubSpot_DDA.csv', 'HubSpot_Loan.csv', 'HubSpot_CD.csv', 'HubSpot_Debit_Card.csv'];

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

    // Archive the processed file
    await archiveFile(filePath, archiveDir);
  }

  console.log(`\nSync complete. ${results.length} tables processed.`);
  return results;
}

module.exports = { runFullSync, syncTable, FILE_TABLE_MAP };
