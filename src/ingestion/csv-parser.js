const fs = require('fs');
const crypto = require('crypto');
const { Transform } = require('stream');
const { parse } = require('csv-parse');
const { pool } = require('../../db/init');
const {
  TABLES,
  CIF_CONTACT_FIELDS,
  CIF_COMPANY_FIELDS,
  classifyCifRow,
} = require('../transform/hubspot-mapping');

/**
 * QuoteFixer — stream transform that rewrites RFC-non-compliant CSV
 * from Silver Lake (unescaped double-quotes inside quoted fields, e.g.
 * `"405 "I" Street"`) into RFC-4180-compliant CSV by doubling any quote
 * that appears mid-field.
 *
 * State machine:
 *   inQuote = false → copy bytes through; flip to true on every `"`
 *   inQuote = true  → `"` is ambiguous:
 *     - followed by delimiter/newline/EOF ⇒ true close-quote (flip to false)
 *     - followed by another `"`          ⇒ already-escaped (pass both through)
 *     - otherwise                         ⇒ embedded unescaped quote — double it
 *
 * Without this, a single bad address halts the entire CIF parse.
 */
class QuoteFixer extends Transform {
  constructor(opts) {
    super(opts);
    this.inQuote = false;
    this.buf = '';
    this.fixCount = 0;
  }
  _transform(chunk, enc, cb) {
    const s = this.buf + chunk.toString('utf8');
    let out = '';
    let i = 0;
    // Process all but the last char so we always have a lookahead.
    while (i < s.length - 1) {
      const c = s[i], n = s[i + 1];
      if (!this.inQuote) {
        out += c;
        if (c === '"') this.inQuote = true;
      } else {
        if (c === '"') {
          if (n === ',' || n === '\n' || n === '\r') {
            out += c;
            this.inQuote = false;
          } else if (n === '"') {
            out += c + n;
            i++;
          } else {
            out += c + c;
            this.fixCount++;
          }
        } else {
          out += c;
        }
      }
      i++;
    }
    this.buf = s.substring(i);
    cb(null, out);
  }
  _flush(cb) {
    cb(null, this.buf);
  }
}

/**
 * Derive expected headers for a source from the mapping module.
 * For CIF, use the union of contact + company CSV columns since the
 * raw CSV doesn't know which row is which yet.
 */
function expectedHeadersFor(source) {
  if (source === 'cif') {
    const set = new Set();
    for (const f of CIF_CONTACT_FIELDS) set.add(f.csv);
    for (const f of CIF_COMPANY_FIELDS) set.add(f.csv);
    return Array.from(set);
  }
  return TABLES[source].fields.map(f => f.csv);
}

function generateRowHash(values) {
  return crypto.createHash('sha256').update(values.join('|')).digest('hex');
}

function generateCompositeKeyFromRaw(row) {
  const cif  = row['CIF#']           || '';
  const acct = row['Acctlast4']      || '';
  const type = row['AcctType']       || '';
  const card = row['Last4DebitCard'] || '';
  const exp  = row['Expiredate']     || '';
  return `${cif}${acct}${type}${card}${exp}`;
}

function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', (c) => hash.update(c));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

function validateHeaders(actual, expected) {
  const trimmedActual = actual.map(h => h.trim());
  const missing = expected.filter(h => !trimmedActual.includes(h));
  if (missing.length > 0) {
    throw new Error(`Missing expected headers: ${missing.join(', ')}`);
  }
  return trimmedActual;
}

/**
 * Map a raw CSV row into an object keyed by HubSpot property names,
 * using the supplied field list. Any field in the list whose CSV column
 * isn't in the row gets null. Fields in the row that aren't in the list
 * are dropped.
 */
function mapRowToHubspotProps(row, fieldList) {
  const out = {};
  for (const { csv, prop } of fieldList) {
    const v = row[csv];
    out[prop] = (v === undefined || v === null || v === '') ? null : v;
  }
  return out;
}

/**
 * Parse a CSV and stage it into the appropriate Postgres staging table(s).
 *
 *   source === 'cif'          → routes to stg_contacts or stg_companies per row
 *   source === 'dda'          → stg_deposits
 *   source === 'loans'        → stg_loans
 *   source === 'cd'           → stg_time_deposits
 *   source === 'debit_cards'  → stg_debit_cards
 *
 * Returns a summary { rowCount, fileHash, byTable: { stg_contacts: N, ... }, unclassified: [...] }.
 */
async function parseAndStage(filePath, source) {
  const expected = expectedHeadersFor(source);
  const fileHash = await hashFile(filePath);

  // Buckets to collect rows per target staging table.
  const buckets = {};
  const unclassified = [];  // raw rows that classifyCifRow returns 'unclassified' for
  let rowCount = 0;

  // Track rows we had to skip at the parser level (malformed CSV — NOT silent).
  const parseSkipped = [];
  const quoteFixer = new QuoteFixer();

  const parser = fs.createReadStream(filePath)
    .pipe(quoteFixer)
    .pipe(parse({
      columns: true,
      trim: true,
      skip_empty_lines: true,
      relax_column_count: true,
      skip_records_with_error: true,
    }));

  parser.on('skip', (err) => {
    const msg = err && err.message ? err.message : 'unknown parse error';
    parseSkipped.push({ message: msg, at: err?.lines });
    console.warn(`⚠  CSV row skipped (line ${err?.lines ?? '?'}): ${msg}`);
  });

  return new Promise((resolve, reject) => {
    parser.on('headers', (headers) => {
      try { validateHeaders(headers, expected); }
      catch (e) { parser.destroy(e); }
    });

    parser.on('data', (rawRow) => {
      rowCount++;

      // Trim string values in place.
      for (const k of Object.keys(rawRow)) {
        if (typeof rawRow[k] === 'string') rawRow[k] = rawRow[k].trim();
      }

      // row_hash computed over the ORIGINAL raw CSV values (sorted for stability).
      const rowHashInput = Object.keys(rawRow).sort().map(k => `${k}=${rawRow[k] ?? ''}`).join('|');
      const rowHash = crypto.createHash('sha256').update(rowHashInput).digest('hex');

      if (source === 'cif') {
        const cls = classifyCifRow(rawRow);
        if (cls === 'unclassified') {
          unclassified.push(rawRow);
          return;
        }
        const targetTable = cls === 'contact' ? 'stg_contacts' : 'stg_companies';
        const fields = cls === 'contact' ? CIF_CONTACT_FIELDS : CIF_COMPANY_FIELDS;
        const mapped = mapRowToHubspotProps(rawRow, fields);
        mapped.row_hash = rowHash;
        (buckets[targetTable] ||= []).push(mapped);
        return;
      }

      const config = TABLES[source];
      const mapped = mapRowToHubspotProps(rawRow, config.fields);
      if (source === 'debit_cards') {
        mapped.composite_key = generateCompositeKeyFromRaw(rawRow);
      }
      mapped.row_hash = rowHash;
      (buckets[config.staging] ||= []).push(mapped);
    });

    parser.on('end', async () => {
      try {
        const byTable = {};
        for (const [table, rows] of Object.entries(buckets)) {
          await insertRows(table, rows);
          byTable[table] = rows.length;
        }
        if (quoteFixer.fixCount > 0) {
          console.warn(`⚠  QuoteFixer repaired ${quoteFixer.fixCount} embedded quotes in ${filePath}`);
        }
        resolve({ rowCount, fileHash, byTable, unclassified, parseSkipped, quotesFixed: quoteFixer.fixCount });
      } catch (err) { reject(err); }
    });

    parser.on('error', reject);
  });
}

async function insertRows(stagingTable, rows) {
  if (rows.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Staging is ephemeral — rebuilt every run. Diff engine uses shipped_records as the
    // audit ledger, so this DELETE does not cause the "everything looks new" bug from before.
    await client.query(`DELETE FROM ${stagingTable}`);

    // Determine the column set from the first row — we built these uniformly in the caller.
    const columns = Object.keys(rows[0]);
    const chunkSize = 500;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const placeholders = chunk.map((_, ri) => {
        const off = ri * columns.length;
        return `(${columns.map((_, ci) => `$${off + ci + 1}`).join(', ')})`;
      }).join(', ');
      const values = chunk.flatMap(r => columns.map(c => r[c] ?? null));
      await client.query(
        `INSERT INTO ${stagingTable} (${columns.join(', ')}) VALUES ${placeholders}`,
        values
      );
    }

    await client.query('COMMIT');
    console.log(`Staged ${rows.length} rows into ${stagingTable}`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  parseAndStage,
  hashFile,
  generateRowHash,
  validateHeaders,
  expectedHeadersFor,
};
