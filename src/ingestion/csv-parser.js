const fs = require('fs');
const crypto = require('crypto');
const { parse } = require('csv-parse');
const { pool } = require('../../db/init');

// Expected headers per table for validation
const EXPECTED_HEADERS = {
  cif: ['FirstName', 'LastName', 'FullName', 'Birthday', 'Address1', 'Address2', 'City', 'State', 'ZipCode', 'Email', 'HashSSN', 'CIFNum', 'PrivBanking', 'DoNotCall', 'Age', 'Minor', 'InsiderCode', 'InsufficientAddress', 'CLFFlag', 'CivAtWork', 'OrigCustDate', 'Deceased', 'Sex', 'ClassCode', 'TaxIdType', 'CustRelNum', 'DigBank', 'NAICSCode', 'TM', 'ATMDebitCard', 'Q2userID', 'LastLogin', 'RecurTrans', 'PhoneNumber', 'StmtType', 'EnrollmentDt', 'CentralGroupID', 'TextOptIn', 'DiscAcpt'],
  dda: ['PrimaryKey', 'Acctlast4', 'CIF#', 'InterestRate', 'accttype', 'acctdesc', 'opendate', 'closedate', 'slsassoc', 'dtlastactive', 'currentbal', 'yestbal', 'branchnum', 'officrcode', 'acctstatus', 'relationship', 'promocode', 'openonline'],
  loans: ['PrimaryKey', 'acctlast4', 'CIFNum', 'InterestRate', 'accttype', 'loantype', 'origdate', 'maturitydate', 'slsassoc', 'lastactivedate', 'currbal', 'branchnum', 'officrcode', 'acctstatus', 'relationship', 'origbal'],
  cd: ['PrimaryKey', 'AcctLast4', 'CIFNum', 'InterestRate', 'accttype', 'acctdesc', 'issuedate', 'maturitydate', 'slsassoc', 'currbal', 'branchnum', 'officrcode', 'acctstatus', 'relationship', 'openonline'],
  debit_cards: ['CIF#', 'Acctlast4', 'AcctType', 'Last4DebitCard', 'CardStatus', 'Expiredate', 'Lastuseddt', 'POSLast30Days', 'ActivePOS'],
};

// Column name mapping: CSV header -> staging table column
const COLUMN_MAP = {
  'CIF#': 'cif_number',
  'CIFNum': 'cif_number',
  'FirstName': 'firstname',
  'LastName': 'lastname',
  'FullName': 'fullname',
  'Birthday': 'birthday',
  'Address1': 'address1',
  'Address2': 'address2',
  'City': 'city',
  'State': 'state',
  'ZipCode': 'zipcode',
  'Email': 'email',
  'HashSSN': 'hashssn',
  'PrivBanking': 'privbanking',
  'DoNotCall': 'donotcall',
  'Age': 'age',
  'Minor': 'minor',
  'InsiderCode': 'insidercode',
  'InsufficientAddress': 'insufficientaddress',
  'CLFFlag': 'clfflag',
  'CivAtWork': 'civatwork',
  'OrigCustDate': 'origcustdate',
  'Deceased': 'deceased',
  'Sex': 'sex',
  'ClassCode': 'classcode',
  'TaxIdType': 'taxidtype',
  'CustRelNum': 'custrelnum',
  'DigBank': 'digbank',
  'NAICSCode': 'naicscode',
  'TM': 'tm',
  'ATMDebitCard': 'atmdebitcard',
  'Q2userID': 'q2userid',
  'LastLogin': 'lastlogin',
  'RecurTrans': 'recurtrans',
  'PhoneNumber': 'phonenumber',
  'StmtType': 'stmttype',
  'EnrollmentDt': 'enrollmentdt',
  'CentralGroupID': 'centralgroupid',
  'TextOptIn': 'textoptin',
  'DiscAcpt': 'discacpt',
  'PrimaryKey': 'primarykey',
  'Acctlast4': 'acctlast4',
  'AcctLast4': 'acctlast4',
  'InterestRate': 'interestrate',
  'accttype': 'accttype',
  'AcctType': 'accttype',
  'acctdesc': 'acctdesc',
  'opendate': 'opendate',
  'closedate': 'closedate',
  'slsassoc': 'slsassoc',
  'dtlastactive': 'dtlastactive',
  'currentbal': 'currentbal',
  'yestbal': 'yestbal',
  'branchnum': 'branchnum',
  'officrcode': 'officrcode',
  'acctstatus': 'acctstatus',
  'relationship': 'relationship',
  'promocode': 'promocode',
  'openonline': 'openonline',
  'loantype': 'loantype',
  'origdate': 'origdate',
  'maturitydate': 'maturitydate',
  'lastactivedate': 'lastactivedate',
  'currbal': 'currbal',
  'origbal': 'origbal',
  'issuedate': 'issuedate',
  'Last4DebitCard': 'last4debitcard',
  'CardStatus': 'cardstatus',
  'Expiredate': 'expiredate',
  'Lastuseddt': 'lastuseddt',
  'POSLast30Days': 'poslast30days',
  'ActivePOS': 'activepos',
};

function generateRowHash(values) {
  return crypto.createHash('sha256').update(values.join('|')).digest('hex');
}

function generateCompositeKey(row) {
  const cif = row['CIF#'] || '';
  const acct = row['Acctlast4'] || '';
  const type = row['AcctType'] || '';
  const card = row['Last4DebitCard'] || '';
  const exp = row['Expiredate'] || '';
  return `${cif}${acct}${type}${card}${exp}`;
}

function hashFile(filePath) {
  const content = fs.readFileSync(filePath);
  return crypto.createHash('sha256').update(content).digest('hex');
}

function validateHeaders(actual, expected) {
  const trimmedActual = actual.map(h => h.trim());
  const missing = expected.filter(h => !trimmedActual.includes(h));
  if (missing.length > 0) {
    throw new Error(`Missing expected headers: ${missing.join(', ')}`);
  }
  return trimmedActual;
}

async function parseAndStage(filePath, tableName) {
  const expected = EXPECTED_HEADERS[tableName];
  if (!expected) {
    throw new Error(`Unknown table name: ${tableName}`);
  }

  const fileHash = hashFile(filePath);
  const stagingTable = `stg_${tableName}`;

  return new Promise((resolve, reject) => {
    const rows = [];
    let rowCount = 0;
    let headersValidated = false;

    const parser = fs.createReadStream(filePath).pipe(
      parse({
        columns: true,
        trim: true,
        skip_empty_lines: true,
        relax_column_count: true,
      })
    );

    parser.on('headers', (headers) => {
      try {
        validateHeaders(headers, expected);
        headersValidated = true;
      } catch (err) {
        parser.destroy(err);
      }
    });

    parser.on('data', (row) => {
      rowCount++;

      // Trim all values
      const trimmed = {};
      for (const [key, val] of Object.entries(row)) {
        trimmed[key] = typeof val === 'string' ? val.trim() : val;
      }

      // Generate row hash from all values
      const rowHash = generateRowHash(Object.values(trimmed));

      // Map CSV column names to staging column names
      const mapped = {};
      for (const [csvCol, value] of Object.entries(trimmed)) {
        const dbCol = COLUMN_MAP[csvCol.trim()];
        if (dbCol) {
          mapped[dbCol] = value;
        }
      }

      // Normalize CIF# -> cif_number for DDA and debit_cards
      if (tableName === 'dda' || tableName === 'debit_cards') {
        mapped.cif_number = trimmed['CIF#'] || '';
      }

      // Generate composite key for debit cards
      if (tableName === 'debit_cards') {
        mapped.composite_key = generateCompositeKey(trimmed);
      }

      mapped.row_hash = rowHash;
      rows.push(mapped);
    });

    parser.on('end', async () => {
      try {
        await insertRows(stagingTable, rows, tableName);
        resolve({ rowCount, fileHash, tableName });
      } catch (err) {
        reject(err);
      }
    });

    parser.on('error', reject);
  });
}

async function insertRows(stagingTable, rows, tableName) {
  if (rows.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Clear existing staging data for this table
    await client.query(`DELETE FROM ${stagingTable}`);

    // Batch insert in chunks of 500
    const chunkSize = 500;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const columns = Object.keys(chunk[0]);
      const valuePlaceholders = chunk.map((_, rowIdx) => {
        const offset = rowIdx * columns.length;
        return `(${columns.map((_, colIdx) => `$${offset + colIdx + 1}`).join(', ')})`;
      }).join(', ');

      const values = chunk.flatMap(row => columns.map(col => row[col] || null));

      await client.query(
        `INSERT INTO ${stagingTable} (${columns.join(', ')}) VALUES ${valuePlaceholders}`,
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
  generateCompositeKey,
  validateHeaders,
  EXPECTED_HEADERS,
};
