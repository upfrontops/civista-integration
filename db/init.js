const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(`
      CREATE TABLE IF NOT EXISTS sync_log (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(50),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        records_attempted INTEGER DEFAULT 0,
        records_created INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        records_failed INTEGER DEFAULT 0,
        records_skipped INTEGER DEFAULT 0,
        error_details JSONB,
        file_hash VARCHAR(64),
        row_count INTEGER
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_cif (
        id SERIAL PRIMARY KEY,
        firstname TEXT,
        lastname TEXT,
        fullname TEXT,
        birthday TEXT,
        address1 TEXT,
        address2 TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        email TEXT,
        hashssn TEXT,
        cif_number TEXT,
        privbanking TEXT,
        donotcall TEXT,
        age TEXT,
        minor TEXT,
        insidercode TEXT,
        insufficientaddress TEXT,
        clfflag TEXT,
        civatwork TEXT,
        origcustdate TEXT,
        deceased TEXT,
        sex TEXT,
        classcode TEXT,
        taxidtype TEXT,
        custrelnum TEXT,
        digbank TEXT,
        naicscode TEXT,
        tm TEXT,
        atmdebitcard TEXT,
        q2userid TEXT,
        lastlogin TEXT,
        recurtrans TEXT,
        phonenumber TEXT,
        stmttype TEXT,
        enrollmentdt TEXT,
        centralgroupid TEXT,
        textoptin TEXT,
        discacpt TEXT,
        record_type VARCHAR(10),
        row_hash VARCHAR(64),
        synced_at TIMESTAMPTZ
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_dda (
        id SERIAL PRIMARY KEY,
        primarykey TEXT,
        acctlast4 TEXT,
        cif_number TEXT,
        interestrate TEXT,
        accttype TEXT,
        acctdesc TEXT,
        opendate TEXT,
        closedate TEXT,
        slsassoc TEXT,
        dtlastactive TEXT,
        currentbal TEXT,
        yestbal TEXT,
        branchnum TEXT,
        officrcode TEXT,
        acctstatus TEXT,
        relationship TEXT,
        promocode TEXT,
        openonline TEXT,
        row_hash VARCHAR(64),
        synced_at TIMESTAMPTZ
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_loans (
        id SERIAL PRIMARY KEY,
        primarykey TEXT,
        acctlast4 TEXT,
        cif_number TEXT,
        interestrate TEXT,
        accttype TEXT,
        loantype TEXT,
        origdate TEXT,
        maturitydate TEXT,
        slsassoc TEXT,
        lastactivedate TEXT,
        currbal TEXT,
        branchnum TEXT,
        officrcode TEXT,
        acctstatus TEXT,
        relationship TEXT,
        origbal TEXT,
        row_hash VARCHAR(64),
        synced_at TIMESTAMPTZ
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_cd (
        id SERIAL PRIMARY KEY,
        primarykey TEXT,
        acctlast4 TEXT,
        cif_number TEXT,
        interestrate TEXT,
        accttype TEXT,
        acctdesc TEXT,
        issuedate TEXT,
        maturitydate TEXT,
        slsassoc TEXT,
        currbal TEXT,
        branchnum TEXT,
        officrcode TEXT,
        acctstatus TEXT,
        relationship TEXT,
        openonline TEXT,
        row_hash VARCHAR(64),
        synced_at TIMESTAMPTZ
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_debit_cards (
        id SERIAL PRIMARY KEY,
        cif_number TEXT,
        acctlast4 TEXT,
        accttype TEXT,
        last4debitcard TEXT,
        cardstatus TEXT,
        expiredate TEXT,
        lastuseddt TEXT,
        poslast30days TEXT,
        activepos TEXT,
        composite_key VARCHAR(255),
        row_hash VARCHAR(64),
        synced_at TIMESTAMPTZ
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS hubspot_id_map (
        id SERIAL PRIMARY KEY,
        source_table VARCHAR(50) NOT NULL,
        source_key VARCHAR(255) NOT NULL,
        hubspot_id VARCHAR(50) NOT NULL,
        object_type VARCHAR(50) NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(source_table, source_key)
      );
    `);

    await client.query('COMMIT');
    console.log('Database initialized: all tables created');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { initDb, pool };
