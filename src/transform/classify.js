const { pool } = require('../../db/init');

/**
 * CIF Classification.
 * Every row gets exactly one of: 'company', 'contact', or 'unclassified'.
 * Unclassified rows are returned to the caller so they can be recorded
 * in sync_errors and quarantined. Nothing is silently dropped.
 */
async function classifyCifRecords() {
  const client = await pool.connect();
  try {
    // Company: TaxIdType = 'T' AND FirstName empty AND LastName empty
    const companyResult = await client.query(`
      UPDATE stg_cif
      SET record_type = 'company'
      WHERE taxidtype = 'T'
        AND (firstname IS NULL OR firstname = '')
        AND (lastname IS NULL OR lastname = '')
    `);

    // Contact: TaxIdType = 'T' AND FirstName+LastName populated
    const contactBizResult = await client.query(`
      UPDATE stg_cif
      SET record_type = 'contact'
      WHERE taxidtype = 'T'
        AND firstname IS NOT NULL AND firstname != ''
        AND lastname IS NOT NULL AND lastname != ''
    `);

    // Contact: TaxIdType has a non-'T' value (not null)
    const contactResult = await client.query(`
      UPDATE stg_cif
      SET record_type = 'contact'
      WHERE taxidtype IS NOT NULL AND taxidtype != 'T'
    `);

    // Anything still unclassified: mark explicitly and return for quarantine.
    // Covers: taxidtype IS NULL, or taxidtype='T' with partial name, etc.
    const unclassifiedUpdate = await client.query(`
      UPDATE stg_cif
      SET record_type = 'unclassified'
      WHERE record_type IS NULL
    `);

    let unclassified = [];
    if (unclassifiedUpdate.rowCount > 0) {
      const unclassifiedRows = await client.query(`
        SELECT * FROM stg_cif WHERE record_type = 'unclassified'
      `);
      unclassified = unclassifiedRows.rows;
    }

    const counts = {
      companies: companyResult.rowCount,
      contacts_biz: contactBizResult.rowCount,
      contacts_individual: contactResult.rowCount,
      unclassified: unclassifiedUpdate.rowCount,
    };

    console.log(
      `CIF classification: ${counts.companies} companies, ` +
      `${counts.contacts_biz + counts.contacts_individual} contacts, ` +
      `${counts.unclassified} unclassified (quarantined)`
    );

    return { counts, unclassified };
  } finally {
    client.release();
  }
}

module.exports = { classifyCifRecords };
