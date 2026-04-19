const { pool } = require('../../db/init');

/**
 * Step 3: CIF Classification Engine
 * Classifies staged CIF records as 'contact' or 'company' based on TaxIdType and name fields.
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

    // Contact: TaxIdType != 'T'
    const contactResult = await client.query(`
      UPDATE stg_cif
      SET record_type = 'contact'
      WHERE taxidtype != 'T'
    `);

    const counts = {
      companies: companyResult.rowCount,
      contacts_biz: contactBizResult.rowCount,
      contacts_individual: contactResult.rowCount,
    };

    console.log(`CIF classification: ${counts.companies} companies, ${counts.contacts_biz + counts.contacts_individual} contacts`);
    return counts;
  } finally {
    client.release();
  }
}

module.exports = { classifyCifRecords };
