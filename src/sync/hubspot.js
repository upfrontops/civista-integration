const { pool } = require('../../db/init');
const {
  normalizeCifContact,
  normalizeCifCompany,
  normalizeDda,
  normalizeLoan,
  normalizeCd,
  normalizeDebitCard,
} = require('../transform/normalize');

const HUBSPOT_API_BASE = 'https://api.hubapi.com';
const BATCH_SIZE = 100;
const MAX_RETRIES = 10;
const INITIAL_DELAY = 100;

const CUSTOM_OBJECTS = {
  dda: '2-60442978',
  loans: '2-60442977',
  cd: '2-60442980',
  debit_cards: '2-60442979',
};

async function hubspotFetch(path, options = {}) {
  const url = `${HUBSPOT_API_BASE}${path}`;
  const headers = {
    'Authorization': `Bearer ${process.env.HUBSPOT_API_KEY}`,
    'Content-Type': 'application/json',
    ...options.headers,
  };

  let delay = INITIAL_DELAY;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const res = await fetch(url, { ...options, headers });

    if (res.status === 429) {
      if (attempt === MAX_RETRIES) {
        throw new Error(`Rate limited after ${MAX_RETRIES} retries: ${path}`);
      }
      console.log(`Rate limited on ${path}, retrying in ${delay}ms (attempt ${attempt + 1})`);
      await new Promise(r => setTimeout(r, delay));
      delay *= 2;
      continue;
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`HubSpot API error ${res.status} on ${path}: ${body}`);
    }

    return res.json();
  }
}

async function batchUpsert(objectType, idProperty, inputs) {
  const results = { created: 0, updated: 0, failed: 0, errors: [] };

  for (let i = 0; i < inputs.length; i += BATCH_SIZE) {
    const batch = inputs.slice(i, i + BATCH_SIZE);
    const endpoint = objectType.startsWith('2-')
      ? `/crm/v3/objects/${objectType}/batch/upsert`
      : `/crm/v3/objects/${objectType}/batch/upsert`;

    try {
      const response = await hubspotFetch(endpoint, {
        method: 'POST',
        body: JSON.stringify({
          inputs: batch,
          idProperty,
        }),
      });

      if (response.results) {
        for (const result of response.results) {
          if (result.new) {
            results.created++;
          } else {
            results.updated++;
          }
        }
      }

      console.log(`Batch upsert ${objectType}: ${i + batch.length}/${inputs.length}`);
    } catch (err) {
      results.failed += batch.length;
      results.errors.push({
        batch: `${i}-${i + batch.length}`,
        error: err.message,
      });
      console.error(`Batch upsert failed for ${objectType} batch ${i}: ${err.message}`);
    }
  }

  return results;
}

async function saveHubspotIds(sourceTable, objectType, apiResults, keyField) {
  if (!apiResults || !apiResults.length) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const result of apiResults) {
      const sourceKey = result.properties[keyField];
      if (!sourceKey) continue;

      await client.query(
        `INSERT INTO hubspot_id_map (source_table, source_key, hubspot_id, object_type, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (source_table, source_key)
         DO UPDATE SET hubspot_id = $3, updated_at = NOW()`,
        [sourceTable, sourceKey, result.id, objectType]
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

// --- Sync functions per object type ---

async function syncCifContacts(rows) {
  const inputs = rows.map(row => {
    const normalized = normalizeCifContact(row);
    return {
      idProperty: 'cif_number',
      id: normalized.cif_number,
      properties: {
        cif_number: normalized.cif_number,
        firstname: normalized.firstname,
        lastname: normalized.lastname,
        fullname: normalized.fullname,
        email: normalized.email,
        phone: normalized.phone,
        address: normalized.address,
        address2: normalized.address2,
        city: normalized.city,
        state: normalized.state,
        zip: normalized.zip,
        date_of_birth: normalized.date_of_birth,
        became_customer_date: normalized.became_customer_date,
        hash_ssn: normalized.hash_ssn,
        digital_banking: normalized.digital_banking,
        private_banking: normalized.private_banking,
        deceased: normalized.deceased,
        minor: normalized.minor,
        do_not_call: normalized.do_not_call,
        q2_user_id: normalized.q2_user_id,
        last_login: normalized.last_login,
      },
    };
  });

  // Remove null/undefined properties
  for (const input of inputs) {
    for (const [key, val] of Object.entries(input.properties)) {
      if (val === null || val === undefined) {
        delete input.properties[key];
      }
    }
  }

  return batchUpsert('contacts', 'cif_number', inputs);
}

async function syncCifCompanies(rows) {
  const inputs = rows.map(row => {
    const normalized = normalizeCifCompany(row);
    return {
      idProperty: 'cif_number',
      id: normalized.cif_number,
      properties: {
        cif_number: normalized.cif_number,
        name: normalized.name,
        phone: normalized.phone,
        address: normalized.address,
        address2: normalized.address2,
        city: normalized.city,
        state: normalized.state,
        zip: normalized.zip,
        became_customer_date: normalized.became_customer_date,
        naics_code: normalized.naics_code,
      },
    };
  });

  for (const input of inputs) {
    for (const [key, val] of Object.entries(input.properties)) {
      if (val === null || val === undefined) {
        delete input.properties[key];
      }
    }
  }

  return batchUpsert('companies', 'cif_number', inputs);
}

async function syncDda(rows) {
  const inputs = rows.map(row => {
    const normalized = normalizeDda(row);
    return {
      idProperty: 'primary_key',
      id: normalized.primary_key,
      properties: {
        primary_key: normalized.primary_key,
        cif_number: normalized.cif_number,
        acctlast4: normalized.acctlast4,
        interest_rate: normalized.interest_rate,
        accttype: normalized.accttype,
        acctdesc: normalized.acctdesc,
        open_date: normalized.open_date,
        close_date: normalized.close_date,
        slsassoc: normalized.slsassoc,
        dt_last_active: normalized.dt_last_active,
        current_bal: normalized.current_bal,
        yest_bal: normalized.yest_bal,
        branch_num: normalized.branch_num,
        officr_code: normalized.officr_code,
        acct_status: normalized.acct_status,
        relationship: normalized.relationship,
        promo_code: normalized.promo_code,
        open_online: normalized.open_online,
      },
    };
  });

  for (const input of inputs) {
    for (const [key, val] of Object.entries(input.properties)) {
      if (val === null || val === undefined) {
        delete input.properties[key];
      }
    }
  }

  return batchUpsert(CUSTOM_OBJECTS.dda, 'primary_key', inputs);
}

async function syncLoans(rows) {
  const inputs = rows.map(row => {
    const normalized = normalizeLoan(row);
    return {
      idProperty: 'primary_key',
      id: normalized.primary_key,
      properties: {
        primary_key: normalized.primary_key,
        cif_number: normalized.cif_number,
        acctlast4: normalized.acctlast4,
        interest_rate: normalized.interest_rate,
        accttype: normalized.accttype,
        loan_type: normalized.loan_type,
        orig_date: normalized.orig_date,
        maturity_date: normalized.maturity_date,
        slsassoc: normalized.slsassoc,
        last_active_date: normalized.last_active_date,
        curr_bal: normalized.curr_bal,
        branch_num: normalized.branch_num,
        officr_code: normalized.officr_code,
        acct_status: normalized.acct_status,
        relationship: normalized.relationship,
        orig_bal: normalized.orig_bal,
      },
    };
  });

  for (const input of inputs) {
    for (const [key, val] of Object.entries(input.properties)) {
      if (val === null || val === undefined) {
        delete input.properties[key];
      }
    }
  }

  return batchUpsert(CUSTOM_OBJECTS.loans, 'primary_key', inputs);
}

async function syncCds(rows) {
  const inputs = rows.map(row => {
    const normalized = normalizeCd(row);
    return {
      idProperty: 'primary_key',
      id: normalized.primary_key,
      properties: {
        primary_key: normalized.primary_key,
        cif_number: normalized.cif_number,
        acctlast4: normalized.acctlast4,
        interest_rate: normalized.interest_rate,
        accttype: normalized.accttype,
        acctdesc: normalized.acctdesc,
        issue_date: normalized.issue_date,
        maturity_date: normalized.maturity_date,
        slsassoc: normalized.slsassoc,
        curr_bal: normalized.curr_bal,
        branch_num: normalized.branch_num,
        officr_code: normalized.officr_code,
        acct_status: normalized.acct_status,
        relationship: normalized.relationship,
        open_online: normalized.open_online,
      },
    };
  });

  for (const input of inputs) {
    for (const [key, val] of Object.entries(input.properties)) {
      if (val === null || val === undefined) {
        delete input.properties[key];
      }
    }
  }

  return batchUpsert(CUSTOM_OBJECTS.cd, 'primary_key', inputs);
}

async function syncDebitCards(rows) {
  const inputs = rows.map(row => {
    const normalized = normalizeDebitCard(row);
    return {
      idProperty: 'composite_key',
      id: normalized.composite_key,
      properties: {
        composite_key: normalized.composite_key,
        cif_number: normalized.cif_number,
        acctlast4: normalized.acctlast4,
        acct_type: normalized.acct_type,
        last4_debit_card: normalized.last4_debit_card,
        card_status: normalized.card_status,
        expire_date: normalized.expire_date,
        last_used_dt: normalized.last_used_dt,
        pos_last_30_days: normalized.pos_last_30_days,
        active_pos: normalized.active_pos,
      },
    };
  });

  for (const input of inputs) {
    for (const [key, val] of Object.entries(input.properties)) {
      if (val === null || val === undefined) {
        delete input.properties[key];
      }
    }
  }

  return batchUpsert(CUSTOM_OBJECTS.debit_cards, 'composite_key', inputs);
}

module.exports = {
  hubspotFetch,
  batchUpsert,
  saveHubspotIds,
  syncCifContacts,
  syncCifCompanies,
  syncDda,
  syncLoans,
  syncCds,
  syncDebitCards,
  CUSTOM_OBJECTS,
  BATCH_SIZE,
};
