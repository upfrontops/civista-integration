const {
  TABLES,
  CIF_CONTACT_FIELDS,
  CIF_COMPANY_FIELDS,
} = require('../transform/hubspot-mapping');
const {
  normalizeBoolean,
  normalizeDeceased,
  normalizeNumber,
  normalizeDate,
  normalizeEmail,
} = require('../transform/normalize');

const HUBSPOT_API_BASE = 'https://api.hubapi.com';
const BATCH_SIZE = 100;
const MAX_RETRIES = 10;
const INITIAL_DELAY = 100;

// Internal columns that must be stripped before sending to HubSpot.
const INTERNAL_COLUMNS = new Set(['id', 'row_hash', 'loaded_at', 'synced_at']);

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
        const retryBody = await res.text().catch(() => '');
        throw new Error(`Rate limited after ${MAX_RETRIES} retries on ${path}: ${retryBody}`);
      }
      console.log(`Rate limited on ${path}, retrying in ${delay}ms (attempt ${attempt + 1})`);
      await new Promise(r => setTimeout(r, delay));
      delay *= 2;
      continue;
    }

    const text = await res.text();
    let parsed;
    try { parsed = JSON.parse(text); } catch { parsed = { raw: text }; }
    return { status: res.status, ok: res.ok, body: parsed };
  }
}

/**
 * Coerce a raw staging-table value (always TEXT) into the type HubSpot expects.
 * Returns null for empty/missing values so they don't get sent as empty strings.
 */
function coerceByType(value, type) {
  if (value === null || value === undefined || value === '') return null;
  switch (type) {
    case 'number':      return normalizeNumber(String(value));
    case 'bool':        return normalizeBoolean(String(value));
    case 'date':        return normalizeDate(String(value));
    case 'enumeration': return String(value);
    case 'string':
    default:            return String(value);
  }
}

/**
 * Special-case the Deceased flag — source uses a literal space character
 * for "alive," which normalizeBoolean would treat as false (correct) but
 * we keep the dedicated function to preserve the original CLAUDE.md rule.
 */
function coerceDeceased(value) {
  return normalizeDeceased(String(value ?? ''));
}

/**
 * Build a HubSpot-ready payload from a staging row using the mapping's
 * type hints. Drops nulls and internal columns.
 */
function buildPayload(row, fields) {
  const props = {};
  for (const { prop, type } of fields) {
    let v = row[prop];
    // Special handling for the Deceased property (space='alive').
    if (prop === 'deceased_flag_yn') {
      v = coerceDeceased(v);
    } else if (prop === 'email') {
      v = normalizeEmail(String(v ?? ''));
    } else {
      v = coerceByType(v, type);
    }
    if (v !== null && v !== undefined) {
      props[prop] = v;
    }
  }
  return props;
}

/**
 * Validate that every input has its unique idProperty value. Anything
 * missing is caught BEFORE we call HubSpot and returned as invalidInputs
 * so the caller can quarantine it.
 */
function validateBatchInputs(inputs, idProperty) {
  const valid = [];
  const invalid = [];
  for (const input of inputs) {
    const val = input?.properties?.[idProperty];
    if (val === undefined || val === null || val === '') {
      invalid.push({ reason: `Missing idProperty value (${idProperty})`, input });
    } else {
      valid.push(input);
    }
  }
  return { valid, invalid };
}

/**
 * Batch upsert. Returns per-input outcome:
 *   succeeded: [{ sourceKey, hubspotId, wasNew }]
 *   failed:    [{ sourceKey, reason }]
 * Any input whose idProperty value isn't echoed back in the response is
 * treated as failed — we never silently assume success.
 */
async function batchUpsert(objectType, idProperty, inputs) {
  const succeeded = [];
  const failed = [];

  for (let i = 0; i < inputs.length; i += BATCH_SIZE) {
    const batch = inputs.slice(i, i + BATCH_SIZE);
    const sentKeys = new Set(batch.map(b => b.properties?.[idProperty]).filter(Boolean));

    let response;
    try {
      response = await hubspotFetch(`/crm/v3/objects/${objectType}/batch/upsert`, {
        method: 'POST',
        body: JSON.stringify({ inputs: batch, idProperty }),
      });
    } catch (err) {
      for (const k of sentKeys) failed.push({ sourceKey: k, reason: `Batch request failed: ${err.message}` });
      continue;
    }

    if (response.ok) {
      const results = Array.isArray(response.body?.results) ? response.body.results : [];
      const returnedKeys = new Set();
      for (const r of results) {
        const k = r?.properties?.[idProperty];
        if (!k) continue;
        returnedKeys.add(k);
        succeeded.push({ sourceKey: k, hubspotId: r.id, wasNew: !!r.new });
      }
      for (const k of sentKeys) {
        if (!returnedKeys.has(k)) {
          failed.push({ sourceKey: k, reason: `HubSpot did not echo back idProperty — record not confirmed` });
        }
      }
    } else {
      const errMsg = response.body?.message || response.body?.raw || `HTTP ${response.status}`;
      for (const k of sentKeys) failed.push({ sourceKey: k, reason: `HubSpot ${response.status}: ${errMsg}` });
    }

    console.log(`Batch upsert ${objectType} [${i}-${i + batch.length}]: running totals ok=${succeeded.length} failed=${failed.length}`);
  }

  return { succeeded, failed };
}

/**
 * Generic sync: takes staging rows, a fields list, HubSpot object ID, and
 * an idProperty. Returns { succeeded, failed, invalidInputs }.
 */
async function syncRows(rows, fields, objectType, idProperty) {
  const inputs = rows.map(row => ({
    idProperty,
    id: row[idProperty],
    properties: buildPayload(row, fields),
  }));

  const { valid, invalid } = validateBatchInputs(inputs, idProperty);
  const { succeeded, failed } = valid.length > 0
    ? await batchUpsert(objectType, idProperty, valid)
    : { succeeded: [], failed: [] };

  return { succeeded, failed, invalidInputs: invalid };
}

// Exported per-table sync functions — each one is a thin wrapper around syncRows.
async function syncContacts(rows)     { return syncRows(rows, CIF_CONTACT_FIELDS, 'contacts', 'cif_number'); }
async function syncCompanies(rows)    { return syncRows(rows, CIF_COMPANY_FIELDS, 'companies', 'cif_number'); }
async function syncDeposits(rows)     { return syncRows(rows, TABLES.dda.fields, TABLES.dda.object, TABLES.dda.idProperty); }
async function syncLoans(rows)        { return syncRows(rows, TABLES.loans.fields, TABLES.loans.object, TABLES.loans.idProperty); }
async function syncTimeDeposits(rows) { return syncRows(rows, TABLES.cd.fields, TABLES.cd.object, TABLES.cd.idProperty); }
async function syncDebitCards(rows)   { return syncRows(rows, TABLES.debit_cards.fields, TABLES.debit_cards.object, TABLES.debit_cards.idProperty); }

module.exports = {
  hubspotFetch,
  batchUpsert,
  validateBatchInputs,
  buildPayload,
  syncContacts,
  syncCompanies,
  syncDeposits,
  syncLoans,
  syncTimeDeposits,
  syncDebitCards,
  BATCH_SIZE,
  INTERNAL_COLUMNS,
};
