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
        const retryBody = await res.text().catch(() => '');
        throw new Error(`Rate limited after ${MAX_RETRIES} retries on ${path}: ${retryBody}`);
      }
      console.log(`Rate limited on ${path}, retrying in ${delay}ms (attempt ${attempt + 1})`);
      await new Promise(r => setTimeout(r, delay));
      delay *= 2;
      continue;
    }

    // 2xx: read once and return parsed body + status.
    // non-2xx: read once, return parsed body + status — caller decides what to do.
    const text = await res.text();
    let parsed;
    try { parsed = JSON.parse(text); } catch { parsed = { raw: text }; }
    return { status: res.status, ok: res.ok, body: parsed };
  }
}

/**
 * Preflight validator. Every input MUST have a non-empty idProperty value,
 * otherwise the batch upsert will either fail opaquely or create orphans.
 *
 * Returns { valid, invalid } — invalid rows should be quarantined with a
 * validation error; only valid rows are sent to HubSpot.
 */
function validateBatchInputs(inputs, idProperty) {
  const valid = [];
  const invalid = [];
  for (const input of inputs) {
    const val = input?.properties?.[idProperty];
    if (val === undefined || val === null || val === '') {
      invalid.push({
        reason: `Missing idProperty value (${idProperty})`,
        input,
      });
    } else {
      valid.push(input);
    }
  }
  return { valid, invalid };
}

/**
 * Upsert a batch of records. Returns per-input outcome:
 *   { succeeded: [{ sourceKey, hubspotId, wasNew }], failed: [{ sourceKey, reason }] }
 *
 * Records are matched between input and response via the idProperty value.
 * Anything in input that isn't echoed back in the response is treated as failed.
 */
async function batchUpsert(objectType, idProperty, inputs) {
  const succeeded = [];
  const failed = [];

  for (let i = 0; i < inputs.length; i += BATCH_SIZE) {
    const batch = inputs.slice(i, i + BATCH_SIZE);
    const endpoint = `/crm/v3/objects/${objectType}/batch/upsert`;

    // Build the set of keys we sent, so we can identify what came back and what didn't.
    const sentKeys = new Set(batch.map(b => b.properties?.[idProperty]).filter(Boolean));

    let response;
    try {
      response = await hubspotFetch(endpoint, {
        method: 'POST',
        body: JSON.stringify({ inputs: batch, idProperty }),
      });
    } catch (err) {
      // Network-level or retry-exhausted failure — entire batch failed.
      for (const key of sentKeys) {
        failed.push({
          sourceKey: key,
          reason: `Batch request failed: ${err.message}`,
        });
      }
      continue;
    }

    // 2xx: process the results array and mark success per-record.
    if (response.ok) {
      const results = Array.isArray(response.body?.results) ? response.body.results : [];
      const returnedKeys = new Set();
      for (const r of results) {
        const key = r?.properties?.[idProperty];
        if (!key) continue;
        returnedKeys.add(key);
        succeeded.push({
          sourceKey: key,
          hubspotId: r.id,
          wasNew: !!r.new,
        });
      }
      // Any sent key not echoed back = failed silently. Record as failure.
      for (const key of sentKeys) {
        if (!returnedKeys.has(key)) {
          failed.push({
            sourceKey: key,
            reason: `HubSpot did not echo back idProperty value — record not confirmed`,
          });
        }
      }
    } else {
      // 4xx/5xx — the whole batch failed. Record all inputs as failed with the API error.
      const errMsg = response.body?.message || response.body?.raw || `HTTP ${response.status}`;
      for (const key of sentKeys) {
        failed.push({
          sourceKey: key,
          reason: `HubSpot ${response.status}: ${errMsg}`,
        });
      }
    }

    console.log(
      `Batch upsert ${objectType} [${i}-${i + batch.length}]: ` +
      `${succeeded.length} cum. ok, ${failed.length} cum. failed`
    );
  }

  return { succeeded, failed };
}

function stripNullProps(input) {
  for (const [k, v] of Object.entries(input.properties)) {
    if (v === null || v === undefined) delete input.properties[k];
  }
  return input;
}

// Each sync function returns { succeeded, failed, invalidInputs }.
// invalidInputs are records that failed preflight (missing idProperty).

function buildInputs(rows, normalizer, idProperty) {
  return rows.map(row => {
    const n = normalizer(row);
    return stripNullProps({
      idProperty,
      id: n[idProperty],
      properties: { ...n },
    });
  });
}

async function syncHubspot(objectType, idProperty, rows, normalizer) {
  const inputs = buildInputs(rows, normalizer, idProperty);
  const { valid, invalid } = validateBatchInputs(inputs, idProperty);
  const { succeeded, failed } = valid.length > 0
    ? await batchUpsert(objectType, idProperty, valid)
    : { succeeded: [], failed: [] };
  return { succeeded, failed, invalidInputs: invalid };
}

async function syncCifContacts(rows)  { return syncHubspot('contacts',               'cif_number',   rows, normalizeCifContact); }
async function syncCifCompanies(rows) { return syncHubspot('companies',              'cif_number',   rows, normalizeCifCompany); }
async function syncDda(rows)          { return syncHubspot(CUSTOM_OBJECTS.dda,        'primary_key',  rows, normalizeDda); }
async function syncLoans(rows)        { return syncHubspot(CUSTOM_OBJECTS.loans,      'primary_key',  rows, normalizeLoan); }
async function syncCds(rows)          { return syncHubspot(CUSTOM_OBJECTS.cd,         'primary_key',  rows, normalizeCd); }
async function syncDebitCards(rows)   { return syncHubspot(CUSTOM_OBJECTS.debit_cards, 'composite_key', rows, normalizeDebitCard); }

module.exports = {
  hubspotFetch,
  batchUpsert,
  validateBatchInputs,
  syncCifContacts,
  syncCifCompanies,
  syncDda,
  syncLoans,
  syncCds,
  syncDebitCards,
  CUSTOM_OBJECTS,
  BATCH_SIZE,
};
