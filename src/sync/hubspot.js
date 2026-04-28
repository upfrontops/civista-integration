const crypto = require('crypto');
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
  coerceDateForHubSpot,
  coerceEmailForHubSpot,
} = require('../transform/normalize');
const loud = require('../monitoring/loud');
const { pool } = require('../../db/init');

const HUBSPOT_API_BASE = 'https://api.hubapi.com';
const BATCH_SIZE = 100;
const MAX_RETRIES = 10;
const INITIAL_DELAY = 100;

// Internal columns that must be stripped before sending to HubSpot.
const INTERNAL_COLUMNS = new Set([
  'id', 'row_hash', 'loaded_at', 'synced_at',
  'raw_csv', 'db_persist_hash', 'db_verified_at',
  'hubspot_persist_hash', 'hubspot_verified_at',
  'hubspot_verify_diff', 'needs_review',
]);

// Custom-object types that have a required `name` property.
// Per HubSpot schema: requiredProperties=['name'], primaryDisplayProperty='name'.
const CUSTOM_OBJECTS_REQUIRING_NAME = new Set([
  '2-60442978', // deposits
  '2-60442977', // loans
  '2-60442980', // time_deposits
  '2-60442979', // debit_cards
]);

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
 *
 * Date type uses coerceDateForHubSpot which can return a `problem` if the
 * source value isn't a parseable date. The caller is responsible for
 * surfacing that problem via loud.warn — buildPayload does so below.
 */
function coerceByType(value, type) {
  if (value === null || value === undefined || value === '') return { value: null };
  switch (type) {
    case 'number':      return { value: normalizeNumber(String(value)) };
    case 'bool':        return { value: normalizeBoolean(String(value)) };
    case 'date':        return coerceDateForHubSpot(value);
    case 'enumeration': return { value: String(value) };
    case 'string':
    default:            return { value: String(value) };
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
 * type hints. Drops nulls, internal columns, and any field flagged
 * `send: false` in the mapping (held mappings — preserved in staging,
 * not transmitted; surfaced to UI via mapping_issues).
 *
 * Returns { props, dateProblems } where dateProblems is an array of
 * { csv, prop, raw, problem } entries the caller can route through loud.warn.
 */
function buildPayload(row, fields) {
  const props = {};
  const coercionProblems = []; // both date and email mismatches surface here
  for (const f of fields) {
    if (f.send === false) continue; // held mapping
    const { csv, prop, type } = f;
    const raw = row[prop];
    let coerced;
    if (prop === 'deceased_flag_yn') {
      coerced = { value: coerceDeceased(raw) };
    } else if (prop === 'email') {
      coerced = coerceEmailForHubSpot(raw);
    } else {
      coerced = coerceByType(raw, type);
    }
    if (coerced.problem) {
      coercionProblems.push({ csv, prop, raw, problem: coerced.problem });
      continue; // omit unparseable value — preserved verbatim in raw_csv
    }
    if (coerced.value !== null && coerced.value !== undefined) {
      props[prop] = coerced.value;
    }
  }
  return { props, dateProblems: coercionProblems };
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
async function batchUpsert(objectType, idProperty, inputs, opts = {}) {
  const { runId = null, sourceTable = null } = opts;
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
      await loud.alarm({
        event: 'hubspot_batch_failed',
        message: `Batch ${objectType} [${i}-${i+batch.length}] request failed: ${err.message}`,
        runId, sourceTable,
        context: { batchSize: batch.length, sentKeys: [...sentKeys].slice(0, 5) },
      });
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
          await loud.alarm({
            event: 'hubspot_record_rejected',
            message: `${sourceTable || objectType}: HubSpot did not echo back idProperty for ${idProperty}=${k}`,
            runId, sourceTable, sourceKey: k,
          });
        }
      }
    } else {
      const errMsg = response.body?.message || response.body?.raw || `HTTP ${response.status}`;
      // Whole batch rejected → alarm once with the body, then mark every input failed.
      await loud.alarm({
        event: 'hubspot_batch_failed',
        message: `Batch ${objectType} [${i}-${i+batch.length}] rejected ${response.status}: ${errMsg}`,
        runId, sourceTable,
        context: { batchSize: batch.length, status: response.status, sentKeys: [...sentKeys].slice(0, 5) },
      });
      for (const k of sentKeys) failed.push({ sourceKey: k, reason: `HubSpot ${response.status}: ${errMsg}` });
    }

    console.log(`Batch upsert ${objectType} [${i}-${i + batch.length}]: running totals ok=${succeeded.length} failed=${failed.length}`);
  }

  return { succeeded, failed };
}

/**
 * Generic sync: takes staging rows, a fields list, HubSpot object ID, an
 * idProperty, and the source staging table (for HASH C verification updates).
 * Returns { succeeded, failed, invalidInputs }.
 *
 * Per memory financial_data_rules.md, this also:
 *  - injects properties[idProperty] (fixes the composite_key bug)
 *  - injects properties.name = row[idProperty] for custom objects (HubSpot
 *    requires it on every deposits/loans/time_deposits/debit_cards record)
 *  - surfaces date-coercion problems via loud.warn
 *  - reads back successful records and verifies HASH C
 */
async function syncRows(rows, fields, objectType, idProperty, opts = {}) {
  const { sourceTable = null, sourceCsv = null, runId = null } = opts;

  const dbPayloadByKey = new Map();
  const inputs = [];

  for (const row of rows) {
    const { props, dateProblems } = buildPayload(row, fields);

    // Surface every coercion problem (date OR email — the property is omitted
    // from the HubSpot payload, but the raw value is still in raw_csv).
    // Loud + persisted so the operator sees it before HubSpot would have
    // rejected the whole batch.
    for (const dp of dateProblems) {
      const event = dp.prop === 'email' ? 'email_coercion_skipped' : 'date_coercion_skipped';
      await loud.warn({
        event,
        message: `${sourceCsv || sourceTable}.${dp.csv} → ${dp.prop}: ${dp.problem}`,
        runId, sourceTable, sourceKey: row[idProperty] || null,
        context: { rawValue: dp.raw, prop: dp.prop, csv: dp.csv },
      });
    }

    // Always include the idProperty value in properties — HubSpot's batch
    // upsert needs to see it under that name. This fixes the debit-cards
    // composite_key bug (composite_key isn't in DEBIT_CARDS_FIELDS so
    // buildPayload never copies it).
    if (row[idProperty] !== undefined && row[idProperty] !== null && props[idProperty] === undefined) {
      props[idProperty] = String(row[idProperty]);
    }

    // Custom objects require `name` (verified via HubSpot schema API:
    // requiredProperties=['name'], primaryDisplayProperty='name'). Source
    // CSVs have no name column, so per user direction we set it to the
    // unique key (primary_key for DDA/Loans/CD, composite_key for Debit
    // Cards). Contacts/companies don't get this — HubSpot derives display
    // names from firstname/lastname and from CIF FullName→name.
    if (CUSTOM_OBJECTS_REQUIRING_NAME.has(objectType) && !props.name && row[idProperty]) {
      props.name = String(row[idProperty]);
    }

    const sk = props[idProperty];
    if (sk) dbPayloadByKey.set(sk, props);

    inputs.push({ idProperty, id: row[idProperty], properties: props });
  }

  const { valid, invalid } = validateBatchInputs(inputs, idProperty);

  // Loud surface for invalid (no idProperty) inputs.
  for (const i of invalid) {
    await loud.alarm({
      event: 'idproperty_missing',
      message: `${sourceTable || objectType}: ${i.reason}`,
      runId, sourceTable,
      context: { input: i.input },
    });
  }

  const { succeeded, failed } = valid.length > 0
    ? await batchUpsert(objectType, idProperty, valid, { runId, sourceTable })
    : { succeeded: [], failed: [] };

  // HASH C — read back from HubSpot and verify the persisted properties
  // match the DB-side payload. Gated by VERIFY_HUBSPOT_READBACK env var.
  // Records that fail HASH C must NOT enter shipped_records — otherwise the
  // diff engine would skip them on future runs and they'd never be retried.
  // We move them from succeeded → failed here.
  let finalSucceeded = succeeded;
  let finalFailed = failed;
  if (succeeded.length > 0 && sourceTable && process.env.VERIFY_HUBSPOT_READBACK !== '0') {
    const { mismatchedKeys } = await verifyHubspotPersistence({
      objectType, idProperty, sourceTable, succeeded, dbPayloadByKey, runId,
    });
    if (mismatchedKeys.size > 0) {
      finalSucceeded = succeeded.filter(s => !mismatchedKeys.has(s.sourceKey));
      const moved = succeeded
        .filter(s => mismatchedKeys.has(s.sourceKey))
        .map(s => ({ sourceKey: s.sourceKey, reason: 'HASH C mismatch — HubSpot value did not match DB payload (record present in HubSpot but flagged needs_review)' }));
      finalFailed = failed.concat(moved);
    }
  }

  return { succeeded: finalSucceeded, failed: finalFailed, invalidInputs: invalid };
}

/**
 * Read each successfully-shipped record back from HubSpot and compare the
 * returned properties to the DB-side payload we sent. Writes
 * hubspot_persist_hash + hubspot_verified_at on match; on mismatch writes
 * hubspot_verify_diff (per-property {db, hubspot}) and sets needs_review.
 *
 * HubSpot responses serialize all property values as strings, so we compare
 * String(value) on both sides.
 */
async function verifyHubspotPersistence({ objectType, idProperty, sourceTable, succeeded, dbPayloadByKey, runId }) {
  // Union of property names we sent across the batch.
  const propNames = new Set();
  for (const p of dbPayloadByKey.values()) for (const k of Object.keys(p)) propNames.add(k);
  const properties = Array.from(propNames);

  // Track which sourceKeys failed verification — caller filters them out
  // of `succeeded` so they don't enter shipped_records and get skipped on
  // future runs (a known-corrupt row in HubSpot must be retried).
  const mismatchedKeys = new Set();

  for (let i = 0; i < succeeded.length; i += BATCH_SIZE) {
    const chunk = succeeded.slice(i, i + BATCH_SIZE);
    let response;
    try {
      response = await hubspotFetch(`/crm/v3/objects/${objectType}/batch/read`, {
        method: 'POST',
        body: JSON.stringify({
          inputs: chunk.map(s => ({ id: s.hubspotId })),
          properties,
        }),
      });
    } catch (err) {
      await loud.alarm({
        event: 'hubspot_readback_failed',
        message: `Read-back request failed for ${objectType}: ${err.message}`,
        runId, sourceTable,
        context: { batchSize: chunk.length },
      });
      // Treat the whole chunk as unverifiable → mismatched (don't enter ledger).
      for (const s of chunk) mismatchedKeys.add(s.sourceKey);
      continue;
    }
    if (!response.ok) {
      await loud.alarm({
        event: 'hubspot_readback_failed',
        message: `Read-back HTTP ${response.status} for ${objectType}: ${response.body?.message || response.body?.raw || ''}`,
        runId, sourceTable,
        context: { batchSize: chunk.length, status: response.status },
      });
      for (const s of chunk) mismatchedKeys.add(s.sourceKey);
      continue;
    }
    const byId = new Map((response.body?.results || []).map(r => [r.id, r.properties || {}]));

    for (const s of chunk) {
      // canonicalize lookup key — both sides should be String, but be defensive
      const lookupKey = s.sourceKey == null ? null : String(s.sourceKey);
      const dbPayload = lookupKey == null ? null : dbPayloadByKey.get(lookupKey);
      if (!dbPayload) {
        // Should never happen — succeeded entry came from the batch we built.
        // Loud-alarm so it can't be silent.
        await loud.alarm({
          event: 'hubspot_readback_no_payload',
          message: `Cannot HASH C verify ${sourceTable} hubspot_id=${s.hubspotId}: no DB payload for sourceKey=${s.sourceKey}`,
          runId, sourceTable, sourceKey: s.sourceKey,
        });
        mismatchedKeys.add(s.sourceKey);
        continue;
      }
      const hsProps = byId.get(s.hubspotId);
      if (!hsProps) {
        await pool.query(
          `UPDATE ${sourceTable} SET needs_review = true WHERE ${idProperty} = $1`,
          [s.sourceKey]
        );
        await loud.alarm({
          event: 'hubspot_persist_mismatch',
          message: `Read-back returned no record for hubspot_id=${s.hubspotId} sourceKey=${s.sourceKey} on ${objectType}`,
          runId, sourceTable, sourceKey: s.sourceKey,
        });
        mismatchedKeys.add(s.sourceKey);
        continue;
      }

      // Compare DB-side payload to HubSpot-returned values for the same props.
      const diff = {};
      for (const k of Object.keys(dbPayload)) {
        const a = canonicalScalar(dbPayload[k]);
        const b = canonicalScalar(hsProps[k]);
        if (a !== b) diff[k] = { db: dbPayload[k], hubspot: hsProps[k] ?? null };
      }

      const dbHash = hashCanonical(dbPayload, Object.keys(dbPayload));
      const hsHash = hashCanonical(hsProps, Object.keys(dbPayload));

      if (Object.keys(diff).length === 0) {
        // HASH C matched. Write the hash + clear the prior diff. DO NOT
        // touch needs_review — if HASH B set it (CSV→DB lost something),
        // we must preserve that signal. Per memory financial_data_rules.md:
        // a successful HASH C does not absolve a HASH B failure.
        await pool.query(
          `UPDATE ${sourceTable}
             SET hubspot_persist_hash = $1, hubspot_verified_at = NOW(),
                 hubspot_verify_diff = NULL
           WHERE ${idProperty} = $2`,
          [hsHash, s.sourceKey]
        );
      } else {
        await pool.query(
          `UPDATE ${sourceTable}
             SET hubspot_persist_hash = $1, hubspot_verified_at = NOW(),
                 hubspot_verify_diff = $2, needs_review = true
           WHERE ${idProperty} = $3`,
          [hsHash, JSON.stringify(diff), s.sourceKey]
        );
        await loud.alarm({
          event: 'hubspot_persist_mismatch',
          message: `HASH C mismatch on ${sourceTable} ${idProperty}=${s.sourceKey}: properties differ → ${Object.keys(diff).join(', ')}`,
          runId, sourceTable, sourceKey: s.sourceKey,
          context: { diff, hubspotId: s.hubspotId, dbHash, hsHash },
        });
        mismatchedKeys.add(s.sourceKey);
      }
    }
  }

  return { mismatchedKeys };
}

function canonicalScalar(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return String(v);
}

function hashCanonical(obj, keys) {
  const sorted = {};
  for (const k of [...keys].sort()) sorted[k] = canonicalScalar(obj[k]);
  return crypto.createHash('sha256').update(JSON.stringify(sorted)).digest('hex');
}

// Exported per-table sync functions — each one is a thin wrapper around syncRows.
// All take optional opts: { sourceTable, sourceCsv, runId } so the row-level
// machinery can do HASH C verification and emit loud events with full context.
async function syncContacts(rows, opts)     { return syncRows(rows, CIF_CONTACT_FIELDS, 'contacts', 'cif_number', opts); }
async function syncCompanies(rows, opts)    { return syncRows(rows, CIF_COMPANY_FIELDS, 'companies', 'cif_number', opts); }
async function syncDeposits(rows, opts)     { return syncRows(rows, TABLES.dda.fields, TABLES.dda.object, TABLES.dda.idProperty, opts); }
async function syncLoans(rows, opts)        { return syncRows(rows, TABLES.loans.fields, TABLES.loans.object, TABLES.loans.idProperty, opts); }
async function syncTimeDeposits(rows, opts) { return syncRows(rows, TABLES.cd.fields, TABLES.cd.object, TABLES.cd.idProperty, opts); }
async function syncDebitCards(rows, opts)   { return syncRows(rows, TABLES.debit_cards.fields, TABLES.debit_cards.object, TABLES.debit_cards.idProperty, opts); }

module.exports = {
  hubspotFetch,
  batchUpsert,
  validateBatchInputs,
  buildPayload,
  verifyHubspotPersistence,
  syncContacts,
  syncCompanies,
  syncDeposits,
  syncLoans,
  syncTimeDeposits,
  syncDebitCards,
  BATCH_SIZE,
  INTERNAL_COLUMNS,
};
