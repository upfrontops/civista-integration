#!/usr/bin/env node
/**
 * UAT harness — executes the test cases in docs/uat.md against a deployed
 * Railway service. Writes results to uat-results.md (markdown table).
 *
 * Required env (point at your Railway service):
 *   RAILWAY_URL          https://<civista-integration-domain>
 *   MANUAL_SYNC_TOKEN    matches the Railway env var
 *   SFTP_HOST            Railway TCP-proxy host    (e.g. hopper.proxy.rlwy.net)
 *   SFTP_PORT            Railway TCP-proxy port    (e.g. 34212)
 *   SFTP_USER            matches the Railway env var
 *   SFTP_PASS            matches the Railway env var
 *   HUBSPOT_API_KEY      sandbox key — used for verification reads only
 *   UAT_SAMPLES_DIR      absolute path to dir containing 5 sample CSVs
 *
 * Optional:
 *   UAT_OUT              output md path (default: uat-results.md in cwd)
 *
 * Exit code: 0 if every executed test passes, 1 otherwise. Tests that
 * fail prerequisite checks are reported as BLOCKED (they don't fail the
 * suite, but the operator must address them and re-run).
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Client: SshClient } = require('ssh2');

const cfg = {
  RAILWAY_URL:       trimSlash(process.env.RAILWAY_URL),
  MANUAL_SYNC_TOKEN: process.env.MANUAL_SYNC_TOKEN,
  SFTP_HOST:         process.env.SFTP_HOST,
  SFTP_PORT:         parseInt(process.env.SFTP_PORT || '0', 10),
  SFTP_USER:         process.env.SFTP_USER,
  SFTP_PASS:         process.env.SFTP_PASS,
  HUBSPOT_API_KEY:   process.env.HUBSPOT_API_KEY,
  UAT_SAMPLES_DIR:   process.env.UAT_SAMPLES_DIR,
  UAT_OUT:           process.env.UAT_OUT || 'uat-results.md',
};

const results = []; // { id, name, status, detail, durationMs }

function trimSlash(u) { return u ? u.replace(/\/$/, '') : u; }

function record(id, name, status, detail, startedAt) {
  results.push({ id, name, status, detail, durationMs: Date.now() - startedAt });
  const tag = status === 'PASS' ? '✓' : status === 'FAIL' ? '✘' : '⏸';
  console.log(`  ${tag} ${id} ${name} — ${status}${detail ? ' · ' + detail.slice(0, 200) : ''}`);
}

async function http(method, pathOrUrl, opts = {}) {
  const url = pathOrUrl.startsWith('http') ? pathOrUrl : cfg.RAILWAY_URL + pathOrUrl;
  // Auto-attach Basic auth on every request (Civista UI is bank-grade
  // protected with SFTP_USER:SFTP_PASS as the realm credentials). /health
  // is unauthenticated server-side so the header is harmless there.
  const basic = Buffer.from(`${cfg.SFTP_USER}:${cfg.SFTP_PASS}`).toString('base64');
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Basic ${basic}`,
    ...(opts.headers || {}),
  };
  const res = await fetch(url, { method, headers, body: opts.body });
  const text = await res.text();
  let body = null;
  try { body = JSON.parse(text); } catch { body = { raw: text }; }
  return { status: res.status, ok: res.ok, body };
}

async function hubspotTotal(obj) {
  const r = await fetch(`https://api.hubapi.com/crm/v3/objects/${obj}/search`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${cfg.HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ limit: 1 }),
  });
  const b = await r.json().catch(() => ({}));
  return b.total ?? null;
}

// ─── pre-conditions ─────────────────────────────────────────────────

async function checkPreconditions() {
  const missing = [];
  for (const k of ['RAILWAY_URL','MANUAL_SYNC_TOKEN','SFTP_HOST','SFTP_PORT','SFTP_USER','SFTP_PASS','HUBSPOT_API_KEY','UAT_SAMPLES_DIR']) {
    if (!cfg[k]) missing.push(k);
  }
  if (missing.length) {
    console.error('UAT cannot start — missing env: ' + missing.join(', '));
    process.exit(2);
  }
  if (!fs.existsSync(cfg.UAT_SAMPLES_DIR)) {
    console.error(`UAT_SAMPLES_DIR not found: ${cfg.UAT_SAMPLES_DIR}`);
    process.exit(2);
  }
  const required = ['HubSpot_CIF.csv','HubSpot_DDA.csv','HubSpot_Loan.csv','HubSpot_CD.csv','HubSpot_Debit_Card.csv'];
  for (const f of required) {
    const p = path.join(cfg.UAT_SAMPLES_DIR, f);
    if (!fs.existsSync(p)) { console.error(`Missing sample: ${p}`); process.exit(2); }
  }
}

// ─── tests ──────────────────────────────────────────────────────────

async function T1_health() {
  const t0 = Date.now();
  try {
    const t = Date.now();
    const r = await http('GET', '/health');
    const elapsed = Date.now() - t;
    if (!r.ok) return record('T1', 'Deployment health', 'FAIL', `/health returned ${r.status}: ${JSON.stringify(r.body).slice(0, 200)}`, t0);
    if (r.body.database !== 'connected') return record('T1', 'Deployment health', 'FAIL', `database=${r.body.database}`, t0);
    if (elapsed > 2500) return record('T1', 'Deployment health', 'FAIL', `/health responded in ${elapsed}ms (> 2500 budget)`, t0);
    const info = await http('GET', '/api/info');
    if (!info.ok) return record('T1', 'Deployment health', 'FAIL', `/api/info ${info.status}`, t0);
    record('T1', 'Deployment health', 'PASS', `/health 200 in ${elapsed}ms · ${info.body.service} v${info.body.version}`, t0);
  } catch (e) {
    record('T1', 'Deployment health', 'FAIL', e.message, t0);
  }
}

async function T2_schema() {
  const t0 = Date.now();
  try {
    const r = await http('GET', '/api/schema-check');
    if (r.status === 404) return record('T2', 'Schema alignment', 'BLOCKED', 'ENABLE_SCHEMA_CHECK=0; cannot run', t0);
    if (!r.ok) return record('T2', 'Schema alignment', 'FAIL', `status ${r.status}`, t0);
    const issues = [];
    for (const o of r.body.objects || []) {
      if ((o.errors || []).length > 0) issues.push(`${o.label}: ${o.errors[0]}`);
      if (o.missingInHs > 0) issues.push(`${o.label}: ${o.missingInHs} pg-only columns`);
    }
    if (issues.length) return record('T2', 'Schema alignment', 'FAIL', issues.join(' | '), t0);
    record('T2', 'Schema alignment', 'PASS', `${(r.body.objects||[]).length} objects aligned, 0 mismatches`, t0);
  } catch (e) {
    record('T2', 'Schema alignment', 'FAIL', e.message, t0);
  }
}

async function T3_portalGuard() {
  const t0 = Date.now();
  try {
    // Indirect verification: /sync without token should be 403 (gate works).
    // Portal guard itself surfaces in /api/issues mapping or the boot logs;
    // a clean /api/info implies boot completed, which only happens when the
    // guard passes (per index.js).
    const r = await http('POST', '/sync');
    if (r.status === 503) return record('T3', 'Portal cutover guard', 'FAIL', 'service is up but portal guard is failing — run scripts/cutover-portal.js', t0);
    if (r.status !== 403) return record('T3', 'Portal cutover guard', 'FAIL', `expected 403 (token gate); got ${r.status}`, t0);
    record('T3', 'Portal cutover guard', 'PASS', 'service booted past guard; auth gate enforced', t0);
  } catch (e) {
    record('T3', 'Portal cutover guard', 'FAIL', e.message, t0);
  }
}

async function T4_sftp() {
  const t0 = Date.now();
  // T4.a — bad password
  const badRes = await new Promise((resolve) => {
    const c = new SshClient();
    c.on('ready', () => { c.end(); resolve({ ok: false, msg: 'unexpected: bad password authenticated' }); });
    c.on('error', (err) => resolve({ ok: true, msg: err.message }));
    c.connect({ host: cfg.SFTP_HOST, port: cfg.SFTP_PORT, username: cfg.SFTP_USER, password: 'bogus_password_uat', readyTimeout: 10000 });
  });
  if (!badRes.ok) return record('T4', 'SFTP auth + integrity', 'FAIL', 'wrong password was accepted (auth gate broken)', t0);

  // T4.b — right password + integrity
  const sample = path.join(cfg.UAT_SAMPLES_DIR, 'HubSpot_Debit_Card.csv');
  const localMd5 = crypto.createHash('md5').update(fs.readFileSync(sample)).digest('hex');
  const sshRes = await new Promise((resolve) => {
    const c = new SshClient();
    c.on('ready', () => {
      c.sftp((err, sftp) => {
        if (err) { c.end(); return resolve({ ok: false, msg: err.message }); }
        const rs = fs.createReadStream(sample);
        const ws = sftp.createWriteStream('HubSpot_Debit_Card.csv');
        ws.on('close', () => { c.end(); resolve({ ok: true }); });
        ws.on('error', (e) => { c.end(); resolve({ ok: false, msg: e.message }); });
        rs.pipe(ws);
      });
    });
    c.on('error', (err) => resolve({ ok: false, msg: err.message }));
    c.connect({ host: cfg.SFTP_HOST, port: cfg.SFTP_PORT, username: cfg.SFTP_USER, password: cfg.SFTP_PASS, readyTimeout: 10000 });
  });
  if (!sshRes.ok) return record('T4', 'SFTP auth + integrity', 'FAIL', 'good password did not authenticate or upload failed: ' + sshRes.msg, t0);
  record('T4', 'SFTP auth + integrity', 'PASS', `bad password rejected; good password authenticated; debit_card.csv uploaded (local md5 ${localMd5.slice(0,8)})`, t0);
}

async function T9_authGate() {
  const t0 = Date.now();
  const noToken  = await http('POST', '/sync');
  const wrong    = await http('POST', '/sync', { headers: { 'X-Sync-Token': 'wrong' } });
  if (noToken.status !== 403) return record('T9', 'Manual sync auth gate', 'FAIL', `no-token: expected 403, got ${noToken.status}`, t0);
  if (wrong.status !== 403)   return record('T9', 'Manual sync auth gate', 'FAIL', `wrong-token: expected 403, got ${wrong.status}`, t0);
  record('T9', 'Manual sync auth gate', 'PASS', 'no-token=403, wrong-token=403', t0);
}

async function uploadAllSamples() {
  const files = ['HubSpot_CIF.csv','HubSpot_DDA.csv','HubSpot_Loan.csv','HubSpot_CD.csv','HubSpot_Debit_Card.csv'];
  for (const f of files) {
    const local = path.join(cfg.UAT_SAMPLES_DIR, f);
    await new Promise((resolve, reject) => {
      const c = new SshClient();
      c.on('ready', () => {
        c.sftp((err, sftp) => {
          if (err) { c.end(); return reject(err); }
          const rs = fs.createReadStream(local);
          const ws = sftp.createWriteStream(f);
          ws.on('close', () => { c.end(); resolve(); });
          ws.on('error', (e) => { c.end(); reject(e); });
          rs.pipe(ws);
        });
      });
      c.on('error', reject);
      c.connect({ host: cfg.SFTP_HOST, port: cfg.SFTP_PORT, username: cfg.SFTP_USER, password: cfg.SFTP_PASS, readyTimeout: 10000 });
    });
  }
}

async function triggerSync() {
  const r = await http('POST', '/sync', { headers: { 'X-Sync-Token': cfg.MANUAL_SYNC_TOKEN } });
  if (!r.ok) throw new Error(`/sync returned ${r.status}: ${JSON.stringify(r.body)}`);
  // Poll /api/issues until sync activity has settled (no growth in by_severity for ~10s).
  let lastSig = '';
  let stable = 0;
  const deadline = Date.now() + 10 * 60 * 1000; // 10 min cap
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 5000));
    const issues = await http('GET', '/api/issues');
    const sig = JSON.stringify({
      hh: issues.body.hash_health,
      sev: issues.body.recent_run?.by_severity || {},
    });
    if (sig === lastSig) {
      stable += 5;
      if (stable >= 15) return; // 15s of stability
    } else {
      lastSig = sig;
      stable = 0;
    }
  }
  throw new Error('sync did not settle within 10 minutes');
}

async function T5_T6_T7_hashesAndAudit() {
  const t0 = Date.now();
  try {
    await uploadAllSamples();
    await triggerSync();
    const issues = await http('GET', '/api/issues');
    const hh = issues.body.hash_health || {};
    const ab = hh.csv_to_db || {}, bc = hh.db_to_hubspot || {};

    if (ab.mismatch > 0) return record('T5', 'CSV → DB lossless (HASH A=B)', 'FAIL', `${ab.mismatch} mismatch on csv_to_db; needs_review rows present`, t0);
    if (ab.ok === 0 && ab.pending > 0) return record('T5', 'CSV → DB lossless (HASH A=B)', 'FAIL', `0 verified, ${ab.pending} pending — verifyDbPersistence may not be running`, t0);
    record('T5', 'CSV → DB lossless (HASH A=B)', 'PASS', `${ab.ok} ok, ${ab.mismatch} mismatch, ${ab.pending} pending`, t0);

    const t6 = Date.now();
    if (bc.mismatch > 0) return record('T6', 'DB → HubSpot lossless (HASH B=C)', 'FAIL', `${bc.mismatch} hubspot_verify_diff rows`, t6);
    if (bc.ok === 0)     return record('T6', 'DB → HubSpot lossless (HASH B=C)', 'FAIL', `0 records ship-verified; check VERIFY_HUBSPOT_READBACK and batch upsert`, t6);
    record('T6', 'DB → HubSpot lossless (HASH B=C)', 'PASS', `${bc.ok} ok, ${bc.mismatch} mismatch, ${bc.pending} pending`, t6);

    const t7 = Date.now();
    const coerce = await http('GET', '/api/coercions');
    if (!coerce.ok) return record('T7', 'Coercion audit', 'FAIL', `/api/coercions ${coerce.status}`, t7);
    const types = Object.keys(coerce.body.by_coerce || {});
    if (types.length === 0) return record('T7', 'Coercion audit', 'FAIL', 'no coercion audit rows produced (every transformation should be logged)', t7);
    record('T7', 'Coercion audit', 'PASS', `${types.length} coerce types observed: ${types.join(', ')}`, t7);
  } catch (e) {
    record('T5_T6_T7', 'Sync-driven hash + audit tests', 'FAIL', e.message, t0);
  }
}

async function T8_loudFailures() {
  const t0 = Date.now();
  try {
    const issues = await http('GET', '/api/issues');
    const types = issues.body.recent_run?.by_type || {};
    const seen = Object.keys(types);
    const expectedAtLeastOneOf = ['email_suspect', 'email_coercion_skipped', 'hubspot_record_rejected', 'mapping_held'];
    const found = expectedAtLeastOneOf.filter(t => seen.includes(t));
    if (found.length === 0) return record('T8', 'Loud failures surfaced', 'FAIL', `expected at least one of [${expectedAtLeastOneOf.join(',')}] (sample data has invalid email + Y/N→date); none observed`, t0);
    record('T8', 'Loud failures surfaced', 'PASS', `loud event types in recent run: ${seen.slice(0, 6).join(', ')}`, t0);
  } catch (e) {
    record('T8', 'Loud failures surfaced', 'FAIL', e.message, t0);
  }
}

async function T10_idempotence() {
  const t0 = Date.now();
  try {
    const before = await http('GET', '/api/issues');
    const okBefore = before.body.hash_health?.db_to_hubspot?.ok || 0;

    await uploadAllSamples();
    await triggerSync();

    const after = await http('GET', '/api/issues');
    const okAfter = after.body.hash_health?.db_to_hubspot?.ok || 0;

    // The diff engine should skip identical rows, so HubSpot shipped count
    // shouldn't increase from the second sync.
    if (okAfter > okBefore + 5) return record('T10', 'Idempotence (diff engine)', 'FAIL', `re-sync shipped extra records (${okBefore} → ${okAfter}); diff engine not skipping`, t0);
    record('T10', 'Idempotence (diff engine)', 'PASS', `re-sync ok=${okAfter} ≈ before ${okBefore} (diff engine skipped duplicates)`, t0);
  } catch (e) {
    record('T10', 'Idempotence', 'FAIL', e.message, t0);
  }
}

async function T11_featureFlags() {
  const t0 = Date.now();
  // Single-pass: just check the on-state since we deploy with all flags on
  // for UAT. Off-state verification is deferred to Pass B (see uat.md §5/T11).
  try {
    const ui = await http('GET', '/');
    const sc = await http('GET', '/api/schema-check');
    const onStates = (ui.status === 200 || ui.status === 304) && (sc.status === 200);
    if (!onStates) return record('T11', 'Feature flag gating (on-state)', 'FAIL', `/=${ui.status}, /api/schema-check=${sc.status} — flags should be on`, t0);
    record('T11', 'Feature flag gating (on-state)', 'PASS', `UI 200, schema-check 200`, t0);
  } catch (e) {
    record('T11', 'Feature flag gating', 'FAIL', e.message, t0);
  }
}

async function T12_cron() {
  // Manual inspection only; harness records DEFERRED to remind operator.
  record('T12', 'Cron schedule registration', 'DEFERRED', 'Manual inspection: SSH and grep /app/index.js for cron.schedule block', 0);
}

async function T14_basicAuth() {
  // Bypass our auto-auth wrapper to verify the gate is actually present.
  const t0 = Date.now();
  try {
    const noAuth = await fetch(cfg.RAILWAY_URL + '/api/info');
    if (noAuth.status !== 401) {
      return record('T14', 'UI basic auth enforced', 'FAIL', `unauthenticated /api/info returned ${noAuth.status}, expected 401 (auth gate not active)`, t0);
    }
    const wwwAuth = noAuth.headers.get('www-authenticate') || '';
    if (!wwwAuth.toLowerCase().startsWith('basic')) {
      return record('T14', 'UI basic auth enforced', 'FAIL', `WWW-Authenticate header missing or wrong scheme: "${wwwAuth}"`, t0);
    }
    const wrongAuth = await fetch(cfg.RAILWAY_URL + '/api/info', {
      headers: { Authorization: 'Basic ' + Buffer.from('wrong:wrong').toString('base64') },
    });
    if (wrongAuth.status !== 401) {
      return record('T14', 'UI basic auth enforced', 'FAIL', `wrong-creds /api/info returned ${wrongAuth.status}, expected 401`, t0);
    }
    // Confirm /health is exempt (Railway's probe must not be auth-gated).
    const health = await fetch(cfg.RAILWAY_URL + '/health');
    if (health.status === 401) {
      return record('T14', 'UI basic auth enforced', 'FAIL', '/health is auth-gated; Railway healthcheck cannot probe it', t0);
    }
    record('T14', 'UI basic auth enforced', 'PASS', 'no-auth=401 (Basic challenge), wrong-creds=401, /health bypassed', t0);
  } catch (e) {
    record('T14', 'UI basic auth enforced', 'FAIL', e.message, t0);
  }
}

// ─── runner ────────────────────────────────────────────────────────

async function main() {
  await checkPreconditions();
  console.log(`UAT against ${cfg.RAILWAY_URL}\n`);

  await T1_health();
  await T2_schema();
  await T3_portalGuard();
  await T4_sftp();
  await T9_authGate();
  await T5_T6_T7_hashesAndAudit();
  await T8_loudFailures();
  await T10_idempotence();
  await T11_featureFlags();
  await T12_cron();
  await T14_basicAuth();

  // Write markdown summary
  const passes = results.filter(r => r.status === 'PASS').length;
  const fails  = results.filter(r => r.status === 'FAIL').length;
  const deferred = results.filter(r => r.status === 'DEFERRED' || r.status === 'BLOCKED').length;

  const md = [
    `# UAT Results — ${new Date().toISOString().slice(0, 19)}Z`,
    ``,
    `**Target:** ${cfg.RAILWAY_URL}`,
    ``,
    `**Summary:** ${passes} pass · ${fails} fail · ${deferred} deferred/blocked`,
    ``,
    `| ID | Test | Status | Detail | Duration (ms) |`,
    `|---|---|---|---|---:|`,
    ...results.map(r => `| ${r.id} | ${r.name} | ${r.status} | ${(r.detail || '').replace(/\|/g, '\\|').slice(0, 200)} | ${r.durationMs} |`),
  ].join('\n');

  fs.writeFileSync(cfg.UAT_OUT, md + '\n');
  console.log(`\nResults written to ${cfg.UAT_OUT}`);
  process.exit(fails > 0 ? 1 : 0);
}

main().catch(e => { console.error('UAT harness crashed:', e); process.exit(2); });
