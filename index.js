const express = require('express');
const path = require('path');
const fs = require('fs');
const os = require('os');
const cron = require('node-cron');
const multer = require('multer');
const { pool, initDb } = require('./db/init');
const { runFullSync } = require('./src/sync/orchestrator');
const { getHealthStatus } = require('./src/monitoring/health');
const { startSftpServer } = require('./src/ingestion/sftp-server');
const { testAuth, uploadFile } = require('./src/sync/sftp-client');
const { eventStream, installConsoleHook } = require('./src/monitoring/event-stream');
const { TABLES, CIF_CONTACT_FIELDS, CIF_COMPANY_FIELDS } = require('./src/transform/hubspot-mapping');

// Forward all console output to SSE subscribers so the UI log panel sees it.
installConsoleHook();

const app = express();
const port = process.env.PORT || 3000;

const INCOMING_DIR = process.env.INCOMING_DIR || path.join(__dirname, 'incoming');
const ARCHIVE_DIR = process.env.ARCHIVE_DIR || path.join(__dirname, 'archive');
const QUARANTINE_DIR = process.env.QUARANTINE_DIR || path.join(__dirname, 'quarantine');

fs.mkdirSync(INCOMING_DIR, { recursive: true });
fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
fs.mkdirSync(QUARANTINE_DIR, { recursive: true });

// Files uploaded by the browser land in the OS tmp dir, then we SFTP them
// into our own SFTP endpoint. We never write them directly to /incoming/ —
// that would bypass the SFTP path we need to prove works.
const TMP_UPLOAD_DIR = path.join(os.tmpdir(), 'civista_browser_uploads');
fs.mkdirSync(TMP_UPLOAD_DIR, { recursive: true });
const upload = multer({ dest: TMP_UPLOAD_DIR });

app.use(express.json());
// Static assets for the debug UI
app.use(express.static(path.join(__dirname, 'public')));

// Service info (was the root handler; keep under /api/info).
app.get('/api/info', (req, res) => {
  res.json({
    service: 'civista-integration',
    version: '1.0.0',
    description: 'Civista Bank HubSpot nightly data sync pipeline',
  });
});

app.get('/health', async (req, res) => {
  try {
    const status = await getHealthStatus();
    res.status(200).json(status);
  } catch (err) {
    res.status(200).json({ status: 'starting', database: 'connecting', error: err.message });
  }
});

// -------------------- Sync --------------------
let syncRunning = false;

app.post('/sync', async (req, res) => {
  if (syncRunning) return res.status(409).json({ error: 'Sync already in progress' });
  syncRunning = true;
  res.json({ message: 'Sync started', startedAt: new Date().toISOString() });
  try {
    await runFullSync(INCOMING_DIR, ARCHIVE_DIR, QUARANTINE_DIR);
  } catch (err) {
    console.error('Sync failed:', err);
  } finally {
    syncRunning = false;
  }
});

// -------------------- SFTP auth (test only) --------------------
app.post('/api/sftp-auth', async (req, res) => {
  const { host, port, username, password } = req.body || {};
  if (!host || !port || !username || !password) {
    return res.status(400).json({ error: 'host, port, username, password are required' });
  }
  try {
    await testAuth({ host, port, username, password });
    res.json({ ok: true });
  } catch (e) {
    console.warn(`SFTP auth failed from UI: ${e.message}`);
    res.status(401).json({ error: e.message });
  }
});

// -------------------- SFTP upload from browser --------------------
// Browser sends a single file + creds. Server uses creds to SFTP-put the
// file into its own SFTP endpoint (same host). If SFTP is down or creds
// are bad, the upload fails — which is exactly the guarantee we want.
app.post('/api/sftp-upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'no file' });
  let creds;
  try { creds = JSON.parse(req.body.creds || '{}'); }
  catch { return res.status(400).json({ error: 'invalid creds json' }); }
  if (!creds.host || !creds.port || !creds.username || !creds.password) {
    return res.status(400).json({ error: 'creds must include host, port, username, password' });
  }

  const remoteName = path.basename(req.file.originalname);
  try {
    await uploadFile(creds, req.file.path, remoteName);
    console.log(`Browser→SFTP: pushed ${remoteName} to ${creds.host}:${creds.port}`);
    res.json({ ok: true, remoteName });
  } catch (e) {
    console.warn(`SFTP upload failed for ${remoteName}: ${e.message}`);
    res.status(502).json({ error: e.message });
  } finally {
    // Clean up the temp upload file
    fs.unlink(req.file.path, () => {});
  }
});

// -------------------- Legacy direct upload (kept for CLI use) --------------------
app.post('/upload', upload.array('files'), (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: 'No files uploaded' });
  }
  const received = [];
  for (const file of req.files) {
    const dest = path.join(INCOMING_DIR, file.originalname);
    fs.renameSync(file.path, dest);
    received.push(file.originalname);
  }
  res.json({ message: `Received ${received.length} files`, files: received });
});

// -------------------- SSE log stream --------------------
app.get('/api/stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders();
  res.write('retry: 3000\n\n');
  const send = (payload) => {
    try {
      res.write(`event: log\ndata: ${JSON.stringify(payload)}\n\n`);
    } catch {
      // client disconnected
    }
  };
  send({ level: 'info', message: 'SSE log stream connected', at: new Date().toISOString() });
  eventStream.on('log', send);
  req.on('close', () => { eventStream.off('log', send); });
});

// -------------------- Schema check (live HubSpot + Postgres) --------------------
const SCHEMA_OBJECTS = [
  { stagingTable: 'stg_contacts',      hsObject: 'contacts',   csvFile: 'HubSpot_CIF.csv',        label: 'Contacts',       fields: CIF_CONTACT_FIELDS },
  { stagingTable: 'stg_companies',     hsObject: 'companies',  csvFile: 'HubSpot_CIF.csv',        label: 'Companies',      fields: CIF_COMPANY_FIELDS },
  { stagingTable: 'stg_deposits',      hsObject: '2-60442978', csvFile: 'HubSpot_DDA.csv',        label: 'Deposits',       fields: TABLES.dda.fields },
  { stagingTable: 'stg_loans',         hsObject: '2-60442977', csvFile: 'HubSpot_Loan.csv',       label: 'Loans',          fields: TABLES.loans.fields },
  { stagingTable: 'stg_time_deposits', hsObject: '2-60442980', csvFile: 'HubSpot_CD.csv',         label: 'Time Deposits',  fields: TABLES.cd.fields },
  { stagingTable: 'stg_debit_cards',   hsObject: '2-60442979', csvFile: 'HubSpot_Debit_Card.csv', label: 'Debit Cards',    fields: TABLES.debit_cards.fields },
];

async function countCsvRows(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return new Promise((resolve, reject) => {
    let count = 0;
    let remainder = '';
    const s = fs.createReadStream(filePath, { encoding: 'utf8' });
    s.on('data', (chunk) => {
      const lines = (remainder + chunk).split('\n');
      remainder = lines.pop();
      count += lines.length;
    });
    s.on('end', () => { if (remainder.trim()) count++; resolve(Math.max(0, count - 1)); });
    s.on('error', reject);
  });
}

app.get('/api/schema-check', async (req, res) => {
  try {
    const apiKey = process.env.HUBSPOT_API_KEY;
    if (!apiKey) return res.status(500).json({ error: 'HUBSPOT_API_KEY not set' });

    const objects = [];
    for (const o of SCHEMA_OBJECTS) {
      // Postgres columns
      const pgCols = await pool.query(
        `SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
        [o.stagingTable]
      );
      const pgColSet = new Set(pgCols.rows.map(r => r.column_name));
      const internal = new Set(['id', 'row_hash', 'loaded_at', 'synced_at']);

      // HubSpot properties
      const hsRes = await fetch(`https://api.hubapi.com/crm/v3/properties/${o.hsObject}`, {
        headers: { Authorization: `Bearer ${apiKey}` },
      });
      const hsBody = await hsRes.json();
      const hsProps = Array.isArray(hsBody.results) ? hsBody.results : [];
      const hsPropMap = new Map(hsProps.map(p => [p.name, p]));

      // Diff: for each non-internal PG column, find matching HS property.
      const rows = [];
      let matched = 0, missingInHs = 0, missingInPg = 0;
      for (const c of pgCols.rows) {
        if (internal.has(c.column_name)) continue;
        const hs = hsPropMap.get(c.column_name);
        if (hs) {
          rows.push({ pgColumn: c.column_name, hsProp: hs.name, hsType: hs.type + '/' + hs.fieldType, status: 'match' });
          matched++;
        } else {
          rows.push({ pgColumn: c.column_name, hsProp: '', hsType: '', status: 'pg-only' });
          missingInHs++;
        }
      }
      // HS-only custom properties that we don't populate.
      const customHs = hsProps.filter(p => !p.hubspotDefined);
      for (const p of customHs) {
        if (pgColSet.has(p.name)) continue;
        rows.push({ pgColumn: '', hsProp: p.name, hsType: p.type + '/' + p.fieldType, status: 'hs-only' });
        missingInPg++;
      }

      // Postgres row count
      let pgCount = 0;
      try {
        const c = await pool.query(`SELECT COUNT(*)::int AS c FROM ${o.stagingTable}`);
        pgCount = c.rows[0].c;
      } catch { /* table may not exist yet */ }

      // HubSpot total (search API returns total)
      let hsCount = null;
      try {
        const sr = await fetch(`https://api.hubapi.com/crm/v3/objects/${o.hsObject}/search`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ limit: 1 }),
        });
        const sb = await sr.json();
        hsCount = sb.total ?? null;
      } catch {}

      // CSV row count if file is in /incoming/
      const csvPath = path.join(INCOMING_DIR, o.csvFile);
      const csvCount = await countCsvRows(csvPath);

      objects.push({
        label: o.label,
        stagingTable: o.stagingTable,
        hsObject: o.hsObject,
        matched, missingInHs, missingInPg,
        pgRowCount: pgCount,
        hsRowCount: hsCount === null ? '—' : hsCount,
        csvRowCount: csvCount,
        fields: rows,
      });
    }
    res.json({ objects });
  } catch (e) {
    console.error('schema-check failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// -------------------- Nightly cron --------------------
cron.schedule('0 2 * * *', async () => {
  if (syncRunning) { console.log('Cron: sync already running, skipping'); return; }
  console.log('Cron: starting nightly sync');
  syncRunning = true;
  try {
    await runFullSync(INCOMING_DIR, ARCHIVE_DIR, QUARANTINE_DIR);
  } catch (err) {
    console.error('Cron sync failed:', err);
  } finally {
    syncRunning = false;
  }
}, { timezone: 'America/New_York' });

// Start SFTP server if configured
startSftpServer({
  incomingDir: INCOMING_DIR,
  onFileReceived: (filePath) => {
    console.log(`SFTP file received: ${path.basename(filePath)}`);
  },
});

// Start Express first, then init DB.
app.listen(port, () => {
  console.log(`civista-integration listening on port ${port}`);
  initDb()
    .then(() => console.log('Database initialized'))
    .catch((err) => console.error('Database init failed (will retry on next request):', err.message));
});
