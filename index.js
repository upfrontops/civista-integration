const express = require('express');
const path = require('path');
const fs = require('fs');
const cron = require('node-cron');
const multer = require('multer');
const { pool, initDb } = require('./db/init');
const { runFullSync } = require('./src/sync/orchestrator');
const { getHealthStatus } = require('./src/monitoring/health');
const { startSftpServer } = require('./src/ingestion/sftp-server');

const app = express();
const port = process.env.PORT || 3000;

const INCOMING_DIR = process.env.INCOMING_DIR || path.join(__dirname, 'incoming');
const ARCHIVE_DIR = process.env.ARCHIVE_DIR || path.join(__dirname, 'archive');
const QUARANTINE_DIR = process.env.QUARANTINE_DIR || path.join(__dirname, 'quarantine');

fs.mkdirSync(INCOMING_DIR, { recursive: true });
fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
fs.mkdirSync(QUARANTINE_DIR, { recursive: true });

// File upload for manual CSV drops
const upload = multer({ dest: INCOMING_DIR });

app.use(express.json());

app.get('/', (req, res) => {
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

// Manual sync trigger
let syncRunning = false;

app.post('/sync', async (req, res) => {
  if (syncRunning) {
    return res.status(409).json({ error: 'Sync already in progress' });
  }

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

// Manual file upload endpoint
app.post('/upload', upload.array('files'), (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: 'No files uploaded' });
  }

  const received = [];
  for (const file of req.files) {
    // Rename from multer temp name to original filename
    const dest = path.join(INCOMING_DIR, file.originalname);
    fs.renameSync(file.path, dest);
    received.push(file.originalname);
  }

  res.json({ message: `Received ${received.length} files`, files: received });
});

// Nightly cron — 2:00 AM ET
cron.schedule('0 2 * * *', async () => {
  if (syncRunning) {
    console.log('Cron: sync already running, skipping');
    return;
  }

  console.log('Cron: starting nightly sync');
  syncRunning = true;
  try {
    await runFullSync(INCOMING_DIR, ARCHIVE_DIR, QUARANTINE_DIR);
  } catch (err) {
    console.error('Cron sync failed:', err);
  } finally {
    syncRunning = false;
  }
}, {
  timezone: 'America/New_York',
});

// Start SFTP server if configured
startSftpServer({
  incomingDir: INCOMING_DIR,
  onFileReceived: (filePath) => {
    console.log(`SFTP file received: ${path.basename(filePath)}`);
  },
});

// Start server first (so Railway healthcheck gets a response), then init DB
app.listen(port, () => {
  console.log(`civista-integration listening on port ${port}`);
  initDb()
    .then(() => console.log('Database initialized'))
    .catch((err) => console.error('Database init failed (will retry on next request):', err.message));
});
