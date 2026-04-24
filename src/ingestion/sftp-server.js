const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('ssh2');

/**
 * Resolve the SSH host key. Two options:
 *   1. SFTP_HOST_KEY_PEM  — full PEM contents as env var (preferred on Railway,
 *                           containers have no persistent disk for a key file)
 *   2. SFTP_HOST_KEY      — filesystem path to a PEM file (local dev)
 *
 * Returns a Buffer of the key, or null if neither is available.
 */
function resolveHostKey() {
  if (process.env.SFTP_HOST_KEY_PEM && process.env.SFTP_HOST_KEY_PEM.trim() !== '') {
    const pem = process.env.SFTP_HOST_KEY_PEM;
    const tmp = path.join(os.tmpdir(), 'civista_sftp_hostkey');
    fs.writeFileSync(tmp, pem, { mode: 0o600 });
    return { source: 'env SFTP_HOST_KEY_PEM', buffer: Buffer.from(pem) };
  }
  const p = process.env.SFTP_HOST_KEY;
  if (p && fs.existsSync(p)) {
    return { source: `file ${p}`, buffer: fs.readFileSync(p) };
  }
  return null;
}

function startSftpServer(options = {}) {
  const {
    port = parseInt(process.env.SFTP_PORT || '2222', 10),
    incomingDir = path.join(__dirname, '../../incoming'),
    onFileReceived,
  } = options;

  const key = resolveHostKey();
  if (!key) {
    console.log('╔════════════════════════════════════════════════════════════════╗');
    console.log('║  SFTP server NOT started: no host key configured              ║');
    console.log('║  Set SFTP_HOST_KEY_PEM (env var) or SFTP_HOST_KEY (file path) ║');
    console.log('╚════════════════════════════════════════════════════════════════╝');
    return null;
  }

  // Refuse to start with an empty password — an empty SFTP_PASS would accept
  // any connection from the configured username.
  if (!process.env.SFTP_PASS || process.env.SFTP_PASS.trim() === '') {
    console.error('╔════════════════════════════════════════════════════════════════╗');
    console.error('║  SFTP server NOT started: SFTP_PASS is empty or unset         ║');
    console.error('║  Refusing to run with unauthenticated access                  ║');
    console.error('╚════════════════════════════════════════════════════════════════╝');
    return null;
  }

  fs.mkdirSync(incomingDir, { recursive: true });

  const hostKey = key.buffer;
  const allowedUser = process.env.SFTP_USER || 'civista';
  const allowedPass = process.env.SFTP_PASS;

  const server = new Server({ hostKeys: [hostKey] }, (client) => {
    console.log('SFTP client connected');

    client.on('authentication', (ctx) => {
      // SSH clients first probe with method='none' to ask what auth methods
      // are supported. We only support password; tell the client that so it
      // knows to send the password next.
      if (ctx.method !== 'password') {
        return ctx.reject(['password']);
      }
      if (ctx.username === allowedUser && ctx.password === allowedPass) {
        ctx.accept();
      } else {
        console.warn(`⚠  SFTP auth rejected: user=${ctx.username}`);
        ctx.reject(['password']);
      }
    });

    client.on('ready', () => {
      console.log('SFTP client authenticated');

      client.on('session', (accept) => {
        const session = accept();

        session.on('sftp', (accept) => {
          const sftp = accept();
          const openFiles = new Map();
          let handleCount = 0;

          sftp.on('OPEN', (reqid, filename, flags) => {
            const handle = Buffer.alloc(4);
            const filePath = path.join(incomingDir, path.basename(filename));
            handle.writeUInt32BE(handleCount++);
            openFiles.set(handle.toString('hex'), {
              path: filePath,
              stream: fs.createWriteStream(filePath),
            });
            sftp.handle(reqid, handle);
          });

          sftp.on('WRITE', (reqid, handle, offset, data) => {
            const file = openFiles.get(handle.toString('hex'));
            if (!file) {
              sftp.status(reqid, 4); // FAILURE
              return;
            }
            file.stream.write(data);
            sftp.status(reqid, 0); // OK
          });

          sftp.on('CLOSE', (reqid, handle) => {
            const file = openFiles.get(handle.toString('hex'));
            if (file) {
              file.stream.end();
              openFiles.delete(handle.toString('hex'));
              console.log(`SFTP: received ${path.basename(file.path)}`);
              if (onFileReceived) {
                onFileReceived(file.path);
              }
            }
            sftp.status(reqid, 0);
          });
        });
      });
    });

    client.on('end', () => {
      console.log('SFTP client disconnected');
    });
  });

  server.listen(port, '0.0.0.0', () => {
    console.log('╔════════════════════════════════════════════════════════════════╗');
    console.log(`║  SFTP server listening on port ${String(port).padEnd(33)}║`);
    console.log(`║  Host key source: ${key.source.padEnd(44)}║`);
    console.log(`║  Auth: password, user: ${allowedUser.padEnd(40)}║`);
    console.log(`║  Incoming dir: ${incomingDir.padEnd(48)}║`);
    console.log('╚════════════════════════════════════════════════════════════════╝');
  });

  return server;
}

module.exports = { startSftpServer };
