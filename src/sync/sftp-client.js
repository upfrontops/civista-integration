/**
 * SFTP client used by the debug UI to push uploaded files into the SFTP
 * endpoint of *this same service*. This makes the UI a true MOVEit
 * simulator — if SFTP is down or credentials are wrong, every UI
 * operation fails, which is exactly what we want to prove the SFTP
 * pipeline works before we ship.
 *
 * All functions take explicit credentials. Nothing is cached server-side.
 * The browser sends credentials with every request.
 */

const fs = require('fs');
const { Client } = require('ssh2');

const DEFAULT_TIMEOUT_MS = 8000;

/**
 * Open a connection, authenticate, run `fn(sftp)`, then close.
 * Rejects on any failure (connection, auth, operation).
 */
function withSftp({ host, port, username, password }, fn) {
  return new Promise((resolve, reject) => {
    const conn = new Client();
    let done = false;
    const finish = (err, value) => {
      if (done) return;
      done = true;
      try { conn.end(); } catch {}
      if (err) reject(err); else resolve(value);
    };

    const timer = setTimeout(() => finish(new Error(`SFTP timeout after ${DEFAULT_TIMEOUT_MS}ms`)), DEFAULT_TIMEOUT_MS);

    conn.on('ready', () => {
      conn.sftp((err, sftp) => {
        if (err) return finish(err);
        Promise.resolve(fn(sftp))
          .then(v => { clearTimeout(timer); finish(null, v); })
          .catch(e => { clearTimeout(timer); finish(e); });
      });
    });
    conn.on('error', (e) => { clearTimeout(timer); finish(e); });
    conn.connect({ host, port, username, password, readyTimeout: DEFAULT_TIMEOUT_MS, algorithms: { serverHostKey: ['ssh-rsa', 'rsa-sha2-256', 'rsa-sha2-512'] } });
  });
}

/**
 * Test credentials by connecting, authenticating, and closing.
 * Returns { ok: true } or throws.
 */
async function testAuth(creds) {
  await withSftp(creds, async () => {});
  return { ok: true };
}

/**
 * Upload a local file to the SFTP endpoint.
 * Returns { ok: true, remoteName } or throws.
 */
async function uploadFile(creds, localPath, remoteName) {
  await withSftp(creds, (sftp) => new Promise((resolve, reject) => {
    const rs = fs.createReadStream(localPath);
    const ws = sftp.createWriteStream(remoteName);
    rs.on('error', reject);
    ws.on('error', reject);
    ws.on('close', resolve);
    rs.pipe(ws);
  }));
  return { ok: true, remoteName };
}

module.exports = { testAuth, uploadFile };
