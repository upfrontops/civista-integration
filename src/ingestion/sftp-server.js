const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('ssh2');

function startSftpServer(options = {}) {
  const {
    port = 2222,
    hostKeyPath = process.env.SFTP_HOST_KEY,
    incomingDir = path.join(__dirname, '../../incoming'),
    onFileReceived,
  } = options;

  if (!hostKeyPath || !fs.existsSync(hostKeyPath)) {
    console.log('SFTP server not started: no host key configured (set SFTP_HOST_KEY env var)');
    return null;
  }

  fs.mkdirSync(incomingDir, { recursive: true });

  const hostKey = fs.readFileSync(hostKeyPath);
  const allowedUser = process.env.SFTP_USER || 'civista';
  const allowedPass = process.env.SFTP_PASS || '';

  const server = new Server({ hostKeys: [hostKey] }, (client) => {
    console.log('SFTP client connected');

    client.on('authentication', (ctx) => {
      if (ctx.method === 'password' && ctx.username === allowedUser && ctx.password === allowedPass) {
        ctx.accept();
      } else {
        ctx.reject();
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
    console.log(`SFTP server listening on port ${port}`);
  });

  return server;
}

module.exports = { startSftpServer };
