/**
 * Tiny pub/sub so backend operations can stream live log events to the
 * browser via Server-Sent Events (SSE).
 *
 * Usage:
 *   const { eventStream, installConsoleHook } = require('./event-stream');
 *   installConsoleHook();   // all console.log/warn/error also go to subscribers
 *   eventStream.emit('log', { level: 'info', message: 'hello' });
 *
 * SSE handler in index.js subscribes: eventStream.on('log', sendToClient).
 */

const { EventEmitter } = require('events');

const eventStream = new EventEmitter();
// Allow many concurrent browser tabs subscribing without warnings.
eventStream.setMaxListeners(100);

let hooked = false;

function installConsoleHook() {
  if (hooked) return;
  hooked = true;

  const origLog   = console.log.bind(console);
  const origWarn  = console.warn.bind(console);
  const origError = console.error.bind(console);

  const emit = (level, args) => {
    const msg = args.map(a => {
      if (typeof a === 'string') return a;
      try { return JSON.stringify(a); } catch { return String(a); }
    }).join(' ');
    eventStream.emit('log', { level, message: msg, at: new Date().toISOString() });
  };

  console.log   = (...a) => { origLog(...a);   emit('info', a); };
  console.warn  = (...a) => { origWarn(...a);  emit('warn', a); };
  console.error = (...a) => { origError(...a); emit('error', a); };
}

module.exports = { eventStream, installConsoleHook };
