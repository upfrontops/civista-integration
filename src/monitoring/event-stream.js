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

/**
 * Emit a HubSpot wire-log event. Subscribers (the UI) get a structured
 * record of every API call we make so they can demonstrate to the client
 * exactly what data is moving. This is in addition to the generic 'log'
 * event — the wire feed is its own channel.
 *
 * Gated by `ENABLE_WIRE_LOG=1`. On a 700k-row sync that's ~14k events; we
 * default OFF in prod so we don't pay the per-record fanout overhead
 * unless someone has explicitly opted in.
 */
function emitHubspot(record) {
  if (process.env.ENABLE_WIRE_LOG !== '1') return;
  eventStream.emit('hubspot', {
    at: new Date().toISOString(),
    ...record,
  });
}

module.exports = { eventStream, installConsoleHook, emitHubspot };
