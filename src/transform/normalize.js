/**
 * Pure value coercion helpers. The old object-building functions
 * (normalizeCifContact, normalizeDda, etc.) are gone — their job is
 * now done by buildPayload() in src/sync/hubspot.js which uses the
 * canonical mapping in src/transform/hubspot-mapping.js.
 *
 * These helpers are called from hubspot.js during type coercion.
 */

function normalizeEmail(val) {
  if (!val || val.toLowerCase() === 'none' || val.trim() === '') return null;
  return val.trim();
}

function normalizeBoolean(val) {
  if (!val) return false;
  return val.trim().toUpperCase() === 'Y';
}

/**
 * Deceased flag — source convention: single space or 'N' means alive (false),
 * 'Y' means deceased (true). Required by CLAUDE.md edge case #5.
 */
function normalizeDeceased(val) {
  if (!val || val.trim() === '' || val === ' ' || val.trim().toUpperCase() === 'N') return false;
  return val.trim().toUpperCase() === 'Y';
}

function normalizeNumber(val) {
  if (!val || val.trim() === '') return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : num;
}

function normalizeDate(val) {
  if (!val || val.trim() === '') return null;
  const trimmed = val.trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
    if (trimmed.includes(' ')) {
      return new Date(trimmed.replace(' ', 'T')).toISOString();
    }
    return trimmed;
  }
  return trimmed;
}

module.exports = {
  normalizeEmail,
  normalizeBoolean,
  normalizeDeceased,
  normalizeNumber,
  normalizeDate,
};
