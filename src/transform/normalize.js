/**
 * Step 4: Data Normalization
 * Transforms staged data before HubSpot sync.
 * Does NOT modify staging tables — returns transformed objects ready for API calls.
 */

function normalizeEmail(val) {
  if (!val || val.toLowerCase() === 'none' || val.trim() === '') return null;
  return val.trim();
}

function normalizeBoolean(val) {
  if (!val) return false;
  return val.trim().toUpperCase() === 'Y';
}

function normalizeDeceased(val) {
  // Space character or 'N' means alive (false), 'Y' means deceased (true)
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
  // Handle timestamps with nanoseconds: "2026-03-18 19:24:34.013000000"
  const trimmed = val.trim();
  // Already ISO-like YYYY-MM-DD or YYYY-MM-DD HH:MM:SS...
  if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
    // Return just the date portion for date-only fields, or full ISO for timestamps
    if (trimmed.includes(' ')) {
      return new Date(trimmed.replace(' ', 'T')).toISOString();
    }
    return trimmed;
  }
  return trimmed;
}

function normalizeCifContact(row) {
  return {
    cif_number: row.cifnum,
    firstname: row.firstname || null,
    lastname: row.lastname || null,
    fullname: row.fullname || null,
    email: normalizeEmail(row.email),
    phone: row.phonenumber || null,
    address: row.address1 || null,
    address2: row.address2 || null,
    city: row.city || null,
    state: row.state || null,
    zip: row.zipcode || null,
    date_of_birth: normalizeDate(row.birthday),
    became_customer_date: normalizeDate(row.origcustdate),
    hash_ssn: row.hashssn || null,
    digital_banking: normalizeBoolean(row.digbank),
    private_banking: normalizeBoolean(row.privbanking),
    deceased: normalizeDeceased(row.deceased),
    minor: normalizeBoolean(row.minor),
    do_not_call: normalizeBoolean(row.donotcall),
    q2_user_id: row.q2userid || null,
    last_login: normalizeDate(row.lastlogin),
  };
}

function normalizeCifCompany(row) {
  return {
    cif_number: row.cifnum,
    name: row.fullname || null,
    phone: row.phonenumber || null,
    address: row.address1 || null,
    address2: row.address2 || null,
    city: row.city || null,
    state: row.state || null,
    zip: row.zipcode || null,
    became_customer_date: normalizeDate(row.origcustdate),
    naics_code: row.naicscode || null,
  };
}

function normalizeDda(row) {
  return {
    primary_key: row.primarykey,
    cif_number: row.cif_number,
    acctlast4: row.acctlast4,
    interest_rate: normalizeNumber(row.interestrate),
    accttype: row.accttype,
    acctdesc: row.acctdesc || null,
    open_date: normalizeDate(row.opendate),
    close_date: normalizeDate(row.closedate),
    slsassoc: row.slsassoc || null,
    dt_last_active: normalizeDate(row.dtlastactive),
    current_bal: normalizeNumber(row.currentbal),
    yest_bal: normalizeNumber(row.yestbal),
    branch_num: row.branchnum || null,
    officr_code: row.officrcode || null,
    acct_status: row.acctstatus,
    relationship: row.relationship,
    promo_code: row.promocode || null,
    open_online: row.openonline || null,
  };
}

function normalizeLoan(row) {
  return {
    primary_key: row.primarykey,
    cif_number: row.cifnum,
    acctlast4: row.acctlast4,
    interest_rate: normalizeNumber(row.interestrate),
    accttype: row.accttype,
    loan_type: row.loantype || null,
    orig_date: normalizeDate(row.origdate),
    maturity_date: normalizeDate(row.maturitydate),
    slsassoc: row.slsassoc || null,
    last_active_date: normalizeDate(row.lastactivedate),
    curr_bal: normalizeNumber(row.currbal),
    branch_num: row.branchnum || null,
    officr_code: row.officrcode || null,
    acct_status: row.acctstatus,
    relationship: row.relationship,
    orig_bal: normalizeNumber(row.origbal),
  };
}

function normalizeCd(row) {
  return {
    primary_key: row.primarykey,
    cif_number: row.cifnum,
    acctlast4: row.acctlast4,
    interest_rate: normalizeNumber(row.interestrate),
    accttype: row.accttype,
    acctdesc: row.acctdesc || null,
    issue_date: normalizeDate(row.issuedate),
    maturity_date: normalizeDate(row.maturitydate),
    slsassoc: row.slsassoc || null,
    curr_bal: normalizeNumber(row.currbal),
    branch_num: row.branchnum || null,
    officr_code: row.officrcode || null,
    acct_status: row.acctstatus,
    relationship: row.relationship,
    open_online: row.openonline || null,
  };
}

function normalizeDebitCard(row) {
  return {
    composite_key: row.composite_key,
    cif_number: row.cif_number,
    acctlast4: row.acctlast4,
    acct_type: row.accttype,
    last4_debit_card: row.last4debitcard,
    card_status: row.cardstatus,
    expire_date: normalizeDate(row.expiredate),
    last_used_dt: normalizeDate(row.lastuseddt),
    pos_last_30_days: normalizeNumber(row.poslast30days),
    active_pos: row.activepos || null,
  };
}

module.exports = {
  normalizeEmail,
  normalizeBoolean,
  normalizeDeceased,
  normalizeNumber,
  normalizeDate,
  normalizeCifContact,
  normalizeCifCompany,
  normalizeDda,
  normalizeLoan,
  normalizeCd,
  normalizeDebitCard,
};
