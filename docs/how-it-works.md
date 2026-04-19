# How It Works

## The 30-Second Version

Every night, Civista Bank's core system exports customer and account data as CSV files. Our middleware picks them up, figures out what changed since yesterday, and pushes only those changes into HubSpot. The whole pipeline is designed so that if anything looks wrong, it stops and waits for a human rather than corrupting CRM data.

## The Pipeline, Step by Step

```
┌─────────────────────────────────────────────────────────────────────┐
│  NIGHTLY SYNC (2:00 AM ET)                                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. RECEIVE       Silver Lake drops 5 CSVs via SFTP                │
│       ↓                                                             │
│  2. VALIDATE      Circuit breaker checks row counts                │
│       ↓           (did the file shrink suspiciously?)               │
│       ↓                                                             │
│  3. STAGE         Parse CSVs into PostgreSQL staging tables         │
│       ↓                                                             │
│  4. CLASSIFY      CIF records sorted into Contacts vs Companies    │
│       ↓                                                             │
│  5. DIFF          Compare to yesterday — find what changed          │
│       ↓                                                             │
│  6. SYNC          Push changes to HubSpot (100 records at a time)  │
│       ↓                                                             │
│  7. ARCHIVE       Move processed files to archive/{date}/          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Step 1: Receive

Civista's Silver Lake system exports 5 files nightly:

| File | What It Contains |
|------|-----------------|
| `HubSpot_CIF.csv` | Customer master records (people and businesses) |
| `HubSpot_DDA.csv` | Checking/savings accounts (Demand Deposit Accounts) |
| `HubSpot_Loan.csv` | All loan products |
| `HubSpot_CD.csv` | Certificates of Deposit / Time Deposits |
| `HubSpot_Debit_Card.csv` | Debit card information |

Files land in the `/incoming/` directory via SFTP.

## Step 2: Validate (Circuit Breaker)

Before processing, we check: **did the file shrink by more than 30%** compared to last time?

If CIF had 261,000 records yesterday and today's file only has 150,000 — something is wrong on Civista's side. Maybe the export failed partway. The system **stops** and doesn't overwrite good data with a bad export.

This single check prevents the most dangerous failure mode: accidentally deleting thousands of records from HubSpot because of an upstream glitch.

## Step 3: Stage

Each CSV gets parsed into a PostgreSQL staging table. This is a temporary holding area where we can inspect, transform, and compare data before it touches HubSpot.

Staging also handles the annoying inconsistencies in the source data:
- The "CIF number" column is called `CIFNum` in some files and `CIF#` in others — we normalize to `cif_number`
- Trailing whitespace in fields gets trimmed
- The word `"none"` in the email field gets converted to null

## Step 4: Classify

The CIF file contains both people and businesses mixed together. We sort them:

| Condition | Classification |
|-----------|---------------|
| Tax ID Type = "T" AND no first/last name | **Company** |
| Tax ID Type = "T" AND has first + last name | **Contact** (person at a business) |
| Tax ID Type is anything else | **Contact** (individual) |

This determines whether the record goes into HubSpot as a Contact or a Company.

## Step 5: Diff

We hash every row and compare it to what we sent last time. Only rows that actually changed get pushed to HubSpot. This means:
- **Faster syncs** — we're not resending 261k records every night
- **Cleaner audit trail** — HubSpot's activity timeline shows real changes, not phantom updates
- **Less API usage** — HubSpot charges by API calls

## Step 6: Sync to HubSpot

Changed records get batched (100 at a time) and upserted into HubSpot using their batch API. "Upsert" means:
- If the record already exists → update it
- If it's new → create it

If HubSpot rate-limits us (says "slow down"), we back off exponentially — wait 100ms, then 200ms, then 400ms, etc. — up to 10 retries.

## Step 7: Archive

Processed CSV files move to `archive/{date}/` so we always have a record of what was synced.

## Processing Order

The system always processes CIF first, then the account files. This ensures contacts and companies exist in HubSpot before their associated accounts get synced.

**Order:** CIF → DDA → Loans → CDs → Debit Cards

## What HubSpot Gets

| Source File | HubSpot Object |
|-------------|---------------|
| CIF (contacts) | Standard **Contacts** |
| CIF (companies) | Standard **Companies** |
| DDA | Custom Object: Deposits |
| Loans | Custom Object: Loans |
| CDs | Custom Object: Time Deposits |
| Debit Cards | Custom Object: Debit Cards |
