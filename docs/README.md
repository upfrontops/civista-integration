# Civista Bank Integration — Documentation

Welcome to the Civista Sync project wiki. This system moves banking data from Civista Bank's core system (Silver Lake / Jack Henry) into HubSpot CRM every night so the team can see customer relationships, accounts, and product holdings without logging into the core banking platform.

## Quick Links

| Document | Who It's For | What It Covers |
|----------|-------------|----------------|
| [How It Works](./how-it-works.md) | Everyone | Plain-English overview of the nightly sync |
| [Data Dictionary](./data-dictionary.md) | Anyone reading HubSpot data | What each field means, where it comes from |
| [Operations Runbook](./operations-runbook.md) | PatchOps team | Monitoring, troubleshooting, manual overrides |
| [Safety & Guardrails](./safety-and-guardrails.md) | Founders / Stakeholders | How the system protects against bad data |
| [Deployment & Environment](./deployment.md) | Engineers | Railway setup, env vars, first-run steps |

## One-Sentence Summary

> Every night at 2:00 AM ET, Civista's banking system drops 5 CSV files. This system picks them up, checks them for problems, and pushes only the changes into HubSpot — contacts, companies, deposits, loans, CDs, and debit cards.

## Key Numbers

- **~261,000** customer records (CIF)
- **~250,000** deposit accounts (DDA)
- **~92,000** loans
- **~39,000** time deposits (CDs)
- **~76,000** debit cards
- **5 files** processed sequentially every night
- **100 records per batch** sent to HubSpot
- **30% drop threshold** — if a file shrinks by more than 30% overnight, the sync halts and alerts
