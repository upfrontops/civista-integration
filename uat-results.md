# UAT Results — 2026-05-05T19:31:14Z

**Target:** https://civista-integration-production.up.railway.app

**Summary:** 12 pass · 0 fail · 1 deferred/blocked

| ID | Test | Status | Detail | Duration (ms) |
|---|---|---|---|---:|
| T1 | Deployment health | PASS | /health 200 in 133ms · civista-integration v1.0.0 | 193 |
| T2 | Schema alignment | PASS | 6 objects aligned, 0 mismatches | 2878 |
| T3 | Portal cutover guard | PASS | service booted past guard; auth gate enforced | 38 |
| T4 | SFTP auth + integrity | PASS | bad password rejected; good password authenticated; debit_card.csv uploaded (local md5 93012cac) | 1163 |
| T9 | Manual sync auth gate | PASS | no-token=403, wrong-token=403 | 62 |
| T5 | CSV → DB lossless (HASH A=B) | PASS | 250 ok, 0 mismatch, 0 pending | 29963 |
| T6 | DB → HubSpot lossless (HASH B=C) | PASS | 209 ok, 0 mismatch, 41 pending | 0 |
| T7 | Coercion audit | PASS | 4 coerce types observed: yn_to_bool, deceased_flag_special, date_only, email_strict | 45 |
| T8 | Loud failures surfaced | PASS | loud event types in recent run: hubspot_record, email_suspect, manual_sync_auth_rejected, hubspot_batch_failed, sftp_auth_rejected | 63 |
| T10 | Idempotence (diff engine) | PASS | re-sync ok=0 ≈ before 209 (diff engine skipped duplicates) | 24461 |
| T11 | Feature flag gating (on-state) | PASS | UI 200, schema-check 200 | 2021 |
| T12 | Cron schedule registration | DEFERRED | Manual inspection: SSH and grep /app/index.js for cron.schedule block | 1778009474584 |
| T14 | UI basic auth enforced | PASS | no-auth=401 (Basic challenge), wrong-creds=401, /health bypassed | 82 |
