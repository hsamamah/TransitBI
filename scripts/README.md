# scripts/

Operational scripts for manual pipeline management. These are run locally against AWS — they submit Glue jobs and poll for completion.

---

## backload.sh

Replays the RT transform + load pipeline (and optionally the static dim load) for a historical date range, assuming raw data is already in S3. The ingestion step (downloading external feeds) is intentionally skipped.

**When to use:**
- Gap recovery — a Glue job failed and left days missing from FactStop/FactTrip/FactServiceDay
- Schema/logic fix — reload N days of fact data after correcting a computation bug
- Initial historical load — populate fact tables from the 90-day `.pb` archive
- Static dim reload — re-run the Redshift load after manually re-staging a corrected GTFS ZIP

**What it runs (in order):**

| Step | Glue job | Scope | Notes |
|------|----------|-------|-------|
| 1 | `gtfs-rt-parse-load-glue` | Once per date | Sequential — stg tables are truncated per run |
| 2a | `factstop-skeleton-and-merge-load` | Full date range | Parallel with 2b |
| 2b | `facttrip-skeleton-and-merge-load` | Full date range | Parallel with 2a |
| 3 | `factserviceday-load` | Full date range | After 2a + 2b complete |
| 4 | `gtfs-static-redshift-load` | Single `--static-date` | Only if `--static-date` is passed |

The `transit-pipeline-inspector` is **skipped** — date params go directly to the fact jobs, bypassing DynamoDB entirely.

### Usage

```bash
bash scripts/backload.sh --start_date YYYY-MM-DD --end_date YYYY-MM-DD [options]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--start_date DATE` | Yes | Start of the date range (inclusive) |
| `--end_date DATE` | Yes | End of the date range (inclusive) |
| `--static-date DATE` | No | Also reload static dims from this staged S3 date |
| `--rt-only` | No | Skip static dims even if `--static-date` is given |
| `--dry-run` | No | Print Glue job submissions without executing them |

### Examples

```bash
# Reload 7 days of RT facts
bash scripts/backload.sh --start_date 2026-03-01 --end_date 2026-03-07

# Single-day gap fix
bash scripts/backload.sh --start_date 2026-03-15 --end_date 2026-03-15

# Reload facts + re-run static dim load for a specific staged date
bash scripts/backload.sh \
  --start_date 2026-03-01 --end_date 2026-03-07 \
  --static-date 2026-03-01

# Dry run — verify job submissions before committing
bash scripts/backload.sh --start_date 2026-03-01 --end_date 2026-03-07 --dry-run
```

### Cost estimate

| Scope | Approx. cost |
|-------|-------------|
| 1 day | ~$0.80 |
| 7 days | ~$5 |
| 30 days | ~$23 |
| 90 days (full archive) | ~$70 |

Based on Glue G.1X pricing ($0.44/DPU-hr): `gtfs-rt-parse-load-glue` runs 10 workers for ~8 min per day of data; the three fact jobs run 2 workers each.

### Constraints

- **90-day RT limit** — raw `.pb` files in `seattle-transit-raw` expire after 90 days (S3 lifecycle policy). Backloads beyond that window will produce empty staging output.
- **Static dims** — `--static-date` requires the corresponding staged `.txt` files to already exist under `s3://seattle-transit-staging/gtfs-static/combined/YYYY/MM/DD/`. The ingestion step that produces these files is not part of backload.
- **Credentials** — requires AWS CLI credentials with `glue:StartJobRun` and `glue:GetJobRun` permissions.
