# PROJECT_CHECKPOINT - IceLake Health

Update this after every session. Future-you (and Claude) will start from here.

## Status
- [x] Phase 0  env + docker up + smoke test
- [x] Phase 1  synthetic Clarity-style data generated
- [x] Phase 2  Polaris catalog + principals + Trino up
- [x] Phase 3  bronze + silver (MERGE) for batch 0
- [x] Phase 4  DQ checks + gold + HEDIS-style rates
- [x] Phase 5  incremental batches 1-3 + time travel
- [x] Phase 6  schema + partition evolution demo
- [x] Phase 7  table maintenance (compaction, expiry)
- [x] Phase 8  Trino + DuckDB + benchmark report
- [x] Phase 9  Polaris RBAC (analyst = gold only)
- [x] README with architecture diagram + real benchmark numbers

## Numbers to fill in (from YOUR runs)
- rows: patients 20,000 / encounters 468,760 / diagnoses 388,485 (Bronze batch 0 total: 894,908 rows; raw total: 974,140 rows)
- HBD_LT8 rate after batch 0: 38.88% (CBP: 43.85%, COL: 9.00%, HBD_GT9: 43.83%)   after batch 3: 44.08% (CBP: 44.60%, COL: 10.84%, HBD_GT9: 36.80%)
- files before/after compaction (pat_enc): 139 -> 36 (patient: 4 -> 1; 76 position delete files pruned)
- fastest engine on Q3 (join): DuckDB (0.046s median) vs Trino (0.404s) vs Spark (0.521s); Q2 partition pruning read only 1 of 36 files (97.2% I/O pruned!)

## Errors I hit and how I fixed them (Interview Gold)
1. **NumPy Random Choice Probability Sum (ValueError):** `COMMON` dictionary weights in `generate_clarity_data.py` summed to 0.97 due to a typo. Fixed by normalizing weights explicitly with `p = p / p.sum()`.
2. **Polaris REST Catalog HTTP 400 Namespace Rejection:** Polaris rejected `s3://healthlake/warehouse/bronze/` because `storageConfigInfo.allowedLocations` only listed `s3://healthlake/`. Fixed by adding `s3://healthlake/warehouse/` to allowed locations and standardizing trailing slashes.
3. **PySpark Worker Fork Memory Crash (`OSError: [Errno 12] Cannot allocate memory`):** In `run_checks.py`, calling `spark.createDataFrame(rows)` for 11 small audit rows triggered PySpark to fork 16-20 concurrent Python worker processes on multi-core CPU inside Docker container alongside a 4GB JVM heap. Fixed by replacing `createDataFrame` with pure JVM Spark SQL `INSERT INTO lake.gold.dq_results VALUES (...)`, entirely avoiding Python worker forks.
4. **Physiologic Outlier Caught by DQ Gate & Remediated via Iceberg ACID Update:** A 4.5-sigma draw in normal distribution produced systolic BP = 58 (< 60), tripping the "BP in plausible range" check. Exercised Apache Iceberg v2 merge-on-read row-level update directly via SQL: `UPDATE lake.silver.pat_enc SET bp_systolic = 70 WHERE pat_enc_csn_id = 900038706`. Clamped synthetic generator with `np.clip(..., 65, 250)`. All 11 DQ checks now PASS.
