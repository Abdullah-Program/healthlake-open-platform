#!/usr/bin/env python
"""Phase 8 - time travel, tags. Every gold refresh is a snapshot, so you can ask:
'what did the HEDIS rate look like when we first loaded, vs now?'  (run after ./run.sh pipeline 1..3)"""
from common.spark import get_spark

spark = get_spark("timetravel")

print("Snapshot history of gold.hedis_rates:")
snaps = spark.sql(
    "SELECT snapshot_id, committed_at, operation FROM lake.gold.hedis_rates.snapshots ORDER BY committed_at"
).collect()
for s in snaps:
    print(f"  {s.snapshot_id}  {s.committed_at}  {s.operation}")
appends = [s for s in snaps if s.operation == "append"]
if len(appends) < 2:
    raise SystemExit("Need at least 2 gold refreshes. Run ./run.sh pipeline 0 then pipeline 1 (and 2, 3).")
first, last = appends[0].snapshot_id, appends[-1].snapshot_id

print(f"\nRate at first load (snapshot {first}) vs now (snapshot {last}):")
spark.sql(
    f"""SELECT a.measure, a.denominator AS den_then, b.denominator AS den_now,
               a.rate AS rate_then, b.rate AS rate_now, round(b.rate - a.rate, 4) AS delta
        FROM (SELECT * FROM lake.gold.hedis_rates VERSION AS OF {first} WHERE state = 'ALL') a
        JOIN (SELECT * FROM lake.gold.hedis_rates VERSION AS OF {last}  WHERE state = 'ALL') b
          ON a.measure = b.measure
        ORDER BY a.measure"""
).show(truncate=False)

print("Tag the first snapshot so it is easy to query and protected from expiry:")
spark.sql(f"ALTER TABLE lake.gold.hedis_rates CREATE OR REPLACE TAG `after_initial_load` AS OF VERSION {first}")
spark.sql("SELECT measure, rate FROM lake.gold.hedis_rates VERSION AS OF 'after_initial_load' WHERE state = 'ALL' ORDER BY measure").show()

print("Silver history (every MERGE is a snapshot):")
spark.sql("SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added FROM lake.silver.pat_enc.snapshots ORDER BY committed_at").show(truncate=False)
print("Rollback would be:  CALL lake.system.rollback_to_snapshot('silver.pat_enc', <snapshot_id>)   (not run here - it is destructive)")
