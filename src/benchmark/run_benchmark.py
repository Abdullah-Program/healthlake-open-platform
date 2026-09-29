#!/usr/bin/env python
"""Phase 9c - honest multi-engine benchmark on YOUR laptop.
1) correctness first: every engine must return the same answer
2) then latency: 1 cold run + N warm runs, report min/median
Writes data/benchmarks/results.csv and report.md.  Report YOUR numbers, never copy claims from blogs."""
from __future__ import annotations

import argparse
import statistics
import time
from decimal import Decimal

import pandas as pd

from common.config import ROOT
from common.spark import get_spark

QUERIES = {
    "Q1 full count": "SELECT count(*) FROM {t}.fact_encounter",
    "Q2 one-month filter": "SELECT count(*) FROM {t}.fact_encounter WHERE contact_date BETWEEN DATE '2025-06-01' AND DATE '2025-06-30'",
    "Q3 join fact+dim by state": "SELECT d.state, count(*) AS c FROM {t}.fact_encounter f JOIN {t}.dim_patient d ON f.pat_key = d.pat_key GROUP BY d.state ORDER BY d.state",
    "Q4 avg BP by visit type": "SELECT enc_type, round(avg(bp_systolic), 1) AS avg_sys, count(*) AS c FROM {t}.fact_encounter WHERE bp_systolic IS NOT NULL GROUP BY enc_type ORDER BY enc_type",
}


def norm(rows):
    return [tuple(round(float(x), 1) if isinstance(x, (float, Decimal)) else x for x in r) for r in rows]


def build_engines():
    engines = {}
    spark = get_spark("bench")
    engines["spark"] = lambda q: [tuple(r) for r in spark.sql(q.format(t="lake.gold")).collect()]
    try:
        from query.trino_queries import cursor

        cur = cursor()

        def trino_run(q):
            cur.execute(q.format(t="iceberg.gold"))
            return cur.fetchall()

        trino_run(QUERIES["Q1 full count"])
        engines["trino"] = trino_run
    except Exception as e:  # noqa: BLE001
        print("! Trino skipped:", str(e).splitlines()[0])
    try:
        from query.duckdb_queries import connect

        con = connect()
        engines["duckdb"] = lambda q: con.sql(q.format(t="lake.gold")).fetchall()
    except Exception as e:  # noqa: BLE001
        print("! DuckDB skipped:", str(e).splitlines()[0])
    return engines, spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    args = ap.parse_args()
    engines, spark = build_engines()
    rows, ok = [], True

    print("== correctness ==")
    for qname, sql in QUERIES.items():
        answers = {e: norm(fn(sql)) for e, fn in engines.items()}
        same = all(a == next(iter(answers.values())) for a in answers.values())
        ok &= same
        print(f"  {qname:<28} {'SAME ANSWER' if same else 'MISMATCH!'} across {list(engines)}")
        if not same:
            for e, a in answers.items():
                print("     ", e, a[:4])

    print("\n== latency (seconds) ==")
    for qname, sql in QUERIES.items():
        for e, fn in engines.items():
            times = []
            for _ in range(args.runs + 1):
                t0 = time.perf_counter()
                fn(sql)
                times.append(time.perf_counter() - t0)
            cold, warm = times[0], times[1:]
            rows.append({"query": qname, "engine": e, "cold_s": round(cold, 3), "median_s": round(statistics.median(warm), 3), "min_s": round(min(warm), 3)})
            print(f"  {qname:<28} {e:<7} cold {cold:6.2f}  median {statistics.median(warm):6.2f}  min {min(warm):6.2f}")

    df = pd.DataFrame(rows)
    out = ROOT / "data" / "benchmarks"
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "results.csv", index=False)

    prune = ""
    try:
        total = spark.sql("SELECT count(*) AS n FROM lake.gold.fact_encounter.data_files").first().n
        month_no = (2025 - 1970) * 12 + (6 - 1)  # Iceberg month transform = months since 1970-01
        hit = spark.sql(f"SELECT count(*) AS n FROM lake.gold.fact_encounter.data_files WHERE partition.contact_date_month = {month_no}").first().n
        prune = f"\nPartition pruning for Q2: the filter touches {hit} of {total} data files.\n"
        print(prune)
    except Exception as e:  # noqa: BLE001
        print("(pruning stat skipped:", str(e).splitlines()[0], ")")

    lines = ["# Benchmark report", "", f"Correctness: {'all engines agree' if ok else 'MISMATCH - investigate before trusting timings'}", "",
             "| query | engine | cold (s) | median (s) | min (s) |", "|---|---|---|---|---|"]
    lines += [f"| {r['query']} | {r['engine']} | {r['cold_s']} | {r['median_s']} | {r['min_s']} |" for r in rows]
    lines += [prune, "Setup: single laptop, Docker, local Garage S3. Compare engines only relative to each other on THIS dataset."]
    (out / "report.md").write_text("\n".join(lines) + "\n")
    print(f"wrote {out / 'results.csv'} and report.md")


if __name__ == "__main__":
    main()
