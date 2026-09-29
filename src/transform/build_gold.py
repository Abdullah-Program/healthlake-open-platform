#!/usr/bin/env python
"""Gold = business-ready + de-identified. Analysts only ever see this layer.

  dim_patient          hashed patient key, sex, state, birth_year  (no name / DOB / ZIP / MRN)
  fact_encounter       hashed keys + visit facts, hidden partition months(contact_date)
  hedis_patient_flags  patient-level numerator/denominator per measure (HEDIS-STYLE, simplified)
  hedis_rates          measure rates overall + by state

Measures (simplified for learning - NOT NCQA-certified, value sets are tiny):
  HBD_LT8  diabetics 18-75 whose latest HbA1c in the year is < 8.0       (higher is better)
  HBD_GT9  diabetics 18-75 with latest HbA1c > 9.0 or no test           (lower is better)
  CBP      hypertensives 18-85 whose latest BP in the year is < 140/90   (higher is better)
  COL      adults 45-75 with colonoscopy in last 10y (data window is only 3y!) or FIT this year
Usage: ./run.sh gold [--my 2025]"""
from __future__ import annotations

import argparse

from common.config import MEASUREMENT_YEAR, SALT
from common.hedis_value_sets import A1C_COMPONENT, COLONOSCOPY_CPT, DIABETES_DX_PREFIX, FIT_CPT, HYPERTENSION_DX, sql_in
from common.spark import get_spark


def flags_sql(my: int) -> str:
    prev = my - 1
    return f"""
WITH pat AS (
  SELECT pat_id, ({my} - year(birth_date)) AS age FROM lake.silver.patient
),
dm_den AS (
  SELECT d.pat_id
  FROM lake.silver.pat_enc_dx d
  JOIN lake.silver.pat_enc e ON e.pat_enc_csn_id = d.pat_enc_csn_id AND e.appt_status = 'Completed'
  WHERE d.icd10_code LIKE '{DIABETES_DX_PREFIX}%'
    AND d.contact_date BETWEEN DATE '{prev}-01-01' AND DATE '{my}-12-31'
  GROUP BY d.pat_id
  HAVING count(DISTINCT d.contact_date) >= 2
),
a1c AS (
  SELECT pat_id, ord_num_value FROM (
    SELECT p.pat_id, r.ord_num_value,
           row_number() OVER (PARTITION BY p.pat_id ORDER BY r.result_date DESC, p.order_proc_id DESC) AS rn
    FROM lake.silver.order_results r
    JOIN lake.silver.order_proc p ON p.order_proc_id = r.order_proc_id
    WHERE r.component_name = '{A1C_COMPONENT}'
      AND r.result_date BETWEEN DATE '{my}-01-01' AND DATE '{my}-12-31'
  ) t WHERE rn = 1
),
hbd AS (
  SELECT dm_den.pat_id, a1c.ord_num_value AS v
  FROM dm_den JOIN pat ON pat.pat_id = dm_den.pat_id
  LEFT JOIN a1c ON a1c.pat_id = dm_den.pat_id
  WHERE pat.age BETWEEN 18 AND 75
),
htn_den AS (
  SELECT d.pat_id
  FROM lake.silver.pat_enc_dx d
  JOIN lake.silver.pat_enc e ON e.pat_enc_csn_id = d.pat_enc_csn_id AND e.appt_status = 'Completed'
  WHERE d.icd10_code IN ({sql_in(HYPERTENSION_DX)})
    AND d.contact_date BETWEEN DATE '{prev}-01-01' AND DATE '{my}-06-30'
  GROUP BY d.pat_id
  HAVING count(DISTINCT d.contact_date) >= 2
),
last_bp AS (
  SELECT pat_id, bp_systolic, bp_diastolic FROM (
    SELECT pat_id, bp_systolic, bp_diastolic,
           row_number() OVER (PARTITION BY pat_id ORDER BY contact_date DESC, pat_enc_csn_id DESC) AS rn
    FROM lake.silver.pat_enc
    WHERE appt_status = 'Completed' AND bp_systolic IS NOT NULL
      AND contact_date BETWEEN DATE '{my}-01-01' AND DATE '{my}-12-31'
  ) t WHERE rn = 1
),
cbp AS (
  SELECT htn_den.pat_id, b.bp_systolic, b.bp_diastolic
  FROM htn_den JOIN pat ON pat.pat_id = htn_den.pat_id
  LEFT JOIN last_bp b ON b.pat_id = htn_den.pat_id
  WHERE pat.age BETWEEN 18 AND 85
),
col_scr AS (
  SELECT DISTINCT pat_id FROM lake.silver.order_proc
  WHERE (proc_code IN ({sql_in(COLONOSCOPY_CPT)}) AND ordering_date BETWEEN DATE '{my - 9}-01-01' AND DATE '{my}-12-31')
     OR (proc_code IN ({sql_in(FIT_CPT)}) AND ordering_date BETWEEN DATE '{my}-01-01' AND DATE '{my}-12-31')
)
SELECT pat_id, 'HBD_LT8' AS measure, coalesce(v < 8.0, false) AS numerator FROM hbd
UNION ALL
SELECT pat_id, 'HBD_GT9' AS measure, coalesce(v > 9.0, true) AS numerator FROM hbd
UNION ALL
SELECT pat_id, 'CBP' AS measure, coalesce(bp_systolic < 140 AND bp_diastolic < 90, false) AS numerator FROM cbp
UNION ALL
SELECT pat.pat_id, 'COL' AS measure, (s.pat_id IS NOT NULL) AS numerator
FROM pat LEFT JOIN col_scr s ON s.pat_id = pat.pat_id
WHERE pat.age BETWEEN 45 AND 75
"""


def build(spark, my: int) -> None:
    print("gold.dim_patient (de-identified) ...")
    spark.sql(
        f"""CREATE OR REPLACE TABLE lake.gold.dim_patient USING iceberg
            TBLPROPERTIES ('format-version'='2') AS
            SELECT sha2(concat(pat_id, '{SALT}'), 256) AS pat_key, sex, state, year(birth_date) AS birth_year
            FROM lake.silver.patient"""
    )
    print("gold.fact_encounter ...")
    spark.sql(
        f"""CREATE OR REPLACE TABLE lake.gold.fact_encounter USING iceberg
            PARTITIONED BY (months(contact_date))
            TBLPROPERTIES ('format-version'='2') AS
            SELECT sha2(concat(cast(pat_enc_csn_id AS string), '{SALT}'), 256) AS enc_key,
                   sha2(concat(pat_id, '{SALT}'), 256) AS pat_key,
                   contact_date, enc_type, appt_status, bp_systolic, bp_diastolic
            FROM lake.silver.pat_enc"""
    )

    print(f"gold.hedis_patient_flags (MY {my}) ...")
    spark.sql(flags_sql(my)).createOrReplaceTempView("_flags")
    spark.sql(
        """CREATE TABLE IF NOT EXISTS lake.gold.hedis_patient_flags (
             pat_key STRING, state STRING, measure STRING, measurement_year INT, numerator BOOLEAN
           ) USING iceberg PARTITIONED BY (measurement_year) TBLPROPERTIES ('format-version'='2')"""
    )
    spark.sql(f"DELETE FROM lake.gold.hedis_patient_flags WHERE measurement_year = {my}")
    spark.sql(
        f"""INSERT INTO lake.gold.hedis_patient_flags
            SELECT sha2(concat(f.pat_id, '{SALT}'), 256), p.state, f.measure, {my}, f.numerator
            FROM _flags f JOIN lake.silver.patient p ON p.pat_id = f.pat_id"""
    )

    print("gold.hedis_rates ...")
    spark.sql(
        """CREATE TABLE IF NOT EXISTS lake.gold.hedis_rates (
             measurement_year INT, measure STRING, state STRING, denominator BIGINT, numerator BIGINT,
             rate DOUBLE, computed_at TIMESTAMP
           ) USING iceberg TBLPROPERTIES ('format-version'='2')"""
    )
    spark.sql(f"DELETE FROM lake.gold.hedis_rates WHERE measurement_year = {my}")
    spark.sql(
        f"""INSERT INTO lake.gold.hedis_rates
            SELECT measurement_year, measure, coalesce(state, 'ALL') AS state,
                   count(*) AS denominator,
                   sum(CASE WHEN numerator THEN 1 ELSE 0 END) AS numerator,
                   round(sum(CASE WHEN numerator THEN 1 ELSE 0 END) / count(*), 4) AS rate,
                   current_timestamp() AS computed_at
            FROM lake.gold.hedis_patient_flags
            WHERE measurement_year = {my}
            GROUP BY GROUPING SETS ((measurement_year, measure, state), (measurement_year, measure))"""
    )
    print("\nOverall rates:")
    spark.sql(
        f"SELECT measure, denominator, numerator, rate FROM lake.gold.hedis_rates "
        f"WHERE measurement_year = {my} AND state = 'ALL' ORDER BY measure"
    ).show(truncate=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--my", type=int, default=MEASUREMENT_YEAR, help="measurement year")
    args = ap.parse_args()
    build(get_spark("gold"), args.my)
