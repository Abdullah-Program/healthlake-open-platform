# IceLake Health: Enterprise Open Lakehouse for Clinical EHR Data

An enterprise-grade, zero-cost, local Open Lakehouse for clinical EHR data (modeled on Epic Systems Clarity) built on **Apache Iceberg**, **Apache Polaris (REST Catalog)**, **Garage (S3-compatible Object Storage)**, **PySpark 3.5**, **Trino 480**, and **DuckDB 1.5**. 

Runs 100% locally in Docker with zero cloud spend.

```
                    ┌────────────────────────────────────────────────────────┐
                    │            Synthetic Epic Clarity Generator            │
                    │      (20,000 Patients | ~1M Clinical EHR Records)      │
                    └───────────────────────────┬────────────────────────────┘
                                                │ Monthly CDC Parquet
                                                ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                   MEDALLION LAKEHOUSE                                     │
│                                                                                           │
│  ┌──────────────────────┐      ┌──────────────────────┐      ┌─────────────────────────┐  │
│  │     BRONZE LAYER     │      │     SILVER LAYER     │      │       GOLD LAYER        │  │
│  │ (Append-Only Ingest) │      │(ACID MERGE / Cleaned)│      │  (De-Identified Marts)  │  │
│  │                      │ ───► │                      │ ───► │                         │  │
│  │ * Raw Batches 00..03 │      │ * Primary-Key Dedupe │      │ * HIPAA Safe-Harbor     │  │
│  │ * Audit Metadata     │      │ * Hidden Partitions  │      │   Salted SHA-256 Hashes │  │
│  │ * Full Fidelity      │      │ * Merge-On-Read (v2) │      │ * HEDIS Quality Metrics │  │
│  └──────────────────────┘      └──────────────────────┘      └─────────────────────────┘  │
│             │                             │                               │               │
│             └─────────────────────────────┼───────────────────────────────┘               │
│                                           │ Open Parquet Data + Avro Manifests            │
│                                           ▼                                               │
│                         ┌───────────────────────────────────┐                             │
│                         │     Garage S3 Object Storage      │                             │
│                         │   (Single-Node, Bucket: health)   │                             │
│                         └─────────────────┬─────────────────┘                             │
└───────────────────────────────────────────┼───────────────────────────────────────────────┘
                                            │
                                            ▼
                         ┌─────────────────────────────────────┐
                         │    Apache Polaris (REST Catalog)    │
                         │    Multi-Engine Central Metadata    │
                         │  Catalog-Level & Namespace RBAC     │
                         └──────────────────┬──────────────────┘
                                            │
             ┌──────────────────────────────┼──────────────────────────────┐
             ▼                              ▼                              ▼
  ┌──────────────────────┐      ┌──────────────────────┐      ┌─────────────────────────┐
  │   Apache Spark 3.5   │      │      Trino 480       │      │      DuckDB 1.5         │
  │   Heavy ETL, MERGE,  │      │ MPP Interactive SQL  │      │ Embedded Fast Analytics │
  │   Maintenance & DQ   │      │ BI Queries & Reports │      │ In-Process Local Query  │
  └──────────────────────┘      └──────────────────────┘      └─────────────────────────┘
```

---

## Key Architectural Highlights

### 1. Medallion Storage Architecture (Apache Iceberg v2)
* **Bronze**: Raw ingest preservation. Append-only, partitioned by batch ID.
* **Silver**: Conformed clinical state. Partitioned with Iceberg hidden partitioning (`months(contact_date)`). Implements ACID `MERGE INTO` with merge-on-read (`write.merge.mode='merge-on-read'`) ensuring idempotent deduplication when incoming records are newer (`s.update_date >= t.update_date`).
* **Gold**: Star-schema reporting marts (`dim_patient`, `fact_encounter`) with HIPAA Safe Harbor de-identification (salted SHA-256 patient keys, clamped birth years) and clinical quality measures (HEDIS CBP, COL, HBD).

### 2. Zero-Rewrite Schema & Partition Evolution
* Columns are identified by unique **Field IDs** rather than ordinal positions or names. Columns can be renamed (`visit_prov_id` $\rightarrow$ `rendering_prov_id`), widened (`INT` $\rightarrow$ `BIGINT`), or added (`telehealth_flag`) with **0 data files rewritten**.
* Partition schemes can evolve dynamically (e.g. adding `bucket(8, pat_id)`). Old files retain `spec_id = 0`, and new writes utilize `spec_id = 1` within the same table.

### 3. Automated Table Maintenance & Compaction
* **Binpack Compaction**: Coalesces small Parquet files produced by incremental CDC batches into uniform chunks via `CALL lake.system.rewrite_data_files()`.
* **Delete-File Consolidation**: Merges positional delete files created by row-level updates into new base Parquet files.
* **Snapshot Expiration & Tagging**: Retains audit tags (`after_initial_load`) for regulatory compliance while pruning intermediate snapshots to reclaim storage.

### 4. Enterprise Security & Multi-Engine RBAC (Polaris)
* Implements least-privilege role-based access control via Polaris REST API.
* **`engineer` principal**: Granted `CATALOG_MANAGE_CONTENT` across Bronze, Silver, and Gold.
* **`analyst` principal**: Restricted strictly to namespace `gold`. Attempts to query Bronze or Silver (containing clinical PHI like real names, birth dates, and ZIP codes) are rejected at the catalog level with **HTTP 403 Forbidden**.

---

## Production Performance Benchmarks

Conducted across all three query engines on identical Iceberg tables stored in Garage S3:

### 1. Engine Latency Comparison

| Query Description | Spark 3.5 Median (s) | Trino 480 Median (s) | DuckDB 1.5 Median (s) | Winner |
| :--- | :---: | :---: | :---: | :---: |
| **Q1: Full Fact Table Scan** (`count(*)`) | 0.078s | 0.081s | **0.018s** | **DuckDB** |
| **Q2: 1-Month Range Filter** (Date Pruning) | 0.083s | 0.066s | **0.018s** | **DuckDB** |
| **Q3: Multi-Table Join** (`fact_encounter` $\bowtie$ `dim_patient`) | 0.521s | 0.404s | **0.046s** | **DuckDB** |
| **Q4: Group-By Aggregation** (Vitals by Visit Type) | 0.259s | 0.114s | **0.021s** | **DuckDB** |

*All engines returned 100% identical analytical results across all queries.*

### 2. Iceberg Partition Pruning Efficiency
* **Total Fact Data Files**: 36
* **Files Scanned for Q2 Filter (`contact_date BETWEEN '2025-06-01' AND '2025-06-30'`)**: **1 file**
* **I/O Reduction**: **97.2% of data skipped** via Iceberg hidden metadata partition pruning without the query author having to explicitly reference partition columns.

### 3. Compaction Impact (Silver Encounters)

| Metric | Pre-Maintenance | Post-Maintenance | Impact |
| :--- | :---: | :---: | :---: |
| **Data Files** | 139 files | **36 files** | **74.1% reduction** in small files |
| **Positional Delete Files** | 77 files | **0 files** | 100% deletes merged |
| **Patient Demographics Files** | 4 files | **1 file** | Consolidated |
| **Metadata Snapshots** | 5 snapshots | **3 snapshots** | Pruned expired state |

---

## Clinical Quality Metrics (HEDIS Measurement Year 2025)

Tracking patient population health across quarterly CDC updates:

| Measure Code | Clinical Quality Description | Batch 0 Baseline | Batch 3 (Q3 End) | Direction |
| :--- | :--- | :---: | :---: | :---: |
| **HBD_LT8** | Comprehensive Diabetes Care: HbA1c Good Control (< 8.0%) | 38.88% | **44.08%** | $\uparrow$ (+5.20%) |
| **HBD_GT9** | Comprehensive Diabetes Care: HbA1c Poor Control (> 9.0%) | 43.83% | **36.80%** | $\downarrow$ (-7.03%) |
| **CBP** | Controlling High Blood Pressure (< 140/90 mm Hg) | 43.85% | **44.60%** | $\uparrow$ (+0.75%) |
| **COL** | Colorectal Cancer Screening (Ages 45–75) | 9.00% | **10.84%** | $\uparrow$ (+1.84%) |

---

## Quickstart Guide

### Prerequisites
* Docker Desktop (Windows, macOS, or Linux)
* PowerShell (Windows) or Bash (macOS/Linux)

### Execution (PowerShell Task Runner)

```powershell
# 1. Initialize environment & boot containers
.\run.ps1 init
.\run.ps1 up
.\run.ps1 smoke

# 2. Bootstrap Polaris REST catalog & Trino
.\run.ps1 catalog
.\run.ps1 trino
.\run.ps1 smoke --spark

# 3. Generate 20,000 patients and ~1M clinical records
.\run.ps1 gen --patients 20000

# 4. Ingest baseline Batch 00 through Medallion pipeline
.\run.ps1 pipeline 0

# 5. Process quarterly CDC deltas (Batches 01, 02, 03)
.\run.ps1 pipeline 1
.\run.ps1 pipeline 2
.\run.ps1 pipeline 3

# 6. Run Time Travel & Snapshot Audit
.\run.ps1 timetravel

# 7. Run Schema & Partition Evolution Demo
.\run.ps1 evolve

# 8. Run Automated Compaction & Table Maintenance
.\run.ps1 maintain

# 9. Multi-Engine Querying & Benchmarks
.\run.ps1 trino-sql
.\run.ps1 duckdb
.\run.ps1 bench

# 10. Verify Least-Privilege RBAC
.\run.ps1 rbac
.\run.ps1 trino-sql --rbac
```

---

## Production Engineering Log (Interview Highlights)

1. **PySpark Worker Fork Memory Crash (`OSError: [Errno 12]`):**
   * *Problem:* Calling `spark.createDataFrame(rows)` on driver-collected audit tuples caused PySpark to fork 16–20 concurrent Python worker subprocesses inside Docker, exhausting virtual memory.
   * *Resolution:* Replaced worker serialization with pure JVM Spark SQL literals (`INSERT INTO lake.gold.dq_results VALUES (...)`), eliminating all Python worker forks.
2. **Polaris REST Catalog HTTP 400 Namespace Rejection:**
   * *Problem:* Polaris rejected S3 namespace creation under `s3://healthlake/warehouse/bronze/` because storage config allowed only `s3://healthlake/`.
   * *Resolution:* Updated catalog setup payload with `allowedLocations: ["s3://healthlake/warehouse/", "s3://healthlake/"]` and explicit trailing slashes.
3. **Outlier Vitals Caught by DQ Gate & Fixed via Iceberg ACID Update:**
   * *Problem:* A 4.5-sigma standard normal draw in synthetic encounter generation produced a systolic blood pressure of 58 (< 60), tripping the Data Quality contract.
   * *Resolution:* Utilized Apache Iceberg v2 merge-on-read row-level SQL update (`UPDATE lake.silver.pat_enc SET bp_systolic = 70 WHERE pat_enc_csn_id = 900038706`), updating the record in-place via positional delete files without rewriting unchanged partitions.
4. **Trino `DROP_WITH_PURGE` Catalog Rejection:**
   * *Problem:* Trino's Iceberg connector attempts to purge data files on `DROP TABLE`, which Polaris disabled by default (returning HTTP 403).
   * *Resolution:* Configured `"polaris.config.drop-with-purge.enabled": "true"` on the Polaris catalog entity, enabling seamless cross-engine table drops.

