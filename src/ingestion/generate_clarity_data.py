#!/usr/bin/env python
"""Synthetic Epic-Clarity-style extract generator. Everything is random - no real patient data.

Tables (Clarity-style names/columns):  PATIENT, PAT_ENC, PAT_ENC_DX, ORDER_PROC, ORDER_RESULTS
Output : data/raw/<TABLE>/batch=NN/part-0.parquet
         batch 00 = initial full load (visits before 2025-10-01)
         batch 01..03 = monthly incrementals (new rows + a few corrected/updated rows, like a real CDC feed)

Usage  : ./run.sh gen --patients 20000        (default)
         ./run.sh gen --patients 100000 --clean   (millions of rows -> better benchmarks)
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

FIRST = ["Aarav", "Maria", "James", "Priya", "Wei", "Fatima", "John", "Sofia", "Ahmed", "Emily", "Liam", "Olivia", "Noah", "Mei", "Carlos", "Anna"]
LAST = ["Smith", "Khan", "Garcia", "Chen", "Patel", "Johnson", "Lee", "Brown", "Nguyen", "Davis", "Miller", "Wilson", "Ali", "Lopez", "Kim", "Singh"]
STATES = np.array(["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI"])
ENC_TYPES = np.array(["Office Visit", "Telehealth", "Emergency", "Inpatient"])
ENC_P = [0.62, 0.18, 0.15, 0.05]

COMMON = {
    "J06.9": ("Acute upper respiratory infection", 0.16),
    "M54.50": ("Low back pain", 0.10),
    "E78.5": ("Hyperlipidemia", 0.10),
    "J45.909": ("Asthma", 0.07),
    "F41.9": ("Anxiety disorder", 0.08),
    "K21.9": ("GERD", 0.08),
    "Z00.00": ("General adult medical exam", 0.20),
    "N39.0": ("Urinary tract infection", 0.05),
    "E66.9": ("Obesity", 0.06),
    "R51.9": ("Headache", 0.10),
}
DM = {
    "E11.9": ("Type 2 diabetes without complications", 0.60),
    "E11.65": ("Type 2 diabetes with hyperglycemia", 0.25),
    "E11.22": ("Type 2 diabetes with diabetic CKD", 0.15),
}
HTN = {"I10": ("Essential hypertension", 1.0)}
DX_NAMES = {k: v[0] for d in (COMMON, DM, HTN) for k, v in d.items()}
DX_IDS = {code: 1000 + i for i, code in enumerate(DX_NAMES)}

PROC_NAMES = {
    "99213": "Office visit, established patient",
    "99285": "Emergency department visit, high severity",
    "99223": "Initial hospital care, high complexity",
    "83036": "Hemoglobin A1c",
    "80061": "Lipid panel",
    "82274": "FIT (fecal immunochemical test)",
    "45378": "Colonoscopy, diagnostic",
}

START = np.datetime64("2023-01-01")
END = np.datetime64("2025-12-31")
CUTS = np.array(["2025-10-01", "2025-11-01", "2025-12-01"], dtype="datetime64[ns]")
EXTRACT_TS = [pd.Timestamp(x) for x in ("2025-10-01", "2025-11-01", "2025-12-01", "2026-01-01")]

DATE_COLS = {
    "PATIENT": ["BIRTH_DATE"],
    "PAT_ENC": ["CONTACT_DATE"],
    "PAT_ENC_DX": ["CONTACT_DATE"],
    "ORDER_PROC": ["ORDERING_DATE"],
    "ORDER_RESULTS": ["RESULT_DATE"],
}


def batch_of(dates: np.ndarray) -> np.ndarray:
    return np.searchsorted(CUTS, dates.astype("datetime64[ns]"), side="right")


def write(df: pd.DataFrame, table: str, batch: int, out: Path) -> None:
    d = out / table / f"batch={batch:02d}"
    d.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    for c in DATE_COLS.get(table, []):
        df[c] = pd.to_datetime(df[c]).dt.date
    # Spark 3.5 cannot read nanosecond timestamps -> force microseconds
    df.to_parquet(d / "part-0.parquet", index=False, coerce_timestamps="us", allow_truncated_timestamps=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--patients", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--clean", action="store_true", help="delete existing data/raw first")
    a = ap.parse_args()
    out = Path(a.out)
    if a.clean and out.exists():
        shutil.rmtree(out)
    rng = np.random.default_rng(a.seed)
    n = a.patients

    # ------------------------------------------------------------------ patients
    age = np.clip(rng.normal(48, 22, n), 2, 95).astype(int)
    birth = END - (age * 365.25 + rng.integers(0, 365, n)).astype("timedelta64[D]")
    pat_ids = np.array([f"Z{i:07d}" for i in range(1, n + 1)])
    p_dm = 0.02 + 0.13 * (age >= 40) + 0.05 * (age >= 60)
    p_htn = 0.04 + 0.22 * (age >= 40) + 0.20 * (age >= 60)
    has_dm = rng.random(n) < p_dm
    has_htn = rng.random(n) < p_htn
    base_a1c = np.where(has_dm, rng.normal(7.7, 1.4, n), 5.4)
    base_sys = np.where(has_htn, rng.normal(138, 11, n), rng.normal(117, 9, n))

    # ------------------------------------------------------------------ encounters
    lam = (3 + 2.5 * has_dm + 2.5 * has_htn + age / 40) * 3.0
    n_enc = rng.poisson(lam)
    m = int(n_enc.sum())
    pidx = np.repeat(np.arange(n), n_enc)
    ndays = int((END - START) / np.timedelta64(1, "D"))
    cd = (START + rng.integers(0, ndays + 1, m).astype("timedelta64[D]")).astype("datetime64[ns]")
    order = np.lexsort((cd, pidx))
    cd, pidx = cd[order], pidx[order]
    csn = 900_000_000 + np.arange(m, dtype="int64")
    upd = cd + np.timedelta64(1, "D")

    status = rng.choice(["Completed", "Canceled", "No Show"], m, p=[0.93, 0.04, 0.03])
    typ = rng.choice(ENC_TYPES, m, p=ENC_P)
    comp = status == "Completed"
    rec_bp = comp & (rng.random(m) < np.where(typ == "Telehealth", 0.15, 0.92))
    sys_bp = np.clip(np.round(base_sys[pidx] + rng.normal(0, 9, m)), 65, 250)
    dia_bp = np.clip(np.round(0.62 * sys_bp + rng.normal(4, 5, m)), 35, 150)
    bp_s = pd.Series(np.where(rec_bp, sys_bp, np.nan)).astype("Int32")
    bp_d = pd.Series(np.where(rec_bp, dia_bp, np.nan)).astype("Int32")

    inp = (typ == "Inpatient") & comp
    adm = np.full(m, np.datetime64("NaT"), dtype="datetime64[ns]")
    dis = np.full(m, np.datetime64("NaT"), dtype="datetime64[ns]")
    adm[inp] = cd[inp] + rng.integers(8, 20, inp.sum()).astype("timedelta64[h]")
    dis[inp] = adm[inp] + rng.integers(1, 8, inp.sum()).astype("timedelta64[D]")

    enc = pd.DataFrame(
        {
            "PAT_ENC_CSN_ID": csn,
            "PAT_ID": pat_ids[pidx],
            "CONTACT_DATE": cd,
            "ENC_TYPE": typ,
            "DEPARTMENT_ID": rng.integers(1000, 1030, m).astype("int64"),
            "VISIT_PROV_ID": np.char.add("P", rng.integers(100, 400, m).astype(str)),
            "APPT_STATUS": status,
            "BP_SYSTOLIC": bp_s,
            "BP_DIASTOLIC": bp_d,
            "HOSP_ADMSN_TIME": adm,
            "HOSP_DISCH_TIME": dis,
            "UPDATE_DATE": upd,
        }
    )
    enc_b = batch_of(cd)

    # ------------------------------------------------------------------ diagnoses
    dm_e, htn_e = has_dm[pidx], has_htn[pidx]
    dm_codes, dm_p = list(DM), np.array([v[1] for v in DM.values()], dtype=np.float64)
    dm_p = dm_p / dm_p.sum()
    common_p = np.array([v[1] for v in COMMON.values()], dtype=np.float64)
    common_p = common_p / common_p.sum()
    prim = rng.choice(list(COMMON), m, p=common_p)
    u1, u2 = rng.random(m), rng.random(m)
    dm_hit = dm_e & (u1 < 0.40)
    prim = np.where(dm_hit, rng.choice(dm_codes, m, p=dm_p), prim)
    prim = np.where(htn_e & ~dm_hit & (u2 < 0.45), "I10", prim)
    sec_dm = dm_e & ~np.isin(prim, dm_codes) & (rng.random(m) < 0.55)
    sec_htn = htn_e & (prim != "I10") & (rng.random(m) < 0.55)
    dm_sec_code = rng.choice(dm_codes, m, p=dm_p)
    frames = []

    def add_dx(mask: np.ndarray, codes: np.ndarray, line: int) -> None:
        idx = np.flatnonzero(mask)
        frames.append(
            pd.DataFrame(
                {
                    "PAT_ENC_CSN_ID": csn[idx],
                    "LINE": np.full(len(idx), line, dtype="int32"),
                    "PAT_ID": pat_ids[pidx[idx]],
                    "ICD10_CODE": codes[idx],
                    "CONTACT_DATE": cd[idx],
                    "UPDATE_DATE": upd[idx],
                }
            )
        )

    add_dx(comp, prim, 1)
    add_dx(comp & sec_dm, dm_sec_code, 2)
    add_dx(comp & sec_htn, np.full(m, "I10"), 3)
    dx = pd.concat(frames, ignore_index=True)
    dx["DX_ID"] = dx["ICD10_CODE"].map(DX_IDS).astype("int64")
    dx["DX_NAME"] = dx["ICD10_CODE"].map(DX_NAMES)
    dx = dx[["PAT_ENC_CSN_ID", "LINE", "PAT_ID", "DX_ID", "ICD10_CODE", "DX_NAME", "CONTACT_DATE", "UPDATE_DATE"]]
    dx_b = batch_of(dx["CONTACT_DATE"].to_numpy())

    # ------------------------------------------------------------------ orders / procedures
    office = comp & np.isin(typ, ["Office Visit", "Telehealth"])
    col_elig = (age[pidx] >= 45) & (age[pidx] <= 75)
    em_code = np.select([typ == "Emergency", typ == "Inpatient"], ["99285", "99223"], default="99213")
    pieces = [
        (comp, em_code),
        (office & has_dm[pidx] & (rng.random(m) < 0.28), np.full(m, "83036")),
        (office & (has_dm[pidx] | has_htn[pidx]) & (rng.random(m) < 0.10), np.full(m, "80061")),
        (office & col_elig & (rng.random(m) < 0.015), np.full(m, "82274")),
        (office & col_elig & (rng.random(m) < 0.004), np.full(m, "45378")),
    ]
    pf = []
    for mask, codes in pieces:
        idx = np.flatnonzero(mask)
        pf.append(
            pd.DataFrame(
                {
                    "PAT_ENC_CSN_ID": csn[idx],
                    "PAT_ID": pat_ids[pidx[idx]],
                    "PROC_CODE": codes[idx],
                    "ORDERING_DATE": cd[idx],
                    "UPDATE_DATE": upd[idx],
                    "_pidx": pidx[idx],
                }
            )
        )
    proc = pd.concat(pf, ignore_index=True).sort_values(["PAT_ENC_CSN_ID", "PROC_CODE"], kind="stable").reset_index(drop=True)
    proc["ORDER_PROC_ID"] = 500_000_000 + np.arange(len(proc), dtype="int64")
    proc["PROC_NAME"] = proc["PROC_CODE"].map(PROC_NAMES)
    proc["ORDER_STATUS"] = "Completed"
    proc["_b"] = batch_of(proc["ORDERING_DATE"].to_numpy())

    a1c = proc[proc["PROC_CODE"] == "83036"]
    res = pd.DataFrame(
        {
            "ORDER_PROC_ID": a1c["ORDER_PROC_ID"].to_numpy(),
            "LINE": np.ones(len(a1c), dtype="int32"),
            "COMPONENT_NAME": "HEMOGLOBIN A1C",
            "ORD_NUM_VALUE": np.clip(np.round(base_a1c[a1c["_pidx"].to_numpy()] + rng.normal(0, 0.5, len(a1c)), 1), 4.5, 14.0),
            "REFERENCE_UNIT": "%",
            "RESULT_DATE": a1c["ORDERING_DATE"].to_numpy() + rng.integers(1, 3, len(a1c)).astype("timedelta64[D]"),
            "UPDATE_DATE": a1c["UPDATE_DATE"].to_numpy() + np.timedelta64(1, "D"),
            "_b": a1c["_b"].to_numpy(),
        }
    )
    proc_cols = ["ORDER_PROC_ID", "PAT_ENC_CSN_ID", "PAT_ID", "PROC_CODE", "PROC_NAME", "ORDERING_DATE", "ORDER_STATUS", "UPDATE_DATE"]

    # ------------------------------------------------------------------ patient rows (appear with first visit)
    fb = pd.Series(enc_b).groupby(pidx).min()
    first_batch = np.zeros(n, dtype=int)
    first_batch[fb.index.to_numpy()] = fb.to_numpy()
    fd = pd.Series(cd).groupby(pidx).min()
    first_date = np.full(n, START.astype("datetime64[ns]"), dtype="datetime64[ns]")
    first_date[fd.index.to_numpy()] = fd.to_numpy()
    pat = pd.DataFrame(
        {
            "PAT_ID": pat_ids,
            "PAT_MRN_ID": rng.integers(10_000_000, 99_999_999, n).astype(str),
            "PAT_NAME": np.char.add(np.char.add(rng.choice(LAST, n), ", "), rng.choice(FIRST, n)),
            "BIRTH_DATE": birth.astype("datetime64[ns]"),
            "SEX": rng.choice(["F", "M"], n),
            "STATE": rng.choice(STATES, n),
            "ZIP": rng.integers(10000, 99999, n).astype(str),
            "UPDATE_DATE": first_date + np.timedelta64(1, "D"),
        }
    )

    # ------------------------------------------------------------------ write batches
    print(f"patients={n:,} encounters={m:,} diagnoses={len(dx):,} orders={len(proc):,} results={len(res):,}")
    for b in range(4):
        ts = EXTRACT_TS[b]
        p_b = pat[first_batch == b]
        e_b = enc[enc_b == b]
        if b > 0:  # CDC-style updates to rows loaded in earlier batches
            cand = np.flatnonzero(first_batch < b)
            pick = rng.choice(cand, size=max(1, int(0.015 * len(cand))), replace=False)
            pu = pat.iloc[pick].copy()
            pu["STATE"] = rng.choice(STATES, len(pu))  # patient moved
            pu["UPDATE_DATE"] = ts
            p_b = pd.concat([p_b, pu], ignore_index=True)

            cand = np.flatnonzero((enc_b < b) & rec_bp)
            pick = rng.choice(cand, size=max(1, int(0.01 * len(cand))), replace=False)
            fix = enc.iloc[pick].copy()  # BP re-entered / corrected
            fix["BP_SYSTOLIC"] = (fix["BP_SYSTOLIC"] - rng.integers(4, 15, len(fix))).astype("Int32")
            fix["BP_DIASTOLIC"] = (fix["BP_DIASTOLIC"] - rng.integers(2, 8, len(fix))).astype("Int32")
            fix["UPDATE_DATE"] = ts
            e_b = pd.concat([e_b, fix], ignore_index=True)

        write(p_b, "PATIENT", b, out)
        write(e_b, "PAT_ENC", b, out)
        write(dx[dx_b == b], "PAT_ENC_DX", b, out)
        write(proc.loc[proc["_b"] == b, proc_cols], "ORDER_PROC", b, out)
        write(res.loc[res["_b"] == b].drop(columns="_b"), "ORDER_RESULTS", b, out)
        print(f"  batch {b:02d}: patient={len(p_b):>7,} enc={len(e_b):>8,} dx={int((dx_b == b).sum()):>8,} "
              f"proc={int((proc['_b'] == b).sum()):>8,} results={int((res['_b'] == b).sum()):>7,}")
    print(f"\nWrote parquet under {out}")


if __name__ == "__main__":
    main()
