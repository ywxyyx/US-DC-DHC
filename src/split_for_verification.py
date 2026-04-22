"""
split_for_verification.py
-------------------------
Split the processed DC dataset into four per-Space-Type CSVs for manual
IT Load verification.

Output: data/manual_verification/{small_dc,mid_colo_dc,hyperscale_dc,ai_specialized}.csv

Kept columns:
    Name, State, Latitude, Longitude, IT Load (MW), PUE, Website

Added columns:
    Manual_Verified_MW   — blank, for the user to fill in
    Source_Confidence    — 1-5 score derived from Classification_Source:
        5 = P1 (IT Load originally reported — used to classify)
        3 = P2 (imputed, but classification driven by operator-name match)
        2 = P3 (imputed, classification from website scrape)
        1 = P4 (purely imputed default)

Run:
    python -m src.split_for_verification
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed_dc_states"
OUTPUT_DIR = PROJECT_ROOT / "data" / "manual_verification"

KEEP_COLS = ["Name", "State", "Latitude", "Longitude",
             "IT Load (MW)", "PUE", "Website"]

SOURCE_CONFIDENCE = {"P1": 5, "P2": 3, "P3": 2, "P4": 1}

SPACE_TYPE_TO_FILE = {
    "Small":          "small_dc.csv",
    "Midsize/Colo":   "mid_colo_dc.csv",
    "Hyperscale":     "hyperscale_dc.csv",
    "AI Specialized": "ai_specialized.csv",
}


def _load_all_processed() -> pd.DataFrame:
    csv_files = sorted(PROCESSED_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSVs in {PROCESSED_DIR}")
    frames = [pd.read_csv(p, dtype=str) for p in csv_files]
    return pd.concat(frames, ignore_index=True)


def _clean_url(val) -> str:
    if not isinstance(val, str):
        return ""
    s = val.strip()
    return "" if s.lower() in {"nan", "none", ""} else s


def split_for_verification() -> dict[str, int]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = _load_all_processed()

    df["Source_Confidence"] = df["Classification_Source"].map(SOURCE_CONFIDENCE)
    df["Website"] = df["Website"].map(_clean_url)
    df["Manual_Verified_MW"] = ""

    out_cols = KEEP_COLS + ["Manual_Verified_MW", "Source_Confidence"]

    row_counts: dict[str, int] = {}
    for stype, fname in SPACE_TYPE_TO_FILE.items():
        subset = df.loc[df["Space Type"] == stype, out_cols].copy()
        subset = subset.sort_values(
            by=["Source_Confidence", "State", "Name"],
            ascending=[True, True, True],
        )
        out_path = OUTPUT_DIR / fname
        subset.to_csv(out_path, index=False)
        row_counts[fname] = len(subset)

    print("\n" + "=" * 56)
    print("  Manual Verification Split")
    print("=" * 56)
    print(f"  Output dir: {OUTPUT_DIR}")
    print("-" * 56)
    for fname, n in row_counts.items():
        print(f"  {fname:<26} {n:>6,} rows")
    print(f"  {'TOTAL':<26} {sum(row_counts.values()):>6,} rows")
    print("=" * 56 + "\n")

    return row_counts


if __name__ == "__main__":
    split_for_verification()
