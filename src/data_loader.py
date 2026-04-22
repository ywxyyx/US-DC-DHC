"""
data_loader.py
--------------
Load and merge all per-state data center CSVs from data/raw_dc_states/.

Column normalisation handles the common header variations produced by different
scraper versions (e.g. 'lat' vs 'Latitude', 'it_load_mw' vs 'IT Load (MW)').
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical column names  →  accepted aliases  (all lower-case for matching)
# ---------------------------------------------------------------------------
COLUMN_ALIASES: dict[str, list[str]] = {
    "name":                  ["name", "dc_name", "datacenter_name", "facility"],
    "state":                 ["state", "state_slug", "state_name"],
    "latitude":              ["latitude", "lat", "y", "lat_deg"],
    "longitude":             ["longitude", "lon", "long", "lng", "x", "lon_deg"],
    "it_load_mw":            ["it load (mw)", "it_load_mw", "it_load", "mw", "capacity_mw"],
    "pue":                   ["pue", "power_usage_effectiveness"],
    "website":               ["website", "url", "web", "homepage"],
    "source_url":            ["source url", "source_url", "scrape_url", "page_url"],
    # Produced by src/classifier.py — present in processed_dc_states/*.csv
    "space_type":            ["space type", "space_type"],
    "classification_source": ["classification_source", "classification source"],
    "is_aggregated":         ["is_aggregated", "is aggregated"],
    "confidence_score":      ["confidence_score", "confidence score"],
}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename df columns to canonical names using case-insensitive alias lookup."""
    lower_map = {alias: canon
                 for canon, aliases in COLUMN_ALIASES.items()
                 for alias in aliases}

    rename = {}
    for col in df.columns:
        target = lower_map.get(col.lower().strip())
        if target and target not in rename.values():
            rename[col] = target

    df = df.rename(columns=rename)

    # Ensure all canonical columns exist (fill missing ones with NaN)
    for canon in COLUMN_ALIASES:
        if canon not in df.columns:
            df[canon] = pd.NA

    return df


def load_dc_data(raw_dir: Path) -> pd.DataFrame:
    """
    Read every CSV in *raw_dir*, normalise column headers, and concatenate into
    a single DataFrame.

    Parameters
    ----------
    raw_dir : Path
        Directory containing per-state CSV files.

    Returns
    -------
    pd.DataFrame
        Combined DC records with canonical column names.
    """
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {raw_dir}")

    frames: list[pd.DataFrame] = []
    total_rows = 0

    for path in csv_files:
        try:
            df = pd.read_csv(path, dtype=str)          # read everything as str first
            df = _normalise_columns(df)
            df["_source_file"] = path.stem             # track origin state
            frames.append(df)
            total_rows += len(df)
            logger.debug("Loaded %s  (%d rows)", path.name, len(df))
        except Exception as exc:
            logger.warning("Could not read %s: %s", path.name, exc)

    combined = pd.concat(frames, ignore_index=True)

    # Convert numeric columns
    for col in ("latitude", "longitude", "it_load_mw", "pue"):
        combined[col] = pd.to_numeric(combined[col], errors="coerce")

    if "confidence_score" in combined.columns:
        combined["confidence_score"] = pd.to_numeric(
            combined["confidence_score"], errors="coerce"
        )
    if "is_aggregated" in combined.columns:
        combined["is_aggregated"] = (
            combined["is_aggregated"].astype(str).str.lower()
                    .isin(["true", "1", "yes"])
        )

    # Drop rows without coordinates (can't geocode)
    before = len(combined)
    combined = combined.dropna(subset=["latitude", "longitude"])
    dropped = before - len(combined)
    if dropped:
        logger.warning("Dropped %d rows missing lat/lon", dropped)

    logger.info(
        "Loaded %d DC records from %d state files (%d rows missing coords dropped)",
        len(combined), len(csv_files), dropped,
    )
    return combined.reset_index(drop=True)
