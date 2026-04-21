"""
analyzer.py
-----------
Core analysis pipeline:
  1. Load & geocode DC data.
  2. Aggregate DC capacity to county level.
  3. Merge with NREL county heating/cooling demand.
  4. Compute waste-heat coverage ratios using physics_engine.
  5. Save output to processed/us_county_analysis.csv.

Designed to be importable (all logic in run_analysis()) so both app.py and
CLI usage share the same code path.
"""

import logging
from pathlib import Path

import pandas as pd

from src.data_loader import load_dc_data
from src.geocoding import add_fips
from src.physics_engine import (
    COOLING_COP,
    HEATING_COP,
    RECOVERY_EFFICIENCY,
    cooling_coverage_ratio,
    cooling_delivered_kwh,
    heat_delivered_kwh,
    heating_coverage_ratio,
    mbtu_to_kwh,
    waste_heat_kwh,
)

logger = logging.getLogger(__name__)

# Project root is one level above src/
_HERE = Path(__file__).parent
PROJECT_ROOT = _HERE.parent

# Default source is now the classifier output (processed_dc_states/), which has
# 100% IT Load / PUE coverage via LBNL-2024 archetype imputation.
DEFAULT_RAW_DIR     = PROJECT_ROOT / "data" / "processed_dc_states"
DEFAULT_NREL_CSV    = PROJECT_ROOT / "data" / "nrel" / "county_space_heating_cooling.csv"
DEFAULT_OUTPUT_CSV  = PROJECT_ROOT / "processed" / "us_county_analysis.csv"
DEFAULT_DC_PARQUET  = PROJECT_ROOT / "processed" / "us_dc_level.parquet"
DEFAULT_CACHE_DIR   = PROJECT_ROOT / "data" / "nrel"   # reuse for Census gazetteer

# Canonical Space Type ordering (matches classifier.ARCHETYPES)
SPACE_TYPES: tuple[str, ...] = ("Hyperscale", "AI Specialized", "Midsize/Colo", "Small")


def _slug(stype: str) -> str:
    """Column-safe slug for a Space Type, e.g. 'AI Specialized' → 'ai_specialized'."""
    return (stype.lower()
                 .replace(" ", "_")
                 .replace("/", "_")
                 .replace("-", "_"))


# ---------------------------------------------------------------------------
# Step 2 — aggregate DC capacity to county level
# ---------------------------------------------------------------------------
def _aggregate_to_county(
    dc_df: pd.DataFrame,
    recovery_efficiency: float,
    heating_cop: float,
    cooling_cop: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each county (FIPS) compute the heat/cooling aggregates plus a
    composition breakdown (DC count and IT-load share per Space Type).

    Returns
    -------
    (county_df, dc_df_enriched)
        `county_df` — one row per FIPS with totals + per-type composition columns.
        `dc_df_enriched` — per-DC rows with physics columns (saved as parquet so
                           the dashboard can slice by Space Type without re-computing).
    """
    df = dc_df.dropna(subset=["fips", "it_load_mw"]).copy()

    before = len(dc_df)
    after  = len(df)
    logger.info(
        "County aggregation: using %d / %d DCs (dropped %d missing FIPS or IT load)",
        after, before, before - after,
    )

    # Per-row physics
    df["recoverable_kwh"] = waste_heat_kwh(
        df["it_load_mw"], df["pue"],
        recovery_efficiency=recovery_efficiency,
    )
    df["heat_delivered_kwh"]    = heat_delivered_kwh(df["recoverable_kwh"], heating_cop)
    df["cooling_delivered_kwh"] = cooling_delivered_kwh(df["recoverable_kwh"], cooling_cop)

    county = (
        df.groupby("fips", as_index=False)
        .agg(
            dc_count            =("name",               "count"),
            total_it_load_mw    =("it_load_mw",         "sum"),
            total_recoverable_kwh=("recoverable_kwh",   "sum"),
            total_heat_delivered_kwh=("heat_delivered_kwh", "sum"),
            total_cooling_delivered_kwh=("cooling_delivered_kwh", "sum"),
        )
    )
    county["fips"] = county["fips"].astype(str).str.zfill(5)

    # ── Per-type composition columns ───────────────────────────────────────
    if "space_type" in df.columns:
        for stype in SPACE_TYPES:
            mask = df["space_type"] == stype
            per_type = (
                df[mask].groupby("fips")
                        .agg(
                            count=("name", "count"),
                            it_mw=("it_load_mw", "sum"),
                        )
                        .reset_index()
            )
            per_type["fips"] = per_type["fips"].astype(str).str.zfill(5)
            per_type = per_type.rename(columns={
                "count": f"dc_count_{_slug(stype)}",
                "it_mw": f"it_load_mw_{_slug(stype)}",
            })
            county = county.merge(per_type, on="fips", how="left")

        for stype in SPACE_TYPES:
            c_col = f"dc_count_{_slug(stype)}"
            m_col = f"it_load_mw_{_slug(stype)}"
            county[c_col] = county[c_col].fillna(0).astype(int)
            county[m_col] = county[m_col].fillna(0.0)

    return county, df


# ---------------------------------------------------------------------------
# Step 3 — merge with NREL demand data
# ---------------------------------------------------------------------------
def _load_nrel(nrel_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(nrel_csv, dtype={"fips": str})
    df["fips"] = df["fips"].str.zfill(5)

    # Convert MBtu demand columns to kWh for consistent units
    for end in ("heating", "cooling"):
        mbtu_col = f"total_space_{end}_mbtu"
        kwh_col  = f"total_space_{end}_kwh"
        if mbtu_col in df.columns and kwh_col not in df.columns:
            df[kwh_col] = mbtu_to_kwh(df[mbtu_col])

    # Keep only the columns we need
    keep = ["fips",
            "total_space_heating_kwh", "total_space_heating_mbtu",
            "total_space_cooling_kwh",  "total_space_cooling_mbtu"]
    return df[[c for c in keep if c in df.columns]]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def run_analysis(
    raw_dir:             Path  = DEFAULT_RAW_DIR,
    nrel_csv:            Path  = DEFAULT_NREL_CSV,
    output_csv:          Path  = DEFAULT_OUTPUT_CSV,
    dc_parquet:          Path  = DEFAULT_DC_PARQUET,
    cache_dir:           Path  = DEFAULT_CACHE_DIR,
    recovery_efficiency: float = RECOVERY_EFFICIENCY,
    heating_cop:         float = HEATING_COP,
    cooling_cop:         float = COOLING_COP,
    force_regeocode:     bool  = False,
) -> pd.DataFrame:
    """
    Run the full analysis pipeline and return the merged county DataFrame.

    Parameters
    ----------
    raw_dir             : Directory with per-state DC CSVs.
    nrel_csv            : Path to county_space_heating_cooling.csv.
    output_csv          : Destination for the processed results CSV.
    cache_dir           : Directory for caching geocoder assets.
    recovery_efficiency : Fraction of waste heat captured (0–1).
    heating_cop         : Effective COP for heat delivery.
    cooling_cop         : COP for thermally-driven cooling.
    force_regeocode     : Re-run geocoding even if cache exists.

    Returns
    -------
    pd.DataFrame with one row per county that has DC presence.
    """
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # ── 1. Load DC data (processed_dc_states: IT Load / PUE already imputed) ─
    logger.info("=== Step 1: Loading DC data from %s", raw_dir)
    dc = load_dc_data(raw_dir)

    # ── 2. Geocode → FIPS ────────────────────────────────────────────────────
    geocache = cache_dir / "dc_geocoded.parquet"
    use_cache = geocache.exists() and not force_regeocode
    if use_cache:
        dc_geo_cached = pd.read_parquet(geocache)
        # Invalidate cache if row count differs OR classifier columns are missing
        # (older caches were built before the P1-P4 classifier existed).
        stale = (len(dc_geo_cached) != len(dc)) or ("space_type" not in dc_geo_cached.columns)
        if stale:
            logger.info("  Cache stale — re-geocoding")
            use_cache = False
        else:
            logger.info("=== Step 2: Loading cached geocoding from %s", geocache)
            dc_geo = dc_geo_cached

    if not use_cache:
        logger.info("=== Step 2: Reverse-geocoding %d DCs …", len(dc))
        dc_geo = add_fips(dc, cache_dir)
        dc_geo.to_parquet(geocache, index=False)

    # ── 3. Aggregate DC capacity to county ──────────────────────────────────
    logger.info("=== Step 3: Aggregating to county level")
    county_dc, dc_enriched = _aggregate_to_county(
        dc_geo, recovery_efficiency, heating_cop, cooling_cop
    )
    logger.info("  %d counties with DC presence", len(county_dc))

    # ── 4. Load NREL demand data ─────────────────────────────────────────────
    logger.info("=== Step 4: Loading NREL county demand data from %s", nrel_csv)
    nrel = _load_nrel(nrel_csv)
    logger.info("  %d counties in NREL dataset", len(nrel))

    # ── 5. Merge ─────────────────────────────────────────────────────────────
    logger.info("=== Step 5: Merging DC county aggregates with NREL data")
    merged = pd.merge(county_dc, nrel, on="fips", how="left")

    # ── 6. Compute coverage ratios ───────────────────────────────────────────
    merged["heating_coverage_ratio"] = heating_coverage_ratio(
        merged["total_heat_delivered_kwh"],
        merged["total_space_heating_kwh"],
    )
    merged["cooling_coverage_ratio"] = cooling_coverage_ratio(
        merged["total_cooling_delivered_kwh"],
        merged["total_space_cooling_kwh"],
    )

    # Convenience: cap display ratio at 2.0 (200 %) so choropleth colour scale
    # is not dominated by tiny counties with one hyperscale DC.
    merged["heating_coverage_pct"] = (merged["heating_coverage_ratio"] * 100).clip(upper=200)
    merged["cooling_coverage_pct"] = (merged["cooling_coverage_ratio"] * 100).clip(upper=200)

    # Average-power equivalents (MW) — useful for direct comparison with DC IT load
    # Annual kWh ÷ 8760 h/yr ÷ 1000 kW/MW  →  average MW
    merged["heating_demand_mw"]   = merged["total_space_heating_kwh"]   / 8_760 / 1_000
    merged["cooling_demand_mw"]   = merged["total_space_cooling_kwh"]   / 8_760 / 1_000
    merged["heat_delivered_mw"]   = merged["total_heat_delivered_kwh"]  / 8_760 / 1_000
    merged["cooling_delivered_mw"]= merged["total_cooling_delivered_kwh"] / 8_760 / 1_000

    # ── 7. Save ──────────────────────────────────────────────────────────────
    # Ensure FIPS is stored as zero-padded string
    merged["fips"] = merged["fips"].astype(str).str.zfill(5)

    # ── 8. Attach county and state names from Census gazetteer ───────────────
    gaz_path = cache_dir / "census_counties_2023.csv"
    if gaz_path.exists():
        gaz = pd.read_csv(gaz_path, dtype={"GEOID": str})[["GEOID", "NAME", "USPS"]]
        gaz = gaz.rename(columns={"GEOID": "fips", "NAME": "county_name", "USPS": "state_abbr"})
        gaz["fips"] = gaz["fips"].str.zfill(5)
        merged = merged.merge(gaz, on="fips", how="left")
        # Build a readable label: "Cook County, IL"
        merged["county_label"] = (
            merged["county_name"].fillna("Unknown")
            + ", "
            + merged["state_abbr"].fillna("")
        )
    else:
        merged["county_name"]  = merged["fips"]
        merged["state_abbr"]   = ""
        merged["county_label"] = merged["fips"]

    merged.to_csv(output_csv, index=False)
    logger.info("=== Done: %d county rows saved → %s", len(merged), output_csv)

    # ── 9. Save DC-level enriched frame (for Space-Type slicing in dashboard) ─
    dc_parquet.parent.mkdir(parents=True, exist_ok=True)
    keep_cols = [c for c in (
        "name", "state", "latitude", "longitude", "fips",
        "it_load_mw", "pue", "space_type", "classification_source",
        "website",
        "recoverable_kwh", "heat_delivered_kwh", "cooling_delivered_kwh",
    ) if c in dc_enriched.columns]
    dc_enriched[keep_cols].to_parquet(dc_parquet, index=False)
    logger.info("=== DC-level saved → %s  (%d rows)", dc_parquet, len(dc_enriched))

    return merged
