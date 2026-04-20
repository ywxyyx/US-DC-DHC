"""
geocoding.py
------------
Reverse-geocode (lat, lon) pairs to 5-digit US county FIPS codes.

Uses the `reverse_geocoder` library (offline KD-tree lookup, no API key needed).
The library returns GeoNames data; we map GeoNames admin2 codes to FIPS using
the Census FIPS reference bundled with `us` or via a lookup table derived from
the reverse_geocoder results combined with known state FIPS prefixes.

Strategy
--------
1. reverse_geocoder.search() → returns {'cc':'US','admin1':'Iowa','admin2':'Polk County',...}
2. Map state name → 2-digit state FIPS using the `us` library.
3. Map county name within state → 3-digit county FIPS using the Census gazetteer
   embedded in this module (auto-downloaded once from Census.gov into data/nrel/).
4. Concatenate → 5-digit FIPS string with leading zeros.

For any record that fails all lookups, FIPS is set to NaN and logged.
"""

import logging
import re
from functools import lru_cache
from pathlib import Path

import pandas as pd
import reverse_geocoder as rg
import us  # pip install us

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Census county gazetteer — downloaded on first use
# ---------------------------------------------------------------------------
_GAZETTEER_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2023_Gazetteer/2023_Gaz_counties_national.zip"
)
_GAZETEER_COLS = ["USPS", "GEOID", "ANSICODE", "NAME", "ALAND", "AWATER",
                  "ALAND_SQMI", "AWATER_SQMI", "INTPTLAT", "INTPTLONG"]


def _load_gazetteer(cache_dir: Path) -> pd.DataFrame:
    """Return Census county gazetteer as DataFrame, downloading if needed."""
    cache_path = cache_dir / "census_counties_2023.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path, dtype={"GEOID": str})

    logger.info("Downloading Census county gazetteer …")
    gz = pd.read_csv(_GAZETTEER_URL, sep="\t", dtype={"GEOID": str},
                     names=_GAZETEER_COLS, header=0,
                     encoding="latin-1", skipinitialspace=True)
    gz = gz[["USPS", "GEOID", "NAME"]].copy()
    gz["GEOID"] = gz["GEOID"].str.zfill(5)
    gz.to_csv(cache_path, index=False)
    logger.info("Gazetteer saved → %s", cache_path)
    return gz


def _build_lookup(gazetteer: pd.DataFrame) -> dict[tuple[str, str], str]:
    """
    Build a dict  (state_abbr_upper, normalised_county_name) → 5-digit FIPS.
    Normalisation: lower-case, strip 'county'/'parish'/'borough'/'census area'.
    """
    suffixes = re.compile(
        r"\s+(county|parish|borough|census area|municipality|"
        r"city and borough|unified government|metro government|"
        r"consolidated government|metropolitan government)\s*$",
        re.IGNORECASE,
    )

    def normalise(name: str) -> str:
        return suffixes.sub("", name).strip().lower()

    lookup: dict[tuple[str, str], str] = {}
    for _, row in gazetteer.iterrows():
        key = (row["USPS"].upper(), normalise(row["NAME"]))
        lookup[key] = row["GEOID"]
    return lookup


@lru_cache(maxsize=1)
def _get_lookup(cache_dir_str: str) -> dict[tuple[str, str], str]:
    cache_dir = Path(cache_dir_str)
    gz = _load_gazetteer(cache_dir)
    return _build_lookup(gz)


# ---------------------------------------------------------------------------
# State name / abbr helpers
# ---------------------------------------------------------------------------
def _state_abbr(admin1_name: str) -> str | None:
    """Convert full state name (from GeoNames) to 2-letter USPS abbreviation."""
    state = us.states.lookup(admin1_name)
    return state.abbr if state else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def add_fips(df: pd.DataFrame, cache_dir: Path) -> pd.DataFrame:
    """
    Add a 'fips' column (5-digit string) to *df* using reverse geocoding.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'latitude' and 'longitude' columns (numeric).
    cache_dir : Path
        Directory for caching the Census gazetteer CSV.

    Returns
    -------
    pd.DataFrame
        Original df with an added 'fips' column. Rows where lookup fails get NaN.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    lookup = _get_lookup(str(cache_dir))

    coords = list(zip(df["latitude"], df["longitude"]))
    logger.info("Reverse-geocoding %d coordinates …", len(coords))

    # Batch reverse geocoding (fast KD-tree, runs locally)
    results = rg.search(coords, verbose=False)

    suffixes = re.compile(
        r"\s+(county|parish|borough|census area|municipality|"
        r"city and borough|unified government|metro government|"
        r"consolidated government|metropolitan government)\s*$",
        re.IGNORECASE,
    )

    fips_list: list[str | None] = []
    failed = 0

    for i, res in enumerate(results):
        admin1 = res.get("admin1", "")
        admin2 = res.get("admin2", "")
        cc     = res.get("cc", "")

        if cc != "US":
            fips_list.append(None)
            failed += 1
            continue

        abbr = _state_abbr(admin1)
        if not abbr:
            logger.debug("Row %d: unknown state '%s'", i, admin1)
            fips_list.append(None)
            failed += 1
            continue

        norm_county = suffixes.sub("", admin2).strip().lower()
        fips = lookup.get((abbr, norm_county))

        if fips is None:
            # Fallback: partial match on county name
            prefix = (abbr, norm_county)
            for k, v in lookup.items():
                if k[0] == abbr and norm_county in k[1]:
                    fips = v
                    break

        if fips is None:
            logger.debug("Row %d: no FIPS for (%s, %s)", i, abbr, admin2)
            failed += 1

        fips_list.append(fips)

    df = df.copy()
    df["fips"] = fips_list
    df["fips"] = df["fips"].where(df["fips"].notna(), other=pd.NA)

    logger.info(
        "Geocoding complete: %d succeeded, %d failed (%.1f%%)",
        len(df) - failed, failed, 100 * failed / max(len(df), 1),
    )
    return df
