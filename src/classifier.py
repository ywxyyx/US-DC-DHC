"""
classifier.py
-------------
Multi-level (P1→P4) classification and LBNL 2024 imputation for raw DC CSVs.

Pipeline per record:
  P1 – IT Load (MW) threshold  →  Hyperscale / Midsize-Colo / Small
  P2 – Operator name keyword   →  same categories + AI Specialized
  P3 – Website scrape: title + meta description + first 2,000 chars of body text
  P4 – Default: Small

After classification, missing IT Load and PUE are filled from LBNL 2024 archetypes.
Annual_Heat_MWh        = IT Load (MW) * 0.9 * 8760
Classification_Source  = P1 / P2 / P3 / P4  (how this record was classified)

Usage
-----
  python -m src.classifier                    # processes all 51 state CSVs
  from src.classifier import process_all      # call programmatically
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LBNL 2024 archetype constants
# ---------------------------------------------------------------------------
ARCHETYPES: dict[str, dict[str, float]] = {
    "Hyperscale":     {"it_load_mw": 50.0, "pue": 1.22},
    "AI Specialized": {"it_load_mw": 20.0, "pue": 1.14},
    "Midsize/Colo":   {"it_load_mw": 15.0, "pue": 1.68},
    "Small":          {"it_load_mw": 1.0,  "pue": 1.91},   # updated: 0.5 → 1.0 MW
}

HEAT_RECOVERY_EFF = 0.9
HOURS_PER_YEAR = 8760

# ---------------------------------------------------------------------------
# P2 – operator keyword lists  (matched as case-insensitive substrings of Name)
# ---------------------------------------------------------------------------
_HYPERSCALE_OPS = [
    "Google", "Amazon", "AWS", "Microsoft", "Azure", "Meta", "Facebook",
    "Apple", "Oracle", "IBM Cloud", "Salesforce", "SAP",
    "Alibaba", "Tencent", "Baidu", "OVHcloud",
]

_AI_OPS = [
    "Nebius", "Lambda", "CoreWeave", "Nvidia", "Applied Digital", "Crusoe",
    "Northern Data", "Iris Energy", "Teraco", "Hut 8", "Bit Digital",
    "GPU", "HPC",
]

_MIDSIZE_OPS = [
    "Equinix", "Digital Realty", "CyrusOne", "QTS", "Iron Mountain",
    "CoreSite", "Switch", "DC BLOX", "Lumen", "TierPoint", "DataBank",
    "Vantage", "Aligned", "Compass", "Sabey", "T5 Data Centers",
    "H5 Data Centers", "NTT Global", "KDDI", "Evoque", "Cyxtera",
    "365 Data Centers",
]

_SMALL_EDGE_OPS = [
    "EdgeConneX", "Vapor IO", "DartPoints", "American Tower",
    "SBA Communications", "AtlasEdge",
]

# Pre-compile operator → space-type mapping
_OP_MAP: list[tuple[re.Pattern, str]] = []
for _ops, _stype in [
    (_HYPERSCALE_OPS, "Hyperscale"),
    (_AI_OPS,         "AI Specialized"),
    (_MIDSIZE_OPS,    "Midsize/Colo"),
    (_SMALL_EDGE_OPS, "Small"),
]:
    for _kw in _ops:
        _OP_MAP.append((re.compile(re.escape(_kw), re.IGNORECASE), _stype))

# ---------------------------------------------------------------------------
# P3 – website keyword lists
#   Matched against: page title + meta description + first 2,000 chars of body
#   Keywords are lower-cased; page text is also lower-cased before matching.
# ---------------------------------------------------------------------------
_HYPERSCALE_WEB = [
    # cloud platform indicators
    "cloud regions", "availability zones", "iaas", "paas",
    "global infrastructure", "hyperscale", "massive scale",
    "cloud computing", "public cloud", "cloud platform",
    "cloud services", "global network", "exabyte",
    # large-campus language
    "data center campus", "campus expansion",
]

_AI_WEB = [
    # GPU / accelerator hardware
    "h100", "a100", "h200", "b200", "dgx", "hgx",
    "amd instinct", "nvidia",
    # workload descriptors
    "training clusters", "inference", "gpu cloud", "gpu cluster",
    "ai compute", "ai infrastructure", "machine learning",
    "deep learning", "large language model", "llm training",
    # cooling for dense AI racks
    "liquid cooling", "immersion cooling", "direct liquid cooling",
    # networking
    "infiniband", "roce",
]

_MIDSIZE_WEB = [
    # colocation core terms  (expanded — "colocation" alone now counts)
    "colocation", "colo facility", "wholesale colocation",
    "carrier-neutral", "carrier neutral", "network-neutral",
    "interconnection", "cross-connect", "peering", "meet-me-room",
    # tenant / service language
    "multi-tenant", "enterprise colocation", "managed colocation",
    "carrier hotel", "internet exchange",
    # common web-page phrases
    "data center services", "managed hosting",
    "power and cooling", "raised floor",
]

_SMALL_WEB = [
    "edge computing", "edge data center", "edge location",
    "micro data center", "modular data center",
    "point of presence", "pop",
    "low latency", "last mile",
]

# Ordered: first match wins (Hyperscale > AI > Midsize > Small)
_WEB_KEYWORD_MAP: list[tuple[list[str], str]] = [
    (_HYPERSCALE_WEB, "Hyperscale"),
    (_AI_WEB,         "AI Specialized"),
    (_MIDSIZE_WEB,    "Midsize/Colo"),
    (_SMALL_WEB,      "Small"),
]

# Maximum body-text characters to extract (keeps scraping fast)
_BODY_CHAR_LIMIT = 2_000


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

def _classify_p1(it_load: float | None) -> str | None:
    """P1: capacity threshold. Returns space type or None."""
    if it_load is None or (isinstance(it_load, float) and pd.isna(it_load)):
        return None
    if it_load >= 30:
        return "Hyperscale"
    if it_load >= 10:
        return "Midsize/Colo"
    return "Small"


def _classify_p2(name: str) -> str | None:
    """P2: case-insensitive operator keyword match on the facility name."""
    if not isinstance(name, str) or not name.strip():
        return None
    for pattern, stype in _OP_MAP:
        if pattern.search(name):
            return stype
    return None


def _fetch_page_text(url: str, timeout: int = 3) -> str:
    """
    Fetch a URL and return lowercased: title + meta description + body excerpt.
    Returns empty string on any network / HTTP error.
    """
    if not isinstance(url, str) or not url.startswith("http"):
        return ""
    try:
        resp = requests.get(url, timeout=timeout,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Title
        title = soup.title.string if soup.title else ""

        # Meta description
        meta = ""
        desc_tag = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        if desc_tag and desc_tag.get("content"):
            meta = desc_tag["content"]

        # Body text excerpt — strip script/style first
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        body_text = (soup.get_text(separator=" ", strip=True))[:_BODY_CHAR_LIMIT]

        return f"{title} {meta} {body_text}".lower()
    except Exception:
        return ""


def _classify_p3(website: str) -> str | None:
    """P3: scrape website and match against expanded keyword lists."""
    text = _fetch_page_text(website)
    if not text.strip():
        return None
    for keywords, stype in _WEB_KEYWORD_MAP:
        if any(kw in text for kw in keywords):
            return stype
    return None


def classify_record(name: str, it_load, website: str) -> tuple[str, str]:
    """
    Apply P1→P2→P3→P4 and return (space_type, source) where source ∈ {P1,P2,P3,P4}.
    """
    result = _classify_p1(it_load)
    if result:
        return result, "P1"
    result = _classify_p2(name)
    if result:
        return result, "P2"
    result = _classify_p3(website)
    if result:
        return result, "P3"
    return "Small", "P4"


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def _process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Space Type + Classification_Source, impute IT Load / PUE,
    and append Annual_Heat_MWh.
    """
    df["IT Load (MW)"] = pd.to_numeric(df.get("IT Load (MW)"), errors="coerce")
    df["PUE"] = pd.to_numeric(df.get("PUE"), errors="coerce")

    # 1. Classify every record
    space_types: list[str] = []
    sources: list[str] = []
    for _, row in df.iterrows():
        stype, src = classify_record(
            name=row.get("Name", ""),
            it_load=row.get("IT Load (MW)"),
            website=row.get("Website", ""),
        )
        space_types.append(stype)
        sources.append(src)

    df["Space Type"] = pd.Categorical(
        space_types,
        categories=["Hyperscale", "AI Specialized", "Midsize/Colo", "Small"],
    )
    df["Classification_Source"] = sources

    # 2. Impute missing IT Load and PUE from LBNL 2024 archetypes
    for stype, defaults in ARCHETYPES.items():
        mask = df["Space Type"] == stype
        df.loc[mask & df["IT Load (MW)"].isna(), "IT Load (MW)"] = defaults["it_load_mw"]
        df.loc[mask & df["PUE"].isna(),          "PUE"]          = defaults["pue"]

    # 3. Derived column
    df["Annual_Heat_MWh"] = df["IT Load (MW)"] * HEAT_RECOVERY_EFF * HOURS_PER_YEAR

    return df


def process_state_file(input_path: Path, output_dir: Path) -> pd.DataFrame:
    """Read one raw state CSV, classify and impute it, write to output_dir."""
    df = pd.read_csv(input_path, dtype=str)

    for col in ("Name", "IT Load (MW)", "PUE", "Website"):
        if col not in df.columns:
            df[col] = pd.NA

    df = _process_dataframe(df)

    output_path = output_dir / f"processed_{input_path.name}"
    df.to_csv(output_path, index=False)
    logger.info("Saved → %s  (%d rows)", output_path.name, len(df))
    return df


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_all(
    raw_dir: Path | None = None,
    output_dir: Path | None = None,
) -> None:
    """
    Process every CSV in raw_dir, save results to output_dir, and print a
    summary report with Space Type counts and Classification_Source breakdown.
    """
    project_root = Path(__file__).resolve().parents[1]

    if raw_dir is None:
        raw_dir = project_root / "data" / "raw_dc_states"
    if output_dir is None:
        output_dir = project_root / "data" / "processed_dc_states"

    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {raw_dir}")

    all_frames: list[pd.DataFrame] = []

    for path in csv_files:
        print(f"  Processing {path.name} …", end=" ", flush=True)
        try:
            df = process_state_file(path, output_dir)
            all_frames.append(df)
            print(f"{len(df)} rows")
        except Exception as exc:
            print(f"ERROR: {exc}")
            logger.error("Failed on %s: %s", path.name, exc)

    if not all_frames:
        print("No files processed.")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    total = len(combined)

    # Space Type counts
    counts = combined["Space Type"].value_counts().reindex(
        ["Hyperscale", "AI Specialized", "Midsize/Colo", "Small"], fill_value=0
    )

    # Classification source breakdown
    src_counts = combined["Classification_Source"].value_counts().reindex(
        ["P1", "P2", "P3", "P4"], fill_value=0
    )

    # P3 hits by Space Type (what P3 actually classified)
    p3_by_type = (
        combined[combined["Classification_Source"] == "P3"]["Space Type"]
        .value_counts()
        .reindex(["Hyperscale", "AI Specialized", "Midsize/Colo", "Small"], fill_value=0)
    )

    print("\n" + "=" * 56)
    print("  DC Classification Summary")
    print("=" * 56)
    print(f"  {'Total records':<32} {total:>8,}")
    print("-" * 56)
    for stype, n in counts.items():
        pct = 100 * n / total if total else 0
        print(f"  {stype:<32} {n:>8,}  ({pct:5.1f}%)")

    print("\n  --- Classification Source Breakdown ---")
    src_labels = {"P1": "P1 (IT Load threshold)",
                  "P2": "P2 (Operator name)",
                  "P3": "P3 (Web scrape)",
                  "P4": "P4 (Default Small)"}
    for src, n in src_counts.items():
        pct = 100 * n / total if total else 0
        print(f"  {src_labels[src]:<32} {n:>8,}  ({pct:5.1f}%)")

    if src_counts.get("P3", 0) > 0:
        print("\n  --- P3 Hits by Space Type ---")
        for stype, n in p3_by_type.items():
            if n > 0:
                print(f"  {stype:<32} {n:>8,}")

    print("=" * 56)
    print(f"  Processed files  → {output_dir}")
    print("=" * 56 + "\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    process_all()
