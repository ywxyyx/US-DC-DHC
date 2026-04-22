"""
classifier.py
-------------
Integrated DC processing pipeline (Phase 0 → 3).

Phase 0 — Spatial & name aggregation (prevents campus-vs-building double count)
    Cluster records that share a known operator keyword AND lie within a 2 km
    radius. Collapse each cluster:
      * Campus + Building coexist  → keep Campus (fill load from building sum
                                     if the campus IT Load is empty), drop
                                     the Building rows.
      * Only multiple Buildings     → merge into one "(Aggregated)" row with
                                     summed IT Load and centroid coordinates.
      * Everything else             → preserve all rows individually.

Phase 1 — P1-P4 classification hierarchy
    P1 capacity threshold → P2 operator keyword → P3 website scrape → P4 default.

Phase 2 — LBNL 2024 archetype imputation
    Fill missing IT Load (MW) and PUE using per-Space-Type archetype constants.

Phase 3 — Output
    data/processed_dc_states/processed_<state>.csv   (all enriched rows)
    data/manual_verification/{hyperscale,ai_specialized,mid_colo,small}_verify.csv
        (split by Space Type for human review)

Run:
    python -m src.classifier
"""

from __future__ import annotations

import logging
import math
import re
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths & physical constants
# ---------------------------------------------------------------------------
PROJECT_ROOT  = Path(__file__).resolve().parents[1]
RAW_DIR       = PROJECT_ROOT / "data" / "raw_dc_states"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed_dc_states"
VERIFY_DIR    = PROJECT_ROOT / "data" / "manual_verification"

CLUSTER_RADIUS_KM = 2.0
HEAT_RECOVERY_EFF = 0.9
HOURS_PER_YEAR    = 8760
P3_TIMEOUT_SEC    = 3
BODY_CHAR_LIMIT   = 2_000

# ---------------------------------------------------------------------------
# LBNL 2024 archetype constants
# ---------------------------------------------------------------------------
ARCHETYPES: dict[str, dict[str, float]] = {
    "Hyperscale":     {"it_load_mw": 50.0, "pue": 1.22},
    "AI Specialized": {"it_load_mw": 20.0, "pue": 1.14},
    "Midsize/Colo":   {"it_load_mw": 15.0, "pue": 1.68},
    "Small":          {"it_load_mw":  1.0, "pue": 1.91},
}
SPACE_TYPES = tuple(ARCHETYPES.keys())

# ---------------------------------------------------------------------------
# P2 — operator keyword lists (case-insensitive substring matches on Name)
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
    "Vantage", "Aligned", "Compass", "Sabey", "NTT", "KDDI",
]
_SMALL_EDGE_OPS = [
    "EdgeConneX", "Vapor IO", "DartPoints", "American Tower",
    "SBA Communications", "AtlasEdge",
]

# Pre-compile Name regex for P2
_OP_MAP: list[tuple[re.Pattern, str]] = []
for _ops, _stype in [
    (_HYPERSCALE_OPS, "Hyperscale"),
    (_AI_OPS,         "AI Specialized"),
    (_MIDSIZE_OPS,    "Midsize/Colo"),
    (_SMALL_EDGE_OPS, "Small"),
]:
    for _kw in _ops:
        _OP_MAP.append((re.compile(re.escape(_kw), re.IGNORECASE), _stype))

# Flat list (longest-first) used by Phase 0 to extract an operator key
_ALL_OPERATORS_SORTED = sorted(
    _HYPERSCALE_OPS + _AI_OPS + _MIDSIZE_OPS + _SMALL_EDGE_OPS,
    key=len, reverse=True,
)

# ---------------------------------------------------------------------------
# P3 — website keyword lists (lower-cased; matched against page text)
# ---------------------------------------------------------------------------
_HYPERSCALE_WEB = [
    "cloud regions", "availability zones", "iaas", "hyperscale",
    "massive scale",
]
_AI_WEB = [
    "h100", "a100", "dgx", "gpu cloud", "liquid cooling", "infiniband",
]
_MIDSIZE_WEB = [
    "colocation", "carrier-neutral", "carrier neutral",
    "interconnection", "peering", "multi-tenant",
]
_SMALL_WEB = [
    "edge computing", "pop", "low latency",
]
_WEB_KEYWORD_MAP: list[tuple[list[str], str]] = [
    (_HYPERSCALE_WEB, "Hyperscale"),
    (_AI_WEB,         "AI Specialized"),
    (_MIDSIZE_WEB,    "Midsize/Colo"),
    (_SMALL_WEB,      "Small"),
]

# Campus / Building regexes
_RE_CAMPUS   = re.compile(r"\bcampus\b",                  re.IGNORECASE)
_RE_BUILDING = re.compile(r"\b(building|bldg|bld)\b",     re.IGNORECASE)


# ===========================================================================
# Phase 0 — Spatial & name aggregation
# ===========================================================================

def _extract_operator(name) -> str | None:
    """Return lowercased operator keyword hit, or None if none matches."""
    if not isinstance(name, str) or not name.strip():
        return None
    low = name.lower()
    for kw in _ALL_OPERATORS_SORTED:
        if kw.lower() in low:
            return kw.lower()
    return None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two WGS84 points."""
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _cluster_indices(group: pd.DataFrame, radius_km: float) -> list[list]:
    """Union-find on pairwise haversine: return lists of original indices."""
    idx = group.index.to_list()
    n = len(idx)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[ri] = rj

    lats = group["Latitude"].to_numpy(dtype=float)
    lons = group["Longitude"].to_numpy(dtype=float)
    for i in range(n):
        for j in range(i + 1, n):
            if _haversine_km(lats[i], lons[i], lats[j], lons[j]) <= radius_km:
                union(i, j)

    comps: dict[int, list] = {}
    for i in range(n):
        comps.setdefault(find(i), []).append(idx[i])
    return list(comps.values())


def _collapse_cluster(cluster: pd.DataFrame, operator: str) -> pd.DataFrame:
    """Apply Phase 0 campus/building rules to a single cluster."""
    if len(cluster) <= 1:
        out = cluster.copy()
        out["Is_Aggregated"] = False
        return out

    names = cluster["Name"].fillna("").astype(str)
    is_campus   = names.str.contains(_RE_CAMPUS)
    is_building = names.str.contains(_RE_BUILDING)

    loads_numeric = pd.to_numeric(cluster["IT Load (MW)"], errors="coerce")

    # Case A — Campus + Building coexist: keep campuses, drop buildings
    if is_campus.any() and is_building.any():
        campus    = cluster[is_campus].copy()
        buildings = cluster[is_building]
        others    = cluster[~is_campus & ~is_building].copy()

        building_load_sum = pd.to_numeric(
            buildings["IT Load (MW)"], errors="coerce"
        ).sum(skipna=True)

        # Fill each campus row only if its own IT Load is blank
        for i in campus.index:
            current = pd.to_numeric(campus.at[i, "IT Load (MW)"], errors="coerce")
            if pd.isna(current) and building_load_sum > 0:
                campus.at[i, "IT Load (MW)"] = building_load_sum

        campus["Is_Aggregated"] = True
        others["Is_Aggregated"] = False
        return pd.concat([campus, others], ignore_index=True)

    # Case B — Buildings only (>=2): merge into a single aggregated row
    if (is_building.sum() >= 2) and (not is_campus.any()):
        buildings = cluster[is_building]
        others    = cluster[~is_building].copy()

        b_loads = pd.to_numeric(buildings["IT Load (MW)"], errors="coerce")
        b_pues  = pd.to_numeric(buildings["PUE"],          errors="coerce")
        lat     = pd.to_numeric(buildings["Latitude"],     errors="coerce").mean()
        lon     = pd.to_numeric(buildings["Longitude"],    errors="coerce").mean()

        state_val = str(buildings.iloc[0].get("State", "") or "").strip()
        first_web = ""
        for w in buildings["Website"].fillna(""):
            s = str(w)
            if s.startswith("http"):
                first_web = s
                break

        agg = {col: buildings.iloc[0].get(col) for col in buildings.columns}
        agg.update({
            "Name":         f"{operator.title()} {state_val.title()} (Aggregated)",
            "State":        state_val,
            "Latitude":     lat,
            "Longitude":    lon,
            "IT Load (MW)": b_loads.sum(skipna=True) if b_loads.notna().any() else pd.NA,
            "PUE":          b_pues.mean(skipna=True) if b_pues.notna().any() else pd.NA,
            "Website":      first_web,
            "Is_Aggregated": True,
        })
        others["Is_Aggregated"] = False
        return pd.concat([pd.DataFrame([agg]), others], ignore_index=True)

    # Case C — no clear campus/building signal: keep all rows individually
    out = cluster.copy()
    out["Is_Aggregated"] = False
    return out


def phase0_dedupe(df: pd.DataFrame, radius_km: float = CLUSTER_RADIUS_KM) -> pd.DataFrame:
    """Phase 0 entry point — cluster and collapse the full dataframe."""
    df = df.copy()
    df["Latitude"]  = pd.to_numeric(df["Latitude"],  errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["_op_key"]   = df["Name"].map(_extract_operator)

    pieces: list[pd.DataFrame] = []

    # Records without a recognised operator key OR without coords are not clustered
    keep_unclustered = df["_op_key"].isna() | df["Latitude"].isna() | df["Longitude"].isna()
    kept = df[keep_unclustered].copy()
    kept["Is_Aggregated"] = False
    pieces.append(kept)

    clusterable = df[~keep_unclustered]
    for (_, _), group in clusterable.groupby(["State", "_op_key"], dropna=False):
        if len(group) == 1:
            row = group.copy()
            row["Is_Aggregated"] = False
            pieces.append(row)
            continue
        op_key = group["_op_key"].iloc[0]
        for cluster_idx in _cluster_indices(group, radius_km):
            pieces.append(_collapse_cluster(df.loc[cluster_idx], op_key))

    out = pd.concat(pieces, ignore_index=True)
    return out.drop(columns=["_op_key"], errors="ignore")


# ===========================================================================
# Phase 1 — Classification
# ===========================================================================

def _classify_p1(it_load) -> str | None:
    val = pd.to_numeric(it_load, errors="coerce")
    if pd.isna(val) or val <= 0:
        return None
    if val >= 30:
        return "Hyperscale"
    if val >= 10:
        return "Midsize/Colo"
    return "Small"


def _classify_p2(name) -> str | None:
    if not isinstance(name, str) or not name.strip():
        return None
    for pattern, stype in _OP_MAP:
        if pattern.search(name):
            return stype
    return None


def _fetch_page_text(url) -> str:
    """Fetch and return lowercased title + meta description + body excerpt."""
    if not isinstance(url, str) or not url.startswith("http"):
        return ""
    try:
        resp = requests.get(url, timeout=P3_TIMEOUT_SEC,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.title.string if soup.title else ""
        meta = ""
        desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        if desc and desc.get("content"):
            meta = desc["content"]
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        body = soup.get_text(separator=" ", strip=True)[:BODY_CHAR_LIMIT]
        return f"{title} {meta} {body}".lower()
    except Exception:
        return ""


def _classify_p3(website) -> str | None:
    text = _fetch_page_text(website)
    if not text.strip():
        return None
    for keywords, stype in _WEB_KEYWORD_MAP:
        if any(kw in text for kw in keywords):
            return stype
    return None


def classify_record(name, it_load, website) -> tuple[str, str]:
    """Apply P1→P2→P3→P4; return (space_type, source_tag)."""
    r = _classify_p1(it_load)
    if r: return r, "P1"
    r = _classify_p2(name)
    if r: return r, "P2"
    r = _classify_p3(website)
    if r: return r, "P3"
    return "Small", "P4"


def phase1_classify(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    types, sources = [], []
    for _, row in df.iterrows():
        stype, src = classify_record(
            name=row.get("Name", ""),
            it_load=row.get("IT Load (MW)"),
            website=row.get("Website", ""),
        )
        types.append(stype)
        sources.append(src)
    df["Space Type"] = pd.Categorical(types, categories=list(SPACE_TYPES))
    df["Classification_Source"] = sources
    return df


# ===========================================================================
# Phase 2 — Archetype imputation
# ===========================================================================

def phase2_impute(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["IT Load (MW)"] = pd.to_numeric(df["IT Load (MW)"], errors="coerce")
    df["PUE"]          = pd.to_numeric(df["PUE"],          errors="coerce")

    # Confidence_Score: 5 if IT Load was reported before imputation, 1 otherwise
    df["Confidence_Score"] = df["IT Load (MW)"].notna().map({True: 5, False: 1})

    for stype, defaults in ARCHETYPES.items():
        mask = df["Space Type"] == stype
        df.loc[mask & df["IT Load (MW)"].isna(), "IT Load (MW)"] = defaults["it_load_mw"]
        df.loc[mask & df["PUE"].isna(),          "PUE"]          = defaults["pue"]

    df["Annual_Heat_MWh"] = df["IT Load (MW)"] * HEAT_RECOVERY_EFF * HOURS_PER_YEAR
    return df


# ===========================================================================
# Phase 3 — Output
# ===========================================================================

_OUTPUT_COL_ORDER = [
    "Name", "State", "Latitude", "Longitude",
    "IT Load (MW)", "PUE", "Website", "Source URL",
    "Space Type", "Classification_Source",
    "Is_Aggregated", "Confidence_Score", "Annual_Heat_MWh",
]

_VERIFY_FILE_MAP = {
    "Hyperscale":     "hyperscale_verify.csv",
    "AI Specialized": "ai_specialized_verify.csv",
    "Midsize/Colo":   "mid_colo_verify.csv",
    "Small":          "small_verify.csv",
}


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    front = [c for c in _OUTPUT_COL_ORDER if c in df.columns]
    rest  = [c for c in df.columns if c not in front]
    return df[front + rest]


def _read_raw_state(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    for col in ("Name", "State", "Latitude", "Longitude",
                "IT Load (MW)", "PUE", "Website", "Source URL"):
        if col not in df.columns:
            df[col] = pd.NA
    return df


def _write_verification_csvs(combined: pd.DataFrame, out_dir: Path) -> dict[str, int]:
    """Split combined dataframe by Space Type and write verify CSVs."""
    out_dir.mkdir(parents=True, exist_ok=True)
    verify_cols = [
        "Name", "State", "Latitude", "Longitude",
        "IT Load (MW)", "PUE", "Website", "Source URL",
        "Space Type", "Classification_Source",
        "Is_Aggregated", "Annual_Heat_MWh", "Confidence_Score",
    ]
    row_counts: dict[str, int] = {}
    for stype, fname in _VERIFY_FILE_MAP.items():
        subset = combined.loc[combined["Space Type"] == stype].copy()
        cols = [c for c in verify_cols if c in subset.columns]
        subset = subset[cols]
        subset["Manual_Verified_MW"] = ""
        # Sort least-reliable records first
        subset = subset.sort_values(
            by=["Confidence_Score", "State", "Name"],
            ascending=[True, True, True],
        )
        out_path = out_dir / fname
        subset.to_csv(out_path, index=False)
        row_counts[fname] = len(subset)
    return row_counts


def process_all(
    raw_dir: Path = RAW_DIR,
    processed_dir: Path = PROCESSED_DIR,
    verify_dir: Path = VERIFY_DIR,
) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    verify_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSVs in {raw_dir}")

    per_state_frames: list[pd.DataFrame] = []
    print("\nPhase 0-2 per state (raw → after Phase 0 dedupe):")
    for path in csv_files:
        raw = _read_raw_state(path)
        raw_n = len(raw)
        dedup = phase0_dedupe(raw)
        classified = phase1_classify(dedup)
        imputed = phase2_impute(classified)
        imputed = _reorder_columns(imputed)

        out_path = processed_dir / f"processed_{path.name}"
        imputed.to_csv(out_path, index=False)
        per_state_frames.append(imputed)
        print(f"  {path.name:<26} {raw_n:>5} → {len(imputed):>5}")

    combined = pd.concat(per_state_frames, ignore_index=True)
    row_counts = _write_verification_csvs(combined, verify_dir)
    _print_summary(combined, row_counts)


def _print_summary(df: pd.DataFrame, verify_counts: dict[str, int]) -> None:
    total = len(df)
    space_counts = df["Space Type"].value_counts().reindex(SPACE_TYPES, fill_value=0)
    src_counts = df["Classification_Source"].value_counts().reindex(
        ["P1", "P2", "P3", "P4"], fill_value=0
    )
    agg_count = int(df["Is_Aggregated"].sum()) if "Is_Aggregated" in df else 0
    conf_orig = int((df["Confidence_Score"] == 5).sum())
    conf_imp  = int((df["Confidence_Score"] == 1).sum())

    bar = "=" * 60
    print("\n" + bar)
    print("  DC Classification Summary")
    print(bar)
    print(f"  {'Total records (post-dedupe)':<34} {total:>8,}")
    print(f"  {'Aggregated rows (Phase 0)':<34} {agg_count:>8,}")
    print("-" * 60)
    for stype, n in space_counts.items():
        pct = 100 * n / total if total else 0
        print(f"  {stype:<34} {n:>8,}  ({pct:5.1f}%)")

    print("\n  --- Classification Source ---")
    labels = {"P1": "P1 (IT Load threshold)",
              "P2": "P2 (Operator name)",
              "P3": "P3 (Web scrape)",
              "P4": "P4 (Default Small)"}
    for src, n in src_counts.items():
        pct = 100 * n / total if total else 0
        print(f"  {labels[src]:<34} {n:>8,}  ({pct:5.1f}%)")

    print("\n  --- Confidence Score ---")
    print(f"  {'5 (IT Load originally reported)':<34} {conf_orig:>8,}")
    print(f"  {'1 (IT Load imputed)':<34} {conf_imp:>8,}")

    print("\n  --- Manual Verification Export ---")
    for fname, n in verify_counts.items():
        print(f"  {fname:<34} {n:>8,}")

    print(bar + "\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    process_all()
