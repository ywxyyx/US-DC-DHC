"""
app.py — Streamlit dashboard
-----------------------------
US Data Center Waste Heat Recovery Analysis

Layout
------
Sidebar : parameter sliders (η_recovery, COP_heating, COP_cooling) + run button
Main    : KPI cards → choropleth map (heating coverage) → scatter plot → data table

Run with:
    streamlit run app.py
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Make src/ importable when running from the project root
sys.path.insert(0, str(Path(__file__).parent))

from src.analyzer import (
    DEFAULT_DC_PARQUET,
    DEFAULT_NREL_CSV,
    DEFAULT_OUTPUT_CSV,
    DEFAULT_RAW_DIR,
    SPACE_TYPES,
    run_analysis,
)
from src.physics_engine import (
    COOLING_COP,
    HEATING_COP,
    RECOVERY_EFFICIENCY,
)

# Space-Type colour palette (consistent across tabs)
_TYPE_COLOUR = {
    "Hyperscale":     "#1f77b4",   # blue
    "AI Specialized": "#d62728",   # red
    "Midsize/Colo":   "#2ca02c",   # green
    "Small":          "#ff7f0e",   # orange
}

# ---------------------------------------------------------------------------
# Logging — route to Streamlit console (visible in terminal)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DC Waste Heat Recovery",
    page_icon="♻️",
    layout="wide",
)

st.title("♻️ US Data Center Waste Heat Recovery Analysis")
st.caption(
    "Source: datacentermap.com (4 879 DCs, P1–P4 classified) × "
    "NREL EULP ResStock/ComStock 2022/23. IT Load & PUE imputed from LBNL 2024 archetypes."
)

# ---------------------------------------------------------------------------
# Sidebar — parameter controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Model Parameters")

    recovery_efficiency = st.slider(
        "Recovery Efficiency η",
        min_value=0.10, max_value=1.00, value=RECOVERY_EFFICIENCY, step=0.05,
        help="Fraction of DC waste heat that can be captured and transported",
    )
    heating_cop = st.slider(
        "Heating COP",
        min_value=1.0, max_value=6.0, value=HEATING_COP, step=0.1,
        help="Effective system COP for delivering heat to buildings",
    )
    cooling_cop = st.slider(
        "Cooling COP",
        min_value=0.1, max_value=1.5, value=COOLING_COP, step=0.05,
        help="COP of waste-heat-driven absorption/adsorption chiller",
    )

    st.divider()
    st.subheader("🏷️ Space Types")
    selected_types = st.multiselect(
        "Include Space Types",
        options=list(SPACE_TYPES),
        default=list(SPACE_TYPES),
        help="Filter DCs by LBNL 2024 archetype. All four are included by default.",
    )

    st.divider()
    run_btn = st.button("▶ Run / Refresh Analysis", type="primary", use_container_width=True)

    st.divider()
    st.markdown(
        """
        **Equations used**

        Annual recoverable waste heat:
        ```
        Q = P_IT × PUE × η × 8760 h
        ```
        Heating coverage:
        ```
        R_heat = Q × COP_heat / Demand_heat
        ```
        """
    )

# ---------------------------------------------------------------------------
# Session state — cache full (unfiltered) county + DC-level frames
# ---------------------------------------------------------------------------
for key in ("result_df", "dc_df", "last_params"):
    if key not in st.session_state:
        st.session_state[key] = None

current_params = (recovery_efficiency, heating_cop, cooling_cop)

# Re-run if button clicked OR slider values changed since last run
if run_btn or st.session_state["last_params"] != current_params:
    with st.spinner("Running analysis … (first run downloads Census gazetteer ~2 MB)"):
        df_full = run_analysis(
            raw_dir=DEFAULT_RAW_DIR,
            nrel_csv=DEFAULT_NREL_CSV,
            output_csv=DEFAULT_OUTPUT_CSV,
            dc_parquet=DEFAULT_DC_PARQUET,
            recovery_efficiency=recovery_efficiency,
            heating_cop=heating_cop,
            cooling_cop=cooling_cop,
        )
    st.session_state["result_df"] = df_full
    st.session_state["dc_df"]     = pd.read_parquet(DEFAULT_DC_PARQUET)
    st.session_state["last_params"] = current_params

df_full: pd.DataFrame | None = st.session_state["result_df"]
dc_df:   pd.DataFrame | None = st.session_state["dc_df"]

# ---------------------------------------------------------------------------
# Load cached results if analysis hasn't run yet this session
# ---------------------------------------------------------------------------
if df_full is None:
    if DEFAULT_OUTPUT_CSV.exists() and DEFAULT_DC_PARQUET.exists():
        df_full = pd.read_csv(DEFAULT_OUTPUT_CSV, dtype={"fips": str})
        df_full["fips"] = df_full["fips"].str.zfill(5)
        dc_df   = pd.read_parquet(DEFAULT_DC_PARQUET)
        st.session_state["result_df"] = df_full
        st.session_state["dc_df"]     = dc_df
        st.info("Showing cached results. Adjust sliders and click **Run / Refresh** to recompute.")
    else:
        st.warning("No results yet. Click **▶ Run / Refresh Analysis** in the sidebar.")
        st.stop()


# ---------------------------------------------------------------------------
# Space-Type filter — re-aggregate from DC-level parquet when subset selected
# ---------------------------------------------------------------------------
def _filter_and_reaggregate(
    full_county_df: pd.DataFrame,
    dc_level_df:    pd.DataFrame,
    types:          list[str],
) -> pd.DataFrame:
    """
    Filter DCs by Space Type, re-aggregate per FIPS, and recompute coverage
    against the NREL demand columns already present on `full_county_df`.
    Preserves county_name / state_abbr / county_label.
    """
    dc_sub = dc_level_df[dc_level_df["space_type"].isin(types)].copy()

    agg = (
        dc_sub.dropna(subset=["fips"])
              .groupby("fips", as_index=False)
              .agg(
                  dc_count                  = ("name",                  "count"),
                  total_it_load_mw          = ("it_load_mw",            "sum"),
                  total_recoverable_kwh     = ("recoverable_kwh",       "sum"),
                  total_heat_delivered_kwh  = ("heat_delivered_kwh",    "sum"),
                  total_cooling_delivered_kwh=("cooling_delivered_kwh", "sum"),
              )
    )
    agg["fips"] = agg["fips"].astype(str).str.zfill(5)

    demand_cols = [
        "fips", "county_name", "state_abbr", "county_label",
        "total_space_heating_kwh", "total_space_cooling_kwh",
    ]
    demand_cols = [c for c in demand_cols if c in full_county_df.columns]
    demand = full_county_df[demand_cols].copy()
    demand["fips"] = demand["fips"].astype(str).str.zfill(5)
    merged = agg.merge(demand, on="fips", how="left")

    with pd.option_context("mode.use_inf_as_na", True):
        merged["heating_coverage_ratio"] = (
            merged["total_heat_delivered_kwh"] / merged["total_space_heating_kwh"]
        )
        merged["cooling_coverage_ratio"] = (
            merged["total_cooling_delivered_kwh"] / merged["total_space_cooling_kwh"]
        )
    merged["heating_coverage_pct"] = (merged["heating_coverage_ratio"] * 100).clip(upper=200)
    merged["cooling_coverage_pct"] = (merged["cooling_coverage_ratio"] * 100).clip(upper=200)
    merged["heating_demand_mw"]    = merged["total_space_heating_kwh"]     / 8_760 / 1_000
    merged["cooling_demand_mw"]    = merged["total_space_cooling_kwh"]     / 8_760 / 1_000
    merged["heat_delivered_mw"]    = merged["total_heat_delivered_kwh"]    / 8_760 / 1_000
    merged["cooling_delivered_mw"] = merged["total_cooling_delivered_kwh"] / 8_760 / 1_000
    return merged


# Active DataFrame reflects the current Space-Type filter
if not selected_types:
    st.warning("Select at least one Space Type in the sidebar.")
    st.stop()

if set(selected_types) == set(SPACE_TYPES):
    df = df_full
else:
    df = _filter_and_reaggregate(df_full, dc_df, selected_types)
    st.caption(f"🔍 Filtered to: {' · '.join(selected_types)}  ({len(df)} counties)")

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

total_dc           = int(df["dc_count"].sum())
total_it_mw        = df["total_it_load_mw"].sum()
counties_w_dc      = len(df)
median_hcov        = df["heating_coverage_pct"].median()
total_heat_dem_mw  = df["heating_demand_mw"].sum() if "heating_demand_mw" in df.columns else None

col1.metric("Total DCs (in filter)",     f"{total_dc:,}")
col2.metric("Total IT Load",             f"{total_it_mw:,.0f} MW")
col3.metric("Total Heating Demand",
            f"{total_heat_dem_mw:,.0f} MW" if total_heat_dem_mw is not None else "—",
            help="Sum of county annual space-heating demand expressed as average power")
col4.metric("Counties with DC Presence", f"{counties_w_dc:,}")
col5.metric("Median Heating Coverage",   f"{median_hcov:.1f} %")

# ── Composition chips — Space Type IT-Load share within current filter ──────
type_breakdown = (
    dc_df[dc_df["space_type"].isin(selected_types)]
      .groupby("space_type")
      .agg(count=("name", "count"), mw=("it_load_mw", "sum"))
      .reindex(selected_types, fill_value=0)
      .reset_index()
)
chip_cols = st.columns(max(len(type_breakdown), 1))
for col, row in zip(chip_cols, type_breakdown.itertuples(index=False)):
    stype, n, mw = row.space_type, int(row.count), float(row.mw)
    share = (mw / total_it_mw * 100) if total_it_mw else 0
    col.markdown(
        f"<div style='border-left:6px solid {_TYPE_COLOUR.get(stype, '#888')};"
        f"padding:4px 10px;margin-bottom:6px;'>"
        f"<b>{stype}</b><br>{n:,} DCs · {mw:,.0f} MW ({share:.0f}%)</div>",
        unsafe_allow_html=True,
    )

st.divider()

# ---------------------------------------------------------------------------
# Choropleth — Heating Coverage (%)
# ---------------------------------------------------------------------------
tab_heat, tab_cool, tab_types, tab_scatter, tab_table = st.tabs(
    ["🌡️ Heating Coverage Map", "❄️ Cooling Coverage Map",
     "🏷️ Space Type Breakdown", "📊 Scatter Plot", "📋 Data Table"]
)

_GEOJSON = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

# Hover-template helper — builds a consistent tooltip string
def _heat_hover(row_df: pd.DataFrame) -> list[str]:
    lines = []
    for _, r in row_df.iterrows():
        label = r.get("county_label") or f"FIPS {r['fips']}"
        it   = f"{r['total_it_load_mw']:.1f}" if pd.notna(r.get("total_it_load_mw")) else "—"
        dem  = f"{r['heating_demand_mw']:.1f}"  if pd.notna(r.get("heating_demand_mw")) else "—"
        delv = f"{r['heat_delivered_mw']:.1f}"  if pd.notna(r.get("heat_delivered_mw")) else "—"
        cov  = f"{r['heating_coverage_pct']:.1f}" if pd.notna(r.get("heating_coverage_pct")) else "—"
        lines.append(
            f"<b>{label}</b><br>"
            f"DCs: {int(r['dc_count'])}<br>"
            f"IT Load: {it} MW<br>"
            f"Heat delivered: {delv} MW<br>"
            f"Heat demand: {dem} MW<br>"
            f"Coverage: {cov} %"
        )
    return lines

def _cool_hover(row_df: pd.DataFrame) -> list[str]:
    lines = []
    for _, r in row_df.iterrows():
        label = r.get("county_label") or f"FIPS {r['fips']}"
        it   = f"{r['total_it_load_mw']:.1f}"    if pd.notna(r.get("total_it_load_mw")) else "—"
        dem  = f"{r['cooling_demand_mw']:.1f}"    if pd.notna(r.get("cooling_demand_mw")) else "—"
        delv = f"{r['cooling_delivered_mw']:.1f}" if pd.notna(r.get("cooling_delivered_mw")) else "—"
        cov  = f"{r['cooling_coverage_pct']:.1f}" if pd.notna(r.get("cooling_coverage_pct")) else "—"
        lines.append(
            f"<b>{label}</b><br>"
            f"DCs: {int(r['dc_count'])}<br>"
            f"IT Load: {it} MW<br>"
            f"Cooling delivered: {delv} MW<br>"
            f"Cooling demand: {dem} MW<br>"
            f"Coverage: {cov} %"
        )
    return lines

with tab_heat:
    fig_heat = px.choropleth(
        df,
        locations="fips",
        locationmode="USA-states",   # overridden by geojson below
        color="heating_coverage_pct",
        color_continuous_scale="RdYlGn",
        range_color=[0, 100],
        scope="usa",
        labels={"heating_coverage_pct": "Heating Coverage (%)"},
        title="DC Waste Heat vs. County Space-Heating Demand  (capped at 200%)",
    )
    fig_heat.update_traces(
        locationmode="geojson-id",
        geojson=_GEOJSON,
        z=df["heating_coverage_pct"],
        locations=df["fips"],
        hovertext=_heat_hover(df),
        hovertemplate="%{hovertext}<extra></extra>",
    )
    fig_heat.update_layout(
        coloraxis_colorbar=dict(title="Coverage (%)"),
        margin=dict(l=0, r=0, t=40, b=0),
        height=520,
    )
    st.plotly_chart(fig_heat, use_container_width=True)

with tab_cool:
    fig_cool = px.choropleth(
        df,
        locations="fips",
        color="cooling_coverage_pct",
        color_continuous_scale="Blues",
        range_color=[0, 100],
        scope="usa",
        labels={"cooling_coverage_pct": "Cooling Coverage (%)"},
        title="DC Waste Heat vs. County Space-Cooling Demand  (capped at 200%)",
    )
    fig_cool.update_traces(
        locationmode="geojson-id",
        geojson=_GEOJSON,
        z=df["cooling_coverage_pct"],
        locations=df["fips"],
        hovertext=_cool_hover(df),
        hovertemplate="%{hovertext}<extra></extra>",
    )
    fig_cool.update_layout(
        coloraxis_colorbar=dict(title="Coverage (%)"),
        margin=dict(l=0, r=0, t=40, b=0),
        height=520,
    )
    st.plotly_chart(fig_cool, use_container_width=True)

# ---------------------------------------------------------------------------
# Space-Type breakdown
# ---------------------------------------------------------------------------
with tab_types:
    # Use the unfiltered DC frame to always show the full picture here
    dc_all = st.session_state["dc_df"]

    # 1) National bar: IT Load + DC count per Space Type
    nat = (
        dc_all.groupby("space_type")
              .agg(count=("name", "count"), mw=("it_load_mw", "sum"))
              .reindex(list(SPACE_TYPES), fill_value=0)
              .reset_index()
    )
    nat["share_pct"] = 100 * nat["mw"] / nat["mw"].sum() if nat["mw"].sum() else 0

    c1, c2 = st.columns(2)
    with c1:
        fig_mw = px.bar(
            nat, x="space_type", y="mw", color="space_type",
            color_discrete_map=_TYPE_COLOUR,
            text="mw",
            title="Total IT Load by Space Type (MW)",
            labels={"space_type": "Space Type", "mw": "IT Load (MW)"},
        )
        fig_mw.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig_mw.update_layout(showlegend=False, height=360, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_mw, use_container_width=True)
    with c2:
        fig_n = px.bar(
            nat, x="space_type", y="count", color="space_type",
            color_discrete_map=_TYPE_COLOUR,
            text="count",
            title="DC Count by Space Type",
            labels={"space_type": "Space Type", "count": "# Data Centers"},
        )
        fig_n.update_traces(textposition="outside")
        fig_n.update_layout(showlegend=False, height=360, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_n, use_container_width=True)

    # 2) Classification-source breakdown (QA visibility)
    if "classification_source" in dc_all.columns:
        src = (
            dc_all["classification_source"].value_counts()
                  .reindex(["P1", "P2", "P3", "P4"], fill_value=0)
                  .reset_index()
        )
        src.columns = ["source", "count"]
        src["label"] = src["source"].map({
            "P1": "P1 IT Load threshold",
            "P2": "P2 Operator name",
            "P3": "P3 Web scrape",
            "P4": "P4 Default Small",
        })
        fig_src = px.bar(
            src, x="label", y="count", color="source",
            text="count",
            title="How Each DC Was Classified (P1–P4 Source)",
            labels={"label": "Classification Step", "count": "# Data Centers"},
        )
        fig_src.update_traces(textposition="outside")
        fig_src.update_layout(showlegend=False, height=340, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_src, use_container_width=True)

    # 3) Top-10 counties by IT Load with stacked Space-Type composition
    comp_cols = {f"it_load_mw_{s.lower().replace(' ', '_').replace('/', '_')}": s
                 for s in SPACE_TYPES}
    comp_cols_present = {c: s for c, s in comp_cols.items() if c in df_full.columns}
    if comp_cols_present:
        top10 = df_full.nlargest(10, "total_it_load_mw")[
            ["county_label", *comp_cols_present.keys()]
        ].copy()
        long = top10.melt(
            id_vars="county_label",
            value_vars=list(comp_cols_present.keys()),
            var_name="type_col", value_name="mw",
        )
        long["space_type"] = long["type_col"].map(comp_cols_present)
        fig_top = px.bar(
            long, x="mw", y="county_label", color="space_type",
            orientation="h",
            color_discrete_map=_TYPE_COLOUR,
            category_orders={"space_type": list(SPACE_TYPES)},
            title="Top 10 Counties by IT Load — Space-Type Composition",
            labels={"mw": "IT Load (MW)", "county_label": "County"},
        )
        fig_top.update_layout(height=420, margin=dict(l=10, r=10, t=40, b=10),
                              yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_top, use_container_width=True)


# ---------------------------------------------------------------------------
# Scatter — IT Load vs Heating Demand (MW), sized by coverage
# ---------------------------------------------------------------------------
with tab_scatter:
    scatter_df = df.dropna(subset=["total_it_load_mw", "heating_demand_mw",
                                    "heating_coverage_pct"])
    fig_sc = px.scatter(
        scatter_df,
        x="total_it_load_mw",
        y="heating_demand_mw",
        size="heating_coverage_pct",
        color="heating_coverage_pct",
        color_continuous_scale="RdYlGn",
        range_color=[0, 100],
        hover_name="county_label" if "county_label" in scatter_df.columns else "fips",
        hover_data={
            "county_label":          False,   # already in hover_name
            "fips":                  True,
            "dc_count":              True,
            "total_it_load_mw":      ":.1f",
            "heating_demand_mw":     ":.1f",
            "heat_delivered_mw":     ":.1f",
            "heating_coverage_pct":  ":.1f",
        },
        labels={
            "total_it_load_mw":     "Total IT Load (MW)",
            "heating_demand_mw":    "Heating Demand (avg MW)",
            "heat_delivered_mw":    "Heat Delivered (avg MW)",
            "heating_coverage_pct": "Heating Coverage (%)",
            "dc_count":             "# DCs in county",
        },
        title="County IT Load (MW) vs. Space-Heating Demand (MW)  —  bubble size = coverage %",
        log_x=True,
        log_y=True,
    )
    fig_sc.update_layout(height=480)
    st.plotly_chart(fig_sc, use_container_width=True)

# ---------------------------------------------------------------------------
# Data table — sortable, filterable
# ---------------------------------------------------------------------------
with tab_table:
    display_cols = [
        "county_label", "fips", "dc_count",
        "total_it_load_mw",
        "heat_delivered_mw",    "heating_demand_mw",
        "cooling_delivered_mw", "cooling_demand_mw",
        "heating_coverage_pct", "cooling_coverage_pct",
    ]
    show_cols = [c for c in display_cols if c in df.columns]

    # Rename for readability
    col_labels = {
        "county_label":          "County",
        "fips":                  "FIPS",
        "dc_count":              "# DCs",
        "total_it_load_mw":      "IT Load (MW)",
        "heat_delivered_mw":     "Heat Delivered (MW)",
        "heating_demand_mw":     "Heat Demand (MW)",
        "cooling_delivered_mw":  "Cooling Delivered (MW)",
        "cooling_demand_mw":     "Cooling Demand (MW)",
        "heating_coverage_pct":  "Heating Coverage (%)",
        "cooling_coverage_pct":  "Cooling Coverage (%)",
    }

    min_cov = st.slider("Filter: min heating coverage (%)", 0, 200, 0, step=5)
    filtered = (
        df[df["heating_coverage_pct"] >= min_cov][show_cols]
        .rename(columns=col_labels)
        .sort_values("Heating Coverage (%)", ascending=False)
        .reset_index(drop=True)
    )

    st.dataframe(
        filtered,
        use_container_width=True,
        height=420,
        column_config={
            "IT Load (MW)":          st.column_config.NumberColumn(format="%.1f"),
            "Heat Delivered (MW)":   st.column_config.NumberColumn(format="%.1f"),
            "Heat Demand (MW)":      st.column_config.NumberColumn(format="%.1f"),
            "Cooling Delivered (MW)":st.column_config.NumberColumn(format="%.1f"),
            "Cooling Demand (MW)":   st.column_config.NumberColumn(format="%.1f"),
            "Heating Coverage (%)":  st.column_config.ProgressColumn(
                                        min_value=0, max_value=200, format="%.1f %%"),
            "Cooling Coverage (%)":  st.column_config.ProgressColumn(
                                        min_value=0, max_value=200, format="%.1f %%"),
        },
    )
    st.caption(f"{len(filtered):,} counties shown")

    csv_bytes = filtered.to_csv(index=False).encode()
    st.download_button(
        "⬇ Download filtered CSV",
        data=csv_bytes,
        file_name="dc_waste_heat_counties.csv",
        mime="text/csv",
    )
