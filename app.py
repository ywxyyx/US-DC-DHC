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
    DEFAULT_NREL_CSV,
    DEFAULT_OUTPUT_CSV,
    DEFAULT_RAW_DIR,
    run_analysis,
)
from src.physics_engine import (
    COOLING_COP,
    HEATING_COP,
    RECOVERY_EFFICIENCY,
)

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
    "Source: datacentermap.com (4 879 DCs) × NREL EULP ResStock/ComStock 2022/23"
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
# Session state — cache analysis result across rerenders
# ---------------------------------------------------------------------------
if "result_df" not in st.session_state:
    st.session_state["result_df"] = None
if "last_params" not in st.session_state:
    st.session_state["last_params"] = None

current_params = (recovery_efficiency, heating_cop, cooling_cop)

# Re-run if button clicked OR parameters changed since last run
if run_btn or st.session_state["last_params"] != current_params:
    with st.spinner("Running analysis … (first run downloads Census gazetteer ~2 MB)"):
        df = run_analysis(
            raw_dir=DEFAULT_RAW_DIR,
            nrel_csv=DEFAULT_NREL_CSV,
            output_csv=DEFAULT_OUTPUT_CSV,
            recovery_efficiency=recovery_efficiency,
            heating_cop=heating_cop,
            cooling_cop=cooling_cop,
        )
    st.session_state["result_df"] = df
    st.session_state["last_params"] = current_params

df: pd.DataFrame | None = st.session_state["result_df"]

# ---------------------------------------------------------------------------
# Load cached results if analysis hasn't run yet this session
# ---------------------------------------------------------------------------
if df is None:
    if DEFAULT_OUTPUT_CSV.exists():
        df = pd.read_csv(DEFAULT_OUTPUT_CSV, dtype={"fips": str})
        df["fips"] = df["fips"].str.zfill(5)
        st.session_state["result_df"] = df
        st.info("Showing cached results. Adjust sliders and click **Run / Refresh** to recompute.")
    else:
        st.warning("No results yet. Click **▶ Run / Refresh Analysis** in the sidebar.")
        st.stop()

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

total_dc           = df["dc_count"].sum()
total_it_mw        = df["total_it_load_mw"].sum()
counties_w_dc      = len(df)
median_hcov        = df["heating_coverage_pct"].median()
total_heat_dem_mw  = df["heating_demand_mw"].sum() if "heating_demand_mw" in df.columns else None

col1.metric("Total DCs Geocoded",        f"{int(total_dc):,}")
col2.metric("Total IT Load",             f"{total_it_mw:,.0f} MW")
col3.metric("Total Heating Demand",
            f"{total_heat_dem_mw:,.0f} MW" if total_heat_dem_mw is not None else "—",
            help="Sum of county annual space-heating demand expressed as average power")
col4.metric("Counties with DC Presence", f"{counties_w_dc:,}")
col5.metric("Median Heating Coverage",   f"{median_hcov:.1f} %")

st.divider()

# ---------------------------------------------------------------------------
# Choropleth — Heating Coverage (%)
# ---------------------------------------------------------------------------
tab_heat, tab_cool, tab_scatter, tab_table = st.tabs(
    ["🌡️ Heating Coverage Map", "❄️ Cooling Coverage Map", "📊 Scatter Plot", "📋 Data Table"]
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
