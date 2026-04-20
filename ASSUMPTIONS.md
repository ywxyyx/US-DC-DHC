# Assumptions & Formulas — DC Waste Heat Recovery Analysis

> Last updated: 2026-04-20
> Status markers: ✅ = grounded in literature / data  |  ⚠️ = placeholder, needs replacement

---

## 1. Data Center Parameters

### 1.1 IT Load (MW)

| Item | Current value | Source | Status |
|------|--------------|--------|--------|
| IT load per facility | Scraped from datacentermap.com (`mw_builtout` or `mw_referenced`) | datacentermap.com | ✅ |
| Missing IT load fill | **Excluded from MW calculation** (3,404 / 4,879 DCs have no reported capacity) | — | ⚠️ Should impute from building size / facility type if available |

### 1.2 Power Usage Effectiveness (PUE)

| Item | Current value | Source | Status |
|------|--------------|--------|--------|
| Per-facility PUE | Scraped from datacentermap.com when available | datacentermap.com | ✅ |
| Default PUE for missing values | **1.58** | Uptime Institute Global Data Center Survey 2022 (US average) | ⚠️ Global average; replace with state- or facility-type-specific values if available |

Waste heat fraction of total facility load:

$$f_{\text{waste}} = \frac{PUE - 1}{PUE}$$

With PUE = 1.58: $f_{\text{waste}} = 0.367$ (36.7% of total facility electricity becomes waste heat).

---

## 2. Waste Heat Recovery

### 2.1 Recovery Efficiency ($\eta_{\text{recovery}}$)

| Item | Current value | Source | Status |
|------|--------------|--------|--------|
| $\eta_{\text{recovery}}$ | **0.90** | Engineering assumption | ⚠️ Placeholder — depends on cooling technology (air-cooled vs. liquid-cooled), distance to heat network, pipe insulation. Typical range: 0.6–0.95 |

### 2.2 Annual Recoverable Waste Heat

$$Q_{\text{recoverable}} \;[\text{kWh/yr}] = P_{\text{IT}} \;[\text{MW}] \times (PUE - 1) \times 1000 \times 8760 \times \eta_{\text{recovery}}$$

As average power:

$$P_{\text{recoverable}} \;[\text{MW}] = P_{\text{IT}} \times (PUE - 1) \times \eta_{\text{recovery}}$$

> **Key assumption:** DC operates at **100% capacity factor** (8,760 h/yr).
> ⚠️ Real DCs fluctuate; typical average utilisation is 50–70% of nameplate IT load.
> Replacing with measured annual energy (MWh/yr) from utility data would eliminate this assumption.

---

## 3. Heat Delivery to Buildings

### 3.1 Heating COP ($COP_{\text{heating}}$)

| Item | Current value | Source | Status |
|------|--------------|--------|--------|
| $COP_{\text{heating}}$ | **3.0** | Engineering assumption | ⚠️ Placeholder — represents the amplification factor when waste heat drives a heat pump or district heating network. Typical range: 2.5–5.0 depending on source/sink temperatures |

This is **not** the COP of a conventional ASHP consuming grid electricity. It describes how much useful building heat is delivered per unit of recovered waste heat fed into the system.

### 3.2 Useful Heat Delivered

$$Q_{\text{delivered}} \;[\text{kWh/yr}] = Q_{\text{recoverable}} \times COP_{\text{heating}}$$

$$P_{\text{heat\_delivered}} \;[\text{MW}] = P_{\text{IT}} \times (PUE - 1) \times \eta_{\text{recovery}} \times COP_{\text{heating}}$$

With defaults (PUE = 1.58, $\eta$ = 0.90, COP = 3.0):

$$P_{\text{heat\_delivered}} = P_{\text{IT}} \times 0.58 \times 0.90 \times 3.0 = P_{\text{IT}} \times 1.566$$

---

## 4. Cooling Delivery

### 4.1 Cooling COP ($COP_{\text{cooling}}$)

| Item | Current value | Source | Status |
|------|--------------|--------|--------|
| $COP_{\text{cooling}}$ | **0.70** | Engineering assumption | ⚠️ Placeholder — COP of a thermally-driven absorption or adsorption chiller using waste heat as the driving energy. Single-effect absorption chillers: 0.6–0.8; double-effect: 1.0–1.4 |

### 4.2 Useful Cooling Delivered

$$Q_{\text{cooling\_delivered}} \;[\text{kWh/yr}] = Q_{\text{recoverable}} \times COP_{\text{cooling}}$$

$$P_{\text{cooling\_delivered}} \;[\text{MW}] = P_{\text{IT}} \times (PUE - 1) \times \eta_{\text{recovery}} \times COP_{\text{cooling}}$$

---

## 5. Building Energy Demand (NREL EULP)

### 5.1 Data sources

| Dataset | Release | Coverage | Status |
|---------|---------|----------|--------|
| ResStock 2022 (`resstock_amy2018_release_1.1`) | 2022 | Residential, all US counties | ✅ |
| ComStock 2023 (`comstock_amy2018_release_2`) | 2023 | Commercial, all US counties | ✅ |

### 5.2 Demand calculation

County-level annual space-heating / cooling demand is aggregated from building-level samples using NREL's provided `weight` column:

$$D_{\text{county}} \;[\text{kWh/yr}] = \sum_i w_i \times E_i$$

where $w_i$ = number of real buildings represented by sample $i$, and $E_i$ = sampled building's annual energy (kWh).

Converted to average power for comparison with DC IT load:

$$P_{\text{demand}} \;[\text{MW}] = \frac{D_{\text{county}} \;[\text{kWh/yr}]}{8760 \times 1000}$$

### 5.3 Heating demand scope

| End-use included | Fuels | Notes |
|-----------------|-------|-------|
| Space heating | Electricity, natural gas, fuel oil, propane (all HP backup variants) | Full fuel mix; not electricity-only |
| Space cooling | Electricity + fans/pumps (residential); electricity + district cooling (commercial) | |

> ⚠️ Climate year: both ResStock and ComStock use **AMY 2018** (Actual Meteorological Year).
> Results represent 2018 weather conditions — an average or TMY year would give different totals.

---

## 6. Coverage Ratios

### 6.1 Heating Coverage Ratio

$$R_{\text{heating}} = \frac{P_{\text{heat\_delivered}}}{P_{\text{heating\_demand}}}$$

- $R = 0.5$ → DC waste heat could cover 50% of county space-heating demand
- $R > 1.0$ → waste heat exceeds total county demand (display capped at 200% in the map)

### 6.2 Cooling Coverage Ratio

$$R_{\text{cooling}} = \frac{P_{\text{cooling\_delivered}}}{P_{\text{cooling\_demand}}}$$

---

## 7. Unit Conversions

| Conversion | Factor |
|-----------|--------|
| 1 MBtu → kWh | × 293.07107 |
| 1 kWh → MBtu | × 0.003412142 |
| 1 kBtu → kWh | × 0.2930711 |
| MW (avg) → kWh/yr | × 1000 × 8760 |
| kWh/yr → MW (avg) | ÷ (1000 × 8760) |

---

## 8. Items That Need Real Data (Priority Order)

| # | Assumption | Ideal replacement | Data source candidates |
|---|-----------|------------------|----------------------|
| 1 | 100% DC capacity factor | Measured annual energy consumption (MWh/yr) | Utility disclosure, EPA ENERGY STAR, state energy commissions |
| 2 | Missing IT load (3,404 DCs) | Facility-level capacity from operator reports or permit data | DOE FEDS, state PUC filings, EPA eGRID plant-level data |
| 3 | Default PUE = 1.58 | Facility-reported or operator-disclosed PUE | EPA ENERGY STAR, operator ESG reports, DOE Better Buildings |
| 4 | $\eta_{\text{recovery}} = 0.90$ | Technology- and distance-specific capture efficiency | Engineering studies; depends on liquid-cooled vs. air-cooled, pipe routing |
| 5 | $COP_{\text{heating}} = 3.0$ | Source/sink temperature-dependent COP curve | ASHRAE, heat pump manufacturer specs, local ground/water temperatures |
| 6 | $COP_{\text{cooling}} = 0.70$ | Absorption chiller model calibrated to local conditions | ASHRAE 90.1, chiller manufacturer datasheets |
| 7 | AMY 2018 weather year | Multi-year average or scenario years (2018, 2019, 2020…) | NREL EULP future releases; NOAA TMY3 |
