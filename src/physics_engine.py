"""
physics_engine.py
-----------------
Unit conversions and waste-heat physics for DC thermal recovery analysis.

All public functions are pure (no side effects) and accept scalars or
pandas Series / numpy arrays interchangeably.

Key equations
-------------
Annual DC waste heat recoverable:
    Q_DC  = P_MW × 8760 h/yr × 1000 kW/MW × η_recovery        [kWh/yr]

Useful heat delivered to building heating system (via heat pump or HEX):
    Q_heat = Q_DC × COP_heating    (heat pump mode)
    Q_cool = Q_DC × COP_cooling    (absorption/adsorption cooling mode)

Unit conversion:
    1 MBtu = 293.07107 kWh   (exact: 1 BTU = 1055.05585 J → 1 MBtu = 293.07107 kWh)
"""

from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Physical constants (SI-consistent)
# ---------------------------------------------------------------------------
KWH_PER_MBTU: float = 293.07107       # 1 MBtu → kWh
MBTU_PER_KWH: float = 1 / KWH_PER_MBTU
HOURS_PER_YEAR: int = 8_760           # non-leap year
KW_PER_MW: int = 1_000

# ---------------------------------------------------------------------------
# Default operational parameters
# ---------------------------------------------------------------------------
RECOVERY_EFFICIENCY: float = 0.90     # fraction of DC waste heat captured
HEATING_COP: float = 3.0              # effective COP for heat delivery to buildings
COOLING_COP: float = 0.70             # COP for waste-heat-driven cooling (absorption)

Numeric = Union[float, np.ndarray, "pd.Series"]


# ---------------------------------------------------------------------------
# Unit conversions
# ---------------------------------------------------------------------------
def mbtu_to_kwh(mbtu: Numeric) -> Numeric:
    """Convert MBtu to kWh.  1 MBtu = 293.07107 kWh."""
    return mbtu * KWH_PER_MBTU


def kwh_to_mbtu(kwh: Numeric) -> Numeric:
    """Convert kWh to MBtu.  1 kWh ≈ 0.003412 MBtu."""
    return kwh * MBTU_PER_KWH


def mw_to_kwh_per_year(mw: Numeric) -> Numeric:
    """Convert average electrical power (MW) to annual energy (kWh/yr)."""
    return mw * KW_PER_MW * HOURS_PER_YEAR


# ---------------------------------------------------------------------------
# Waste heat physics
# ---------------------------------------------------------------------------
def waste_heat_kwh(
    it_load_mw: Numeric,
    pue: Numeric,
    recovery_efficiency: float = RECOVERY_EFFICIENCY,
) -> Numeric:
    """
    Compute annual recoverable waste heat from a data center.

    Parameters
    ----------
    it_load_mw : IT electrical load in MW (not total facility load).
    pue        : Power Usage Effectiveness of the facility.
    recovery_efficiency : Fraction of waste heat that can be captured (0–1).

    Returns
    -------
    Annual recoverable waste heat in kWh/yr.

    Notes
    -----
    Total facility load  = IT load × PUE
    Waste heat           = (PUE - 1) × IT load   [the non-IT overhead]
    Recoverable heat     = Waste heat × η_recovery
    """
    total_facility_mw = it_load_mw * pue
    waste_mw = total_facility_mw - it_load_mw          # overhead heat rejection
    recoverable_kwh = mw_to_kwh_per_year(waste_mw) * recovery_efficiency
    return recoverable_kwh


def heat_delivered_kwh(
    recoverable_kwh: Numeric,
    heating_cop: float = HEATING_COP,
) -> Numeric:
    """
    Useful space-heating energy delivered after coupling to a heat pump.

    Heat pump amplifies the waste heat source:
        Q_delivered = Q_recoverable × COP_heating

    Parameters
    ----------
    recoverable_kwh : Recoverable waste heat (kWh/yr) from waste_heat_kwh().
    heating_cop     : System-level heating COP (default 3.0).
    """
    return recoverable_kwh * heating_cop


def cooling_delivered_kwh(
    recoverable_kwh: Numeric,
    cooling_cop: float = COOLING_COP,
) -> Numeric:
    """
    Useful cooling energy from waste-heat-driven absorption/adsorption chiller.

    Parameters
    ----------
    recoverable_kwh : Recoverable waste heat (kWh/yr).
    cooling_cop     : COP of the thermally-driven cooling system (default 0.7).
    """
    return recoverable_kwh * cooling_cop


# ---------------------------------------------------------------------------
# Coverage ratios
# ---------------------------------------------------------------------------
def heating_coverage_ratio(
    heat_delivered_kwh_: Numeric,
    county_heating_kwh: Numeric,
) -> Numeric:
    """
    Fraction of county annual space-heating demand coverable by DC waste heat.

    Returns values in [0, ∞).  Values > 1 mean waste heat exceeds total demand.
    NaN is returned when county demand is 0 or missing.
    """
    county = pd.to_numeric(county_heating_kwh, errors="coerce")
    delivered = pd.to_numeric(heat_delivered_kwh_, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(county > 0, delivered / county, np.nan)
    return ratio


def cooling_coverage_ratio(
    cooling_delivered_kwh_: Numeric,
    county_cooling_kwh: Numeric,
) -> Numeric:
    """Fraction of county annual space-cooling demand coverable by DC waste heat."""
    county = pd.to_numeric(county_cooling_kwh, errors="coerce")
    delivered = pd.to_numeric(cooling_delivered_kwh_, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(county > 0, delivered / county, np.nan)
    return ratio
