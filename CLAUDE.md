# DC Waste Heat Recovery Analysis — 项目上下文

> Last updated: 2026-04-20
> 上级 CLAUDE.md: `/Users/youweixian/Desktop/PhD/CLAUDE.md`

## 项目目标

将美国 4,879 个数据中心的废热潜力与 NREL EULP 县级供暖/制冷需求匹配，
计算每个县的废热覆盖率，通过 Streamlit 仪表板可视化。

## 目录结构

```
DC-waste heat/
├── data/
│   ├── raw_dc_states/          # 51 个州的 DC CSV（来自 datacentermap.com）
│   └── nrel/
│       ├── county_space_heating_cooling.csv   # NREL EULP 县级供暖/制冷需求
│       ├── census_counties_2023.csv           # Census 地名录（FIPS→县名，自动下载）
│       └── dc_geocoded.parquet                # 反向地理编码缓存
├── processed/
│   └── us_county_analysis.csv  # 最终输出（353 行 × 18 列）
├── src/
│   ├── data_loader.py          # 合并 51 州 CSV，模糊列名匹配
│   ├── geocoding.py            # (lat,lon) → 5位FIPS，用 reverse_geocoder + Census 地名录
│   ├── physics_engine.py       # 物理公式 + 单位换算（纯函数，无副作用）
│   └── analyzer.py             # 完整流水线：load→geocode→aggregate→merge→coverage→save
├── app.py                      # Streamlit 仪表板
├── ASSUMPTIONS.md              # ⭐ 所有假设、公式、待替换项清单
└── requirements.txt
```

## 快速运行

```bash
# 在此目录下运行
cd "/Users/youweixian/Desktop/PhD/DC-waste heat"

# 重新跑分析（参数变更后）
python -c "from src.analyzer import run_analysis; run_analysis(force_regeocode=False)"

# 启动仪表板
streamlit run app.py
# 默认端口 8501（若占用自动升为 8502）
```

## 输出列说明（processed/us_county_analysis.csv）

| 列名 | 含义 | 单位 |
|------|------|------|
| `fips` | 5位县级 FIPS（零填充字符串） | — |
| `county_name` | 县名（如 "Cook County"） | — |
| `state_abbr` | 州缩写（如 "IL"） | — |
| `county_label` | 显示标签（如 "Cook County, IL"） | — |
| `dc_count` | 该县有 IT Load 数据的 DC 数量 | — |
| `total_it_load_mw` | 县内 DC IT Load 之和 | MW |
| `total_recoverable_kwh` | 年可回收废热总量 | kWh/yr |
| `total_heat_delivered_kwh` | 年可输送供暖热量 | kWh/yr |
| `total_cooling_delivered_kwh` | 年可输送制冷量 | kWh/yr |
| `total_space_heating_kwh` | NREL 县级供暖需求 | kWh/yr |
| `total_space_cooling_kwh` | NREL 县级制冷需求 | kWh/yr |
| `heating_coverage_ratio` | 供暖覆盖率（原始值，可 >1） | — |
| `cooling_coverage_ratio` | 制冷覆盖率（原始值，可 >1） | — |
| `heating_coverage_pct` | 供暖覆盖率（上限 200%，用于地图色阶） | % |
| `cooling_coverage_pct` | 制冷覆盖率（上限 200%） | % |
| `heating_demand_mw` | 供暖需求（平均功率） | MW |
| `cooling_demand_mw` | 制冷需求（平均功率） | MW |
| `heat_delivered_mw` | 可输送供暖热量（平均功率） | MW |
| `cooling_delivered_mw` | 可输送制冷量（平均功率） | MW |

## 核心公式

$$P_{\text{heat\_delivered}} = P_{\text{IT}} \times (PUE - 1) \times \eta_{\text{recovery}} \times COP_{\text{heating}}$$

$$R_{\text{heating}} = \frac{P_{\text{heat\_delivered}}}{P_{\text{heating\_demand}}}$$

**默认参数：** PUE 缺失值 = 1.58，η = 0.90，COP_heat = 3.0，COP_cool = 0.70

完整假设推导和待替换项见 `ASSUMPTIONS.md`。

## 数据来源

| 数据 | 原始位置 | 脚本 |
|------|----------|------|
| DC 爬虫原始文件 | `2025/Gas2Geo_02/states/` → 已复制到 `data/raw_dc_states/` | `2025/Gas2Geo_02/scrape_dc_usa_v2.py` |
| NREL EULP 下载脚本 | `2025/Gas2Geo_02/eulp_county_heating_cooling.py` | 产出 `data/nrel/county_space_heating_cooling.csv` |

## 已知局限（详见 ASSUMPTIONS.md §8）

1. DC 全年按 100% 满载计算（实际 ~50–70%）
2. 3,404 个 DC 无 IT Load，被排除在外
3. 4,744 个 DC 无 PUE，用全国均值 1.58 填充
4. 反向地理编码失败率 4.2%（205 / 4,874），主要是边境地区
5. NREL 数据使用 AMY 2018 气候年
