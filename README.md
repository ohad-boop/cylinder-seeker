# 🔍 CylinderSeeker

**Automated Market Research Agent — US Hydraulic Elevator Modernization TAM**

Built for Geothermico to map the Total Addressable Market for compact, confined-space drilling technology targeting in-ground hydraulic cylinder replacements.

---

## What It Does

CylinderSeeker loops through the **Top 50 US Metropolitan Statistical Areas**, queries Perplexity AI for each city, and extracts:

| Data Point | Description |
|---|---|
| Total Hydraulic Elevators | Estimated installed base in the metro |
| Units Past Lifespan | Units 20-25+ years old needing modernization |
| Units Needing In-Ground Drilling | Single-bottom cylinder type requiring jack-hole re-drilling |
| Units Already Modernized | Recently converted units (5-10 years) |
| Proxy Data | Low-rise commercial buildings 1970-1995 (fallback) |
| Citations | Source URLs for all data points |

---

## Setup

### 1. Install dependencies

```bash
pip install requests pandas tqdm
```

### 2. API Key

The API key is already set in the script. To update it:
Open `CylinderSeeker.py` and find line ~35:

```python
PERPLEXITY_API_KEY = "your-key-here"
```

### 3. Run it

```bash
# Full run — all 50 cities
python3 CylinderSeeker.py

# Test with first 5 cities only
python3 CylinderSeeker.py --cities 5

# Resume if interrupted
python3 CylinderSeeker.py --resume

# Custom output filename
python3 CylinderSeeker.py --output my_results.csv
```

---

## Output

- **`cylinder_seeker_market_data.csv`** — Full structured dataset
- **`cylinder_seeker.log`** — Detailed run log
- **`cylinder_seeker_checkpoint.json`** — Auto-saved progress (deleted on clean finish)

---

## Estimated Run Time

~50 cities × ~5 seconds each ≈ **~5 minutes total**

---

## Proxy Logic

If Perplexity cannot find direct elevator data for a city, CylinderSeeker automatically:
1. Queries for low-rise commercial buildings (2-6 stories) built 1970-1995
2. Estimates hydraulic elevator count from building count
3. Flags the row with `proxy_used = True` and `data_quality = "proxy"`

---

## Derived Estimates

When specific breakdowns are missing, the agent applies industry averages:
- **65%** of total hydraulic elevators estimated past lifespan
- **55%** of aging units are single-bottom cylinder type (needing drilling)
- **12%** modernization rate (already converted in last 10 years)
