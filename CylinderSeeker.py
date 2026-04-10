#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════════════╗
║           CylinderSeeker — Hydraulic Elevator Market Research Agent       ║
║           Target: US Hydraulic Elevator Modernization TAM                 ║
║           Output: cylinder_seeker_market_data.csv                         ║
╚═══════════════════════════════════════════════════════════════════════════╝

Usage:
    python3 CylinderSeeker.py
    python3 CylinderSeeker.py --cities 10          # Run first N cities only
    python3 CylinderSeeker.py --resume             # Resume from last checkpoint
    python3 CylinderSeeker.py --output my_data.csv # Custom output filename
"""

import os
import re
import json
import time
import random
import argparse
import logging
import subprocess
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd
from tqdm import tqdm

# ── Load .env if present (optional dependency) ────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed — rely on shell environment variables
    pass

# ─────────────────────────────────────────────────────────────────────────────
# ██  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# ┌─────────────────────────────────────────────────────────┐
# │  API KEY — set via environment variable or .env file    │
# │  export PERPLEXITY_API_KEY="pplx-..."                   │
# └─────────────────────────────────────────────────────────┘
PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")

PERPLEXITY_MODEL   = "sonar-pro"          # sonar | sonar-pro | sonar-reasoning
API_ENDPOINT       = "https://api.perplexity.ai/chat/completions"
REQUEST_TIMEOUT    = 45                   # seconds per API call
DELAY_BETWEEN_CITIES = 2.5               # seconds (be gentle on the API)
JITTER             = 1.0                  # random jitter added to delay
MAX_RETRIES        = 3                    # retries per failed request
OUTPUT_FILE        = "cylinder_seeker_market_data.csv"
CHECKPOINT_FILE    = "cylinder_seeker_checkpoint.json"
LOG_FILE           = "cylinder_seeker.log"
GITHUB_REPO        = "ohad-boop/cylinder-seeker"   # auto-push target
GITHUB_AUTO_PUSH   = True                           # set False to disable

# Google Drive folder IDs (pre-created structure)
GDRIVE_ENABLED           = True
GDRIVE_CYLINDER_SEEKER_FOLDER = "1nFZqbG2AYItFiRmqI0Ntk94MXL2tbggK"  # OpenClaw/Agents/CylinderSeeker
GDRIVE_ACCOUNT           = "ohad@geothermico.com"

# ─────────────────────────────────────────────────────────────────────────────
# ██  TOP 50 US METROPOLITAN STATISTICAL AREAS
# ─────────────────────────────────────────────────────────────────────────────

TOP_50_MSAs = [
    # (City Label, State, Approx Pop Rank)
    ("New York",          "NY"),
    ("Los Angeles",       "CA"),
    ("Chicago",           "IL"),
    ("Dallas",            "TX"),
    ("Houston",           "TX"),
    ("Washington DC",     "DC"),
    ("Philadelphia",      "PA"),
    ("Miami",             "FL"),
    ("Atlanta",           "GA"),
    ("Phoenix",           "AZ"),
    ("Boston",            "MA"),
    ("Riverside",         "CA"),
    ("Seattle",           "WA"),
    ("Minneapolis",       "MN"),
    ("San Diego",         "CA"),
    ("Tampa",             "FL"),
    ("Denver",            "CO"),
    ("St. Louis",         "MO"),
    ("Baltimore",         "MD"),
    ("Portland",          "OR"),
    ("San Antonio",       "TX"),
    ("Sacramento",        "CA"),
    ("Orlando",           "FL"),
    ("Pittsburgh",        "PA"),
    ("Austin",            "TX"),
    ("Las Vegas",         "NV"),
    ("Cincinnati",        "OH"),
    ("Kansas City",       "MO"),
    ("Columbus",          "OH"),
    ("Indianapolis",      "IN"),
    ("Cleveland",         "OH"),
    ("San Jose",          "CA"),
    ("Nashville",         "TN"),
    ("Virginia Beach",    "VA"),
    ("Jacksonville",      "FL"),
    ("Charlotte",         "NC"),
    ("Raleigh",           "NC"),
    ("Hartford",          "CT"),
    ("Salt Lake City",    "UT"),
    ("New Orleans",       "LA"),
    ("Buffalo",           "NY"),
    ("Richmond",          "VA"),
    ("Providence",        "RI"),
    ("Memphis",           "TN"),
    ("Oklahoma City",     "OK"),
    ("Louisville",        "KY"),
    ("Birmingham",        "AL"),
    ("Milwaukee",         "WI"),
    ("Tucson",            "AZ"),
    ("Rochester",         "NY"),
]

# ─────────────────────────────────────────────────────────────────────────────
# ██  LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("CylinderSeeker")

# ─────────────────────────────────────────────────────────────────────────────
# ██  PROMPTS
# ─────────────────────────────────────────────────────────────────────────────

PRIMARY_PROMPT = """You are a building infrastructure research analyst. I need a sourced market estimate for hydraulic elevators in {city}, {state}.

## Approach (use in this order):

**Step 1 — Direct data (preferred):** Search for state elevator inspection records, city/county permit databases, NAESA data, or Elevator World industry reports that mention {city} or {state} elevator counts.

**Step 2 — National ratio method (fallback):** If no direct city data exists, use this documented approach:
- The US has approximately 900,000 elevators total (source: National Elevator Industry Inc. / Bureau of Labor Statistics)
- Roughly 30% are hydraulic type (~270,000 nationally)
- Distribute by metro population share (use Census MSA population data)
- {city} MSA population ÷ US population (335 million) × 270,000 = estimated hydraulic elevators

**Step 3 — Industry ratios for breakdown:**
- ~65% of hydraulic elevators are 20+ years old (installed pre-2004) per Elevator World industry reports
- ~55% of pre-1995 units are single-bottom in-ground cylinder type per ASME A17.1 historical install data
- ~10-15% modernization rate in last decade per NAESA industry surveys

For EACH field, specify:
- The numeric value you calculated
- Whether it came from direct data (cite URL) or ratio-based derivation (state which ratio and cite its source)
- A brief quote or description of the source

Return ONLY valid JSON (no markdown, no preamble):
{{
  "total_hydraulic_elevators": {{
    "value": <integer>,
    "method": "<direct|ratio-derived>",
    "source_name": "<source name>",
    "source_url": "<url or empty string>",
    "evidence_quote": "<quote, paraphrase, or derivation explanation>"
  }},
  "units_past_lifespan": {{
    "value": <integer>,
    "method": "<direct|ratio-derived>",
    "source_name": "<source name>",
    "source_url": "<url or empty string>",
    "evidence_quote": "<derivation: e.g. X × 65% per Elevator World>"
  }},
  "units_needing_drilling": {{
    "value": <integer>,
    "method": "<direct|ratio-derived>",
    "source_name": "<source name>",
    "source_url": "<url or empty string>",
    "evidence_quote": "<derivation explanation>"
  }},
  "units_already_modernized": {{
    "value": <integer>,
    "method": "<direct|ratio-derived>",
    "source_name": "<source name>",
    "source_url": "<url or empty string>",
    "evidence_quote": "<derivation explanation>"
  }},
  "msa_population": <integer>,
  "population_source_url": "<census url>",
  "data_quality": "<high|medium|low|estimated>",
  "overall_notes": "<brief methodology summary>"
}}"""

PROXY_PROMPT = """You are a research analyst. I need census and building data to estimate hydraulic elevator counts in {city}, {state}.

Find the following with citations:

1. **MSA population** of {city}, {state} from US Census (census.gov). Use this to derive elevator estimate:
   - Formula: (MSA population ÷ 335,000,000) × 270,000 = estimated hydraulic elevators
   - Source for 270,000 figure: National Elevator Industry Inc. estimates ~900,000 total US elevators, ~30% hydraulic

2. **Low-rise commercial buildings (2-6 stories, built 1970-1995)** — from Census American Community Survey, CoStar, or city permit data.

Return ONLY valid JSON:
{{
  "msa_population": {{
    "value": <integer or null>,
    "source_name": "<e.g. US Census Bureau 2023 ACS>",
    "source_url": "<census.gov url>",
    "evidence_quote": "<quote or paraphrase>"
  }},
  "derived_hydraulic_estimate": {{
    "value": <integer or null>,
    "source_name": "Ratio derivation: NEII national elevator count × hydraulic share × population ratio",
    "source_url": "https://www.neii.org",
    "evidence_quote": "NEII reports ~900,000 total US elevators; ~30% hydraulic = 270,000; applied MSA population ratio"
  }},
  "low_rise_commercial_1970_1995": {{
    "value": <integer or null>,
    "source_name": "<source or null>",
    "source_url": "<url or empty>",
    "evidence_quote": "<quote or null>"
  }},
  "overall_notes": "<confidence and methodology>"
}}"""

# ─────────────────────────────────────────────────────────────────────────────
# ██  PERPLEXITY API CALLER
# ─────────────────────────────────────────────────────────────────────────────

def call_perplexity(prompt: str, retries: int = MAX_RETRIES) -> dict:
    """Call Perplexity API and return parsed JSON from the response."""
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "model": PERPLEXITY_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a precise market research analyst. "
                    "Always respond with valid JSON only — no markdown, no code blocks, no preamble. "
                    "Your response must start with { and end with }."
                )
            },
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 1500,
        "return_citations": True,
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                API_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()

            # Strip markdown code fences if model wraps in ```json ... ```
            content = re.sub(r"^```(?:json)?\s*", "", content)
            content = re.sub(r"\s*```$", "", content)
            content = content.strip()

            # Find first { ... } block
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if match:
                return json.loads(match.group())
            else:
                raise ValueError(f"No JSON object found in response: {content[:200]}")

        except requests.exceptions.Timeout:
            log.warning(f"  Timeout on attempt {attempt}/{retries}")
        except requests.exceptions.HTTPError as e:
            log.warning(f"  HTTP error {e.response.status_code} on attempt {attempt}/{retries}")
            if e.response.status_code in (401, 403):
                raise RuntimeError("Invalid API key — check PERPLEXITY_API_KEY") from e
            if e.response.status_code == 429:
                wait = 30 * attempt
                log.warning(f"  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
        except json.JSONDecodeError as e:
            log.warning(f"  JSON parse error on attempt {attempt}: {e}")
        except Exception as e:
            log.warning(f"  Unexpected error on attempt {attempt}: {e}")

        if attempt < retries:
            sleep = 5 * attempt + random.uniform(0, 2)
            log.info(f"  Retrying in {sleep:.1f}s...")
            time.sleep(sleep)

    return {}  # Empty dict signals failure

# ─────────────────────────────────────────────────────────────────────────────
# ██  CHECKPOINT MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"completed": [], "rows": []}

def save_checkpoint(checkpoint: dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)

# ─────────────────────────────────────────────────────────────────────────────
# ██  CORE RESEARCH FUNCTION (per city)
# ─────────────────────────────────────────────────────────────────────────────

def extract_field(data: dict, field: str) -> tuple:
    """Extract value, source_name, source_url, evidence_quote from a nested field dict."""
    f = data.get(field, {})
    if isinstance(f, dict):
        return (
            f.get("value"),
            f.get("source_name"),
            f.get("source_url"),
            f.get("evidence_quote")
        )
    # Fallback: old flat format
    return (f, None, None, None)


def research_city(city: str, state: str) -> tuple:
    """
    Run primary + optional proxy research for one city.
    Returns: (row dict for CSV, evidence list for audit log)
    """
    log.info(f"  Querying primary data for {city}, {state}...")

    row = {
        "city":                      city,
        "state":                     state,
        "total_hydraulic_elevators": None,
        "units_past_lifespan":       None,
        "units_needing_drilling":    None,
        "units_already_modernized":  None,
        "data_quality":              None,
        "proxy_used":                False,
        "low_rise_buildings_proxy":  None,
        "estimated_from_proxy":      None,
        "notes":                     None,
        "all_sources":               None,
        "query_timestamp":           datetime.utcnow().isoformat() + "Z",
        "status":                    "ok"
    }

    # Evidence log entries for this city
    evidence = []

    def add_evidence(field, value, source_name, source_url, quote, derived=False):
        evidence.append({
            "city":        city,
            "state":       state,
            "field":       field,
            "value":       value,
            "source_name": source_name or ("DERIVED" if derived else "NOT FOUND"),
            "source_url":  source_url  or "",
            "quote":       quote       or "",
            "derived":     derived,
            "timestamp":   datetime.utcnow().isoformat() + "Z"
        })

    # ── Primary query ───────────────────────────────────────────────────────
    primary = call_perplexity(PRIMARY_PROMPT.format(city=city, state=state))

    all_sources = []

    if primary:
        row["data_quality"] = primary.get("data_quality", "estimated")
        row["notes"]        = primary.get("overall_notes", "")

        for field, row_key in [
            ("total_hydraulic_elevators", "total_hydraulic_elevators"),
            ("units_past_lifespan",       "units_past_lifespan"),
            ("units_needing_drilling",    "units_needing_drilling"),
            ("units_already_modernized",  "units_already_modernized"),
        ]:
            f = primary.get(field, {})
            if isinstance(f, dict):
                val      = f.get("value")
                src_name = f.get("source_name") or f.get("method")
                src_url  = f.get("source_url")
                quote    = f.get("evidence_quote")
            else:
                val, src_name, src_url, quote = f, None, None, None
            row[row_key] = val
            add_evidence(field, val, src_name, src_url, quote)
            if src_url:
                all_sources.append(src_url)

    else:
        log.warning(f"  Primary query failed for {city}.")
        row["status"]       = "primary_failed"
        row["data_quality"] = "failed"

    # ── Proxy fallback ───────────────────────────────────────────────────────
    if row["total_hydraulic_elevators"] is None:
        log.info(f"  No elevator count — running proxy query for {city}...")
        time.sleep(1.5 + random.uniform(0, 0.5))

        proxy = call_perplexity(PROXY_PROMPT.format(city=city, state=state))
        if proxy:
            row["proxy_used"] = True

            # Population-derived estimate
            pop_field = proxy.get("msa_population", {})
            if isinstance(pop_field, dict):
                pop_val = pop_field.get("value")
                add_evidence("msa_population", pop_val,
                             pop_field.get("source_name"), pop_field.get("source_url"),
                             pop_field.get("evidence_quote"))
                if pop_field.get("source_url"):
                    all_sources.append(pop_field["source_url"])

            est_field = proxy.get("derived_hydraulic_estimate", {})
            if isinstance(est_field, dict):
                est_val = est_field.get("value")
                row["estimated_from_proxy"] = est_val
                add_evidence("derived_hydraulic_estimate", est_val,
                             est_field.get("source_name"), est_field.get("source_url"),
                             est_field.get("evidence_quote"))
                if est_field.get("source_url"):
                    all_sources.append(est_field["source_url"])
                if est_val:
                    row["total_hydraulic_elevators"] = est_val
                    row["data_quality"] = "proxy"

            lr_field = proxy.get("low_rise_commercial_1970_1995", {})
            if isinstance(lr_field, dict):
                lr_val = lr_field.get("value")
                row["low_rise_buildings_proxy"] = lr_val
                add_evidence("low_rise_commercial_1970_1995", lr_val,
                             lr_field.get("source_name"), lr_field.get("source_url"),
                             lr_field.get("evidence_quote"))
                if lr_field.get("source_url"):
                    all_sources.append(lr_field["source_url"])

            proxy_notes = proxy.get("overall_notes", "")
            row["notes"] = f"{row['notes'] or ''} | PROXY: {proxy_notes}".strip(" |")

    # ── Derived estimates (flagged clearly) ──────────────────────────────────
    total = row["total_hydraulic_elevators"]
    if total:
        if row["units_past_lifespan"] is None:
            derived_val = int(total * 0.65)
            row["units_past_lifespan"] = derived_val
            add_evidence(
                "units_past_lifespan", derived_val,
                "DERIVED: 65% of total — industry benchmark (NAESA/Elevator World)",
                "https://www.elevatorworld.com", "~65% of hydraulic elevators installed pre-2004 are past 20-year lifespan per industry consensus",
                derived=True
            )

        if row["units_needing_drilling"] is None:
            past = row["units_past_lifespan"] or int(total * 0.65)
            derived_val = int(past * 0.55)
            row["units_needing_drilling"] = derived_val
            add_evidence(
                "units_needing_drilling", derived_val,
                "DERIVED: 55% of aging units — single-bottom cylinder prevalence (ASME A17.1 era)",
                "https://www.asme.org/codes-standards/find-codes-standards/a17-1-safety-code-elevators-escalators",
                "~55% of pre-1995 hydraulic elevators used conventional single-bottom in-ground cylinders per ASME A17.1 historical install data",
                derived=True
            )

        if row["units_already_modernized"] is None:
            derived_val = int(total * 0.12)
            row["units_already_modernized"] = derived_val
            add_evidence(
                "units_already_modernized", derived_val,
                "DERIVED: 12% modernization rate — industry average (Elevator World 2022)",
                "https://www.elevatorworld.com",
                "Approximately 10-15% of aging hydraulic elevator stock has been modernized in the last decade per industry reports",
                derived=True
            )

    row["all_sources"] = " | ".join(set(filter(None, all_sources)))
    return row, evidence

# ─────────────────────────────────────────────────────────────────────────────
# ██  GITHUB AUTO-COMMIT & PUSH
# ─────────────────────────────────────────────────────────────────────────────

def gdrive_upload(output_file: str, evidence_file: str, md_file: str, run_label: str):
    """Create a timestamped run folder in Google Drive and upload all result files."""
    if not GDRIVE_ENABLED:
        return

    try:
        # Create a run folder under CylinderSeeker
        result = subprocess.run(
            ["gog", "drive", "mkdir", run_label,
             "--parent", GDRIVE_CYLINDER_SEEKER_FOLDER,
             "--account", GDRIVE_ACCOUNT, "--json"],
            capture_output=True, text=True, check=True
        )
        folder_data = json.loads(result.stdout)
        run_folder_id = folder_data["folder"]["id"]
        folder_url    = folder_data["folder"]["webViewLink"]
        log.info(f"  Google Drive: run folder created → {folder_url}")

        # Upload files
        files_to_upload = [output_file, evidence_file, md_file, "CylinderSeeker.py", "README.md"]
        for f in files_to_upload:
            if not Path(f).exists():
                continue
            up = subprocess.run(
                ["gog", "drive", "upload", f,
                 "--parent", run_folder_id,
                 "--account", GDRIVE_ACCOUNT, "--json"],
                capture_output=True, text=True, check=True
            )
            up_data = json.loads(up.stdout)
            log.info(f"  Google Drive: uploaded {f} → {up_data['file']['webViewLink']}")

        print(f"\n  📂 Google Drive: results uploaded → {folder_url}")

    except subprocess.CalledProcessError as e:
        log.warning(f"  ⚠ Google Drive upload failed: {e.stderr}")
        print(f"\n  ⚠ Google Drive upload failed (results still saved locally + GitHub)")
    except Exception as e:
        log.warning(f"  ⚠ Google Drive error: {e}")


def git_push(output_file: str, evidence_file: str, md_file: str):
    """Commit and push results to GitHub after each run."""
    if not GITHUB_AUTO_PUSH:
        return

    repo_dir = Path(__file__).parent
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    try:
        files_to_add = [output_file, evidence_file, md_file, LOG_FILE]
        subprocess.run(
            ["git", "add"] + [f for f in files_to_add if Path(repo_dir / f).exists()],
            cwd=repo_dir, check=True, capture_output=True
        )

        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=repo_dir, capture_output=True
        )
        if result.returncode == 0:
            log.info("  GitHub: nothing new to commit.")
            return

        subprocess.run(
            ["git", "commit", "-m", f"CylinderSeeker run — {timestamp}"],
            cwd=repo_dir, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "push", "origin", "main"],
            cwd=repo_dir, check=True, capture_output=True
        )
        log.info(f"  ✅ GitHub: results pushed to https://github.com/{GITHUB_REPO}")
        print(f"\n  🐙 GitHub: pushed to https://github.com/{GITHUB_REPO}")

    except subprocess.CalledProcessError as e:
        log.warning(f"  ⚠ GitHub push failed: {e.stderr.decode() if e.stderr else str(e)}")
        print(f"\n  ⚠ GitHub push failed (results still saved locally)")


# ─────────────────────────────────────────────────────────────────────────────
# ██  MAIN AGENT LOOP
# ─────────────────────────────────────────────────────────────────────────────

def run_agent(cities_limit: int = None, resume: bool = False, output_file: str = OUTPUT_FILE):
    print("\n" + "═"*70)
    print("  🔍  CylinderSeeker — Hydraulic Elevator Market Research Agent")
    print(f"  Model: {PERPLEXITY_MODEL}  |  Output: {output_file}")
    print("═"*70 + "\n")

    if not PERPLEXITY_API_KEY or PERPLEXITY_API_KEY == "YOUR_PERPLEXITY_API_KEY_HERE":
        print("❌  ERROR: Set your PERPLEXITY_API_KEY at the top of this script.")
        return

    cities = TOP_50_MSAs
    if cities_limit:
        cities = cities[:cities_limit]

    # Resume from checkpoint
    checkpoint = load_checkpoint() if resume else {"completed": [], "rows": []}
    completed   = set(checkpoint.get("completed", []))
    rows        = checkpoint.get("rows", [])

    remaining = [(c, s) for c, s in cities if c not in completed]

    if resume and completed:
        print(f"  ▶  Resuming: {len(completed)} cities done, {len(remaining)} remaining.\n")

    failed_cities = []
    all_evidence  = checkpoint.get("evidence", [])

    with tqdm(total=len(cities), initial=len(completed), unit="city",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:

        for city, state in remaining:
            pbar.set_description(f"{city}, {state}")
            log.info(f"\n{'─'*50}")
            log.info(f"Processing: {city}, {state}")

            try:
                row, evidence = research_city(city, state)
                rows.append(row)
                all_evidence.extend(evidence)
                completed.add(city)

                # Save checkpoint after every city
                save_checkpoint({"completed": list(completed), "rows": rows, "evidence": all_evidence})

                status_icon = "✓" if row["status"] == "ok" else "⚠"
                tqdm.write(
                    f"  {status_icon} {city}, {state:2s} — "
                    f"Elevators: {row['total_hydraulic_elevators'] or 'N/A':>6} | "
                    f"Needs drill: {row['units_needing_drilling'] or 'N/A':>5} | "
                    f"Quality: {row['data_quality'] or '?'}"
                )

            except Exception as e:
                log.error(f"  ✗ FAILED {city}: {e}")
                failed_cities.append(city)
                rows.append({
                    "city": city, "state": state,
                    "status": f"error: {str(e)[:100]}",
                    "query_timestamp": datetime.utcnow().isoformat() + "Z"
                })
                all_evidence.append({
                    "city": city, "state": state, "field": "ALL",
                    "value": None, "source_name": "ERROR", "source_url": "",
                    "quote": str(e), "derived": False,
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                })

            finally:
                pbar.update(1)

            # Polite delay between cities
            if (city, state) != remaining[-1]:
                sleep = DELAY_BETWEEN_CITIES + random.uniform(0, JITTER)
                time.sleep(sleep)

    # ── Build DataFrame & export ─────────────────────────────────────────────
    df = pd.DataFrame(rows)

    col_order = [
        "city", "state",
        "total_hydraulic_elevators", "units_past_lifespan",
        "units_needing_drilling", "units_already_modernized",
        "data_quality", "proxy_used", "low_rise_buildings_proxy",
        "estimated_from_proxy", "notes", "all_sources", "query_timestamp", "status"
    ]
    for col in col_order:
        if col not in df.columns:
            df[col] = None
    df = df[col_order]
    df.to_csv(output_file, index=False)

    # ── Evidence / Audit Log ─────────────────────────────────────────────────
    evidence_file = output_file.replace(".csv", "_EVIDENCE_LOG.csv")
    ev_df = pd.DataFrame(all_evidence)
    ev_col_order = ["city", "state", "field", "value", "derived",
                    "source_name", "source_url", "quote", "timestamp"]
    for col in ev_col_order:
        if col not in ev_df.columns:
            ev_df[col] = None
    ev_df = ev_df[ev_col_order]
    ev_df.to_csv(evidence_file, index=False)

    # ── Markdown Audit Report ────────────────────────────────────────────────
    md_file = output_file.replace(".csv", "_AUDIT_REPORT.md")
    with open(md_file, "w") as md:
        md.write(f"# CylinderSeeker — Evidence & Source Audit Report\n\n")
        md.write(f"**Generated:** {datetime.utcnow().isoformat()}Z  \n")
        md.write(f"**Model:** {PERPLEXITY_MODEL}  \n")
        md.write(f"**Cities:** {len(df)}  \n\n")
        md.write("---\n\n")
        md.write("> ⚠️ Fields marked `DERIVED` are calculated using published industry ratios, not direct data. ")
        md.write("Verify derived figures against the cited sources before using in investor materials.\n\n")
        md.write("---\n\n")

        for _, city_row in df.iterrows():
            city_name = f"{city_row['city']}, {city_row['state']}"
            md.write(f"## {city_name}\n\n")
            md.write(f"**Data Quality:** `{city_row.get('data_quality', 'unknown')}`  \n")
            md.write(f"**Proxy Used:** `{city_row.get('proxy_used', False)}`  \n\n")

            city_evidence = [e for e in all_evidence
                             if e["city"] == city_row["city"] and e["state"] == city_row["state"]]

            if city_evidence:
                md.write("| Field | Value | Derived? | Source | URL | Evidence Quote |\n")
                md.write("|---|---|---|---|---|---|\n")
                for e in city_evidence:
                    derived_flag = "⚠️ Yes" if e.get("derived") else "No"
                    quote = (e.get("quote") or "").replace("\n", " ").replace("|", "/")[:120]
                    src_url = e.get("source_url") or ""
                    src_link = f"[link]({src_url})" if src_url else "—"
                    md.write(
                        f"| {e.get('field','')} "
                        f"| {e.get('value','N/A')} "
                        f"| {derived_flag} "
                        f"| {e.get('source_name','') or '—'} "
                        f"| {src_link} "
                        f"| {quote} |\n"
                    )
            else:
                md.write("_No evidence entries recorded._\n")

            notes = city_row.get("notes") or ""
            if notes:
                md.write(f"\n**Notes:** {notes}\n")
            md.write("\n---\n\n")

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "═"*70)
    print("  📊  CYLINDERSEEKER — SUMMARY REPORT")
    print("═"*70)

    valid = df[df["total_hydraulic_elevators"].notna()]
    print(f"\n  Cities processed:         {len(df)}")
    print(f"  Cities with data:         {len(valid)}")
    print(f"  Cities failed/no data:    {len(df) - len(valid)}")

    if len(valid) > 0:
        total_elevators  = valid["total_hydraulic_elevators"].sum()
        total_aging      = valid["units_past_lifespan"].sum()
        total_drilling   = valid["units_needing_drilling"].sum()
        total_modernized = valid["units_already_modernized"].sum()
        remaining_tam    = total_drilling - total_modernized if total_modernized else total_drilling

        print(f"\n  ── TAM ESTIMATES (Top {len(cities)} US MSAs) ──")
        print(f"  Total hydraulic elevators:      {int(total_elevators):>10,}")
        print(f"  Units past lifespan (aging):    {int(total_aging):>10,}")
        print(f"  Units needing in-ground drill:  {int(total_drilling):>10,}")
        print(f"  Already modernized:             {int(total_modernized):>10,}")
        print(f"  ► Remaining TAM (unaddressed):  {int(remaining_tam):>10,}")

        quality_counts = valid["data_quality"].value_counts().to_dict()
        print(f"\n  Data quality breakdown: {quality_counts}")

    if failed_cities:
        print(f"\n  ⚠  Failed cities: {', '.join(failed_cities)}")

    print(f"\n  ✅  Data saved to:         {output_file}")
    print(f"  📋  Evidence log saved to: {evidence_file}")
    print(f"  📄  Audit report saved to: {md_file}")
    print(f"  🪵  Run log saved to:      {LOG_FILE}")
    print("═"*70 + "\n")

    # Auto-push to GitHub
    git_push(output_file, evidence_file, md_file)

    # Auto-upload to Google Drive
    run_label = f"Run_{datetime.utcnow().strftime('%Y-%m-%d')}_{len(df)}cities"
    gdrive_upload(output_file, evidence_file, md_file, run_label)

    # Clean up checkpoint on success
    if not failed_cities and Path(CHECKPOINT_FILE).exists():
        Path(CHECKPOINT_FILE).unlink()

    return df

# ─────────────────────────────────────────────────────────────────────────────
# ██  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CylinderSeeker — Hydraulic Elevator Market Research Agent"
    )
    parser.add_argument("--cities",  type=int,  default=None,
                        help="Limit to first N cities (default: all 50)")
    parser.add_argument("--resume",  action="store_true",
                        help="Resume from last checkpoint")
    parser.add_argument("--output",  type=str,  default=OUTPUT_FILE,
                        help=f"Output CSV filename (default: {OUTPUT_FILE})")
    args = parser.parse_args()

    run_agent(
        cities_limit=args.cities,
        resume=args.resume,
        output_file=args.output
    )
