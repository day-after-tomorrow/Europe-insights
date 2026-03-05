#!/usr/bin/env python3
"""
TÜV SÜD Competitive Intelligence — Backend Fetcher
====================================================
Fetches RSS + World Bank data for 15 European markets.
Outputs intelligence_data.json consumed by the dashboard.

Install:  pip install requests feedparser
Run:      python fetch_intelligence.py
Single:   python fetch_intelligence.py --country Poland
"""

import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import feedparser
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tic")

# ── Countries ──────────────────────────────────────────────────────────────────
COUNTRIES = [
    "Poland", "Czech Republic", "Slovakia", "Hungary", "Romania", "Turkey",
    "United Kingdom", "France", "Belgium", "Italy", "Switzerland",
    "Spain", "Austria", "Denmark", "Slovenia",
]

WORLD_BANK = {
    "Poland": "POL", "Czech Republic": "CZE", "Slovakia": "SVK",
    "Hungary": "HUN", "Romania": "ROU", "Turkey": "TUR",
    "United Kingdom": "GBR", "France": "FRA", "Belgium": "BEL",
    "Italy": "ITA", "Switzerland": "CHE", "Spain": "ESP",
    "Austria": "AUT", "Denmark": "DNK", "Slovenia": "SVN",
}

CENTRAL_BANKS = {
    "Poland":         ("NBP",  "5.75"),
    "Czech Republic": ("CNB",  "3.75"),
    "Slovakia":       ("ECB",  "4.50"),
    "Hungary":        ("MNB",  "6.50"),
    "Romania":        ("BNR",  "6.50"),
    "Turkey":         ("CBRT", "42.50"),
    "United Kingdom": ("BoE",  "4.75"),
    "France":         ("ECB",  "4.50"),
    "Belgium":        ("ECB",  "4.50"),
    "Italy":          ("ECB",  "4.50"),
    "Switzerland":    ("SNB",  "1.00"),
    "Spain":          ("ECB",  "4.50"),
    "Austria":        ("ECB",  "4.50"),
    "Denmark":        ("DN",   "3.35"),
    "Slovenia":       ("ECB",  "4.50"),
}

ACCRED = {
    "Poland": "PCA", "Czech Republic": "ČIA", "Slovakia": "SNAS",
    "Hungary": "NAH", "Romania": "RENAR", "Turkey": "TÜRKAK",
    "United Kingdom": "UKAS", "France": "COFRAC", "Belgium": "BELAC",
    "Italy": "ACCREDIA", "Switzerland": "SAS", "Spain": "ENAC",
    "Austria": "BMAW", "Denmark": "DANAK", "Slovenia": "SA",
}

CURRENCY = {
    "Poland": "PLN", "Czech Republic": "CZK", "Slovakia": "EUR",
    "Hungary": "HUF", "Romania": "RON", "Turkey": "TRY",
    "United Kingdom": "GBP", "France": "EUR", "Belgium": "EUR",
    "Italy": "EUR", "Switzerland": "CHF", "Spain": "EUR",
    "Austria": "EUR", "Denmark": "DKK", "Slovenia": "EUR",
}

COMPETITORS_CORE = [
    "DEKRA", "TÜV Rheinland", "SGS", "Bureau Veritas",
    "TÜV Austria", "TÜV Nord", "Eurofins", "Intertek",
]
COMPETITORS_EXTENDED = ["Applus+", "Lloyd's Register", "DNV"]
ALL_COMPETITORS = COMPETITORS_CORE + COMPETITORS_EXTENDED

def comp_q():
    return " OR ".join(f'"{c}"' for c in ALL_COMPETITORS)

# ── Feed query builder ─────────────────────────────────────────────────────────
def feed_queries(country: str) -> list[tuple[str, str]]:
    c  = country
    cq = comp_q()
    ac = ACCRED[country]
    return [
        ("competitor",
         f'({cq}) "{c}" certification testing inspection laboratory announce 2025 2026'),
        ("jobs",
         f'({cq}) "{c}" auditor inspector engineer "laboratory manager" hiring jobs 2025 2026'),
        ("market",
         f'certification inspection regulation "{c}" ISO CSRD DORA "Cyber Resilience Act" EV battery 2025 2026'),
        ("investments",
         f'investment factory plant "{c}" EV battery automotive energy gigafactory semiconductor pharma "data centre" 2025 2026'),
        ("regulatory",
         '"Cyber Resilience Act" OR "CSRD" OR "DORA" OR "AI Act" '
         'certification testing inspection accreditation 2025 2026'),
        ("ma",
         f'({cq}) OR "TIC sector" OR "testing inspection certification" '
         f'acquisition merger buyout "private equity" laboratory deal 2025 2026'),
        ("standards",
         f'ISO IEC "new standard" "standard update" ILAC {ac} "{c}" '
         f'accreditation laboratory certification 2025 2026'),
        ("tenders",
         f'"{c}" tender contract "testing services" OR "inspection services" '
         f'OR "certification services" government infrastructure EU-funded 2025 2026'),
        ("emerging",
         f'"{c}" "EV battery certification" OR "cybersecurity audit" OR "AI certification" '
         f'OR "medical device" OR "hydrogen certification" OR "digital product passport" 2025 2026'),
        ("esg",
         f'"{c}" CSRD "scope 3" "ESG audit" "green hydrogen" "sustainability assurance" '
         f'"carbon verification" "ESG certification" 2025 2026'),
    ]

# ── Fetch helpers ──────────────────────────────────────────────────────────────
TIMEOUT   = 12
MAX_ITEMS = 6
HEADERS   = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

def gnews_url(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"

def score_urgency(title: str) -> str:
    t = title.lower()
    if any(k in t for k in [
        "acqui", "merger", "launch", "expan", "wins", "awarded", "secures",
        "major", "new lab", "opens", "completes", "closes deal", "takeover",
        "buyout", "ipo", "strategic",
    ]):
        return "high"
    if any(k in t for k in [
        "hire", "hiring", "invest", "partner", "certif", "announces",
        "tender", "contract", "standard", "regulation", "accredit",
    ]):
        return "medium"
    return "low"

def fetch_rss(feed_id: str, query: str, country: str) -> tuple[str, list[dict]]:
    try:
        resp  = requests.get(gnews_url(query), headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        feed  = feedparser.parse(resp.content)
        items = []
        for entry in feed.entries[:MAX_ITEMS]:
            title = entry.get("title", "").strip()
            if not title:
                continue
            items.append({
                "title":   title,
                "link":    entry.get("link", "").strip(),
                "source":  entry.get("source", {}).get("title", ""),
                "pubDate": entry.get("published", ""),
                "urgency": score_urgency(title),
            })
        log.info("  ✓ %-12s %-24s %d items", feed_id, f"({country})", len(items))
        return feed_id, items
    except Exception as exc:
        log.warning("  ✗ %-12s %-24s %s", feed_id, f"({country})", exc)
        return feed_id, []

def fetch_world_bank(wb_code: str) -> dict:
    result = {}
    for key, ind in [("gdp", "NY.GDP.MKTP.KD.ZG"), ("inflation", "FP.CPI.TOTL.ZG")]:
        try:
            url  = (f"https://api.worldbank.org/v2/country/{wb_code}"
                    f"/indicator/{ind}?format=json&mrv=3&per_page=3")
            data = requests.get(url, timeout=TIMEOUT).json()
            rows = [r for r in (data[1] or []) if r.get("value") is not None]
            result[key] = {"value": round(rows[0]["value"], 2), "year": rows[0]["date"]} if rows else None
            if rows:
                log.info("  ✓ WorldBank  %-10s %s → %.2f%%", key, wb_code, rows[0]["value"])
        except Exception as exc:
            log.warning("  ✗ WorldBank  %-10s %s  %s", key, wb_code, exc)
            result[key] = None
    return result

# ── Per-country fetch ──────────────────────────────────────────────────────────
def fetch_country(country: str) -> dict:
    log.info("── %s %s", country, "─" * max(1, 44 - len(country)))
    cb_name, cb_rate = CENTRAL_BANKS[country]
    feeds: dict[str, list] = {}

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(fetch_rss, fid, query, country): fid
            for fid, query in feed_queries(country)
        }
        for f in as_completed(futures):
            fid, items = f.result()
            feeds[fid] = items

    macro = fetch_world_bank(WORLD_BANK[country])
    high_count = sum(1 for v in feeds.values() for i in v if i.get("urgency") == "high")

    return {
        "country":   country,
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "highCount": high_count,
        "feeds":     feeds,
        "macro": {
            "gdp":          macro.get("gdp"),
            "inflation":    macro.get("inflation"),
            "interestRate": {
                "bank":  cb_name,
                "value": f"{cb_rate}%",
                "note":  "Official rate — update manually after central bank decisions",
            },
        },
    }

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="TIC Intelligence Fetcher")
    parser.add_argument("--country", choices=COUNTRIES)
    parser.add_argument("--output",  default="intelligence_data.json")
    parser.add_argument("--delay",   type=float, default=1.5)
    args = parser.parse_args()

    countries = [args.country] if args.country else COUNTRIES
    output    = Path(args.output)

    existing: dict[str, dict] = {}
    if output.exists():
        try:
            existing = {d["country"]: d for d in json.loads(output.read_text())["countries"]}
            log.info("Loaded existing data for: %s", ", ".join(existing.keys()))
        except Exception:
            pass

    results = dict(existing)
    t0 = time.time()

    for i, country in enumerate(countries):
        if i > 0:
            time.sleep(args.delay)
        try:
            results[country] = fetch_country(country)
        except Exception as exc:
            log.error("Failed %s: %s", country, exc)

    ordered = [results[c] for c in COUNTRIES if c in results]

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "countries":   ordered,
        "meta": {
            "competitors":     ALL_COMPETITORS,
            "competitorsCore": COMPETITORS_CORE,
            "accredBodies":    ACCRED,
            "currencies":      CURRENCY,
            "feedLabels": {
                "competitor": "Competitor Moves",
                "jobs":       "Hiring Signals",
                "market":     "Market & Regulatory",
                "investments":"Investment Projects",
                "regulatory": "EU Regulatory Watch",
                "ma":         "M&A Activity",
                "standards":  "Accreditation & Standards",
                "tenders":    "Tenders & Contracts",
                "emerging":   "Emerging Sectors",
                "esg":        "Sustainability & ESG",
            },
        },
    }

    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    elapsed     = time.time() - t0
    total_items = sum(len(v) for r in ordered for v in r["feeds"].values())
    log.info("")
    log.info("✅  Done in %.1fs  →  %s", elapsed, output)
    log.info("    Countries: %d  ·  Total items: %d", len(ordered), total_items)

if __name__ == "__main__":
    main()
