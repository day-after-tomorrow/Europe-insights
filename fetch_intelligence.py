#!/usr/bin/env python3
"""
TÜV SÜD Competitive Intelligence — Backend Fetcher  v3
========================================================
Fetches Google News RSS + World Bank data for 15 European markets.
Outputs intelligence_data.json consumed by the dashboard.

ROOT CAUSE FIX: Google News RSS silently returns 0 results for queries
longer than ~80 characters or with complex boolean chains. All queries
are now ≤60 characters — short, focused keyword sets that reliably work.

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

# Short aliases used in search queries (GNews works better with these)
SEARCH_NAME = {
    "United Kingdom": "UK",
    "Czech Republic": "Czech",
}

def sn(country):
    return SEARCH_NAME.get(country, country)

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
    "Poland": "PCA", "Czech Republic": "CIA", "Slovakia": "SNAS",
    "Hungary": "NAH", "Romania": "RENAR", "Turkey": "TURKAK",
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
    "DEKRA", "TUV Rheinland", "SGS", "Bureau Veritas",
    "TUV Austria", "TUV Nord", "Eurofins", "Intertek",
]
COMPETITORS_EXTENDED = ["Applus", "Lloyds Register", "DNV"]
ALL_COMPETITORS = COMPETITORS_CORE + COMPETITORS_EXTENDED


# ── Feed queries ───────────────────────────────────────────────────────────────
# KEY RULE: keep every query under 65 characters.
# Google News RSS returns 0 results silently for longer/complex queries.
# No long OR chains, no multi-word quoted strings, no parentheses.

def feed_queries(country: str) -> list[tuple[str, str, str]]:
    """Return list of (feed_id, label, query) tuples."""
    c = sn(country)
    return [
        ("competitor", "Competitor Moves",
            f"DEKRA SGS Intertek {c} testing certification"),

        ("competitor2", "Competitor Moves",           # second pass: BV + Eurofins
            f"Bureau Veritas Eurofins {c} laboratory inspection"),

        ("jobs", "Hiring Signals",
            f"DEKRA SGS Intertek {c} hiring auditor jobs"),

        ("jobs2", "Hiring Signals",                    # second pass: more roles
            f"Bureau Veritas Eurofins {c} recruit engineer inspector"),

        ("ma", "M&A Activity",
            f"DEKRA SGS Eurofins {c} acquisition merger deal"),

        ("ma2", "M&A Activity",
            f"Bureau Veritas Intertek {c} laboratory buyout 2025"),

        ("market", "Market & Regulatory",
            f"{c} certification inspection regulation laboratory"),

        ("regulatory", "EU Regulatory Watch",
            f"CSRD DORA AI Act certification compliance 2025"),

        ("tenders", "Tenders & Contracts",
            f"{c} inspection certification tender contract government"),

        ("standards", "Accreditation & Standards",
            f"{c} ISO standard accreditation laboratory 2025"),

        ("investments", "Investment Projects",
            f"{c} EV battery factory gigafactory investment 2025"),

        ("investments2", "Investment Projects",
            f"{c} energy plant automotive semiconductor investment"),

        ("emerging", "Emerging Sectors",
            f"{c} cybersecurity AI certification medical device"),

        ("emerging2", "Emerging Sectors",
            f"{c} hydrogen EV battery digital certification 2025"),

        ("esg", "Sustainability & ESG",
            f"{c} CSRD ESG sustainability audit carbon 2025"),
    ]


# ── Fetch helpers ──────────────────────────────────────────────────────────────
TIMEOUT   = 15
MAX_ITEMS = 8   # fetch more per query since we merge duplicates
HEADERS   = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def gnews_url(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"


def score_urgency(title: str) -> str:
    t = title.lower()
    if any(k in t for k in [
        "acqui", "merger", "acquires", "takeover", "buyout",
        "launches", "launch", "opens", "expands", "expansion",
        "wins contract", "awarded", "secures contract",
        "ipo", "record deal", "major contract",
    ]):
        return "high"
    if any(k in t for k in [
        "hire", "hiring", "recruit", "invest", "investment",
        "partner", "certif", "announce", "tender", "contract",
        "standard", "regulation", "accredit", "compliance",
        "new service", "new lab", "new facility",
    ]):
        return "medium"
    return "low"


def clean_title(raw: str) -> tuple[str, str]:
    """Google News appends ' - Source Name' — split it off."""
    if " - " in raw:
        parts = raw.rsplit(" - ", 1)
        return parts[0].strip(), parts[1].strip()
    return raw.strip(), ""


def fetch_rss(feed_id: str, query: str, country: str) -> tuple[str, list[dict]]:
    url = gnews_url(query)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items  = []
        for entry in parsed.entries[:MAX_ITEMS]:
            raw   = entry.get("title", "").strip()
            if not raw:
                continue
            title, src_guess = clean_title(raw)
            source = entry.get("source", {}).get("title", "") or src_guess
            items.append({
                "title":   title,
                "link":    entry.get("link", "").strip(),
                "source":  source,
                "pubDate": entry.get("published", ""),
                "urgency": score_urgency(title),
            })
        log.info("  ✓ %-14s (%s) %d items — q: %s",
                 feed_id, country, len(items), query[:55])
        return feed_id, items
    except Exception as exc:
        log.warning("  ✗ %-14s (%s) ERROR: %s", feed_id, country, exc)
        return feed_id, []


def fetch_world_bank(wb_code: str) -> dict:
    result = {}
    for key, ind in [
        ("gdp",       "NY.GDP.MKTP.KD.ZG"),
        ("inflation", "FP.CPI.TOTL.ZG"),
    ]:
        try:
            url  = (f"https://api.worldbank.org/v2/country/{wb_code}"
                    f"/indicator/{ind}?format=json&mrv=3&per_page=3")
            data = requests.get(url, timeout=TIMEOUT).json()
            rows = [r for r in (data[1] or []) if r.get("value") is not None]
            if rows:
                result[key] = {"value": round(rows[0]["value"], 2), "year": rows[0]["date"]}
                log.info("  ✓ WorldBank %-10s %s → %.2f%%", key, wb_code, rows[0]["value"])
            else:
                result[key] = None
        except Exception as exc:
            log.warning("  ✗ WorldBank %-10s %s %s", key, wb_code, exc)
            result[key] = None
    return result


# ── Feed ID mapping — queries with suffix (competitor2, jobs2 …) merge into base ──
BASE_FEEDS = [
    "competitor", "jobs", "ma", "market", "regulatory",
    "tenders", "standards", "investments", "emerging", "esg",
]
MAX_PER_FEED = 6   # cap items per logical feed after merging

def merge_feeds(raw_feeds: dict[str, list]) -> dict[str, list]:
    """
    Merge *2 query results into their base feed, deduplicate by title,
    cap at MAX_PER_FEED items, sort HIGH → MED → LOW.
    """
    merged: dict[str, list] = {f: [] for f in BASE_FEEDS}
    urgency_rank = {"high": 0, "medium": 1, "low": 2}

    for key, items in raw_feeds.items():
        base = key.rstrip("0123456789")   # "jobs2" → "jobs", "ma2" → "ma"
        if base in merged:
            merged[base].extend(items)

    for base in merged:
        seen   = set()
        unique = []
        for item in merged[base]:
            t = item["title"].lower()[:60]
            if t not in seen:
                seen.add(t)
                unique.append(item)
        unique.sort(key=lambda x: urgency_rank.get(x["urgency"], 2))
        merged[base] = unique[:MAX_PER_FEED]

    return merged


# ── Per-country fetch ──────────────────────────────────────────────────────────
def fetch_country(country: str) -> dict:
    log.info("── %s %s", country, "─" * max(1, 42 - len(country)))
    cb_name, cb_rate = CENTRAL_BANKS[country]
    raw_feeds: dict[str, list] = {}

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {
            pool.submit(fetch_rss, fid, query, country): fid
            for fid, _label, query in feed_queries(country)
        }
        for fut in as_completed(futures):
            fid, items = fut.result()
            raw_feeds[fid] = items

    feeds      = merge_feeds(raw_feeds)
    macro      = fetch_world_bank(WORLD_BANK[country])
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
    parser = argparse.ArgumentParser(description="TIC Intelligence Fetcher v3")
    parser.add_argument("--country", choices=COUNTRIES, metavar="COUNTRY")
    parser.add_argument("--output",  default="intelligence_data.json")
    parser.add_argument("--delay",   type=float, default=2.0,
                        help="Seconds between countries to avoid rate-limiting")
    args = parser.parse_args()

    countries = [args.country] if args.country else COUNTRIES
    output    = Path(args.output)

    # Keep existing data for countries not being refreshed this run
    existing: dict[str, dict] = {}
    if output.exists():
        try:
            existing = {
                d["country"]: d
                for d in json.loads(output.read_text())["countries"]
            }
            log.info("Cached: %s", ", ".join(existing.keys()))
        except Exception:
            pass

    results = dict(existing)
    t0      = time.time()

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
                "competitor":  "Competitor Moves",
                "jobs":        "Hiring Signals",
                "market":      "Market & Regulatory",
                "investments": "Investment Projects",
                "regulatory":  "EU Regulatory Watch",
                "ma":          "M&A Activity",
                "standards":   "Accreditation & Standards",
                "tenders":     "Tenders & Contracts",
                "emerging":    "Emerging Sectors",
                "esg":         "Sustainability & ESG",
            },
        },
    }

    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    elapsed     = time.time() - t0
    total_items = sum(len(v) for r in ordered for v in r["feeds"].values())

    log.info("")
    log.info("✅  Done in %.1fs  →  %s", elapsed, output)
    log.info("    Countries: %d  ·  Items: %d  (avg %.1f/feed)",
             len(ordered), total_items,
             total_items / max(1, len(ordered) * len(BASE_FEEDS)))


if __name__ == "__main__":
    main()
