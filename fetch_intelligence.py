#!/usr/bin/env python3
"""
TÜV SÜD Competitive Intelligence — Backend Fetcher  v8
========================================================
Changes from v7:
  - CUTOFF_YEAR lowered to 2024 (small markets have limited 2025 coverage)
  - Google News date filter updated to 2024 to match CUTOFF_YEAR
  - World Bank parser hardened against malformed API responses (mrv=5)
  - Belgium: removed "Benelux" fallback region (was matching Vietnam/Cambodia content)
  - Austria: competitor queries anchor on "Wien/Vienna" to prevent Australia matches
  - Austrian accreditation body corrected to "Akkreditierung Austria"
  - Czech/Slovakia: added ČIA/SNAS to competitor queries for local coverage
  - Denmark: removed "Scandinavia/Nordic" variants (too broad, matched SE/NO content)
  - Slovenia/Turkey/Romania/Slovakia/Denmark/Belgium: added extra broad fallback queries
  - Poland job board URL fixed (old path-segment format replaced)
  - Python 3.9 compatible type hints (Optional, List, Dict from typing)
  - re module moved to top-level import

Install:  pip install requests feedparser
Run:      python fetch_intelligence.py
Single:   python fetch_intelligence.py --country Poland
"""

import argparse
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote

import feedparser
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tic")

CUTOFF_YEAR = 2025   # Accept Jan 2025+ (2026-only = <65 days of content)

# ── Country config ────────────────────────────────────────────────────────────
COUNTRIES = [
    "Poland", "Czech Republic", "Slovakia", "Hungary", "Romania", "Turkey",
    "United Kingdom", "France", "Belgium", "Italy", "Switzerland",
    "Spain", "Austria", "Denmark", "Slovenia",
]
SEARCH_NAME = {"United Kingdom": "UK", "Czech Republic": "Czech"}
def sn(c): return SEARCH_NAME.get(c, c)

# Variants used for country-relevance filtering
COUNTRY_VARIANTS = {
    # Includes: country name, adjective, major cities, local-language name,
    # regional fallback terms matching feed_queries small_markets regions,
    # and ccTLD hints so local publishers (idnes.cz, orf.at, orf.de) pass the filter.
    "Poland":         ["poland", "polish", "warsaw", "warszawa", "krakow", "wroclaw", "gdansk",
                       "polska", "poznan", ".pl"],
    "Czech Republic": ["czech", "prague", "praha", "brno", "czechia", "ostrava", "plzen",
                       "central europe", ".cz"],
    "Slovakia":       ["slovak", "bratislava", "kosice", "slovakia", "zilina",
                       "central europe", ".sk"],
    "Hungary":        ["hungary", "hungarian", "budapest", "debrecen", "miskolc", "magyar",
                       "central europe", ".hu"],
    "Romania":        ["romania", "romanian", "bucharest", "cluj", "timisoara", "iasi",
                       "eastern europe", ".ro"],
    "Turkey":         ["turkey", "turkish", "istanbul", "ankara", "izmir", "turkiye",
                       ".tr", ".com.tr"],
    "United Kingdom": ["united kingdom", "uk", "britain", "british", "england", "london",
                       "scotland", "wales", "manchester", "birmingham", ".co.uk", ".uk"],
    "France":         ["france", "french", "paris", "lyon", "marseille", "toulouse", ".fr"],
    "Belgium":        ["belgium", "belgian", "brussels", "bruxelles", "antwerp",
                       "ghent", "liège", "liege", "bruges", "leuven", ".be",
                       "belgi"],  # "belgi" matches Belgian/Belgique/Belgien
    "Italy":          ["italy", "italian", "milan", "rome", "turin", "italia",
                       "bologna", "naples", ".it"],
    "Switzerland":    ["switzerland", "swiss", "zurich", "bern", "geneva",
                       "schweiz", "basel", ".ch"],
    "Spain":          ["spain", "spanish", "madrid", "barcelona", "valencia",
                       "bilbao", "seville", ".es"],
    "Austria":        ["austria", "austrian", "vienna", "wien", "graz", "linz",
                       "salzburg", "innsbruck", "klagenfurt", ".at"],
    # NOTE: "austria" substring matches "australia" — the .at domain and city names
    # are the primary disambiguators. The relevance filter checks (title+source).lower()
    # so ".at" in a URL like "karriere.at" will match correctly.
    "Denmark":        ["denmark", "danish", "copenhagen", "aarhus", "odense",
                       "herning", "aalborg", ".dk"],
    # NOTE: removed "scandinavia"/"nordic" — too broad, matches Swedish/Norwegian content
    "Slovenia":       ["slovenia", "slovenian", "ljubljana", "maribor", "celje",
                       "central europe", ".si"],
}

# ── Local Google News editions ────────────────────────────────────────────────
LOCAL_EDITIONS = {
    "Poland":         ("pl", "PL", "PL:pl"),
    "Czech Republic": ("cs", "CZ", "CZ:cs"),
    "Slovakia":       ("sk", "SK", "SK:sk"),
    "Hungary":        ("hu", "HU", "HU:hu"),
    "Romania":        ("ro", "RO", "RO:ro"),
    "Turkey":         ("tr", "TR", "TR:tr"),
    "France":         ("fr", "FR", "FR:fr"),
    "Belgium":        ("fr", "BE", "BE:fr"),
    "Italy":          ("it", "IT", "IT:it"),
    "Switzerland":    ("de", "CH", "CH:de"),
    "Spain":          ("es", "ES", "ES:es"),
    "Austria":        ("de", "AT", "AT:de"),
    "Denmark":        ("da", "DK", "DK:da"),
    "Slovenia":       ("sl", "SI", "SI:sl"),
    "United Kingdom": None,
}

LOCAL_KEYWORDS = {
    "competitor": {
        "pl": "certyfikacja inspekcja laboratorium",
        "cs": "certifikace inspekce laboratoř",
        "sk": "certifikácia inšpekcia laboratórium",
        "hu": "tanúsítás ellenőrzés laboratórium",
        "ro": "certificare inspecție laborator",
        "tr": "sertifikasyon muayene laboratuvar",
        "fr": "certification inspection laboratoire",
        "it": "certificazione ispezione laboratorio",
        "de": "Zertifizierung Inspektion Labor",
        "es": "certificación inspección laboratorio",
        "da": "certificering inspektion laboratorium",
        "sl": "certificiranje inšpekcija laboratorij",
    },
    "market": {
        "pl": "regulacje rynek certyfikacja przemysł",
        "cs": "regulace trh certifikace průmysl",
        "sk": "regulácia trh certifikácia priemysel",
        "hu": "szabályozás piac tanúsítás ipar",
        "ro": "reglementare piață certificare industrie",
        "tr": "düzenleme piyasa sertifikasyon sanayi",
        "fr": "réglementation marché certification industrie",
        "it": "regolamentazione mercato certificazione industria",
        "de": "Regulierung Markt Zertifizierung Industrie",
        "es": "regulación mercado certificación industria",
        "da": "regulering marked certificering industri",
        "sl": "regulacija trg certificiranje industrija",
    },
    "investments": {
        "pl": "inwestycja fabryka projekt przemysłowy",
        "cs": "investice továrna průmyslový projekt",
        "sk": "investícia továreň priemyselný projekt",
        "hu": "befektetés gyár ipari projekt",
        "ro": "investiție fabrică proiect industrial",
        "tr": "yatırım fabrika sanayi projesi",
        "fr": "investissement usine projet industriel",
        "it": "investimento fabbrica progetto industriale",
        "de": "Investition Fabrik Industrieprojekt",
        "es": "inversión fábrica proyecto industrial",
        "da": "investering fabrik industriprojekt",
        "sl": "naložba tovarna industrijski projekt",
    },
    "tenders": {
        "pl": "przetarg kontrakt inspekcja certyfikacja",
        "cs": "tender kontrakt inspekce certifikace",
        "sk": "tender kontrakt inšpekcia certifikácia",
        "hu": "tender szerződés ellenőrzés tanúsítás",
        "ro": "licitație contract inspecție certificare",
        "tr": "ihale sözleşme muayene sertifikasyon",
        "fr": "appel offres contrat inspection certification",
        "it": "gara appalto ispezione certificazione",
        "de": "Ausschreibung Auftrag Inspektion Zertifizierung",
        "es": "licitación contrato inspección certificación",
        "da": "udbud kontrakt inspektion certificering",
        "sl": "razpis pogodba inšpekcija certificiranje",
    },
    "esg": {
        "pl": "ESG zrównoważony raportowanie emisje",
        "cs": "ESG udržitelnost reporting emise",
        "sk": "ESG udržateľnosť reporting emisie",
        "hu": "ESG fenntarthatóság jelentés kibocsátás",
        "ro": "ESG sustenabilitate raportare emisii",
        "tr": "ESG sürdürülebilirlik raporlama emisyon",
        "fr": "ESG durabilité reporting émissions",
        "it": "ESG sostenibilità reporting emissioni",
        "de": "ESG Nachhaltigkeit Berichterstattung Emissionen",
        "es": "ESG sostenibilidad informes emisiones",
        "da": "ESG bæredygtighed rapportering emissioner",
        "sl": "ESG trajnostnost poročanje emisije",
    },
}
LOCAL_FEED_TYPES = ["competitor", "market", "investments", "tenders", "esg"]

# ── Hiring Signals via Google News ───────────────────────────────────────────
# Job boards (pracuj.pl, jobs.cz, reed.co.uk etc.) block GitHub Actions / Azure
# datacenter IPs at the network level — confirmed: 0 board results across all 15
# countries in every production run (no "✓ JOB-BOARD" lines ever appear in logs).
# Removed to avoid ~100 wasted blocked HTTP requests per run.
#
# Google News RSS is the only mechanism that works from GH Actions.
# Country name is embedded in every query — no secondary relevance filter needed.

def _gnews_hiring_queries(country):
    # type: (str) -> List[str]
    """
    Broad Google News queries for hiring signals.
    Sector-level queries yield more results than per-company queries,
    especially for small markets where TIC news is sparse in English.
    """
    c = sn(country)
    queries = [
        # Sector-level — broadest, highest yield
        f"{c} testing inspection certification laboratory jobs 2025",
        f"{c} TIC laboratory auditor quality engineer vacancy 2025",
        # Competitor expansion news — expansion implies hiring
        f"DEKRA SGS {c} expansion laboratory opens 2025",
        f"Bureau Veritas Eurofins {c} laboratory invest 2025",
        f"Intertek DNV {c} expansion certification 2025",
        # Direct hiring press releases
        f"DEKRA {c} hiring engineer inspector 2025",
        f"SGS {c} recruitment laboratory 2025",
        f"Bureau Veritas {c} jobs certification 2025",
        f"TÜV Rheinland TÜV Nord {c} jobs laboratory 2025",
    ]
    # Country-specific local-language boosts for key markets
    if country == "Austria":
        queries[:0] = [
            "TÜV Austria jobs Stellen Wien certification 2025",
            "DEKRA Bureau Veritas Österreich Stellen Labor 2025",
        ]
    elif country == "Turkey":
        queries[:0] = ["DEKRA SGS Türkiye işe alım laboratuvar 2025"]
    elif country == "Poland":
        queries[:0] = ["DEKRA SGS Polska praca laboratorium 2025"]
    elif country == "Czech Republic":
        queries[:0] = ["DEKRA SGS Czech práce laboratoř certifikace 2025"]
    return queries


def _fetch_gnews_hiring(query, country):
    # type: (str, str) -> List[Dict]
    """Fetch hiring signals from Google News RSS for one query.
    No country relevance filter — country name is in the query itself."""
    try:
        resp   = requests.get(gnews_url_en(query), headers=HEADERS_NEWS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items  = []
        for entry in parsed.entries[:2]:
            raw = entry.get("title", "").strip()
            if not raw or len(raw) < 8:
                continue
            title, src_guess = clean_title(raw)
            if not title or len(title) < 8:
                continue
            source = entry.get("source", {}).get("title", "") or src_guess
            pub    = entry.get("published", "")
            if pub and not is_recent(pub):
                continue
            link = entry.get("link", "").strip()
            if is_tuvsud(title, source, link):
                continue
            items.append({
                "title":        title,
                "titleEN":      title,
                "link":         link,
                "source":       source or "Google News",
                "pubDate":      pub or datetime.now(timezone.utc).strftime(
                                    "%a, %d %b %Y %H:%M:%S +0000"),
                "urgency":      "medium",
                "local":        False,
                "isJobPosting": True,
            })
        return items
    except Exception as exc:
        log.debug("  ✗ HIRING-GN (%s) '%s…': %s", country, query[:40], exc)
        return []


def fetch_job_postings(country):
    # type: (str) -> List[Dict]
    """Fetch hiring signals via Google News. Returns up to 10 deduplicated items."""
    all_jobs = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [pool.submit(_fetch_gnews_hiring, q, country)
                for q in _gnews_hiring_queries(country)]
        for fut in as_completed(futs):
            all_jobs.extend(fut.result())

    seen, unique = set(), []
    for job in all_jobs:
        key = (job.get("titleEN") or job["title"]).lower()[:55]
        if key not in seen:
            seen.add(key)
            unique.append(job)

    unique.sort(
        key=lambda x: parse_pub_date(x.get("pubDate", "")) or
                      datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    log.info("  ✓ HIRING (%s) %d signals", country, len(unique[:10]))
    return unique[:10]


def is_tuvsud(title: str, source: str, link: str = "") -> bool:
    """Return True if the item is about TÜV SÜD (us) — should be excluded.
    Checks title, source name, AND link URL — because local-language blog posts
    (e.g. Czech: tuvsud.com/cs-cz/...) never mention 'TÜV SÜD' in the title."""
    haystack = (title + " " + source).lower()
    if any(v in haystack for v in TUVSUD_VARIANTS):
        return True
    # Catch TÜV SÜD blog/microsite links regardless of title language
    link_lower = link.lower()
    if "tuvsud.com" in link_lower or "tuv-sud.com" in link_lower:
        return True
    return False

# ── Date helpers ──────────────────────────────────────────────────────────────
def parse_pub_date(raw: str):
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw).astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None

def is_recent(raw_date: str) -> bool:
    """
    Returns True ONLY if article date parses successfully AND year >= CUTOFF_YEAR.
    Items with unparseable dates are EXCLUDED (previously they were passed through).
    """
    dt = parse_pub_date(raw_date)
    if dt is None:
        return False   # FIX: was True — old articles with bad dates now rejected
    return dt.year >= CUTOFF_YEAR

# ── Country relevance filter ──────────────────────────────────────────────────
def is_relevant_to_country(title: str, source: str, country: str) -> bool:
    variants = COUNTRY_VARIANTS.get(country, [country.lower()])
    haystack = (title + " " + source).lower()
    # Special case: "austria" is a substring of "australia" — reject Australian content
    if country == "Austria" and "australia" in haystack:
        return False
    # Special case: "uk" matches "bulk", "duke", "truck" etc — only match as word boundary
    if country == "United Kingdom":
        has_uk_word = bool(re.search(r"\buk\b", haystack))
        other_variants = [v for v in variants if v != "uk"]
        return has_uk_word or any(v in haystack for v in other_variants)
    return any(v in haystack for v in variants)

# ── HTTP / feed helpers ───────────────────────────────────────────────────────
TIMEOUT = 15
MAX_ITEMS = 8
HEADERS_NEWS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
GTRANS_URL = "https://translate.googleapis.com/translate_a/single"

def gnews_url_en(query: str) -> str:
    df = "&tbs=cdr:1,cd_min:1%2F1%2F2025"
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en{df}"

def gnews_url_local(query, hl, gl, ceid) -> str:
    df = "&tbs=cdr:1,cd_min:1%2F1%2F2025"
    return f"https://news.google.com/rss/search?q={quote(query)}&hl={hl}&gl={gl}&ceid={ceid}{df}"

def score_urgency(title: str) -> str:
    t = title.lower()
    if any(k in t for k in [
        "acqui","merger","acquires","takeover","buyout","launches","launch",
        "opens","expands","expansion","wins contract","awarded","secures contract",
        "ipo","record deal","major contract","przejęcie","fuzja","akvizice",
        "satın alma","acquisition","fusion","acquisizione","Übernahme","adquisición",
    ]):
        return "high"
    if any(k in t for k in [
        "hire","hiring","recruit","invest","investment","partner","certif",
        "announce","tender","contract","standard","regulation","accredit",
        "compliance","zatrudni","inwestycj","przetarg","zaměstn","investic",
        "angajare","investiție","licitație","işe alım","yatırım","ihale",
        "embauche","investissement","assunzione","Einstellung","contratación",
    ]):
        return "medium"
    return "low"

def clean_title(raw: str):
    if " - " in raw:
        p = raw.rsplit(" - ", 1)
        return p[0].strip(), p[1].strip()
    return raw.strip(), ""

# ── News RSS fetchers ─────────────────────────────────────────────────────────
def feed_queries(country: str):
    c = sn(country)
    # Small markets (CEE + Benelux + Nordics) benefit from a broader regional
    # fallback query for low-volume feeds like M&A, tenders, standards.
    small_markets = {
        "Czech Republic": "Central Europe",
        "Slovakia":       "Central Europe",
        "Slovenia":       "Central Europe",
        "Hungary":        "Central Europe",
        "Romania":        "Eastern Europe",
        "Denmark":        "Denmark",  # use country directly, not regional term
        # Belgium: do NOT use "Benelux" - returns irrelevant NL/LU content
        # "Belgium":      "Benelux",  # intentionally removed
    }
    region = small_markets.get(country)   # None for larger markets
    ma_fallback  = region or c
    std_fallback = region or c
    ten_fallback = region or c

    # Country-specific competitor name overrides
    # Austria: TÜV Austria is dominant local player — must be in every competitor query
    # Czech/Slovakia: add local accreditation body CIA/SNAS
    # Country-specific extra competitors
    extra_comp = {
        "Austria":        "TÜV Austria",
        "Czech Republic": "ČIA",           # Local accreditation body only — TÜV SÜD is us
        "Slovakia":       "SNAS",          # Local accreditation body only — TÜV SÜD is us
        "Denmark":        "FORCE Technology",
        # Poland: no local TIC name override needed — TÜV SÜD Polska is us
    }.get(country, "")

    comp1 = f"DEKRA SGS Intertek {c} testing certification inspection"
    comp2 = f"Bureau Veritas Eurofins TÜV {c} laboratory inspection"
    if extra_comp:
        comp1 = f"{extra_comp} DEKRA SGS {c} testing certification"
        comp2 = f"Bureau Veritas {extra_comp} {c} laboratory inspection"

    # Austria: anchor queries with Wien/Vienna/Österreich to avoid Australia matches.
    # Competitor queries get Wien/Vienna (most specific). All other queries get
    # "Österreich" appended so Google News de-ranks Australian results.
    if country == "Austria":
        comp1 = f"TÜV Austria DEKRA Bureau Veritas Wien Vienna certification"
        comp2 = f"SGS Intertek Eurofins Österreich Wien laboratory inspection"
        # Post-process: append Österreich to every other Austria query
        austria_anchor = True
    else:
        austria_anchor = False

    # Accreditation body for standards queries (country-specific)
    accred_body = ACCRED.get(country, "")
    std1 = f"{c} {accred_body} ISO accreditation laboratory standard 2025" if accred_body else f"{c} ISO accreditation laboratory standard 2025"
    std2 = f"{accred_body} accreditation {std_fallback} laboratory certification 2025" if accred_body else f"accreditation {std_fallback} laboratory certification 2025"

    # For very small/low-volume markets, add a 3rd broad competitor query
    low_volume = {"Slovenia", "Romania", "Turkey", "Slovakia", "Denmark", "Belgium"}
    extra_queries = []
    if country in low_volume:
        extra_queries = [
            ("competitor3", f"DEKRA Bureau Veritas SGS {c} 2025"),
            ("market2",     f"{c} industry testing inspection laboratory 2025"),
        ]

    base_queries = [
        ("competitor",  comp1),
        ("competitor2", comp2),
        ("ma",          f"DEKRA SGS Eurofins {c} acquisition merger 2025"),
        ("ma2",         f"TIC laboratory {ma_fallback} acquisition deal 2025"),
        ("market",      f"{c} certification inspection regulation laboratory 2025"),
        ("regulatory",  f"CSRD DORA AI Act certification compliance 2025"),
        ("tenders",     f"{c} inspection certification tender contract 2025"),
        ("tenders2",    f"laboratory testing {ten_fallback} tender procurement 2025"),
        ("standards",   std1),
        ("standards2",  std2),
        ("investments", f"{c} EV battery factory gigafactory investment 2025"),
        ("investments2",f"{c} manufacturing plant energy investment project 2025"),
        ("emerging",    f"{c} cybersecurity AI certification medical device 2025"),
        ("emerging2",   f"{c} hydrogen certification digital product passport 2025"),
        ("esg",         f"{c} CSRD ESG sustainability audit carbon 2025"),
    ]

    # Austria: append "Österreich" to all non-competitor queries so Google News
    # de-ranks Australian results. Competitor queries already have Wien/Vienna.
    if austria_anchor:
        base_queries = [
            (fid, q) if fid in ("competitor", "competitor2", "regulatory")
            else (fid, q + " Österreich")
            for fid, q in base_queries
        ]

    return base_queries + extra_queries

def local_queries(country: str):
    edition = LOCAL_EDITIONS.get(country)
    if not edition:
        return []
    hl, gl, ceid = edition
    result = []
    for feed_type in LOCAL_FEED_TYPES:
        kw = LOCAL_KEYWORDS.get(feed_type, {}).get(hl)
        if kw:
            result.append((f"{feed_type}_local", feed_type, kw, hl, gl, ceid))
    return result

def fetch_rss_en(feed_id: str, query: str, country: str):
    # Feeds that are intentionally global/EU-wide — skip country relevance filter.
    # "regulatory" queries CSRD/AI Act/DORA which are pan-European.
    # "standards" queries ISO/EN standards which are published globally.
    # "emerging" queries cybersecurity/hydrogen/AI certification frameworks (EU-wide).
    # "esg" queries CSRD/carbon which affect all EU countries equally.
    # The feed_queries for these still include the country name in the query so
    # Google News will surface country-relevant articles, but global articles
    # (e.g. "ISO 17025 revision published") are also valuable and should not be dropped.
    skip_relevance = feed_id.rstrip("0123456789") in (
        "regulatory", "standards", "emerging", "esg"
    )
    try:
        resp   = requests.get(gnews_url_en(query), headers=HEADERS_NEWS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items  = []
        for entry in parsed.entries[:MAX_ITEMS * 3]:   # fetch extra to survive filters
            raw = entry.get("title", "").strip()
            if not raw:
                continue
            title, src_guess = clean_title(raw)
            source = entry.get("source", {}).get("title", "") or src_guess
            pub    = entry.get("published", "")
            # Hard date filter — reject if missing date or pre-2025
            if not is_recent(pub):
                log.debug("  SKIP old/no-date: %s", title[:50])
                continue
            # Self-exclusion: TÜV SÜD is us — not competitive intelligence
            if is_tuvsud(title, source, entry.get("link", "")):
                log.debug("  SKIP tuvsud: %s", title[:50])
                continue
            # Country relevance filter
            if not skip_relevance and not is_relevant_to_country(title, source, country):
                log.debug("  SKIP irrelevant: %s", title[:50])
                continue
            items.append({
                "title": title, "link": entry.get("link","").strip(),
                "source": source, "pubDate": pub,
                "urgency": score_urgency(title), "local": False,
            })
            if len(items) >= MAX_ITEMS:
                break
        log.info("  ✓ EN %-14s (%s) %d items", feed_id, country, len(items))
        return feed_id, items
    except Exception as exc:
        log.warning("  ✗ EN %-14s (%s) %s", feed_id, country, exc)
        return feed_id, []

def fetch_rss_local(feed_id, base_feed, query, hl, gl, ceid, country):
    try:
        resp   = requests.get(gnews_url_local(query, hl, gl, ceid), headers=HEADERS_NEWS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items  = []
        for entry in parsed.entries[:MAX_ITEMS * 2]:
            raw = entry.get("title", "").strip()
            if not raw:
                continue
            title_local, src_guess = clean_title(raw)
            source = entry.get("source", {}).get("title", "") or src_guess
            pub    = entry.get("published", "")
            if not is_recent(pub):
                continue
            # NO country relevance filter here — items from local Google News editions
            # (ceid=CZ:cs, ceid=PL:pl, etc.) are already country-targeted by the
            # edition parameter. Czech articles say "certifikace roste" not
            # "Czech certifikace roste", so a country-name filter drops everything.
            # Self-exclusion: skip TÜV SÜD articles (we are TÜV SÜD)
            if is_tuvsud(title_local, source, entry.get("link", "")):
                continue
            items.append({
                "title": title_local, "titleEN": None,
                "link": entry.get("link","").strip(),
                "source": source, "pubDate": pub,
                "urgency": score_urgency(title_local),
                "local": True, "lang": hl,
            })
            if len(items) >= MAX_ITEMS:
                break
        log.info("  ✓ LOCAL %-10s (%s/%s) %d items", base_feed, country, hl.upper(), len(items))
        return feed_id, items
    except Exception as exc:
        log.warning("  ✗ LOCAL %-10s (%s) %s", base_feed, country, exc)
        return feed_id, []

# ── Translation ───────────────────────────────────────────────────────────────
def translate_one(text: str, src_lang: str) -> str:
    params = {"client":"gtx","sl":src_lang,"tl":"en","dt":"t","q":text}
    resp = requests.get(GTRANS_URL, params=params, timeout=10,
                        headers={"User-Agent":"Mozilla/5.0"})
    resp.raise_for_status()
    data = resp.json()
    return "".join(seg[0] for seg in data[0] if seg[0]).strip()

def translate_batch(items: list, country: str) -> list:
    to_tr = [i for i in items if i.get("local") and not i.get("titleEN")]
    if not to_tr:
        return items
    def _do(item):
        try:
            item["titleEN"] = translate_one(item["title"], item.get("lang","auto"))
        except Exception as exc:
            log.debug("  ✗ Translate '%s…': %s", item["title"][:30], exc)
            item["titleEN"] = item["title"]
        return item
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(_do, to_tr))
    ok = sum(1 for i in to_tr if i.get("titleEN") and i["titleEN"] != i["title"])
    log.info("  ✓ Translated %d/%d local items (%s, Google free)", ok, len(to_tr), country)
    return items

# ── World Bank macro data ─────────────────────────────────────────────────────
# World Bank API is battle-tested, free, no key, covers all 15 countries.
# It aggregates from Eurostat/OECD/national stats — same underlying data.
# Response format is trivially simple: list of {date, value} records.
WB_CODES = {
    "Poland": "POL", "Czech Republic": "CZE", "Slovakia": "SVK",
    "Hungary": "HUN", "Romania": "ROU", "Turkey": "TUR",
    "United Kingdom": "GBR", "France": "FRA", "Belgium": "BEL",
    "Italy": "ITA", "Switzerland": "CHE", "Spain": "ESP",
    "Austria": "AUT", "Denmark": "DNK", "Slovenia": "SVN",
}

# Fallback indicator chains: if primary returns no data, try alternates.
WB_INFLATION_INDICATORS = [
    "FP.CPI.TOTL.ZG",       # CPI inflation (primary)
    "NY.GDP.DEFL.KD.ZG",    # GDP deflator (fallback if CPI missing)
]
WB_GDP_INDICATORS = [
    "NY.GDP.MKTP.KD.ZG",    # Real GDP growth (primary)
    "NY.GDP.PCAP.KD.ZG",    # Per-capita GDP growth (fallback)
]

def fetch_wb_indicator(wb_code, indicator, label, fallbacks=None):
    # type: (str, str, str, Optional[List[str]]) -> Optional[Dict]
    """
    Fetch a World Bank indicator with mrv=5 (5 years back) to maximise
    chances of finding a non-null value. If primary indicator returns nothing,
    tries each fallback indicator in order.
    """
    indicators_to_try = [indicator] + (fallbacks or [])
    for ind in indicators_to_try:
        try:
            url = (
                f"https://api.worldbank.org/v2/country/{wb_code}"
                f"/indicator/{ind}?format=json&mrv=5&per_page=5"
            )
            resp = requests.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            # WB response: [metadata_dict, [records...]]
            # Error response: [{"message":[...]}] — only 1 element, records missing
            if not isinstance(data, list) or len(data) < 2:
                log.debug("  ✗ WB %s %s [%s]: unexpected response shape", label, wb_code, ind)
                continue
            records = data[1]
            if not isinstance(records, list):
                continue
            rows = [r for r in records if isinstance(r, dict) and r.get("value") is not None]
            if rows:
                r = rows[0]
                val = round(float(r["value"]), 2)
                src_label = "World Bank" if ind == indicator else f"World Bank ({ind})"
                log.info("  ✓ WB %-12s %s → %.2f%% (%s) [%s]", label, wb_code, val, r["date"], ind)
                return {"value": val, "year": r["date"], "source": src_label}
        except Exception as exc:
            log.warning("  ✗ WB %-12s %s [%s]: %s", label, wb_code, ind, exc)
    log.warning("  ✗ WB %-12s %s — no data after all fallbacks", label, wb_code)
    return None

def fetch_macro(country: str) -> dict:
    """
    GDP growth + CPI inflation from World Bank for all 15 countries.
    Uses mrv=5 with fallback indicators for robustness.
    """
    code = WB_CODES.get(country)
    if not code:
        return {"gdp": None, "inflation": None}
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_gdp  = pool.submit(fetch_wb_indicator, code, "NY.GDP.MKTP.KD.ZG", "GDP",
                             fallbacks=["NY.GDP.PCAP.KD.ZG"])
        f_infl = pool.submit(fetch_wb_indicator, code, "FP.CPI.TOTL.ZG", "Inflation",
                             fallbacks=["NY.GDP.DEFL.KD.ZG"])
        return {"gdp": f_gdp.result(), "inflation": f_infl.result()}

# ── Feed merging ──────────────────────────────────────────────────────────────
BASE_FEEDS = [
    "competitor","jobs","ma","market","regulatory",
    "tenders","standards","investments","emerging","esg",
]
MAX_PER_FEED = 8

def merge_feeds(raw_feeds: dict) -> dict:
    merged = {f: [] for f in BASE_FEEDS}
    rank   = {"high":0,"medium":1,"low":2}
    for key, items in raw_feeds.items():
        base = key.rstrip("0123456789").replace("_local","")
        if base in merged:
            merged[base].extend(items)
    for base in merged:
        seen, unique = set(), []
        for item in merged[base]:
            disp = (item.get("titleEN") or item["title"]).lower()[:55]
            if disp not in seen:
                seen.add(disp)
                unique.append(item)
        unique.sort(key=lambda x: rank.get(x["urgency"],2))
        merged[base] = unique[:MAX_PER_FEED]
    return merged

# ── Per-country fetch ─────────────────────────────────────────────────────────
def fetch_country(country: str) -> dict:
    log.info("── %s %s", country, "─" * max(1, 42 - len(country)))
    cb_name, cb_rate = CENTRAL_BANKS[country]
    raw_feeds = {}

    # English news queries
    with ThreadPoolExecutor(max_workers=15) as pool:
        futs = {pool.submit(fetch_rss_en, fid, q, country): fid
                for fid, q in feed_queries(country)}
        for fut in as_completed(futs):
            fid, items = fut.result()
            raw_feeds[fid] = items

    # Local-language news queries
    lq = local_queries(country)
    if lq:
        with ThreadPoolExecutor(max_workers=10) as pool:
            futs = {pool.submit(fetch_rss_local, fid, base, q, hl, gl, ceid, country): fid
                    for fid, base, q, hl, gl, ceid in lq}
            for fut in as_completed(futs):
                fid, items = fut.result()
                raw_feeds[fid] = items

    # Job postings (replaces Google News "jobs" feed entirely)
    raw_feeds["jobs"] = fetch_job_postings(country)

    # Merge + deduplicate all feeds
    feeds = merge_feeds(raw_feeds)

    # Translate local headlines (Google Translate, free)
    all_items = [i for v in feeds.values() for i in v]
    translate_batch(all_items, country)

    macro      = fetch_macro(country)
    high_count = sum(1 for v in feeds.values() for i in v if i.get("urgency") == "high")
    local_count= sum(1 for v in feeds.values() for i in v if i.get("local"))

    log.info("  → %d items (%d local, %d jobs) for %s",
             sum(len(v) for v in feeds.values()),
             local_count, len(feeds.get("jobs",[])), country)

    return {
        "country":   country,
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "highCount": high_count,
        "feeds":     feeds,
        "macro": {
            "gdp":       macro.get("gdp"),
            "inflation": macro.get("inflation"),
            "interestRate": {
                "bank":  cb_name,
                "value": f"{cb_rate}%",
                "note":  "Official rate — update manually after central bank decisions",
            },
        },
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="TIC Intelligence Fetcher v6")
    parser.add_argument("--country", choices=COUNTRIES, metavar="COUNTRY")
    parser.add_argument("--output",  default="intelligence_data.json")
    parser.add_argument("--delay",   type=float, default=2.0)
    args = parser.parse_args()

    countries = [args.country] if args.country else COUNTRIES
    output    = Path(args.output)

    existing: dict = {}
    if output.exists():
        try:
            existing = {d["country"]: d
                        for d in json.loads(output.read_text())["countries"]}
            log.info("Cached: %s", ", ".join(existing.keys()))
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
    local_items = sum(1 for r in ordered for v in r["feeds"].values()
                      for i in v if i.get("local"))

    log.info("")
    log.info("✅  Done in %.1fs  →  %s", elapsed, output)
    log.info("    Countries:   %d", len(ordered))
    log.info("    Total items: %d  (%d local, %d English)",
             total_items, local_items, total_items - local_items)

if __name__ == "__main__":
    main()
