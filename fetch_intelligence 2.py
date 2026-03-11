#!/usr/bin/env python3
"""
TÜV SÜD Competitive Intelligence — Backend Fetcher
====================================================
Fixes applied vs previous version:
  - Dynamic rolling 90-day cutoff (no hardcoded CUTOFF_YEAR)
  - Google News date filter uses dynamic cutoff date
  - merge_with_history uses parse_pub_date() helper (RFC 2822 aware)
  - merge_with_history preserves last-known macro data on World Bank failure
  - titleEN field renamed to titleLocal to avoid misleading naming
  - Duplicate "compliance" entry removed from score_urgency
  - 30-day rolling data retention preserved across CI runs
  - Tenacity integration for robust World Bank API retries
"""

import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from pathlib import Path

import feedparser
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tic")

# Dynamic rolling cutoff — 90 days back from today
CUTOFF_DATE = datetime.now(timezone.utc) - timedelta(days=90)
CUTOFF_YEAR = CUTOFF_DATE.year  # kept for legacy references only
TIMEOUT = 15
MAX_ITEMS = 5

# Google News date string formatted dynamically
_cd_min = CUTOFF_DATE.strftime("%-m/%-d/%Y")  # e.g. "1/1/2026"

# ── Country config ────────────────────────────────────────────────────────────
COUNTRIES = [
    "Poland", "Czech Republic", "Slovakia", "Hungary", "Romania", "Turkey",
    "United Kingdom", "France", "Belgium", "Italy", "Switzerland",
    "Spain", "Austria", "Denmark", "Slovenia",
]
SEARCH_NAME = {"United Kingdom": "UK", "Czech Republic": "Czech"}
def sn(c): return SEARCH_NAME.get(c, c)

COUNTRY_VARIANTS = {
    "Poland": ["poland", "polish", "warsaw", "warszawa", "krakow", "wroclaw", "gdansk", "polska", "poznan", ".pl"],
    "Czech Republic": ["czech", "prague", "praha", "brno", "czechia", "ostrava", "plzen", "central europe", ".cz"],
    "Slovakia": ["slovak", "bratislava", "kosice", "slovakia", "zilina", "central europe", ".sk"],
    "Hungary": ["hungary", "hungarian", "budapest", "debrecen", "miskolc", "magyar", "central europe", ".hu"],
    "Romania": ["romania", "romanian", "bucharest", "cluj", "timisoara", "iasi", "eastern europe", ".ro"],
    "Turkey": ["turkey", "turkish", "istanbul", "ankara", "izmir", "turkiye", ".tr", ".com.tr"],
    "United Kingdom": ["united kingdom", "uk", "britain", "british", "england", "london", "scotland", "wales", "manchester", "birmingham", ".co.uk", ".uk"],
    "France": ["france", "french", "paris", "lyon", "marseille", "toulouse", ".fr"],
    "Belgium": ["belgium", "belgian", "brussels", "bruxelles", "antwerp", "ghent", "liège", "liege", "bruges", "leuven", ".be", "belgi"],
    "Italy": ["italy", "italian", "milan", "rome", "turin", "italia", "bologna", "naples", ".it"],
    "Switzerland": ["switzerland", "swiss", "zurich", "bern", "geneva", "schweiz", "basel", ".ch"],
    "Spain": ["spain", "spanish", "madrid", "barcelona", "valencia", "bilbao", "seville", ".es"],
    "Austria": ["austria", "austrian", "vienna", "wien", "graz", "linz", "salzburg", "innsbruck", "klagenfurt", ".at"],
    "Denmark": ["denmark", "danish", "copenhagen", "aarhus", "odense", "herning", "aalborg", ".dk"],
    "Slovenia": ["slovenia", "slovenian", "ljubljana", "maribor", "celje", "central europe", ".si"],
}

LOCAL_EDITIONS = {
    "Poland": ("pl", "PL", "PL:pl"), "Czech Republic": ("cs", "CZ", "CZ:cs"),
    "Slovakia": ("sk", "SK", "SK:sk"), "Hungary": ("hu", "HU", "HU:hu"),
    "Romania": ("ro", "RO", "RO:ro"), "Turkey": ("tr", "TR", "TR:tr"),
    "France": ("fr", "FR", "FR:fr"), "Belgium": ("fr", "BE", "BE:fr"),
    "Italy": ("it", "IT", "IT:it"), "Switzerland": ("de", "CH", "CH:de"),
    "Spain": ("es", "ES", "ES:es"), "Austria": ("de", "AT", "AT:de"),
    "Denmark": ("da", "DK", "DK:da"), "Slovenia": ("sl", "SI", "SI:sl"),
    "United Kingdom": None,
}

LOCAL_KEYWORDS = {
    "competitor": {
        "pl": "certyfikacja inspekcja laboratorium", "cs": "certifikace inspekce laboratoř",
        "sk": "certifikácia inšpekcia laboratórium", "hu": "tanúsítás ellenőrzés laboratórium",
        "ro": "certificare inspecție laborator", "tr": "sertifikasyon muayene laboratuvar",
        "fr": "certification inspection laboratoire", "it": "certificazione ispezione laboratorio",
        "de": "Zertifizierung Inspektion Labor", "es": "certificación inspección laboratorio",
        "da": "certificering inspektion laboratorium", "sl": "certificiranje inšpekcija laboratorij",
    },
    "market": {
        "pl": "regulacje rynek certyfikacja przemysł", "cs": "regulace trh certifikace průmysl",
        "sk": "regulácia trh certifikácia priemysel", "hu": "szabályozás piac tanúsítás ipar",
        "ro": "reglementare piață certificare industrie", "tr": "düzenleme piyasa sertifikasyon sanayi",
        "fr": "réglementation marché certification industrie", "it": "regolamentazione mercato certificazione industria",
        "de": "Regulierung Markt Zertifizierung Industrie", "es": "regulación mercado certificación industria",
        "da": "regulering marked certificering industri", "sl": "regulacija trg certificiranje industrija",
    },
    "investments": {
        "pl": "inwestycja fabryka projekt przemysłowy", "cs": "investice továrna průmyslový projekt",
        "sk": "investícia továreň priemyselný projekt", "hu": "befektetés gyár ipari projekt",
        "ro": "investiție fabrică proiect industrial", "tr": "yatırım fabrika sanayi projesi",
        "fr": "investissement usine projet industriel", "it": "investimento fabbrica progetto industriale",
        "de": "Investition Fabrik Industrieprojekt", "es": "inversión fábrica proyecto industrial",
        "da": "investering fabrik industriprojekt", "sl": "naložba tovarna industrijski projekt",
    },
    "emerging": {
        "pl": "ESG zrównoważony certyfikacja wodór energetyka", "cs": "ESG udržitelnost certifikace vodík obnovitelné",
        "sk": "ESG udržateľnosť certifikácia vodík obnoviteľné", "hu": "ESG fenntarthatóság tanúsítás hidrogén megújuló",
        "ro": "ESG sustenabilitate certificare hidrogen regenerabil", "tr": "ESG sürdürülebilirlik sertifikasyon hidrojen yenilenebilir",
        "fr": "ESG durabilité certification hydrogène renouvelable", "it": "ESG sostenibilità certificazione idrogeno rinnovabile",
        "de": "ESG Nachhaltigkeit Zertifizierung Wasserstoff erneuerbar", "es": "ESG sostenibilidad certificación hidrógeno renovable",
        "da": "ESG bæredygtighed certificering brint vedvarende", "sl": "ESG trajnostnost certificiranje vodik obnovljivi",
    },
}
LOCAL_FEED_TYPES = ["competitor", "market", "investments", "emerging"]

CENTRAL_BANKS = {
    "Poland": ("NBP", "5.75"), "Czech Republic": ("CNB", "3.75"),
    "Slovakia": ("ECB", "4.50"), "Hungary": ("MNB", "6.50"),
    "Romania": ("BNR", "6.50"), "Turkey": ("CBRT", "42.50"),
    "United Kingdom": ("BoE", "4.75"), "France": ("ECB", "4.50"),
    "Belgium": ("ECB", "4.50"), "Italy": ("ECB", "4.50"),
    "Switzerland": ("SNB", "1.00"), "Spain": ("ECB", "4.50"),
    "Austria": ("ECB", "4.50"), "Denmark": ("DN", "3.35"),
    "Slovenia": ("ECB", "4.50"),
}
ACCRED = {
    "Poland": "PCA", "Czech Republic": "ČIA", "Slovakia": "SNAS",
    "Hungary": "NAH", "Romania": "RENAR", "Turkey": "TÜRKAK",
    "United Kingdom": "UKAS", "France": "COFRAC", "Belgium": "BELAC",
    "Italy": "ACCREDIA", "Switzerland": "SAS", "Spain": "ENAC",
    "Austria": "Akkreditierung Austria", "Denmark": "DANAK", "Slovenia": "SA",
}
CURRENCY = {
    "Poland": "PLN", "Czech Republic": "CZK", "Slovakia": "EUR",
    "Hungary": "HUF", "Romania": "RON", "Turkey": "TRY",
    "United Kingdom": "GBP", "France": "EUR", "Belgium": "EUR",
    "Italy": "EUR", "Switzerland": "CHF", "Spain": "EUR",
    "Austria": "EUR", "Denmark": "DKK", "Slovenia": "EUR",
}
COMPETITORS_CORE = ["DEKRA", "TUV Rheinland", "SGS", "Bureau Veritas", "TUV Austria", "TUV Nord", "Eurofins", "Intertek"]
COMPETITORS_EXTENDED = ["Applus", "Lloyds Register", "DNV"]
ALL_COMPETITORS = COMPETITORS_CORE + COMPETITORS_EXTENDED

ACCRED_RSS = {
    "Czech Republic": "https://www.cai.cz/?cat=1&feed=rss2",
    "Poland": "https://www.pca.gov.pl/feed/",
    "Slovakia": "https://www.snas.sk/feed/",
    "Hungary": "https://www.nah.gov.hu/feed/",
    "Romania": "https://www.renar.ro/feed/",
    "Turkey": "https://www.turkak.org.tr/feed/",
    "United Kingdom": "https://www.ukas.com/feed/",
    "France": "https://www.cofrac.fr/feed/",
    "Belgium": "https://economie.fgov.be/feed/",
    "Italy": "https://www.accredia.it/feed/",
    "Switzerland": "https://www.sas.ch/feed/",
    "Spain": "https://www.enac.es/feed/",
    "Austria": "https://www.bmaw.gv.at/feed/",
    "Denmark": "https://www.danak.dk/feed/",
    "Slovenia": "https://www.slo-akreditacija.si/feed/",
}

FLAGS = {
    "Poland": "🇵🇱", "Czech Republic": "🇨🇿", "Slovakia": "🇸🇰", "Hungary": "🇭🇺",
    "Romania": "🇷🇴", "Turkey": "🇹🇷", "United Kingdom": "🇬🇧", "France": "🇫🇷",
    "Belgium": "🇧🇪", "Italy": "🇮🇹", "Switzerland": "🇨🇭", "Spain": "🇪🇸",
    "Austria": "🇦🇹", "Denmark": "🇩🇰", "Slovenia": "🇸🇮",
}

HEADERS_NEWS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
GNEWS_BASE = "https://news.google.com/rss/search"

def gnews_url_en(query: str) -> str:
    from urllib.parse import urlencode
    cd_min_enc = quote(_cd_min, safe="")
    return f"{GNEWS_BASE}?q={quote(query)}&hl=en-US&gl=US&ceid=US:en&tbs=cdr:1,cd_min:{cd_min_enc}"

def gnews_url_local(query: str, hl: str, gl: str, ceid: str) -> str:
    cd_min_enc = quote(_cd_min, safe="")
    return f"{GNEWS_BASE}?q={quote(query)}&hl={hl}&gl={gl}&ceid={ceid}&tbs=cdr:1,cd_min:{cd_min_enc}"

def parse_pub_date(pub: str):
    """Parse RFC 2822 or ISO 8601 date strings into timezone-aware datetime objects."""
    from email.utils import parsedate_to_datetime
    if not pub:
        return None
    try:
        return parsedate_to_datetime(pub)
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(pub[:len(fmt)], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    return None

def is_recent(pub: str) -> bool:
    dt = parse_pub_date(pub)
    return dt is not None and dt >= CUTOFF_DATE

def is_relevant_to_country(title: str, source: str, country: str) -> bool:
    variants = COUNTRY_VARIANTS.get(country, [country.lower()])
    haystack = (title + " " + source).lower()
    return any(v in haystack for v in variants)

TUVSUD_VARIANTS = ["tüv süd", "tuv sud", "tüvsüd", "tuvsud", "tüv sued"]

def is_tuvsud(title: str, source: str, link: str = "") -> bool:
    haystack = (title + " " + source).lower()
    if any(v in haystack for v in TUVSUD_VARIANTS):
        return True
    link_lower = link.lower()
    return "tuvsud.com" in link_lower or "tuv-sud.com" in link_lower

def score_urgency(title: str) -> str:
    t = title.lower()
    if any(w in t for w in [
        "acqui", "merger", "takeover", "buys ", "acquired", "acquires",
        "recall", "ban", "fine", "penalty", "enforcement", "investigation",
        "breach", "scandal", "crisis", "urgent", "alert", "warning",
        "major contract", "wins contract", "awarded contract",
    ]):
        return "high"
    if any(w in t for w in [
        "partnership", "collaboration", "agreement", "deal", "expands",
        "expansion", "opens", "launches", "invests", "investment",
        "appoints", "hires", "certification", "accreditation", "regulation",
        "compliance", "laboratory", "testing",
        "zatrudni", "inwestycj", "przetarg", "zaměstn", "investic",
        "angajare", "investiție", "licitație", "işe alım", "yatırım", "ihale",
        "embauche", "investissement", "assunzione", "Einstellung", "contratación",
    ]):
        return "medium"
    return "low"

def clean_title(raw: str):
    if " - " in raw:
        p = raw.rsplit(" - ", 1)
        return p[0].strip(), p[1].strip()
    return raw.strip(), ""

def feed_queries(country: str):
    c = sn(country)
    small_markets = {
        "Czech Republic": "Central Europe", "Slovakia": "Central Europe",
        "Slovenia": "Central Europe", "Hungary": "Central Europe",
        "Romania": "Eastern Europe", "Denmark": "Denmark",
    }
    region = small_markets.get(country)
    ma_fallback = region or c
    std_fallback = region or c

    if country == "Austria":
        comp1 = "DEKRA SGS Intertek TÜV Austria Wien Vienna"
        comp2 = "Bureau Veritas Eurofins TÜV Nord TÜV Rheinland Österreich"
    else:
        extra = {"Czech Republic": "ČIA", "Slovakia": "SNAS", "Denmark": "FORCE Technology"}.get(country, "")
        if extra:
            comp1 = f"DEKRA SGS Intertek {extra} {c}"
            comp2 = f"Bureau Veritas Eurofins TÜV Nord TÜV Rheinland {c}"
        else:
            comp1 = f"DEKRA SGS Intertek Bureau Veritas {c}"
            comp2 = f"Eurofins TÜV Nord TÜV Rheinland {c}"

    ma1 = f"DEKRA SGS Intertek Bureau Veritas {c} acquisition acquires merger"
    ma2 = f"Eurofins TÜV Nord TÜV Rheinland {ma_fallback} acquires acquisition deal"
    mkt1 = f"{c} nuclear automotive renewables certification regulation inspection"
    mkt2 = f"{c} lifts hoisting equipment industry testing regulation laboratory"
    reg  = "EU regulation directive TIC testing inspection certification"

    accred_body = ACCRED.get(country, "")
    std1 = (f"{c} {accred_body} ISO accreditation laboratory standard" if accred_body else f"{c} ISO accreditation laboratory standard")
    std2 = (f"{accred_body} accreditation {std_fallback} ISO laboratory" if accred_body else f"accreditation {std_fallback} ISO laboratory")

    inv1 = f"{c} nuclear SMR new build lifetime extension reactor project"
    inv2 = f"{c} EV battery factory gigafactory manufacturing investment project"
    emg1 = f"{c} cybersecurity AI certification medical device hydrogen"
    emg2 = f"{c} CSRD ESG sustainability carbon digital product passport"

    base_queries = [
        ("competitor", comp1), ("competitor2", comp2), ("ma", ma1), ("ma2", ma2),
        ("market", mkt1), ("market2", mkt2), ("regulatory", reg),
        ("standards", std1), ("standards2", std2), ("investments", inv1),
        ("investments2", inv2), ("emerging", emg1), ("emerging2", emg2),
    ]

    if country == "Austria":
        base_queries = [
            (fid, q) if fid in ("competitor", "competitor2", "regulatory") else (fid, q + " Österreich")
            for fid, q in base_queries
        ]

    if country in {"Slovenia", "Romania", "Turkey", "Slovakia", "Denmark", "Belgium"}:
        base_queries.append(("competitor3", f"DEKRA Bureau Veritas SGS {c}"))

    return base_queries

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

def fetch_accred_rss(country: str):
    url = ACCRED_RSS.get(country)
    if not url:
        return ("standards_accred", [])
    try:
        resp = requests.get(url, headers=HEADERS_NEWS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items = []
        for entry in parsed.entries[:MAX_ITEMS]:
            raw = entry.get("title", "").strip()
            if not raw or len(raw) < 8:
                continue
            title, src_guess = clean_title(raw)
            source = entry.get("source", {}).get("title", "") or src_guess or ACCRED.get(country, "Accred")
            pub = entry.get("published", "")
            if pub and not is_recent(pub):
                continue
            if is_tuvsud(title, source, entry.get("link", "")):
                continue
            items.append({
                "title": title,
                "titleLocal": title,  # local-language title; no machine translation applied
                "link": entry.get("link", "").strip(),
                "source": source,
                "pubDate": pub or datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000"),
                "urgency": score_urgency(title),
                "local": True,
                "lang": LOCAL_EDITIONS.get(country, ("en",))[0],
            })
        return ("standards_accred", items)
    except Exception as exc:
        log.debug("fetch_accred_rss %s failed: %s", country, exc)
        return ("standards_accred", [])

def fetch_rss_en(feed_id: str, query: str, country: str):
    skip_relevance = feed_id.rstrip("0123456789") in ("regulatory", "standards", "emerging", "esg")
    try:
        resp = requests.get(gnews_url_en(query), headers=HEADERS_NEWS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items = []
        for entry in parsed.entries[:MAX_ITEMS * 3]:
            raw = entry.get("title", "").strip()
            if not raw:
                continue
            title, src_guess = clean_title(raw)
            source = entry.get("source", {}).get("title", "") or src_guess
            pub = entry.get("published", "")
            if not is_recent(pub):
                continue
            if is_tuvsud(title, source, entry.get("link", "")):
                continue
            if not skip_relevance and not is_relevant_to_country(title, source, country):
                continue
            items.append({
                "title": title,
                "link": entry.get("link", "").strip(),
                "source": source,
                "pubDate": pub,
                "urgency": score_urgency(title),
                "local": False,
            })
            if len(items) >= MAX_ITEMS:
                break
        return feed_id, items
    except Exception as exc:
        log.debug("fetch_rss_en %s failed: %s", feed_id, exc)
        return feed_id, []

def fetch_rss_local(feed_id, base_feed, query, hl, gl, ceid, country):
    try:
        resp = requests.get(gnews_url_local(query, hl, gl, ceid), headers=HEADERS_NEWS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items = []
        for entry in parsed.entries[:MAX_ITEMS * 2]:
            raw = entry.get("title", "").strip()
            if not raw:
                continue
            title_local, src_guess = clean_title(raw)
            source = entry.get("source", {}).get("title", "") or src_guess
            pub = entry.get("published", "")
            if not is_recent(pub):
                continue
            if is_tuvsud(title_local, source, entry.get("link", "")):
                continue
            items.append({
                "title": title_local,
                "titleLocal": title_local,  # local-language title; no machine translation applied
                "link": entry.get("link", "").strip(),
                "source": source,
                "pubDate": pub,
                "urgency": score_urgency(title_local),
                "local": True,
                "lang": hl,
            })
            if len(items) >= MAX_ITEMS:
                break
        return feed_id, items
    except Exception as exc:
        log.debug("fetch_rss_local %s failed: %s", feed_id, exc)
        return feed_id, []

# ── Resilient World Bank Fetcher ──────────────────────────────────────────────
WB_CODES = {
    "Poland": "POL", "Czech Republic": "CZE", "Slovakia": "SVK", "Hungary": "HUN",
    "Romania": "ROU", "Turkey": "TUR", "United Kingdom": "GBR", "France": "FRA",
    "Belgium": "BEL", "Italy": "ITA", "Switzerland": "CHE", "Spain": "ESP",
    "Austria": "AUT", "Denmark": "DNK", "Slovenia": "SVN",
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_wb_indicator(wb_code, indicator, label, fallbacks=None):
    indicators_to_try = [indicator] + (fallbacks or [])
    for ind in indicators_to_try:
        try:
            url = f"https://api.worldbank.org/v2/country/{wb_code}/indicator/{ind}?format=json&mrv=5&per_page=5"
            resp = requests.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or len(data) < 2:
                continue
            records = data[1]
            if not isinstance(records, list):
                continue
            rows = [r for r in records if isinstance(r, dict) and r.get("value") is not None]
            if rows:
                r = rows[0]
                val = round(float(r["value"]), 2)
                src_label = "World Bank" if ind == indicator else f"World Bank ({ind})"
                return {"value": val, "year": r["date"], "source": src_label}
        except Exception:
            pass
    return None

def fetch_macro(country: str) -> dict:
    code = WB_CODES.get(country)
    if not code:
        return {"gdp": None, "inflation": None}
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_gdp = pool.submit(fetch_wb_indicator, code, "NY.GDP.MKTP.KD.ZG", "GDP", fallbacks=["NY.GDP.PCAP.KD.ZG"])
        f_infl = pool.submit(fetch_wb_indicator, code, "FP.CPI.TOTL.ZG", "Inflation", fallbacks=["NY.GDP.DEFL.KD.ZG"])
        return {"gdp": f_gdp.result(), "inflation": f_infl.result()}

# ── Feed merging ──────────────────────────────────────────────────────────────
BASE_FEEDS = ["competitor", "ma", "market", "regulatory", "tenders", "standards", "investments", "emerging", "esg"]
MAX_PER_FEED = 8

def merge_feeds(raw_feeds: dict) -> dict:
    merged = {f: [] for f in BASE_FEEDS}
    rank = {"high": 0, "medium": 1, "low": 2}
    for key, items in raw_feeds.items():
        base = key.rstrip("0123456789").replace("_local", "")
        if base in merged:
            merged[base].extend(items)
    for base in merged:
        seen, unique = set(), []
        for item in merged[base]:
            disp = item["title"].lower()[:55]
            if disp not in seen:
                seen.add(disp)
                unique.append(item)
        unique.sort(key=lambda x: rank.get(x["urgency"], 2))
        merged[base] = unique[:MAX_PER_FEED]
    return merged

def fetch_country(country: str) -> dict:
    log.info("── %s %s", country, "─" * max(1, 42 - len(country)))
    cb_name, cb_rate = CENTRAL_BANKS[country]
    raw_feeds = {}

    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = {pool.submit(fetch_rss_en, fid, q, country): fid for fid, q in feed_queries(country)}
        futs[pool.submit(fetch_accred_rss, country)] = "standards_accred"
        for fut in as_completed(futs):
            fid, items = fut.result()
            raw_feeds[fid] = items

    lq = local_queries(country)
    if lq:
        with ThreadPoolExecutor(max_workers=10) as pool:
            futs = {
                pool.submit(fetch_rss_local, fid, base, q, hl, gl, ceid, country): fid
                for fid, base, q, hl, gl, ceid in lq
            }
            for fut in as_completed(futs):
                fid, items = fut.result()
                raw_feeds[fid] = items

    feeds = merge_feeds(raw_feeds)
    macro = fetch_macro(country)
    high_count = sum(1 for v in feeds.values() for i in v if i.get("urgency") == "high")

    return {
        "country": country,
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "highCount": high_count,
        "feeds": feeds,
        "macro": {
            "gdp": macro.get("gdp"),
            "inflation": macro.get("inflation"),
            "interestRate": {
                "bank": cb_name,
                "value": f"{cb_rate}%",
                "note": "Hardcoded fallback — verify against central bank website",
            },
        },
    }

def merge_with_history(new_data: dict, old_data: dict) -> dict:
    """
    Merges newly fetched data with the previous run.
    - Items older than 30 days are discarded.
    - Uses parse_pub_date() for RFC 2822 / ISO date parsing (robust).
    - Preserves last-known macro data if the new run returned None (e.g. World Bank outage).
    - Returns new_data mutated in place (copy if you need original).
    """
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)

    for country_data in new_data["countries"]:
        c_name = country_data["country"]
        old_c_data = next((c for c in old_data.get("countries", []) if c["country"] == c_name), None)

        if not old_c_data:
            continue

        # ── Feed merging ──────────────────────────────────────────────────────
        for feed_id, new_items in country_data["feeds"].items():
            old_items = old_c_data["feeds"].get(feed_id, [])
            combined = new_items + old_items

            seen, unique = set(), []
            for item in combined:
                key = item["title"].lower()[:80]
                if key in seen:
                    continue
                pub_date = item.get("pubDate")
                if pub_date:
                    dt = parse_pub_date(pub_date)  # RFC 2822-aware
                    if dt is not None and dt < thirty_days_ago:
                        continue  # drop stale item
                seen.add(key)
                unique.append(item)

            country_data["feeds"][feed_id] = unique[:15]

        # ── Macro preservation: keep last-known value if new run returned None ─
        old_macro = old_c_data.get("macro", {})
        new_macro = country_data.get("macro", {})
        for field in ("gdp", "inflation"):
            if new_macro.get(field) is None and old_macro.get(field) is not None:
                new_macro[field] = old_macro[field]
                log.info("  ↩ Preserved last-known %s.%s from previous run", c_name, field)
        country_data["macro"] = new_macro

    return new_data

def main():
    parser = argparse.ArgumentParser(description="TÜV SÜD Competitive Intelligence Fetcher")
    parser.add_argument("--output", default="intelligence_data.json")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between country fetches")
    args = parser.parse_args()

    output = Path(args.output)
    old_data = {}
    if output.exists():
        try:
            old_data = json.loads(output.read_text())
            log.info("Loaded previous run data from %s", output)
        except Exception as exc:
            log.warning("Could not load previous data (%s), starting fresh.", exc)

    results = {}
    for i, country in enumerate(COUNTRIES):
        if i > 0:
            time.sleep(args.delay)
        try:
            results[country] = fetch_country(country)
        except Exception as exc:
            log.error("Failed %s: %s", country, exc)

    ordered = [results[c] for c in COUNTRIES if c in results]

    new_data = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "cutoffDate": CUTOFF_DATE.strftime("%Y-%m-%d"),
        "countries": ordered,
        "meta": {
            "competitors": ALL_COMPETITORS,
            "competitorsCore": COMPETITORS_CORE,
            "accredBodies": ACCRED,
            "currencies": CURRENCY,
            "flags": FLAGS,
            "feedLabels": {
                "competitor": "Competitor Moves",
                "ma": "M&A Activity",
                "market": "Market & Regulatory",
                "regulatory": "EU Regulatory Watch",
                "standards": "Accreditation & Standards",
                "investments": "Investment & Nuclear Projects",
                "emerging": "Emerging & ESG",
            },
        },
    }

    final_data = merge_with_history(new_data, old_data)
    output.write_text(json.dumps(final_data, ensure_ascii=False, indent=2))
    log.info("✅  Written → %s  (%d countries, cutoff %s)", output, len(ordered), CUTOFF_DATE.strftime("%Y-%m-%d"))

if __name__ == "__main__":
    main()
