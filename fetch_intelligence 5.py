#!/usr/bin/env python3
"""
TÜV SÜD Competitive Intelligence — Backend Fetcher  v4
========================================================
Fetches Google News RSS (English + local-language editions) + World Bank.
Local-language headlines are translated to English via the Anthropic API.
Outputs intelligence_data.json consumed by the dashboard.

Install:  pip install requests feedparser anthropic
Run:      python fetch_intelligence.py
Single:   python fetch_intelligence.py --country Poland
"""

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import feedparser
import requests

# Anthropic client (only used for translation — gracefully skipped if unavailable)
try:
    import anthropic
    _ANTHROPIC = anthropic.Anthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY", "")
    )
    _HAS_ANTHROPIC = bool(os.environ.get("ANTHROPIC_API_KEY"))
except ImportError:
    _ANTHROPIC = None
    _HAS_ANTHROPIC = False

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

SEARCH_NAME = {
    "United Kingdom": "UK",
    "Czech Republic": "Czech",
}
def sn(country): return SEARCH_NAME.get(country, country)

# ── Local Google News editions per country ────────────────────────────────────
# Each entry: (hl, gl, ceid)  — the locale parameters for Google News RSS
# Plus local-language keywords for each feed type
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
    "United Kingdom": None,   # already English
}

# Local search terms per feed type — indexed by language code
# These are short (≤5 words) native-language keywords for each feed
LOCAL_KEYWORDS = {
    # competitor: TIC company activity
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
    # jobs: hiring signals
    "jobs": {
        "pl": "praca audytor inspektor certyfikacja",
        "cs": "práce auditor inspektor certifikace",
        "sk": "práca audítor inšpektor certifikácia",
        "hu": "állás auditor inspektor tanúsítás",
        "ro": "job auditor inspector certificare",
        "tr": "iş ilanı denetçi muayene",
        "fr": "emploi auditeur inspecteur certification",
        "it": "lavoro auditor ispettore certificazione",
        "de": "Stelle Auditor Inspektor Zertifizierung",
        "es": "empleo auditor inspector certificación",
        "da": "job revisor inspektør certificering",
        "sl": "zaposlitev revizor inšpektor certificiranje",
    },
    # market: regulatory & market news
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
    # investments: factory/infrastructure projects
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
    # tenders: public contracts
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
    # esg
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

# Feed types to query in local language (skip 'regulatory' — EU news is English)
LOCAL_FEED_TYPES = ["competitor", "jobs", "market", "investments", "tenders", "esg"]

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


# ── English feed queries (unchanged from v3) ──────────────────────────────────
def feed_queries(country: str) -> list[tuple[str, str, str]]:
    c = sn(country)
    return [
        ("competitor",  "Competitor Moves",
            f"DEKRA SGS Intertek {c} testing certification"),
        ("competitor2", "Competitor Moves",
            f"Bureau Veritas Eurofins {c} laboratory inspection"),
        ("jobs",        "Hiring Signals",
            f"auditor inspector certification jobs {c} 2025"),
        ("jobs2",       "Hiring Signals",
            f"testing laboratory engineer careers {c} hiring"),
        ("ma",          "M&A Activity",
            f"DEKRA SGS Eurofins {c} acquisition merger deal"),
        ("ma2",         "M&A Activity",
            f"Bureau Veritas Intertek {c} laboratory buyout 2025"),
        ("market",      "Market & Regulatory",
            f"{c} certification inspection regulation laboratory"),
        ("regulatory",  "EU Regulatory Watch",
            f"CSRD DORA AI Act certification compliance 2025"),
        ("tenders",     "Tenders & Contracts",
            f"{c} inspection certification tender contract government"),
        ("standards",   "Accreditation & Standards",
            f"{c} ISO standard accreditation laboratory 2025"),
        ("investments", "Investment Projects",
            f"{c} EV battery factory gigafactory investment 2025"),
        ("investments2","Investment Projects",
            f"{c} manufacturing plant energy investment project"),
        ("emerging",    "Emerging Sectors",
            f"{c} cybersecurity AI certification medical device"),
        ("emerging2",   "Emerging Sectors",
            f"{c} hydrogen certification digital product passport 2025"),
        ("esg",         "Sustainability & ESG",
            f"{c} CSRD ESG sustainability audit carbon 2025"),
    ]


# ── Local-language queries ────────────────────────────────────────────────────
def local_queries(country: str) -> list[tuple[str, str, str, str]]:
    """
    Returns list of (feed_id, label, query, locale_suffix).
    locale_suffix is appended to gnews URL to target local edition.
    Returns [] for English-only countries.
    """
    edition = LOCAL_EDITIONS.get(country)
    if not edition:
        return []
    hl, gl, ceid = edition
    queries = []
    for feed_type in LOCAL_FEED_TYPES:
        kw = LOCAL_KEYWORDS.get(feed_type, {}).get(hl)
        if not kw:
            continue
        feed_id = f"{feed_type}_local"
        queries.append((feed_id, feed_type, kw, hl, gl, ceid))
    return queries


# ── Fetch helpers ─────────────────────────────────────────────────────────────
TIMEOUT   = 15
MAX_ITEMS = 8
HEADERS   = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def gnews_url_en(query: str) -> str:
    date_filter = "&tbs=cdr:1,cd_min:1%2F1%2F2025"
    return (f"https://news.google.com/rss/search"
            f"?q={quote(query)}&hl=en-US&gl=US&ceid=US:en{date_filter}")

def gnews_url_local(query: str, hl: str, gl: str, ceid: str) -> str:
    date_filter = "&tbs=cdr:1,cd_min:1%2F1%2F2025"
    return (f"https://news.google.com/rss/search"
            f"?q={quote(query)}&hl={hl}&gl={gl}&ceid={ceid}{date_filter}")


def score_urgency(title: str) -> str:
    t = title.lower()
    if any(k in t for k in [
        "acqui", "merger", "acquires", "takeover", "buyout",
        "launches", "launch", "opens", "expands", "expansion",
        "wins contract", "awarded", "secures contract",
        "ipo", "record deal", "major contract",
        "przejęcie", "fuzja", "otwarcie", "inwestycja",   # Polish
        "akvizice", "fúze", "otevření",                    # Czech
        "aquisição", "fusão", "abertura",                  # Romanian
        "satın alma", "birleşme", "açılış",                # Turkish
        "acquisition", "fusion", "ouverture",              # French
        "acquisizione", "fusione", "apertura",             # Italian
        "Übernahme", "Fusion", "Eröffnung",                # German
        "adquisición", "fusión", "apertura",               # Spanish
    ]):
        return "high"
    if any(k in t for k in [
        "hire", "hiring", "recruit", "invest", "investment",
        "partner", "certif", "announce", "tender", "contract",
        "standard", "regulation", "accredit", "compliance",
        "zatrudni", "inwestycj", "przetarg",               # Polish
        "zaměstn", "investic", "tendr",                    # Czech
        "angajare", "investiție", "licitație",             # Romanian
        "işe alım", "yatırım", "ihale",                    # Turkish
        "embauche", "investissement", "appel",             # French
        "assunzione", "investimento", "gara",              # Italian
        "Einstellung", "Investition", "Ausschreibung",     # German
        "contratación", "inversión", "licitación",        # Spanish
    ]):
        return "medium"
    return "low"


def clean_title(raw: str) -> tuple[str, str]:
    if " - " in raw:
        parts = raw.rsplit(" - ", 1)
        return parts[0].strip(), parts[1].strip()
    return raw.strip(), ""


def fetch_rss_en(feed_id: str, query: str, country: str) -> tuple[str, list[dict]]:
    url = gnews_url_en(query)
    try:
        resp   = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items  = []
        for entry in parsed.entries[:MAX_ITEMS]:
            raw = entry.get("title", "").strip()
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
                "local":   False,
            })
        log.info("  ✓ EN %-14s (%s) %d items", feed_id, country, len(items))
        return feed_id, items
    except Exception as exc:
        log.warning("  ✗ EN %-14s (%s) %s", feed_id, country, exc)
        return feed_id, []


def fetch_rss_local(feed_id: str, base_feed: str, query: str,
                    hl: str, gl: str, ceid: str, country: str) -> tuple[str, list[dict]]:
    url = gnews_url_local(query, hl, gl, ceid)
    try:
        resp   = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        items  = []
        for entry in parsed.entries[:MAX_ITEMS]:
            raw = entry.get("title", "").strip()
            if not raw:
                continue
            title_local, src_guess = clean_title(raw)
            source = entry.get("source", {}).get("title", "") or src_guess
            items.append({
                "title":       title_local,   # original language (translated later)
                "titleEN":     None,           # filled in by translate_batch
                "link":        entry.get("link", "").strip(),
                "source":      source,
                "pubDate":     entry.get("published", ""),
                "urgency":     score_urgency(title_local),
                "local":       True,
                "lang":        hl,
            })
        log.info("  ✓ LOCAL %-10s (%s/%s) %d items", base_feed, country, hl.upper(), len(items))
        return feed_id, items
    except Exception as exc:
        log.warning("  ✗ LOCAL %-10s (%s) %s", base_feed, country, exc)
        return feed_id, []


# ── Batch translation via Anthropic ──────────────────────────────────────────
def translate_batch(items: list[dict], country: str) -> list[dict]:
    """
    Translate all items where local=True and titleEN is None.
    Uses a single Anthropic API call per country for efficiency.
    Falls back to original title if API unavailable.
    """
    to_translate = [i for i in items if i.get("local") and not i.get("titleEN")]
    if not to_translate:
        return items
    if not _HAS_ANTHROPIC:
        # No API key — use original title as fallback
        for item in to_translate:
            item["titleEN"] = item["title"]
        log.warning("  ⚠ No ANTHROPIC_API_KEY — local headlines shown in original language")
        return items

    titles = [it["title"] for it in to_translate]
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))
    prompt = (
        f"Translate these {len(titles)} news headlines from their original language to English. "
        f"These are about business, industry and regulation in {country}. "
        f"Return ONLY a JSON array of translated strings, one per line, same order. "
        f"Keep proper nouns, company names and acronyms unchanged. "
        f"Be concise — match the style of a news headline.\n\n"
        f"Headlines:\n{numbered}\n\n"
        f"Return only valid JSON array, example: [\"Headline 1 in English\", \"Headline 2 in English\"]"
    )
    try:
        msg = _ANTHROPIC.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown fences if present
        raw = raw.strip("`").strip()
        if raw.startswith("json"):
            raw = raw[4:].strip()
        translations = json.loads(raw)
        if isinstance(translations, list) and len(translations) == len(to_translate):
            for item, translated in zip(to_translate, translations):
                item["titleEN"] = translated.strip()
            log.info("  ✓ Translated %d local headlines for %s", len(to_translate), country)
        else:
            raise ValueError(f"Expected {len(to_translate)} translations, got {len(translations)}")
    except Exception as exc:
        log.warning("  ✗ Translation failed for %s: %s — using original", country, exc)
        for item in to_translate:
            if not item.get("titleEN"):
                item["titleEN"] = item["title"]
    return items


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


# ── Feed merging ──────────────────────────────────────────────────────────────
BASE_FEEDS = [
    "competitor", "jobs", "ma", "market", "regulatory",
    "tenders", "standards", "investments", "emerging", "esg",
]
MAX_PER_FEED = 8   # increased slightly to accommodate local sources

def merge_feeds(raw_feeds: dict[str, list]) -> dict[str, list]:
    """
    Merge numbered variants + _local variants into base feeds.
    Deduplicates by title, sorts HIGH → MED → LOW.
    Local items are interleaved with English items.
    """
    merged: dict[str, list] = {f: [] for f in BASE_FEEDS}
    urgency_rank = {"high": 0, "medium": 1, "low": 2}

    for key, items in raw_feeds.items():
        # "jobs2" → "jobs", "competitor_local" → "competitor"
        base = key.rstrip("0123456789")
        base = base.replace("_local", "")
        if base in merged:
            merged[base].extend(items)

    for base in merged:
        seen   = set()
        unique = []
        for item in merged[base]:
            # Deduplicate by first 55 chars of display title
            display = (item.get("titleEN") or item["title"]).lower()[:55]
            if display not in seen:
                seen.add(display)
                unique.append(item)
        unique.sort(key=lambda x: urgency_rank.get(x["urgency"], 2))
        merged[base] = unique[:MAX_PER_FEED]

    return merged


# ── Per-country fetch ─────────────────────────────────────────────────────────
def fetch_country(country: str) -> dict:
    log.info("── %s %s", country, "─" * max(1, 42 - len(country)))
    cb_name, cb_rate = CENTRAL_BANKS[country]
    raw_feeds: dict[str, list] = {}

    # ── English queries ──
    en_futures = {}
    with ThreadPoolExecutor(max_workers=15) as pool:
        for fid, _label, query in feed_queries(country):
            en_futures[pool.submit(fetch_rss_en, fid, query, country)] = fid
        for fut in as_completed(en_futures):
            fid, items = fut.result()
            raw_feeds[fid] = items

    # ── Local-language queries ──
    lq = local_queries(country)
    if lq:
        local_futures = {}
        with ThreadPoolExecutor(max_workers=10) as pool:
            for feed_id, base_feed, query, hl, gl, ceid in lq:
                local_futures[pool.submit(
                    fetch_rss_local, feed_id, base_feed, query, hl, gl, ceid, country
                )] = feed_id
            for fut in as_completed(local_futures):
                fid, items = fut.result()
                raw_feeds[fid] = items

    # ── Merge all feeds ──
    feeds = merge_feeds(raw_feeds)

    # ── Translate local headlines ──
    all_items = [item for feed_items in feeds.values() for item in feed_items]
    all_items = translate_batch(all_items, country)
    # Re-build feeds dict with translated items (translate_batch mutates in-place)

    # ── Macro ──
    macro      = fetch_world_bank(WORLD_BANK[country])
    high_count = sum(1 for v in feeds.values() for i in v if i.get("urgency") == "high")

    local_count = sum(1 for v in feeds.values() for i in v if i.get("local"))
    log.info("  → %d total items (%d local-language) for %s",
             sum(len(v) for v in feeds.values()), local_count, country)

    return {
        "country":    country,
        "fetchedAt":  datetime.now(timezone.utc).isoformat(),
        "highCount":  high_count,
        "feeds":      feeds,
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


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="TIC Intelligence Fetcher v4")
    parser.add_argument("--country", choices=COUNTRIES, metavar="COUNTRY")
    parser.add_argument("--output",  default="intelligence_data.json")
    parser.add_argument("--delay",   type=float, default=2.0,
                        help="Seconds between countries to avoid rate-limiting")
    args = parser.parse_args()

    if _HAS_ANTHROPIC:
        log.info("Anthropic API key found — local headlines will be translated ✓")
    else:
        log.warning("No ANTHROPIC_API_KEY env var — local headlines shown in original language")

    countries = [args.country] if args.country else COUNTRIES
    output    = Path(args.output)

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
    local_items = sum(
        1 for r in ordered
        for v in r["feeds"].values()
        for i in v if i.get("local")
    )

    log.info("")
    log.info("✅  Done in %.1fs  →  %s", elapsed, output)
    log.info("    Countries:   %d", len(ordered))
    log.info("    Total items: %d  (%d local-language, %d English)",
             total_items, local_items, total_items - local_items)


if __name__ == "__main__":
    main()
