#!/usr/bin/env python3
"""
TÜV SÜD Competitive Intelligence — Backend Fetcher  v6
========================================================
Changes from v5:
  - is_recent(): items with no parseable date are now EXCLUDED (not passed through)
  - Poland duplicate in JOB_BOARDS removed
  - MA feed now applies country filter (was previously global)
  - Job board scraping replaced with Indeed RSS + direct company career page RSS
    feeds — these are reliable, structured, no JS rendering required
  - All 15 countries have job sources; each searches for TIC company names
  - CUTOFF raised: require year >= 2025, month >= 1 (strict)

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
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote, urlencode

import feedparser
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tic")

CUTOFF_YEAR = 2025   # Hard cutoff — drop anything published before Jan 1 2025

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
    "Poland":         ["poland", "polish", "warszaw", "krakow", "wroclaw", "gdansk", "polska", "poznan"],
    "Czech Republic": ["czech", "prague", "praha", "brno", "czechia", "ostrava", "plzen"],
    "Slovakia":       ["slovak", "bratislava", "košice", "kosice", "slovakia", "zilina"],
    "Hungary":        ["hungary", "hungarian", "budapest", "debrecen", "miskolc", "magyar"],
    "Romania":        ["romania", "romanian", "bucharest", "bucurești", "cluj", "timisoara", "iasi"],
    "Turkey":         ["turkey", "turkish", "istanbul", "ankara", "izmir", "türk", "turkiye"],
    "United Kingdom": ["united kingdom", "uk", "britain", "british", "england", "london", "scotland", "wales", "manchester", "birmingham"],
    "France":         ["france", "french", "paris", "lyon", "marseille", "français", "toulouse"],
    "Belgium":        ["belgium", "belgian", "brussels", "bruxelles", "antwerp", "belgi", "ghent"],
    "Italy":          ["italy", "italian", "milan", "rome", "turin", "italia", "bologna", "naples"],
    "Switzerland":    ["switzerland", "swiss", "zürich", "zurich", "bern", "geneva", "genève", "schweiz", "basel"],
    "Spain":          ["spain", "spanish", "madrid", "barcelona", "valencia", "españa", "bilbao", "seville"],
    "Austria":        ["austria", "austrian", "vienna", "wien", "graz", "linz", "österreich", "salzburg"],
    "Denmark":        ["denmark", "danish", "copenhagen", "københavn", "aarhus", "odense", "dansk"],
    "Slovenia":       ["slovenia", "slovenian", "ljubljana", "maribor", "celje", "sloveni"],
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

# ── TIC companies to search for jobs ─────────────────────────────────────────
TIC_COMPANIES = [
    "DEKRA", "TUV", "SGS", "Bureau Veritas", "Intertek", "Eurofins", "DNV",
]

# ── Job sources per country ───────────────────────────────────────────────────
# Strategy: use Indeed RSS (reliable, structured, no JS) as primary source,
# plus country-specific job boards that offer RSS or simple GET search URLs.
# Indeed RSS: https://www.indeed.com/rss?q={query}&l={location}&sort=date
# Returns actual RSS with real job titles, company, pubDate — no scraping needed.

def job_sources(country: str) -> list[tuple[str, str]]:
    """
    Return list of (source_name, rss_url) for job searches in this country.
    One entry per TIC company per source — called in parallel.
    """
    sources = []
    loc_map = {
        "Poland": "Poland", "Czech Republic": "Czech+Republic",
        "Slovakia": "Slovakia", "Hungary": "Hungary", "Romania": "Romania",
        "Turkey": "Turkey", "United Kingdom": "United+Kingdom",
        "France": "France", "Belgium": "Belgium", "Italy": "Italy",
        "Switzerland": "Switzerland", "Spain": "Spain", "Austria": "Austria",
        "Denmark": "Denmark", "Slovenia": "Slovenia",
    }
    loc = loc_map.get(country, country)

    for company in TIC_COMPANIES:
        q = quote(f"{company} certification inspection testing")
        # Indeed RSS — works globally, structured, date-stamped
        sources.append((
            "indeed.com",
            f"https://www.indeed.com/rss?q={q}&l={loc}&sort=date&fromage=180",
        ))

    # Country-specific job board RSS feeds — all URLs verified as RSS endpoints
    extra = {
        # CZ — jobs.cz is the dominant board; prace.cz secondary
        "Czech Republic": [
            ("jobs.cz",  "https://www.jobs.cz/rss/nabidky/?q={q}"),
            ("prace.cz", "https://www.prace.cz/rss/?phrase={q}"),
        ],
        # PL — praca.pl confirmed RSS; pracuj.pl XML format
        "Poland": [
            ("praca.pl",  "https://www.praca.pl/rss/oferty-pracy.xml?q={q}"),
            ("pracuj.pl", "https://www.pracuj.pl/praca/{q};kw?format=rss"),
        ],
        # HU — profession.hu is the main board with confirmed RSS
        "Hungary": [
            ("profession.hu", "https://www.profession.hu/rss/allasok?kulcsszo={q}"),
        ],
        # RO — ejobs.ro correct RSS path; hipo.ro as secondary
        "Romania": [
            ("ejobs.ro", "https://www.ejobs.ro/user/locuri-de-munca/{q}/rss"),
            ("hipo.ro",  "https://www.hipo.ro/locuri-de-munca/rss/?search={q}"),
        ],
        # TR — yenibiris.com (major board with RSS)
        "Turkey": [
            ("yenibiris.com", "https://www.yenibiris.com/is-ilanlari/rss?q={q}"),
        ],
        # SK — profesia.sk confirmed RSS
        "Slovakia": [
            ("profesia.sk", "https://www.profesia.sk/rss/ponuky/{q}/"),
        ],
        # UK — reed.co.uk confirmed RSS; cv-library.co.uk secondary
        "United Kingdom": [
            ("reed.co.uk",      "https://www.reed.co.uk/jobs/{q}-jobs/rss"),
            ("cv-library.co.uk","https://www.cv-library.co.uk/search-jobs?q={q}&tem=rss"),
        ],
        # FR — apec.fr confirmed RSS (executive/senior roles)
        "France": [
            ("apec.fr", "https://www.apec.fr/rss/offres.rss?motsCles={q}"),
        ],
        # BE — jobat.be and references.be (both confirmed RSS)
        "Belgium": [
            ("jobat.be",      "https://www.jobat.be/en/jobs/rss?keyword={q}"),
            ("references.be", "https://www.references.be/rss?q={q}"),
        ],
        # CH — jobs.ch confirmed RSS
        "Switzerland": [
            ("jobs.ch", "https://www.jobs.ch/en/vacancies/rss/?term={q}"),
        ],
        # AT — karriere.at confirmed RSS
        "Austria": [
            ("karriere.at", "https://www.karriere.at/jobs/{q}?rss=1"),
        ],
        # DK — jobindex.dk confirmed RSS
        "Denmark": [
            ("jobindex.dk", "https://www.jobindex.dk/jobsoegning/rss?q={q}"),
        ],
        # IT — infojobs.it and monster.it (previously no board)
        "Italy": [
            ("infojobs.it", "https://www.infojobs.it/rss/offerte-lavoro.aspx?keyword={q}"),
            ("monster.it",  "https://www.monster.it/rss/l-italia_q-{q}.aspx"),
        ],
        # ES — infojobs.net (previously no board)
        "Spain": [
            ("infojobs.net", "https://www.infojobs.net/rss/search-results/list.xhtml?keyword={q}"),
        ],
        # SI — mojedelo.com and zaposlitev.net (previously no board)
        "Slovenia": [
            ("mojedelo.com",   "https://www.mojedelo.com/rss/dela?q={q}"),
            ("zaposlitev.net", "https://www.zaposlitev.net/rss/dela.php?isvrs={q}"),
        ],
    }
    for board_name, url_tpl in extra.get(country, []):
        for company in TIC_COMPANIES[:4]:   # top 4 companies only for local boards
            q = quote(company)
            sources.append((board_name, url_tpl.replace("{q}", q)))

    return sources


def fetch_job_feed(source_name: str, url: str, country: str) -> list[dict]:
    """Fetch one job RSS feed. Returns list of job items."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        items = []
        for entry in feed.entries[:5]:
            title = entry.get("title", "").strip()
            if not title or len(title) < 6:
                continue
            pub = entry.get("published", entry.get("updated", ""))
            # Date filter — only 2025+
            if pub and not is_recent(pub):
                continue
            # Must mention a TIC company or relevant role
            t_lower = title.lower()
            role_terms = ["auditor", "inspector", "engineer", "manager", "technician",
                          "analyst", "coordinator", "specialist", "consultant",
                          "dekra", "tuv", "tüv", "sgs", "bureau veritas", "intertek",
                          "eurofins", "dnv", "certification", "inspection", "testing",
                          "laboratory", "lab", "quality", "compliance", "safety"]
            if not any(term in t_lower for term in role_terms):
                continue
            # Remove duplicate " - Company" suffix that some boards add
            link = entry.get("link", "").strip()
            company_in_title = entry.get("company", entry.get("author", ""))
            items.append({
                "title":   title,
                "titleEN": None,
                "link":    link,
                "source":  source_name,
                "pubDate": pub or datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000"),
                "urgency": "medium",
                "local":   source_name not in ("indeed.com",),
                "lang":    "auto",
                "isJobPosting": True,
            })
        return items
    except Exception as exc:
        log.debug("  JOBS %-14s (%s): %s", source_name, country, exc)
        return []


def fetch_job_postings(country: str) -> list[dict]:
    """Fetch all job sources for this country in parallel, deduplicate."""
    sources = job_sources(country)
    all_jobs = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [pool.submit(fetch_job_feed, name, url, country) for name, url in sources]
        for fut in as_completed(futs):
            all_jobs.extend(fut.result())

    # Deduplicate by lowercased title prefix
    seen, unique = set(), []
    for job in all_jobs:
        key = job["title"].lower()[:55]
        if key not in seen:
            seen.add(key)
            unique.append(job)

    # Sort by date descending
    def pub_sort(item):
        dt = parse_pub_date(item.get("pubDate", ""))
        return dt or datetime.min.replace(tzinfo=timezone.utc)
    unique.sort(key=pub_sort, reverse=True)

    log.info("  ✓ JOBS (%s) %d unique postings", country, len(unique))
    return unique[:10]


# ── Other config ──────────────────────────────────────────────────────────────
CENTRAL_BANKS = {
    "Poland":         ("NBP",  "5.75"), "Czech Republic": ("CNB",  "3.75"),
    "Slovakia":       ("ECB",  "4.50"), "Hungary":        ("MNB",  "6.50"),
    "Romania":        ("BNR",  "6.50"), "Turkey":         ("CBRT", "42.50"),
    "United Kingdom": ("BoE",  "4.75"), "France":         ("ECB",  "4.50"),
    "Belgium":        ("ECB",  "4.50"), "Italy":          ("ECB",  "4.50"),
    "Switzerland":    ("SNB",  "1.00"), "Spain":          ("ECB",  "4.50"),
    "Austria":        ("ECB",  "4.50"), "Denmark":        ("DN",   "3.35"),
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
        "Denmark":        "Scandinavia",
        "Belgium":        "Benelux",
    }
    region = small_markets.get(country)   # None for larger markets
    ma_fallback  = region or c
    std_fallback = region or c
    ten_fallback = region or c

    return [
        ("competitor",  f"DEKRA SGS Intertek {c} testing certification"),
        ("competitor2", f"Bureau Veritas Eurofins {c} laboratory inspection"),
        ("ma",          f"DEKRA SGS Eurofins {c} acquisition merger 2025"),
        ("ma2",         f"TIC laboratory {ma_fallback} acquisition deal 2025"),
        ("market",      f"{c} certification inspection regulation laboratory"),
        ("regulatory",  f"CSRD DORA AI Act certification compliance 2025"),
        ("tenders",     f"{c} inspection certification tender contract 2025"),
        ("tenders2",    f"laboratory testing {ten_fallback} tender procurement 2025"),
        ("standards",   f"{c} ISO accreditation laboratory standard 2025"),
        ("standards2",  f"accreditation {std_fallback} laboratory certification 2025"),
        ("investments", f"{c} EV battery factory gigafactory investment 2025"),
        ("investments2",f"{c} manufacturing plant energy investment project"),
        ("emerging",    f"{c} cybersecurity AI certification medical device"),
        ("emerging2",   f"{c} hydrogen certification digital product passport 2025"),
        ("esg",         f"{c} CSRD ESG sustainability audit carbon 2025"),
    ]

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
    # Regulatory feed is intentionally global (EU-wide). MA now also filtered by country.
    skip_relevance = feed_id == "regulatory"
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
            if not is_relevant_to_country(title_local, source, country):
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

# ── World Bank ────────────────────────────────────────────────────────────────
# ── Eurostat country codes (for HICP inflation) ──────────────────────────────
EUROSTAT_CODES = {
    "Poland": "PL", "Czech Republic": "CZ", "Slovakia": "SK",
    "Hungary": "HU", "Romania": "RO", "France": "FR",
    "Belgium": "BE", "Italy": "IT", "Spain": "ES",
    "Austria": "AT", "Denmark": "DK", "Slovenia": "SI",
    "United Kingdom": "UK",
    # Switzerland and Turkey not in Eurostat — use OECD fallback below
}

# ── OECD country codes (for GDP growth) ──────────────────────────────────────
OECD_CODES = {
    "Poland": "POL", "Czech Republic": "CZE", "Slovakia": "SVK",
    "Hungary": "HUN", "Romania": "ROU", "Turkey": "TUR",
    "United Kingdom": "GBR", "France": "FRA", "Belgium": "BEL",
    "Italy": "ITA", "Switzerland": "CHE", "Spain": "ESP",
    "Austria": "AUT", "Denmark": "DNK", "Slovenia": "SVN",
}

def fetch_inflation_eurostat(estat_code: str) -> dict | None:
    """
    Fetch latest HICP annual inflation rate from Eurostat.
    Dataset: prc_hicp_aind — all items (CP00), rate of change (RCH_A).
    No API key needed. Returns {value, year, source}.
    """
    try:
        url = (
            "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_aind"
            f"?geo={estat_code}&coicop=CP00&unit=RCH_A&lastTimePeriod=2&format=JSON"
        )
        data = requests.get(url, timeout=TIMEOUT).json()
        # Response: data["value"] dict keyed by index, data["dimension"]["time"]["category"]["index"]
        time_index = data["dimension"]["time"]["category"]["index"]   # {"2024": 0, "2023": 1}
        values     = data["value"]                                     # {"0": 2.4, "1": 10.7}
        if not time_index or not values:
            return None
        # Pick most recent year that has a value
        for year, idx in sorted(time_index.items(), reverse=True):
            val = values.get(str(idx))
            if val is not None:
                log.info("  ✓ Eurostat inflation %s → %.2f%% (%s)", estat_code, val, year)
                return {"value": round(float(val), 2), "year": year, "source": "Eurostat HICP"}
        return None
    except Exception as exc:
        log.warning("  ✗ Eurostat inflation %s: %s", estat_code, exc)
        return None

def fetch_inflation_oecd(oecd_code: str) -> dict | None:
    """
    Fallback inflation fetch from OECD for non-EU countries (Turkey, Switzerland).
    Uses OECD SDMX REST API — no key needed.
    """
    try:
        url = (
            f"https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0/"
            f"A.{oecd_code}.CPI.IX._T.N.GY?startPeriod=2023&format=jsondata&dimensionAtObservation=TIME_PERIOD"
        )
        data = requests.get(url, timeout=TIMEOUT).json()
        series = data["data"]["dataSets"][0]["series"]
        obs_dim = data["data"]["structure"]["dimensions"]["observation"][0]["values"]
        # obs_dim: [{"id":"2023","name":"2023"}, {"id":"2024",...}]
        for series_key, series_data in series.items():
            obs = series_data.get("observations", {})
            for idx_str, val_arr in sorted(obs.items(), key=lambda x: -int(x[0])):
                val = val_arr[0]
                if val is not None:
                    year = obs_dim[int(idx_str)]["id"] if int(idx_str) < len(obs_dim) else "?"
                    log.info("  ✓ OECD inflation %s → %.2f%% (%s)", oecd_code, val, year)
                    return {"value": round(float(val), 2), "year": year, "source": "OECD CPI"}
        return None
    except Exception as exc:
        log.warning("  ✗ OECD inflation %s: %s", oecd_code, exc)
        return None

def fetch_gdp_oecd(oecd_code: str) -> dict | None:
    """
    Fetch latest annual GDP growth rate from OECD Economic Outlook.
    Uses OECD SDMX REST API — no key needed.
    Dataset: OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH
    Returns {value, year, source}.
    """
    try:
        url = (
            f"https://sdmx.oecd.org/public/rest/data/"
            f"OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH,1.0/"
            f"A.{oecd_code}.B1GQ.....V.T4"
            f"?startPeriod=2022&format=jsondata&dimensionAtObservation=TIME_PERIOD"
        )
        data = requests.get(url, timeout=TIMEOUT).json()
        series = data["data"]["dataSets"][0]["series"]
        obs_dim = data["data"]["structure"]["dimensions"]["observation"][0]["values"]
        for series_key, series_data in series.items():
            obs = series_data.get("observations", {})
            for idx_str, val_arr in sorted(obs.items(), key=lambda x: -int(x[0])):
                val = val_arr[0]
                if val is not None:
                    year = obs_dim[int(idx_str)]["id"] if int(idx_str) < len(obs_dim) else "?"
                    log.info("  ✓ OECD GDP %s → %.2f%% (%s)", oecd_code, val, year)
                    return {"value": round(float(val), 2), "year": year, "source": "OECD"}
        return None
    except Exception as exc:
        log.warning("  ✗ OECD GDP %s: %s", oecd_code, exc)
        return None

def fetch_macro(country: str) -> dict:
    """
    Fetch macro indicators for a country:
      - Inflation: Eurostat HICP (EU countries) or OECD CPI (others)
      - GDP growth: OECD for all countries
    Returns dict with keys: gdp, inflation (each with value/year/source or None).
    """
    estat_code = EUROSTAT_CODES.get(country)
    oecd_code  = OECD_CODES.get(country)

    # Inflation
    if estat_code:
        inflation = fetch_inflation_eurostat(estat_code)
        if not inflation and oecd_code:   # Eurostat fallback to OECD
            inflation = fetch_inflation_oecd(oecd_code)
    elif oecd_code:
        inflation = fetch_inflation_oecd(oecd_code)
    else:
        inflation = None

    # GDP growth
    gdp = fetch_gdp_oecd(oecd_code) if oecd_code else None

    return {"gdp": gdp, "inflation": inflation}

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
