#!/usr/bin/env python3
"""
Bexar County (Texas) Motivated Seller Lead Scraper
===================================================
Uses Playwright to render JS-heavy clerk portal pages, scrapes results
tables, enriches with ArcGIS parcel data + BCAD owner-name lookup,
scores leads, exports JSON + GHL CSV.

Clerk Portal : https://bexar.tx.publicsearch.us/
Parcel Data  : https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0
Foreclosure  : https://maps.bexar.org/foreclosures/
BCAD lookup  : https://bexar.trueautomation.com/clientdb/   (owner-name fallback)

Run:
    python scraper/fetch.py                # default 7-day lookback
    python scraper/fetch.py --days 14      # custom lookback
    python scraper/fetch.py --skip-parcel
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import random
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COUNTY = "Bexar"
STATE = "TX"
CLERK_BASE_URL = "https://bexar.tx.publicsearch.us"
CLERK_RESULTS = f"{CLERK_BASE_URL}/results"
PARCEL_API_URL = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
BCAD_BASE = "https://bexar.trueautomation.com/clientdb"
BCAD_CID = "110"

LOOKBACK_DAYS = 7
PROBATE_LOOKBACK_DAYS = 60   # probate filings move slow; widen window
PAGE_SIZE = 50
REQUEST_TIMEOUT = 30
PARCEL_BATCH = 25
RETRY_COUNT = 3
RETRY_DELAY = 3
TABLE_WAIT_MS = 20000
BCAD_MAX_AMBIGUOUS = 3       # leave address blank if BCAD returns >N matches

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bexar_scraper")

# ---------------------------------------------------------------------------
# Search terms -> (category code, human label)
# Probate-related terms run with the wider lookback window.
# ---------------------------------------------------------------------------
SEARCH_TERMS = [
    ("lis pendens", "LP", "Lis Pendens"),
    ("substitute trustee", "FC", "Substitute Trustee Deed"),
    ("deed of trust", "FC", "Deed of Trust"),
    ("tax deed", "FC", "Tax Deed / Tax Sale"),
    ("judgment", "JUD", "Judgment"),
    ("abstract of judgment", "JUD", "Abstract of Judgment"),
    ("federal tax lien", "LIEN", "Federal Tax Lien"),
    ("state tax lien", "LIEN", "State Tax Lien"),
    ("mechanic lien", "LIEN", "Mechanic Lien"),
    ("hoa lien", "LIEN", "HOA Lien"),
    ("hospital lien", "LIEN", "Hospital Lien"),
    ("probate", "PRO", "Probate"),
    ("affidavit of heirship", "PRO", "Affidavit of Heirship"),
    ("letters testamentary", "PRO", "Letters Testamentary"),
    ("small estate affidavit", "PRO", "Small Estate Affidavit"),
    ("muniment of title", "PRO", "Muniment of Title"),
    ("notice of commencement", "OTHER","Notice of Commencement"),
]

PROBATE_CATS = {"PRO"}

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
GHL_FIELDS = [
    "doc_num","doc_type","cat","cat_label","filed","owner","grantee",
    "amount","prop_address","prop_city","prop_state","prop_zip",
    "mail_address","mail_city","mail_state","mail_zip","legal","clerk_url","score","flags",
]
GHL_HEADERS = {f: f.replace("_", " ").title() for f in GHL_FIELDS}

@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    cat: str = ""
    cat_label: str = ""
    filed: str = ""
    owner: str = ""
    grantee: str = ""
    amount: float = 0.0
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = STATE
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = STATE
    mail_zip: str = ""
    clerk_url: str = ""
    flags: list = field(default_factory=list)
    score: int = 0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


def retry_get(session, url, **kwargs):
    for attempt in range(RETRY_COUNT):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code == 200:
                return r
        except Exception as exc:
            log.warning("Request error (attempt %d): %s", attempt + 1, exc)
        if attempt < RETRY_COUNT - 1:
            time.sleep(RETRY_DELAY + random.random())
    return None


def normalize_owner_for_bcad(name: str) -> str:
    """
    BCAD search expects 'LASTNAME FIRSTNAME' format.
    Clerk records often give 'LASTNAME, FIRSTNAME M' or 'FIRSTNAME LASTNAME'.
    We normalize to 'LASTNAME FIRSTNAME' (no comma, no middle initial).
    """
    if not name:
        return ""
    n = re.sub(r"\s+", " ", name).strip().upper()
    # Strip trailing entity markers
    n = re.sub(r"\b(LLC|INC|CORP|TRUST|ESTATE|TR|LP|LLP|LTD)\.?$", "", n).strip()
    # If it has a comma, assume "LAST, FIRST MIDDLE"
    if "," in n:
        last, first = n.split(",", 1)
        first = first.strip().split()
        first_token = first[0] if first else ""
        return f"{last.strip()} {first_token}".strip()
    # Otherwise keep first two tokens (assume "LAST FIRST" or "FIRST LAST")
    parts = n.split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return n

# ---------------------------------------------------------------------------
# Clerk portal scraper - Playwright
# ---------------------------------------------------------------------------
class ClerkScraper:
    """
    Uses Playwright (headless Chromium) to load the clerk portal results page
    and waits for the JS-rendered table to appear before scraping.

    Confirmed table column order (from live browser inspection):
        [0]="" [1]="" [2]="" [3]="Grantor" [4]="Grantee" [5]="Doc Type"
        [6]="Recorded Date" [7]="Doc Number" [8]="Book/Volume/Page"
        [9]="Legal Description" [10]="Lot" [11]="Block" [12]="NCB"
        [13]="County Block" [14]="Property Address"
    """
    COL_GRANTOR = 3
    COL_GRANTEE = 4
    COL_DOCTYPE = 5
    COL_DATE = 6
    COL_DOCNUM = 7
    COL_LEGAL = 9
    COL_ADDR = 14

    def __init__(self, default_start: datetime, default_end: datetime,
                 probate_start: datetime):
        self.default_start = default_start
        self.default_end = default_end
        self.probate_start = probate_start

    def _date_range(self, cat: str) -> str:
        if cat in PROBATE_CATS:
            return f"{self.probate_start.strftime('%Y%m%d')},{self.default_end.strftime('%Y%m%d')}"
        return f"{self.default_start.strftime('%Y%m%d')},{self.default_end.strftime('%Y%m%d')}"

    def _build_url(self, search_value: str, cat: str, page: int = 1) -> str:
        from urllib.parse import urlencode
        params = {
            "department": "RP",
            "searchType": "quickSearch",
            "searchValue": search_value,
            "recordedDateRange": self._date_range(cat),
            "keywordSearch": "false",
            "searchOcrText": "false",
            "resultsPerPage": PAGE_SIZE,
            "currentPage": page,
        }
        return f"{CLERK_RESULTS}?{urlencode(params)}"

    def _parse_html(self, html: str, cat: str, cat_label: str) -> list:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            return []
        records = []
        rows = table.find_all("tr")
        for tr in rows[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) <= self.COL_DOCNUM:
                continue
            def cell(idx: int) -> str:
                return cells[idx].get_text(strip=True) if idx < len(cells) else ""
            grantor = cell(self.COL_GRANTOR)
            grantee = cell(self.COL_GRANTEE)
            doc_type = cell(self.COL_DOCTYPE)
            filed = cell(self.COL_DATE)
            doc_num = cell(self.COL_DOCNUM)
            legal = cell(self.COL_LEGAL)
            addr_raw = cell(self.COL_ADDR)
            if not doc_num and not grantor:
                continue
            prop_address = prop_city = prop_zip = ""
            if addr_raw:
                parts = addr_raw.rsplit(",", 2)
                if len(parts) >= 2:
                    prop_address = parts[0].strip()
                    last = parts[-1].strip()
                    zip_m = re.search(r"(\d{5})", last)
                    if zip_m:
                        prop_zip = zip_m.group(1)
                    if len(parts) >= 3:
                        prop_city = parts[1].strip()
                else:
                    prop_address = addr_raw
            clerk_url = ""
            link = tr.find("a", href=True)
            if link:
                href = link["href"]
                clerk_url = href if href.startswith("http") else CLERK_BASE_URL + href
            rec = LeadRecord(
                doc_num=doc_num, doc_type=doc_type, cat=cat, cat_label=cat_label,
                filed=normalize_date(filed) if filed else "",
                owner=grantor, grantee=grantee, legal=legal,
                prop_address=prop_address, prop_city=prop_city, prop_state=STATE,
                prop_zip=prop_zip, clerk_url=clerk_url,
            )
            records.append(rec)
        return records

    def _count_total_pages(self, html: str) -> int:
        soup = BeautifulSoup(html, "lxml")
        for el in soup.find_all(string=re.compile(r"\d+\s+result", re.I)):
            m = re.search(r"([\d,]+)\s+result", el, re.I)
            if m:
                total = int(m.group(1).replace(",", ""))
                return max(1, -(-total // PAGE_SIZE))
        return 1

    def run(self) -> list:
        seen = set()
        all_records = []
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            try:
                page.goto(CLERK_BASE_URL, wait_until="domcontentloaded", timeout=30000)
                log.info("Session warmed")
            except Exception as exc:
                log.warning("Warmup failed: %s", exc)
            for search_value, cat, cat_label in SEARCH_TERMS:
                log.info("Searching: '%s' [%s]", search_value, cat)
                pg_num = 1
                term_count = 0
                while True:
                    url = self._build_url(search_value, cat, pg_num)
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_selector("table tr:nth-child(2)", timeout=TABLE_WAIT_MS)
                    except PWTimeout:
                        log.info("  No results (timeout) for '%s' page %d", search_value, pg_num)
                        break
                    except Exception as exc:
                        log.warning("  Page error for '%s' page %d: %s", search_value, pg_num, exc)
                        break
                    html = page.content()
                    recs = self._parse_html(html, cat, cat_label)
                    if not recs:
                        break
                    for r in recs:
                        key = r.doc_num or f"{r.owner}|{r.filed}|{r.doc_type}"
                        if key and key not in seen:
                            seen.add(key)
                            all_records.append(r)
                            term_count += 1
                    total_pages = self._count_total_pages(html)
                    if pg_num >= total_pages:
                        break
                    pg_num += 1
                    time.sleep(1)
                log.info("  -> %d unique records for '%s'", term_count, search_value)
                time.sleep(0.5)
            browser.close()
        log.info("Clerk portal: %d unique records collected", len(all_records))
        return all_records

# ---------------------------------------------------------------------------
# BCAD owner-name lookup (fallback enrichment for probate / no-address records)
# ---------------------------------------------------------------------------
class BCADLookup:
    """
    Searches BCAD's public property database by owner name.
    Returns (street_address, city, zip) only when there is exactly ONE
    unambiguous match (or up to BCAD_MAX_AMBIGUOUS for non-probate; for
    probate we stay strict to avoid attaching wrong address).
    """
    URL_HOME = f"{BCAD_BASE}/?cid={BCAD_CID}"
    URL_RESULTS = f"{BCAD_BASE}/SearchResults.aspx?cid={BCAD_CID}"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._viewstate = None
        self._eventval = None
        self._vsgen = None
        self._cache = {}

    def warm(self) -> bool:
        try:
            r = self.session.get(self.URL_HOME, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                log.warning("BCAD warmup failed: %s", r.status_code)
                return False
            self._extract_state(r.text)
            return True
        except Exception as exc:
            log.warning("BCAD warmup error: %s", exc)
            return False

    def _extract_state(self, html: str) -> None:
        soup = BeautifulSoup(html, "lxml")
        for fname, attr in [
            ("__VIEWSTATE", "_viewstate"),
            ("__EVENTVALIDATION", "_eventval"),
            ("__VIEWSTATEGENERATOR", "_vsgen"),
        ]:
            tag = soup.find("input", {"name": fname})
            if tag and tag.get("value") is not None:
                setattr(self, attr, tag["value"])

    def search_owner(self, owner_name: str, strict: bool = True) -> dict | None:
        """
        Returns dict with prop_address/prop_city/prop_zip/mail_* on a single
        confident match, else None.
        strict=True -> require exactly 1 match.
        strict=False -> allow up to BCAD_MAX_AMBIGUOUS (returns first row).
        """
        norm = normalize_owner_for_bcad(owner_name)
        if not norm or len(norm) < 4:
            return None
        if norm in self._cache:
            return self._cache[norm]
        if not self._viewstate:
            if not self.warm():
                return None
        payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": self._viewstate or "",
            "__VIEWSTATEGENERATOR": self._vsgen or "",
            "__EVENTVALIDATION": self._eventval or "",
            "propertySearchOptions:searchText": norm,
            "propertySearchOptions:search": "Search",
            "propertySearchOptions:ownerName": "",
            "propertySearchOptions:streetNumber": "",
            "propertySearchOptions:streetName": "",
            "propertySearchOptions:propertyid": "",
            "propertySearchOptions:geoid": "",
            "propertySearchOptions:dba": "",
        }
        try:
            r = self.session.post(
                f"{BCAD_BASE}/propertysearch.aspx?cid={BCAD_CID}",
                data=payload, timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
        except Exception as exc:
            log.debug("BCAD search error for '%s': %s", norm, exc)
            self._cache[norm] = None
            return None
        result = self._parse_results(r.text, strict)
        self._cache[norm] = result
        # refresh state for next request
        self._extract_state(r.text)
        time.sleep(0.5 + random.random() * 0.4)
        return result

    def _parse_results(self, html: str, strict: bool) -> dict | None:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", id="propertySearchResults_resultsTable")
        if not table:
            return None
        rows = [tr for tr in table.find_all("tr") if "tableHeader" not in (tr.get("class") or [])]
        if not rows:
            return None
        if strict and len(rows) > 1:
            # ambiguous: per safety rule, leave blank
            return None
        if not strict and len(rows) > BCAD_MAX_AMBIGUOUS:
            return None
        first = rows[0]
        cells = first.find_all("td")
        if len(cells) < 7:
            return None
        # Column order from live inspection:
        # 0=checkbox 1=PropID 2=GeoID 3=Type 4=Address 5=Legal 6=Owner 7=DBA 8=Value 9=Details 10=Map
        addr_full = cells[4].get_text(" ", strip=True)
        # "127 COLEMAN ST SAN ANTONIO, TX 78208"
        addr, city, zipc = addr_full, "", ""
        m = re.match(r"^(.*?),\s*([A-Z .]+),?\s+TX\s+(\d{5})", addr_full)
        if m:
            addr = m.group(1).strip()
            city = m.group(2).strip()
            zipc = m.group(3).strip()
        else:
            m2 = re.match(r"^(.*?)\s+([A-Z][A-Z .]+?)\s*,?\s*TX\s+(\d{5})", addr_full)
            if m2:
                addr = m2.group(1).strip()
                city = m2.group(2).strip()
                zipc = m2.group(3).strip()
        if not addr:
            return None
        return {
            "prop_address": addr,
            "prop_city": city,
            "prop_zip": zipc,
        }

# ---------------------------------------------------------------------------
# Parcel enrichment (ArcGIS exact-match) + BCAD fallback
# ---------------------------------------------------------------------------
def enrich_parcels(records: list, use_bcad: bool = True) -> None:
    needs = [r for r in records if r.owner and not r.prop_address]
    if not needs:
        log.info("All records already have addresses - skipping enrichment")
        return
    log.info("Enriching %d records via ArcGIS parcels...", len(needs))
    session = requests.Session()
    session.headers["User-Agent"] = "BexarLeadScraper/2.1"
    for i in range(0, len(needs), PARCEL_BATCH):
        batch = needs[i: i + PARCEL_BATCH]
        names = [r.owner for r in batch]
        parts = [f"UPPER(OWNER)='{n.upper().replace(chr(39), chr(39)*2)}'" for n in names]
        params = {
            "where": " OR ".join(parts),
            "outFields": "OWNER,SITUS_ADD,SITUS_CITY,SITUS_ZIP,MAIL_ADD,MAIL_CITY,MAIL_STATE,MAIL_ZIP",
            "returnGeometry": "false",
            "f": "json",
        }
        try:
            r = session.get(PARCEL_API_URL, params=params, timeout=REQUEST_TIMEOUT)
            addr_map = {}
            for feat in r.json().get("features", []):
                att = feat.get("attributes", {})
                k = (att.get("OWNER") or "").upper().strip()
                if k:
                    addr_map[k] = att
            for rec in batch:
                att = addr_map.get(rec.owner.upper().strip())
                if att:
                    rec.prop_address = att.get("SITUS_ADD", "").strip()
                    rec.prop_city = att.get("SITUS_CITY", "").strip()
                    rec.prop_zip = str(att.get("SITUS_ZIP", "")).strip()
                    rec.mail_address = att.get("MAIL_ADD", "").strip()
                    rec.mail_city = att.get("MAIL_CITY", "").strip()
                    rec.mail_state = att.get("MAIL_STATE", STATE).strip() or STATE
                    rec.mail_zip = str(att.get("MAIL_ZIP", "")).strip()
        except Exception as exc:
            log.warning("Parcel batch error: %s", exc)
        time.sleep(0.5)
    # BCAD fallback
    if not use_bcad:
        return
    still_need = [r for r in records if r.owner and not r.prop_address]
    if not still_need:
        log.info("ArcGIS covered all records - skipping BCAD lookup")
        return
    log.info("Attempting BCAD owner-lookup for %d unmatched records...", len(still_need))
    bcad = BCADLookup()
    if not bcad.warm():
        log.warning("BCAD unavailable - leaving %d records without address", len(still_need))
        return
    hits = 0
    for rec in still_need:
        # For probate records, require strict (exactly 1 match) to avoid wrong addresses
        strict = rec.cat in PROBATE_CATS
        info = bcad.search_owner(rec.owner, strict=strict)
        if info:
            rec.prop_address = info["prop_address"]
            rec.prop_city = info["prop_city"]
            rec.prop_state = STATE
            rec.prop_zip = info["prop_zip"]
            hits += 1
    log.info("BCAD enrichment: %d/%d records matched", hits, len(still_need))

# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def score_records(records: list, start: datetime) -> None:
    for r in records:
        s, flags = 30, []
        if r.cat == "LP": s += 10; flags.append("LIS_PENDENS")
        if r.cat == "FC": s += 15; flags.append("FORECLOSURE")
        if r.cat in ("LP","FC"): s += 5
        if r.cat == "JUD": s += 8; flags.append("JUDGMENT")
        if r.cat == "LIEN": s += 7; flags.append("LIEN")
        if r.cat == "PRO": s += 12; flags.append("PROBATE")
        if r.amount > 100000: s += 15; flags.append("HIGH_AMOUNT")
        elif r.amount > 50000: s += 10; flags.append("MID_AMOUNT")
        if r.filed:
            try:
                if datetime.strptime(r.filed, "%Y-%m-%d") >= start:
                    s += 5; flags.append("NEW_THIS_WEEK")
            except ValueError:
                pass
        if r.prop_address:
            s += 5; flags.append("HAS_ADDRESS")
        r.score = min(s, 100)
        r.flags = flags

# ---------------------------------------------------------------------------
# Foreclosure GIS
# ---------------------------------------------------------------------------
def fetch_foreclosure_gis(start, end):
    records = []
    base = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"
    params = {
        "where": "1=1",
        "outFields": "ADDRESS,DOC_NUMBER,YEAR,MONTH,TYPE,CITY,ZIP,SCHOOL_DIST",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        for layer in [0, 1]:
            url = f"{base}/{layer}/query"
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            features = r.json().get("features", [])
            log.info("ForeclosuresProd layer %d: %d features", layer, len(features))
            for feat in features:
                att = feat.get("attributes", {}) or {}
                year = att.get("YEAR", 0) or 0
                month = att.get("MONTH", 0) or 0
                if year < start.year or (year == start.year and month < start.month - 1):
                    continue
                filed = f"{int(year):04d}-{int(month):02d}-01" if year and month else ""
                addr_raw = (att.get("ADDRESS") or "").strip()
                city = (att.get("CITY") or "").strip()
                zipcode = str(att.get("ZIP") or "").strip()
                doc_num = (att.get("DOC_NUMBER") or "").strip()
                fc_type = (att.get("TYPE") or "").strip()
                rec = LeadRecord(
                    doc_num=doc_num,
                    doc_type=f"FORECLOSURE_{fc_type}",
                    cat="FC",
                    cat_label=f"Foreclosure Notice ({fc_type.title()})",
                    filed=filed,
                    prop_address=addr_raw,
                    prop_city=city,
                    prop_state=STATE,
                    prop_zip=zipcode,
                    clerk_url=f"https://maps.bexar.org/foreclosures/" if not doc_num else f"https://bexar.tx.publicsearch.us/results?department=RP&searchType=quickSearch&searchValue={doc_num}&keywordSearch=false&searchOcrText=false",
                )
                if rec.prop_address:
                    records.append(rec)
        log.info("ForeclosuresProd total: %d records", len(records))
    except Exception as exc:
        log.warning("ForeclosuresProd error: %s", exc)
    return records

def write_outputs(records: list, start: datetime, end: datetime) -> None:
    base = Path(__file__).parent.parent
    for d in [base / "dashboard", base / "data"]:
        d.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": datetime.utcnow().isoformat(),
        "source": f"{COUNTY} County, {STATE} -- Clerk Portal + Parcel API + BCAD",
        "date_range": {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")},
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address),
        "by_cat": {c: sum(1 for r in records if r.cat == c) for c in ("FC","LP","JUD","LIEN","PRO","OTHER")},
        "records": [asdict(r) for r in records],
    }
    for path in [base / "dashboard" / "records.json", base / "data" / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info("JSON written: %s (%d records)", path, len(records))
    csv_path = base / "data" / "ghl_export.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(GHL_HEADERS.values()))
        writer.writeheader()
        for r in records:
            d = asdict(r)
            writer.writerow({GHL_HEADERS[k]: ("|".join(d[k]) if k=="flags" else d[k]) for k in GHL_FIELDS})
    log.info("GHL CSV written: %s (%d records)", csv_path, len(records))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Bexar County lead scraper")
    parser.add_argument("--days", type=int, default=LOOKBACK_DAYS)
    parser.add_argument("--probate-days", type=int, default=PROBATE_LOOKBACK_DAYS)
    parser.add_argument("--skip-parcel", action="store_true")
    parser.add_argument("--skip-bcad", action="store_true")
    args = parser.parse_args()
    end = datetime.now()
    start = end - timedelta(days=args.days)
    probate_start = end - timedelta(days=args.probate_days)
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback default=%dd  probate=%dd", args.days, args.probate_days)
    log.info("=" * 60)
    log.info("Range: default %s->%s | probate %s->%s",
             start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y"),
             probate_start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y"))
    scraper = ClerkScraper(start, end, probate_start)
    records = scraper.run()
    gis_recs = fetch_foreclosure_gis(start, end)
    existing = {r.prop_address.upper() for r in records if r.prop_address}
    for r in gis_recs:
        if r.prop_address.upper() not in existing:
            records.append(r)
    if not args.skip_parcel:
        enrich_parcels(records, use_bcad=not args.skip_bcad)
    score_records(records, start)
    records.sort(key=lambda r: r.score, reverse=True)
    if not records:
        log.warning("No records found. Writing empty output files.")
    else:
        log.info("Total after dedup + enrichment: %d", len(records))
    write_outputs(records, start, end)
    pro_count = sum(1 for r in records if r.cat == "PRO")
    pro_with_addr = sum(1 for r in records if r.cat == "PRO" and r.prop_address)
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("  Total records  : %d", len(records))
    log.info("  With address   : %d", sum(1 for r in records if r.prop_address))
    log.info("  Probates       : %d (%d with address)", pro_count, pro_with_addr)
    log.info("  Score >= 70    : %d", sum(1 for r in records if r.score >= 70))
    log.info("  Score >= 50    : %d", sum(1 for r in records if r.score >= 50))


if __name__ == "__main__":
    main()
