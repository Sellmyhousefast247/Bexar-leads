#!/usr/bin/env python3
"""
Bexar County (Texas) Motivated Seller Lead Scraper
===================================================
Uses Playwright to render JS-heavy clerk portal pages, scrapes results tables,
enriches with ArcGIS parcel data, scores leads, exports JSON + GHL CSV.

Clerk Portal  : https://bexar.tx.publicsearch.us/
Parcel Data   : https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0
Foreclosure   : https://maps.bexar.org/foreclosures/

Run:
    python scraper/fetch.py              # default 7-day lookback
    python scraper/fetch.py --days 14    # custom lookback
    python scraper/fetch.py --skip-parcel
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
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
STATE  = "TX"

CLERK_BASE_URL = "https://bexar.tx.publicsearch.us"
CLERK_RESULTS  = f"{CLERK_BASE_URL}/results"
PARCEL_API_URL = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

LOOKBACK_DAYS   = 7
PAGE_SIZE       = 50
REQUEST_TIMEOUT = 30
PARCEL_BATCH    = 25
RETRY_COUNT     = 3
RETRY_DELAY     = 3
# Wait up to 20s for table rows to appear after navigation
TABLE_WAIT_MS   = 20000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bexar_scraper")

# ---------------------------------------------------------------------------
# Search terms -> (category code, human label)
# ---------------------------------------------------------------------------
SEARCH_TERMS = [
    ("lis pendens",              "LP",   "Lis Pendens"),
    ("substitute trustee",       "FC",   "Substitute Trustee Deed"),
    ("deed of trust",            "FC",   "Deed of Trust"),
    ("tax deed",                 "FC",   "Tax Deed / Tax Sale"),
    ("judgment",                 "JUD",  "Judgment"),
    ("abstract of judgment",     "JUD",  "Abstract of Judgment"),
    ("federal tax lien",         "LIEN", "Federal Tax Lien"),
    ("state tax lien",           "LIEN", "State Tax Lien"),
    ("mechanic lien",            "LIEN", "Mechanic Lien"),
    ("hoa lien",                 "LIEN", "HOA Lien"),
    ("hospital lien",            "LIEN", "Hospital Lien"),
    ("probate",                  "PRO",  "Probate"),
    ("affidavit of heirship",    "PRO",  "Affidavit of Heirship"),
    ("letters testamentary",     "PRO",  "Letters Testamentary"),
    ("notice of commencement",   "OTHER","Notice of Commencement"),
]

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class LeadRecord:
    doc_num:      str = ""
    doc_type:     str = ""
    cat:          str = ""
    cat_label:    str = ""
    filed:        str = ""
    owner:        str = ""
    grantee:      str = ""
    amount:       float = 0.0
    legal:        str = ""
    prop_address: str = ""
    prop_city:    str = ""
    prop_state:   str = STATE
    prop_zip:     str = ""
    mail_address: str = ""
    mail_city:    str = ""
    mail_state:   str = STATE
    mail_zip:     str = ""
    clerk_url:    str = ""
    flags:        list = field(default_factory=list)
    score:        int = 0

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
            time.sleep(RETRY_DELAY)
    return None

# ---------------------------------------------------------------------------
# Clerk portal scraper â Playwright
# ---------------------------------------------------------------------------
class ClerkScraper:
    """
    Uses Playwright (headless Chromium) to load the clerk portal results page
    and waits for the JS-rendered table to appear before scraping.

    Confirmed table column order (from live browser inspection):
      [0]=""  [1]=""  [2]=""  [3]="Grantor"  [4]="Grantee"  [5]="Doc Type"
      [6]="Recorded Date"  [7]="Doc Number"  [8]="Book/Volume/Page"
      [9]="Legal Description"  [10]="Lot"  [11]="Block"  [12]="NCB"
      [13]="County Block"  [14]="Property Address"
    """

    # Column indices (0-based) confirmed from live page
    COL_GRANTOR  = 3
    COL_GRANTEE  = 4
    COL_DOCTYPE  = 5
    COL_DATE     = 6
    COL_DOCNUM   = 7
    COL_LEGAL    = 9
    COL_ADDR     = 14

    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end   = end
        # YYYYMMDD,YYYYMMDD â confirmed working format
        self.date_range = f"{start.strftime('%Y%m%d')},{end.strftime('%Y%m%d')}"

    def _build_url(self, search_value: str, page: int = 1) -> str:
        from urllib.parse import urlencode
        params = {
            "department":        "RP",
            "searchType":        "quickSearch",
            "searchValue":       search_value,
            "recordedDateRange": self.date_range,
            "keywordSearch":     "false",
            "searchOcrText":     "false",
            "resultsPerPage":    PAGE_SIZE,
            "currentPage":       page,
        }
        return f"{CLERK_RESULTS}?{urlencode(params)}"

    def _parse_html(self, html: str, cat: str, cat_label: str) -> list[LeadRecord]:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            log.debug("No table found in rendered HTML")
            return []

        records = []
        rows = table.find_all("tr")
        log.debug("Table has %d rows (including header)", len(rows))

        for tr in rows[1:]:  # skip header
            cells = tr.find_all(["td", "th"])
            if len(cells) <= self.COL_DOCNUM:
                continue

            def cell(idx: int) -> str:
                return cells[idx].get_text(strip=True) if idx < len(cells) else ""

            grantor  = cell(self.COL_GRANTOR)
            grantee  = cell(self.COL_GRANTEE)
            doc_type = cell(self.COL_DOCTYPE)
            filed    = cell(self.COL_DATE)
            doc_num  = cell(self.COL_DOCNUM)
            legal    = cell(self.COL_LEGAL)
            addr_raw = cell(self.COL_ADDR)

            if not doc_num and not grantor:
                continue

            # Parse "419 JOCKEY, SAN ANTONIO, TEXAS 78260" style address
            prop_address = prop_city = prop_zip = ""
            if addr_raw:
                # Try to split city/state from address
                parts = addr_raw.rsplit(",", 2)
                if len(parts) >= 2:
                    prop_address = parts[0].strip()
                    # last part may be "TEXAS 78260"
                    last = parts[-1].strip()
                    zip_m = re.search(r"(\d{5})", last)
                    if zip_m:
                        prop_zip = zip_m.group(1)
                    if len(parts) >= 3:
                        prop_city = parts[1].strip()
                else:
                    prop_address = addr_raw

            # Clerk record URL
            clerk_url = ""
            link = tr.find("a", href=True)
            if link:
                href = link["href"]
                clerk_url = href if href.startswith("http") else CLERK_BASE_URL + href

            rec = LeadRecord(
                doc_num      = doc_num,
                doc_type     = doc_type,
                cat          = cat,
                cat_label    = cat_label,
                filed        = normalize_date(filed) if filed else "",
                owner        = grantor,
                grantee      = grantee,
                legal        = legal,
                prop_address = prop_address,
                prop_city    = prop_city,
                prop_state   = STATE,
                prop_zip     = prop_zip,
                clerk_url    = clerk_url,
            )
            records.append(rec)

        return records

    def _count_total_pages(self, html: str) -> int:
        soup = BeautifulSoup(html, "lxml")
        # Look for "X results" text
        for el in soup.find_all(string=re.compile(r"\d+\s+result", re.I)):
            m = re.search(r"([\d,]+)\s+result", el, re.I)
            if m:
                total = int(m.group(1).replace(",", ""))
                return max(1, -(-total // PAGE_SIZE))
        return 1

    def run(self) -> list[LeadRecord]:
        seen: set[str] = set()
        all_records: list[LeadRecord] = []

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

            # Warm session â load homepage to get cookies
            try:
                page.goto(CLERK_BASE_URL, wait_until="domcontentloaded", timeout=30000)
                log.info("Session warmed")
            except Exception as exc:
                log.warning("Warmup failed: %s", exc)

            for search_value, cat, cat_label in SEARCH_TERMS:
                log.info("Searching: '%s'", search_value)
                pg_num = 1
                term_count = 0

                while True:
                    url = self._build_url(search_value, pg_num)
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        # Wait for table rows to appear (JS renders them)
                        page.wait_for_selector("table tr:nth-child(2)", timeout=TABLE_WAIT_MS)
                    except PWTimeout:
                        log.info("  No results (timeout waiting for rows) for '%s' page %d",
                                 search_value, pg_num)
                        break
                    except Exception as exc:
                        log.warning("  Page load error for '%s' page %d: %s",
                                    search_value, pg_num, exc)
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
# Parcel enrichment
# ---------------------------------------------------------------------------
def enrich_parcels(records: list[LeadRecord]) -> None:
    needs = [r for r in records if r.owner and not r.prop_address]
    if not needs:
        log.info("All records already have addresses â skipping parcel enrichment")
        return
    log.info("Enriching %d records with parcel data...", len(needs))
    session = requests.Session()
    session.headers["User-Agent"] = "BexarLeadScraper/2.0"

    for i in range(0, len(needs), PARCEL_BATCH):
        batch = needs[i: i + PARCEL_BATCH]
        names = [r.owner for r in batch]
        parts = [f"UPPER(OWNER)='{n.upper().replace(chr(39), chr(39)*2)}'" for n in names]
        params = {
            "where":          " OR ".join(parts),
            "outFields":      "OWNER,SITUS_ADD,SITUS_CITY,SITUS_ZIP,MAIL_ADD,MAIL_CITY,MAIL_STATE,MAIL_ZIP",
            "returnGeometry": "false",
            "f":              "json",
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
                    rec.prop_city    = att.get("SITUS_CITY", "").strip()
                    rec.prop_zip     = str(att.get("SITUS_ZIP", "")).strip()
                    rec.mail_address = att.get("MAIL_ADD", "").strip()
                    rec.mail_city    = att.get("MAIL_CITY", "").strip()
                    rec.mail_state   = att.get("MAIL_STATE", STATE).strip() or STATE
                    rec.mail_zip     = str(att.get("MAIL_ZIP", "")).strip()
        except Exception as exc:
            log.warning("Parcel batch error: %s", exc)
        time.sleep(0.5)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def score_records(records: list[LeadRecord], start: datetime) -> None:
    for r in records:
        s, flags = 30, []
        if r.cat == "LP":   s += 10; flags.append("LIS_PENDENS")
        if r.cat == "FC":   s += 15; flags.append("FORECLOSURE")
        if r.cat in ("LP","FC"): s += 5
        if r.cat == "JUD":  s += 8;  flags.append("JUDGMENT")
        if r.cat == "LIEN": s += 7;  flags.append("LIEN")
        if r.cat == "PRO":  s += 6;  flags.append("PROBATE")
        if r.amount > 100000: s += 15; flags.append("HIGH_AMOUNT")
        elif r.amount > 50000: s += 10; flags.append("MID_AMOUNT")
        if r.filed:
            try:
                if datetime.strptime(r.filed, "%Y-%m-%d") >= start:
                    s += 5; flags.append("NEW_THIS_WEEK")
            except ValueError:
                pass
        if r.prop_address: s += 5; flags.append("HAS_ADDRESS")
        r.score = min(s, 100)
        r.flags = flags


# ---------------------------------------------------------------------------
# Foreclosure GIS
# ---------------------------------------------------------------------------
def fetch_foreclosure_gis(start: datetime, end: datetime) -> list[LeadRecord]:
    records = []
    try:
        gis_url = "https://maps.bexar.org/arcgis/rest/services/ForeclosureNotices/MapServer/0/query"
        r = requests.get(gis_url,
                         params={"where":"1=1","outFields":"*","returnGeometry":"false","f":"json"},
                         timeout=REQUEST_TIMEOUT)
        for feat in r.json().get("features", []):
            att = feat.get("attributes", {}) or {}
            sale_ms = att.get("SALEDATE") or att.get("SaleDate") or att.get("SALE_DATE")
            filed = ""
            if sale_ms:
                try:
                    dt = datetime.fromtimestamp(int(sale_ms) / 1000)
                    if not (start <= dt <= end + timedelta(days=60)):
                        continue
                    filed = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass
            rec = LeadRecord(
                doc_type     = "FORECLOSURE_GIS",
                cat          = "FC",
                cat_label    = "Foreclosure (GIS)",
                filed        = filed,
                owner        = (att.get("OWNER") or "").strip(),
                prop_address = (att.get("SITUS_ADD") or att.get("SITEADD") or "").strip(),
                prop_city    = (att.get("SITUS_CITY") or att.get("CITY") or "").strip(),
                prop_zip     = str(att.get("SITUS_ZIP") or "").strip(),
                legal        = (att.get("LEGAL") or att.get("LEGALDESC") or "").strip(),
            )
            if rec.owner or rec.prop_address:
                records.append(rec)
        log.info("Foreclosure GIS layer: %d records", len(records))
    except Exception as exc:
        log.warning("Foreclosure GIS error: %s", exc)
    return records


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
GHL_FIELDS = [
    "doc_num","doc_type","cat","cat_label","filed","owner","grantee","amount",
    "prop_address","prop_city","prop_state","prop_zip",
    "mail_address","mail_city","mail_state","mail_zip",
    "legal","clerk_url","score","flags",
]
GHL_HEADERS = {
    "doc_num":"Document Number","doc_type":"Document Type",
    "cat":"Category Code","cat_label":"Category","filed":"Filed Date",
    "owner":"First Name","grantee":"Last Name","amount":"Amount",
    "prop_address":"Address","prop_city":"City","prop_state":"State","prop_zip":"Postal Code",
    "mail_address":"Mailing Address","mail_city":"Mailing City",
    "mail_state":"Mailing State","mail_zip":"Mailing Zip",
    "legal":"Legal Description","clerk_url":"Clerk URL",
    "score":"Lead Score","flags":"Flags",
}


def write_outputs(records: list[LeadRecord], start: datetime, end: datetime) -> None:
    base = Path(__file__).parent.parent
    for d in [base / "dashboard", base / "data"]:
        d.mkdir(parents=True, exist_ok=True)

    payload = {
        "fetched_at":   datetime.utcnow().isoformat(),
        "source":       f"{COUNTY} County, {STATE} -- Clerk Portal + Parcel API",
        "date_range":   {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.prop_address),
        "records":      [asdict(r) for r in records],
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
            writer.writerow({GHL_HEADERS[k]: ("|".join(d[k]) if k=="flags" else d[k])
                             for k in GHL_FIELDS})
    log.info("GHL CSV written: %s (%d records)", csv_path, len(records))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Bexar County lead scraper")
    parser.add_argument("--days", type=int, default=LOOKBACK_DAYS)
    parser.add_argument("--skip-parcel", action="store_true")
    args = parser.parse_args()

    end   = datetime.now()
    start = end - timedelta(days=args.days)

    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback: %d days", args.days)
    log.info("=" * 60)
    log.info("Date range: %s -> %s", start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y"))

    scraper = ClerkScraper(start, end)
    records = scraper.run()

    gis_recs = fetch_foreclosure_gis(start, end)
    existing = {r.prop_address.upper() for r in records if r.prop_address}
    for r in gis_recs:
        if r.prop_address.upper() not in existing:
            records.append(r)

    if not args.skip_parcel:
        enrich_parcels(records)

    score_records(records, start)
    records.sort(key=lambda r: r.score, reverse=True)

    if not records:
        log.warning("No records found. Writing empty output files.")
    else:
        log.info("Total after dedup + enrichment: %d", len(records))

    write_outputs(records, start, end)

    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("  Total records : %d", len(records))
    log.info("  With address  : %d", sum(1 for r in records if r.prop_address))
    log.info("  Score >= 70   : %d", sum(1 for r in records if r.score >= 70))
    log.info("  Score >= 50   : %d", sum(1 for r in records if r.score >= 50))


if __name__ == "__main__":
    main()
