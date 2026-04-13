#!/usr/bin/env python3
"""
Bexar County (Texas) Motivated Seller Lead Scraper
===================================================
Scrapes the Bexar County Clerk portal for distressed-property document filings
(lis pendens, foreclosure notices, tax deeds, judgments, liens, probate, etc.),
enriches each record with parcel data from the Bexar County Appraisal District,
scores every lead, and exports to JSON + GHL-ready CSV.

Clerk Portal  : https://bexar.tx.publicsearch.us/
Parcel Data   : https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0
Foreclosure   : https://maps.bexar.org/foreclosures/

Run:
    python scraper/fetch.py              # default 7-day lookback
    python scraper/fetch.py --days 14    # custom lookback
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COUNTY = "Bexar"
STATE  = "TX"

CLERK_BASE_URL  = "https://bexar.tx.publicsearch.us"
CLERK_RESULTS   = f"{CLERK_BASE_URL}/results"
PARCEL_API_URL  = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
FORECLOSURE_URL = "https://maps.bexar.org/foreclosures/"

LOOKBACK_DAYS    = 7
PAGE_SIZE        = 50
REQUEST_TIMEOUT  = 30
PARCEL_BATCH     = 25
RETRY_COUNT      = 3
RETRY_DELAY      = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bexar_scraper")

# ---------------------------------------------------------------------------
# Search terms -> category mapping
# ---------------------------------------------------------------------------
SEARCH_TERMS = [
    ("lis pendens",              "LP",   "Lis Pendens"),
    ("notice of foreclosure",    "FC",   "Notice of Foreclosure"),
    ("notice of trustee sale",   "FC",   "Notice of Trustee Sale"),
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
def retry_get(session, url, **kwargs):
    for attempt in range(RETRY_COUNT):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code == 200:
                return r
            log.warning("HTTP %d for %s (attempt %d)", r.status_code, url, attempt + 1)
        except Exception as exc:
            log.warning("Request error %s (attempt %d): %s", url, attempt + 1, exc)
        if attempt < RETRY_COUNT - 1:
            time.sleep(RETRY_DELAY)
    return None


def normalize_date(raw):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()

# ---------------------------------------------------------------------------
# Clerk portal scraper
# ---------------------------------------------------------------------------
class ClerkScraper:
    def __init__(self, start, end):
        self.start = start
        self.end   = end
        # CORRECT format discovered by browser inspection: YYYYMMDD,YYYYMMDD
        self.date_range = f"{start.strftime('%Y%m%d')},{end.strftime('%Y%m%d')}"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": CLERK_BASE_URL,
        })

    def _warm(self):
        try:
            r = self.session.get(CLERK_BASE_URL, timeout=REQUEST_TIMEOUT)
            log.info("Session warmed -- status %d", r.status_code)
        except Exception as exc:
            log.warning("Could not warm session: %s", exc)

    def _fetch_page(self, search_value, page=1):
        """
        Fetch one page of results using the CORRECT URL format.

        Discovered by watching actual browser requests:
          /results?department=RP
                  &searchType=quickSearch
                  &searchValue=<term>
                  &recordedDateRange=YYYYMMDD,YYYYMMDD
                  &keywordSearch=false
                  &searchOcrText=false
                  &resultsPerPage=50
                  &currentPage=<n>
        """
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
        resp = retry_get(self.session, CLERK_RESULTS, params=params)
        if resp is None:
            log.warning("No response for '%s' page %d", search_value, page)
            return None
        return BeautifulSoup(resp.text, "lxml")

    def _parse_soup(self, soup, cat, cat_label):
        """
        Parse the HTML results table.
        Columns confirmed live: GRANTOR | GRANTEE | DOC TYPE |
        RECORDED DATE | DOC NUMBER | BOOK/VOLUME/PAGE | LEGAL DESCRIPTION | ...
        """
        records = []
        table = soup.find("table")
        if not table:
            return records

        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).upper()
                       for th in header_row.find_all(["th", "td"])]

        def col(cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h and i < len(cells):
                        return cells[i].get_text(strip=True)
            return ""

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 4:
                continue

            if headers:
                grantor   = col(cells, "GRANTOR")
                grantee   = col(cells, "GRANTEE")
                doc_type  = col(cells, "DOC TYPE")
                filed_raw = col(cells, "RECORDED DATE", "FILED DATE", "DATE")
                doc_num   = col(cells, "DOC NUMBER", "DOC NUM", "INSTRUMENT")
                legal     = col(cells, "LEGAL DESCRIPTION", "LEGAL")
            else:
                texts = [c.get_text(strip=True) for c in cells]
                grantor   = texts[0] if len(texts) > 0 else ""
                grantee   = texts[1] if len(texts) > 1 else ""
                doc_type  = texts[2] if len(texts) > 2 else ""
                filed_raw = texts[3] if len(texts) > 3 else ""
                doc_num   = texts[4] if len(texts) > 4 else ""
                legal     = texts[5] if len(texts) > 5 else ""

            if not doc_num and not grantor:
                continue

            clerk_url = ""
            doc_link = tr.find("a", href=True)
            if doc_link:
                href = doc_link["href"]
                clerk_url = href if href.startswith("http") else CLERK_BASE_URL + href

            rec = LeadRecord(
                doc_num   = doc_num,
                doc_type  = doc_type,
                cat       = cat,
                cat_label = cat_label,
                filed     = normalize_date(filed_raw) if filed_raw else "",
                owner     = grantor,
                grantee   = grantee,
                legal     = legal,
                clerk_url = clerk_url,
            )
            records.append(rec)

        return records

    def _total_pages(self, soup):
        for el in soup.find_all(string=re.compile(r"\d+\s+result", re.I)):
            m = re.search(r"([\d,]+)\s+result", el, re.I)
            if m:
                total = int(m.group(1).replace(",", ""))
                return max(1, -(-total // PAGE_SIZE))
        pages = soup.find_all("a", string=re.compile(r"^\d+$"))
        if pages:
            nums = [int(a.get_text()) for a in pages if a.get_text().isdigit()]
            return max(nums) if nums else 1
        return 1

    def run(self):
        self._warm()
        seen = set()
        all_records = []

        for search_value, cat, cat_label in SEARCH_TERMS:
            log.info("Searching: '%s'", search_value)
            page = 1
            term_count = 0

            while True:
                soup = self._fetch_page(search_value, page)
                if soup is None:
                    break

                recs = self._parse_soup(soup, cat, cat_label)
                if not recs:
                    break

                for r in recs:
                    key = r.doc_num or f"{r.owner}|{r.filed}|{r.doc_type}"
                    if key and key not in seen:
                        seen.add(key)
                        all_records.append(r)
                        term_count += 1

                total_pages = self._total_pages(soup)
                if page >= total_pages:
                    break
                page += 1
                time.sleep(1)

            log.info("  -> %d unique records for '%s'", term_count, search_value)
            time.sleep(1)

        log.info("Clerk portal: %d unique records collected", len(all_records))
        return all_records


# ---------------------------------------------------------------------------
# Parcel enrichment
# ---------------------------------------------------------------------------
def enrich_parcels(records):
    needs = [r for r in records if r.owner and not r.prop_address]
    if not needs:
        return
    log.info("Enriching %d records with parcel data...", len(needs))
    session = requests.Session()
    session.headers["User-Agent"] = "BexarLeadScraper/2.0"

    for i in range(0, len(needs), PARCEL_BATCH):
        batch = needs[i : i + PARCEL_BATCH]
        names = [r.owner for r in batch]
        parts = [f"UPPER(OWNER)='{n.upper().replace(chr(39), chr(39)*2)}'" for n in names]
        where = " OR ".join(parts)
        params = {
            "where":          where,
            "outFields":      "OWNER,SITUS_ADD,SITUS_CITY,SITUS_ZIP,MAIL_ADD,MAIL_CITY,MAIL_STATE,MAIL_ZIP",
            "returnGeometry": "false",
            "f":              "json",
        }
        try:
            r = session.get(PARCEL_API_URL, params=params, timeout=REQUEST_TIMEOUT)
            features = r.json().get("features", [])
            addr_map = {}
            for feat in features:
                att = feat.get("attributes", {})
                key = (att.get("OWNER") or "").upper().strip()
                if key:
                    addr_map[key] = att
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
def score_records(records, start):
    for r in records:
        s = 30
        flags = []
        if r.cat == "LP":
            s += 10; flags.append("LIS_PENDENS")
        if r.cat == "FC":
            s += 15; flags.append("FORECLOSURE")
        if r.cat in ("LP", "FC"):
            s += 5
        if r.cat == "JUD":
            s += 8;  flags.append("JUDGMENT")
        if r.cat == "LIEN":
            s += 7;  flags.append("LIEN")
        if r.cat == "PRO":
            s += 6;  flags.append("PROBATE")
        if r.amount > 100000:
            s += 15; flags.append("HIGH_AMOUNT")
        elif r.amount > 50000:
            s += 10; flags.append("MID_AMOUNT")
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
    try:
        gis_url = "https://maps.bexar.org/arcgis/rest/services/ForeclosureNotices/MapServer/0/query"
        params = {"where": "1=1", "outFields": "*", "returnGeometry": "false", "f": "json"}
        r = requests.get(gis_url, params=params, timeout=REQUEST_TIMEOUT)
        features = r.json().get("features", [])
        for feat in features:
            att = feat.get("attributes", {}) or {}
            sale_ms = att.get("SALEDATE") or att.get("SaleDate") or att.get("SALE_DATE")
            filed = ""
            if sale_ms:
                try:
                    dt = datetime.fromtimestamp(int(sale_ms) / 1000)
                    if start <= dt <= end + timedelta(days=60):
                        filed = dt.strftime("%Y-%m-%d")
                    else:
                        continue
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
# Export
# ---------------------------------------------------------------------------
GHL_FIELDS = [
    "doc_num","doc_type","cat","cat_label","filed",
    "owner","grantee","amount",
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


def write_outputs(records, start, end):
    base     = Path(__file__).parent.parent
    dash_dir = base / "dashboard"
    data_dir = base / "data"
    dash_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "fetched_at": datetime.utcnow().isoformat(),
        "source":     f"{COUNTY} County, {STATE} -- Clerk Portal + Parcel API",
        "date_range": {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.prop_address),
        "records":      [asdict(r) for r in records],
    }

    for path in [dash_dir / "records.json", data_dir / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info("JSON written: %s (%d records)", path, len(records))

    csv_path = data_dir / "ghl_export.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(GHL_HEADERS.values()))
        writer.writeheader()
        for r in records:
            d = asdict(r)
            row = {GHL_HEADERS[k]: ("|".join(d[k]) if k == "flags" else d[k])
                   for k in GHL_FIELDS}
            writer.writerow(row)
    log.info("GHL CSV written: %s (%d records)", csv_path, len(records))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
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
    log.info("Clerk scraper: %s -> %s (%d days)",
             start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y"), args.days)

    scraper = ClerkScraper(start, end)
    records = scraper.run()

    gis_records = fetch_foreclosure_gis(start, end)
    existing_addrs = {r.prop_address.upper() for r in records if r.prop_address}
    for r in gis_records:
        if r.prop_address.upper() not in existing_addrs:
            records.append(r)

    if not args.skip_parcel:
        enrich_parcels(records)

    score_records(records, start)
    records.sort(key=lambda r: r.score, reverse=True)

    if not records:
        log.warning("No records found. Writing empty output files.")
    else:
        log.info("Total records after dedup + enrichment: %d", len(records))

    write_outputs(records, start, end)

    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("  Total records   : %d", len(records))
    log.info("  With address    : %d", sum(1 for r in records if r.prop_address))
    log.info("  Score >= 70     : %d", sum(1 for r in records if r.score >= 70))
    log.info("  Score >= 50     : %d", sum(1 for r in records if r.score >= 50))


if __name__ == "__main__":
    main()
