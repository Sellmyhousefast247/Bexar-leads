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
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COUNTY = "Bexar"
STATE = "TX"
STATE_FULL = "Texas"

CLERK_PORTAL_URL = "https://bexar.tx.publicsearch.us/"
CLERK_SEARCH_URL = "https://bexar.tx.publicsearch.us/results"
CLERK_DOC_URL = "https://bexar.tx.publicsearch.us/doc/{doc_id}"

PARCEL_API_URL = (
    "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
)
FORECLOSURE_MAP_URL = "https://maps.bexar.org/foreclosures/"

LOOKBACK_DAYS = 7
MAX_RETRIES = 3
RETRY_DELAY = 2          # seconds between retries
PAGE_SIZE = 50            # results per clerk-portal page
REQUEST_TIMEOUT = 30      # seconds
PARCEL_BATCH_SIZE = 25    # owner-name lookups per ArcGIS call

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bexar_scraper")

# ---------------------------------------------------------------------------
# Document-type mapping
# ---------------------------------------------------------------------------
DOC_CATEGORIES: dict[str, tuple[str, str]] = {
    # key in clerk portal → (internal category code, human label)
    "LIS PENDENS":              ("LP",       "Lis Pendens"),
    "LIS PENDEN":               ("LP",       "Lis Pendens"),
    "NOTICE OF FORECLOSURE":    ("NOFC",     "Notice of Foreclosure"),
    "NOTICE OF TRUSTEE SALE":   ("NOFC",     "Notice of Foreclosure"),
    "NOTICE OF SUBSTITUTE TRUSTEE SALE": ("NOFC", "Notice of Foreclosure"),
    "NOTICE TRUSTEE":           ("NOFC",     "Notice of Foreclosure"),
    "TAX DEED":                 ("TAXDEED",  "Tax Deed"),
    "TAX SALE":                 ("TAXDEED",  "Tax Deed"),
    "JUDGMENT":                 ("JUD",      "Judgment"),
    "DEFAULT JUDGMENT":         ("JUD",      "Judgment"),
    "ABSTRACT OF JUDGMENT":     ("JUD",      "Judgment"),
    "CERTIFIED JUDGMENT":       ("CCJ",      "Certified Judgment"),
    "DOMESTIC JUDGMENT":        ("DRJUD",    "Domestic Judgment"),
    "FEDERAL TAX LIEN":         ("LNFED",    "Federal Tax Lien"),
    "STATE TAX LIEN":           ("LNCORPTX", "Corp / State Tax Lien"),
    "IRS LIEN":                 ("LNIRS",    "IRS Lien"),
    "TAX LIEN":                 ("LNCORPTX", "Tax Lien"),
    "LIEN":                     ("LN",       "Lien"),
    "MECHANIC LIEN":            ("LNMECH",   "Mechanic Lien"),
    "MECHANICS LIEN":           ("LNMECH",   "Mechanic Lien"),
    "MATERIALMAN LIEN":         ("LNMECH",   "Mechanic Lien"),
    "HOA LIEN":                 ("LNHOA",    "HOA Lien"),
    "HOSPITAL LIEN":            ("MEDLN",    "Medicaid / Hospital Lien"),
    "MEDICAID LIEN":            ("MEDLN",    "Medicaid Lien"),
    "PROBATE":                  ("PRO",      "Probate"),
    "LETTERS TESTAMENTARY":     ("PRO",      "Probate"),
    "LETTERS OF ADMINISTRATION":("PRO",      "Probate"),
    "AFFIDAVIT OF HEIRSHIP":    ("PRO",      "Probate / Heirship"),
    "NOTICE OF COMMENCEMENT":   ("NOC",      "Notice of Commencement"),
    "RELEASE LIS PENDENS":      ("RELLP",    "Release Lis Pendens"),
    "RELEASE OF LIS PENDENS":   ("RELLP",    "Release Lis Pendens"),
}

# Which doc_type keywords to search for on the clerk portal
SEARCH_KEYWORDS: list[str] = [
    "lis pendens",
    "notice of foreclosure",
    "notice of trustee sale",
    "notice of substitute trustee",
    "tax deed",
    "judgment",
    "abstract of judgment",
    "default judgment",
    "federal tax lien",
    "state tax lien",
    "tax lien",
    "mechanic lien",
    "mechanics lien",
    "hospital lien",
    "hoa lien",
    "probate",
    "affidavit of heirship",
    "letters testamentary",
    "notice of commencement",
    "release lis pendens",
]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    cat: str = ""
    cat_label: str = ""
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
    flags: list[str] = field(default_factory=list)
    score: int = 0


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def safe(func, *a, default=None, **kw):
    """Call *func* and swallow exceptions, returning *default* on failure."""
    try:
        return func(*a, **kw)
    except Exception:
        return default


def retry(func, *args, attempts: int = MAX_RETRIES, **kwargs):
    """Retry a callable up to *attempts* times with exponential back-off."""
    last_err = None
    for i in range(attempts):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_err = exc
            wait = RETRY_DELAY * (2 ** i)
            log.warning("Attempt %d/%d failed: %s — retrying in %ds", i + 1, attempts, exc, wait)
            time.sleep(wait)
    log.error("All %d attempts failed. Last error: %s", attempts, last_err)
    return None


def parse_currency(text: str) -> float:
    """Extract a dollar amount from text like '$123,456.78'."""
    if not text:
        return 0.0
    m = re.search(r"[\d,]+\.?\d*", text.replace("$", "").replace(",", ""))
    if m:
        try:
            return float(m.group().replace(",", ""))
        except ValueError:
            pass
    return 0.0


def normalize_name(name: str) -> str:
    """Upper-case, collapse whitespace, strip punctuation."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", "", name.upper())).strip()


def name_variants(name: str) -> list[str]:
    """
    Given 'JOHN DOE' return ['JOHN DOE', 'DOE JOHN', 'DOE, JOHN'].
    Given 'DOE, JOHN' return ['DOE JOHN', 'JOHN DOE', 'DOE, JOHN'].
    """
    n = normalize_name(name)
    if not n:
        return []
    parts = [p.strip() for p in n.replace(",", " ").split() if p.strip()]
    if len(parts) < 2:
        return [n]
    first, last = parts[0], parts[-1]
    return list(
        dict.fromkeys(
            [
                f"{first} {last}",
                f"{last} {first}",
                f"{last}, {first}",
                n,
            ]
        )
    )


# ---------------------------------------------------------------------------
# Clerk Portal scraper (requests + BeautifulSoup)
# ---------------------------------------------------------------------------
class ClerkScraper:
    """
    Scrape the Bexar County Clerk's publicsearch.us portal.

    The portal is a fairly standard ASP.NET / React search app that returns
    JSON from an API endpoint.  We hit the search API directly rather than
    driving a browser, which is faster and more reliable.
    """

    API_BASE = "https://bexar.tx.publicsearch.us/api/v2"

    def __init__(self, lookback_days: int = LOOKBACK_DAYS):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Referer": CLERK_PORTAL_URL,
            }
        )
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=lookback_days)
        log.info(
            "Clerk scraper: %s → %s (%d days)",
            self.start_date.strftime("%m/%d/%Y"),
            self.end_date.strftime("%m/%d/%Y"),
            lookback_days,
        )

    # ---- internal helpers ----

    def _warm_session(self) -> None:
        """Hit the landing page so the server sets session cookies."""
        try:
            r = self.session.get(CLERK_PORTAL_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            log.info("Session warmed — status %d", r.status_code)
        except Exception as exc:
            log.warning("Could not warm session: %s", exc)

    def _search_api(self, keyword: str, page: int = 1) -> dict | None:
        """
        Call the clerk portal's search API.

        The publicsearch.us platform exposes a JSON API at:
            /api/v2/search
        with query parameters for department, date range, and free-text.
        """
        params = {
            "department": "RP",           # Real Property / Land Records
            "searchOfficialRecords": True,
            "searchType": "quickSearch",
            "query": keyword,
            "page": page,
            "pageSize": PAGE_SIZE,
            "orderBy": "filedDate",
            "orderDirection": "desc",
            "startDate": self.start_date.strftime("%m/%d/%Y"),
            "endDate": self.end_date.strftime("%m/%d/%Y"),
        }
        url = f"{self.API_BASE}/search"
        resp = retry(
            self.session.get, url, params=params, timeout=REQUEST_TIMEOUT
        )
        if resp is None or resp.status_code != 200:
            # Fallback: try the HTML search results page and parse it
            return self._search_html_fallback(keyword, page)
        try:
            return resp.json()
        except Exception:
            return self._search_html_fallback(keyword, page)

    def _search_html_fallback(self, keyword: str, page: int = 1) -> dict | None:
        """
        Fallback scraper that loads the HTML results page and parses the table.
        Used when the JSON API isn't available or returns non-JSON.
        """
        params = {
            "department": "RP",
            "searchOfficialRecords": True,
            "searchType": "quickSearch",
            "query": keyword,
            "page": page,
            "pageSize": PAGE_SIZE,
            "startDate": self.start_date.strftime("%m/%d/%Y"),
            "endDate": self.end_date.strftime("%m/%d/%Y"),
        }
        url = CLERK_SEARCH_URL
        resp = retry(
            self.session.get, url, params=params, timeout=REQUEST_TIMEOUT
        )
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        records = []

        # publicsearch.us renders result rows in <div class="result-row"> or
        # <tr> elements depending on version.  Try both patterns.
        rows = soup.select("div.result-row, table.results tbody tr, div.search-result")
        for row in rows:
            rec = self._parse_html_row(row)
            if rec:
                records.append(rec)

        return {"records": records, "totalResults": len(records), "page": page}

    def _parse_html_row(self, el) -> dict | None:
        """Extract fields from a single HTML result row."""
        try:
            text = el.get_text(" | ", strip=True)
            cells = [c.get_text(strip=True) for c in el.find_all(["td", "span", "div"])]
            if len(cells) < 3:
                return None
            # Try to identify columns by position / label
            rec: dict[str, Any] = {}
            for cell_text in cells:
                if re.match(r"\d{4}-\d+", cell_text):
                    rec["docNumber"] = cell_text
                elif re.match(r"\d{1,2}/\d{1,2}/\d{4}", cell_text):
                    rec["filedDate"] = cell_text
                elif any(
                    kw in cell_text.upper()
                    for kw in ["LIEN", "JUDGMENT", "DEED", "PROBATE", "LIS", "NOTICE", "RELEASE"]
                ):
                    rec.setdefault("docType", cell_text)
            # Grantor / grantee often in specific spans
            grantor_el = el.select_one(".grantor, [data-label='Grantor']")
            grantee_el = el.select_one(".grantee, [data-label='Grantee']")
            if grantor_el:
                rec["grantor"] = grantor_el.get_text(strip=True)
            if grantee_el:
                rec["grantee"] = grantee_el.get_text(strip=True)
            # Link
            link = el.find("a", href=True)
            if link:
                rec["detailUrl"] = link["href"]
            return rec if rec.get("docNumber") or rec.get("docType") else None
        except Exception:
            return None

    # ---- main parsing logic ----

    def _classify(self, doc_type_raw: str) -> tuple[str, str]:
        """Return (cat, cat_label) for a raw document-type string."""
        upper = doc_type_raw.upper().strip()
        for pattern, (cat, label) in DOC_CATEGORIES.items():
            if pattern in upper:
                return cat, label
        return "OTHER", doc_type_raw.title()

    def _record_from_api(self, item: dict) -> LeadRecord | None:
        """Convert a single JSON record from the API into a LeadRecord."""
        try:
            doc_type_raw = (
                item.get("docType", "")
                or item.get("documentType", "")
                or item.get("doc_type", "")
                or ""
            )
            cat, cat_label = self._classify(doc_type_raw)
            if cat == "OTHER":
                return None  # skip unrecognized doc types

            doc_num = (
                item.get("docNumber", "")
                or item.get("instrumentNumber", "")
                or item.get("doc_num", "")
                or ""
            )
            filed_raw = (
                item.get("filedDate", "")
                or item.get("recordedDate", "")
                or item.get("filed", "")
                or ""
            )
            # Normalize date to MM/DD/YYYY
            filed = ""
            if filed_raw:
                for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m-%d-%Y"):
                    parsed = safe(datetime.strptime, filed_raw[:10], fmt)
                    if parsed:
                        filed = parsed.strftime("%m/%d/%Y")
                        break
                if not filed:
                    filed = filed_raw[:10]

            grantor = (
                item.get("grantor", "")
                or item.get("grantors", "")
                or item.get("owner", "")
                or ""
            )
            if isinstance(grantor, list):
                grantor = "; ".join(str(g) for g in grantor)

            grantee = item.get("grantee", "") or item.get("grantees", "") or ""
            if isinstance(grantee, list):
                grantee = "; ".join(str(g) for g in grantee)

            legal = item.get("legalDescription", "") or item.get("legal", "") or ""
            amount_raw = item.get("amount", "") or item.get("consideration", "") or "0"
            amount = parse_currency(str(amount_raw))

            detail_id = item.get("id", "") or doc_num
            clerk_url = (
                item.get("detailUrl", "")
                or CLERK_DOC_URL.format(doc_id=detail_id)
            )
            if clerk_url and not clerk_url.startswith("http"):
                clerk_url = CLERK_PORTAL_URL.rstrip("/") + "/" + clerk_url.lstrip("/")

            return LeadRecord(
                doc_num=str(doc_num),
                doc_type=doc_type_raw.strip(),
                filed=filed,
                cat=cat,
                cat_label=cat_label,
                owner=str(grantor).strip(),
                grantee=str(grantee).strip(),
                amount=amount,
                legal=str(legal).strip(),
                clerk_url=clerk_url,
            )
        except Exception as exc:
            log.debug("Skipping bad record: %s", exc)
            return None

    # ---- public entry-point ----

    def scrape(self) -> list[LeadRecord]:
        """Run all searches and return de-duplicated LeadRecords."""
        self._warm_session()

        seen: set[str] = set()
        results: list[LeadRecord] = []

        for keyword in SEARCH_KEYWORDS:
            log.info("Searching: '%s'", keyword)
            page = 1
            while True:
                data = self._search_api(keyword, page=page)
                if not data:
                    break

                items = data.get("records", data.get("results", []))
                if not items:
                    break

                for item in items:
                    rec = self._record_from_api(item)
                    if rec is None:
                        continue
                    key = f"{rec.doc_num}|{rec.doc_type}|{rec.owner}"
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(rec)

                total = data.get("totalResults", data.get("total", 0))
                if page * PAGE_SIZE >= (total or len(items)):
                    break
                page += 1
                time.sleep(0.5)  # politeness delay

        log.info("Clerk portal: %d unique records collected", len(results))
        return results


# ---------------------------------------------------------------------------
# Parcel data enrichment via ArcGIS REST API
# ---------------------------------------------------------------------------
class ParcelEnricher:
    """
    Look up property and mailing addresses from the Bexar County
    ArcGIS Parcel MapServer using owner-name queries.
    """

    OUT_FIELDS = (
        "Owner,Situs,MailAddr,MailCity,MailState,MailZip,"
        "SitusCity,SitusZip,PropID"
    )

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 BexarLeadScraper/1.0"
        )
        self._cache: dict[str, dict] = {}

    def _query(self, where_clause: str) -> list[dict]:
        """Run a single ArcGIS REST query and return features."""
        params = {
            "where": where_clause,
            "outFields": self.OUT_FIELDS,
            "returnGeometry": "false",
            "f": "json",
            "resultRecordCount": 5,
        }
        resp = retry(
            self.session.get,
            PARCEL_API_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        if resp is None or resp.status_code != 200:
            return []
        try:
            data = resp.json()
            return data.get("features", [])
        except Exception:
            return []

    def lookup_owner(self, owner_name: str) -> dict | None:
        """
        Search parcels by owner name and return the first match with
        keys: prop_address, prop_city, prop_zip, mail_address, etc.
        """
        norm = normalize_name(owner_name)
        if not norm or len(norm) < 3:
            return None
        if norm in self._cache:
            return self._cache[norm]

        # Try several name variants
        for variant in name_variants(owner_name):
            escaped = variant.replace("'", "''")
            where = f"UPPER(Owner) LIKE '%{escaped}%'"
            features = self._query(where)
            if features:
                attrs = features[0].get("attributes", {})
                result = {
                    "prop_address": (attrs.get("Situs") or "").strip(),
                    "prop_city": (attrs.get("SitusCity") or "San Antonio").strip(),
                    "prop_zip": str(attrs.get("SitusZip") or "").strip(),
                    "mail_address": (attrs.get("MailAddr") or "").strip(),
                    "mail_city": (attrs.get("MailCity") or "").strip(),
                    "mail_state": (attrs.get("MailState") or STATE).strip(),
                    "mail_zip": str(attrs.get("MailZip") or "").strip(),
                }
                self._cache[norm] = result
                return result
            time.sleep(0.3)

        self._cache[norm] = None
        return None

    def enrich(self, records: list[LeadRecord]) -> list[LeadRecord]:
        """Add parcel data (addresses) to each record by owner lookup."""
        total = len(records)
        enriched = 0
        for i, rec in enumerate(records):
            if rec.prop_address:
                enriched += 1
                continue
            parcel = self.lookup_owner(rec.owner)
            if parcel:
                rec.prop_address = parcel["prop_address"]
                rec.prop_city = parcel["prop_city"]
                rec.prop_zip = parcel["prop_zip"]
                rec.mail_address = parcel["mail_address"]
                rec.mail_city = parcel["mail_city"]
                rec.mail_state = parcel["mail_state"]
                rec.mail_zip = parcel["mail_zip"]
                enriched += 1
            if (i + 1) % 25 == 0:
                log.info("Parcel enrichment: %d/%d done (%d matched)", i + 1, total, enriched)

        log.info("Parcel enrichment complete: %d/%d records have addresses", enriched, total)
        return records


# ---------------------------------------------------------------------------
# Foreclosure map scraper (bonus: pull from GIS layer)
# ---------------------------------------------------------------------------
class ForeclosureScraper:
    """
    Pull current foreclosure notices from the Bexar County GIS
    foreclosure ArcGIS layer and merge into the lead set.
    """

    LAYER_URL = (
        "https://maps.bexar.org/arcgis/rest/services/"
        "Foreclosures/MapServer/0/query"
    )

    def __init__(self):
        self.session = requests.Session()

    def scrape(self) -> list[LeadRecord]:
        """Query the foreclosure GIS layer for current-month notices."""
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultRecordCount": 500,
        }
        resp = retry(
            self.session.get,
            self.LAYER_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        if resp is None or resp.status_code != 200:
            log.warning("Foreclosure GIS layer unavailable — skipping")
            return []
        try:
            data = resp.json()
        except Exception:
            log.warning("Foreclosure GIS layer returned invalid JSON")
            return []

        features = data.get("features", [])
        records: list[LeadRecord] = []
        for feat in features:
            attrs = feat.get("attributes", {})
            try:
                rec = LeadRecord(
                    doc_num=str(attrs.get("InstrNo", attrs.get("OBJECTID", ""))),
                    doc_type="Notice of Foreclosure (GIS)",
                    filed=self._epoch_to_date(attrs.get("RecDate", attrs.get("PostDate", 0))),
                    cat="NOFC",
                    cat_label="Notice of Foreclosure",
                    owner=(attrs.get("Grantor", "") or attrs.get("Owner", "") or "").strip(),
                    grantee=(attrs.get("Grantee", "") or "").strip(),
                    amount=float(attrs.get("Amount", 0) or 0),
                    legal=(attrs.get("Legal", "") or "").strip(),
                    prop_address=(attrs.get("Situs", "") or attrs.get("Address", "") or "").strip(),
                    prop_city=(attrs.get("City", "") or "San Antonio").strip(),
                    prop_zip=str(attrs.get("Zip", "") or "").strip(),
                    clerk_url=FORECLOSURE_MAP_URL,
                )
                records.append(rec)
            except Exception:
                continue

        log.info("Foreclosure GIS layer: %d records", len(records))
        return records

    @staticmethod
    def _epoch_to_date(epoch_ms) -> str:
        """Convert epoch-milliseconds to MM/DD/YYYY or return empty."""
        try:
            if epoch_ms and int(epoch_ms) > 0:
                return datetime.fromtimestamp(int(epoch_ms) / 1000).strftime("%m/%d/%Y")
        except Exception:
            pass
        return ""


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------
def compute_flags(rec: LeadRecord) -> list[str]:
    """Determine which distress flags apply to a record."""
    flags: list[str] = []
    cat = rec.cat.upper()

    if cat == "LP":
        flags.append("Lis pendens")
    if cat in ("NOFC", "TAXDEED"):
        flags.append("Pre-foreclosure")
    if cat in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if cat in ("LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")
    if cat in ("LNMECH",):
        flags.append("Mechanic lien")
    if cat in ("LNHOA",):
        flags.append("HOA lien")
    if cat in ("LN", "MEDLN"):
        flags.append("Lien")
    if cat in ("PRO",):
        flags.append("Probate / estate")
    # LLC / corp owner
    owner_upper = rec.owner.upper()
    if any(kw in owner_upper for kw in ("LLC", "INC", "CORP", "TRUST", "LTD", "LP")):
        flags.append("LLC / corp owner")
    # New this week
    if rec.filed:
        try:
            filed_dt = datetime.strptime(rec.filed, "%m/%d/%Y")
            if (datetime.now() - filed_dt).days <= 7:
                flags.append("New this week")
        except Exception:
            pass

    return flags


def compute_score(rec: LeadRecord) -> int:
    """
    Seller Score (0-100):
      Base 30
      +10 per flag
      +20 if LP + Foreclosure combo
      +15 if amount > $100k
      +10 if amount > $50k
      +5  if new this week
      +5  if has property address
    """
    score = 30
    flag_names = [f.lower() for f in rec.flags]

    score += 10 * len(rec.flags)

    if "lis pendens" in flag_names and "pre-foreclosure" in flag_names:
        score += 20
    if rec.amount > 100_000:
        score += 15
    elif rec.amount > 50_000:
        score += 10
    if "new this week" in flag_names:
        score += 5
    if rec.prop_address:
        score += 5

    return min(score, 100)


def score_all(records: list[LeadRecord]) -> list[LeadRecord]:
    """Compute flags and score for every record."""
    for rec in records:
        rec.flags = compute_flags(rec)
        rec.score = compute_score(rec)
    records.sort(key=lambda r: r.score, reverse=True)
    return records


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------
def split_name(full_name: str) -> tuple[str, str]:
    """Best-effort split of 'LAST, FIRST' or 'FIRST LAST' into (first, last)."""
    name = full_name.strip()
    if not name:
        return ("", "")
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        return (parts[1] if len(parts) > 1 else "", parts[0])
    parts = name.split()
    if len(parts) == 1:
        return ("", parts[0])
    return (parts[0], " ".join(parts[1:]))


def write_json(records: list[LeadRecord], path: Path, lookback_days: int) -> None:
    """Write the JSON output file."""
    now = datetime.now()
    payload = {
        "fetched_at": now.isoformat(),
        "source": f"{COUNTY} County, {STATE_FULL} — Clerk Portal + Parcel API",
        "date_range": {
            "start": (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d"),
            "end": now.strftime("%Y-%m-%d"),
        },
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address),
        "records": [asdict(r) for r in records],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("JSON written: %s (%d records)", path, len(records))


def write_ghl_csv(records: list[LeadRecord], path: Path) -> None:
    """Write the GoHighLevel-importable CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "First Name",
        "Last Name",
        "Mailing Address",
        "Mailing City",
        "Mailing State",
        "Mailing Zip",
        "Property Address",
        "Property City",
        "Property State",
        "Property Zip",
        "Lead Type",
        "Document Type",
        "Date Filed",
        "Document Number",
        "Amount/Debt Owed",
        "Seller Score",
        "Motivated Seller Flags",
        "Source",
        "Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            first, last = split_name(rec.owner)
            writer.writerow(
                {
                    "First Name": first,
                    "Last Name": last,
                    "Mailing Address": rec.mail_address,
                    "Mailing City": rec.mail_city,
                    "Mailing State": rec.mail_state,
                    "Mailing Zip": rec.mail_zip,
                    "Property Address": rec.prop_address,
                    "Property City": rec.prop_city,
                    "Property State": rec.prop_state,
                    "Property Zip": rec.prop_zip,
                    "Lead Type": rec.cat_label,
                    "Document Type": rec.doc_type,
                    "Date Filed": rec.filed,
                    "Document Number": rec.doc_num,
                    "Amount/Debt Owed": f"${rec.amount:,.2f}" if rec.amount else "",
                    "Seller Score": rec.score,
                    "Motivated Seller Flags": "; ".join(rec.flags),
                    "Source": f"{COUNTY} County Clerk ({STATE_FULL})",
                    "Public Records URL": rec.clerk_url,
                }
            )
    log.info("GHL CSV written: %s (%d records)", path, len(records))


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Bexar County Motivated Seller Scraper")
    parser.add_argument("--days", type=int, default=LOOKBACK_DAYS, help="Lookback days (default 7)")
    parser.add_argument("--skip-parcel", action="store_true", help="Skip parcel enrichment")
    parser.add_argument("--skip-foreclosure", action="store_true", help="Skip foreclosure GIS layer")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback: %d days", args.days)
    log.info("=" * 60)

    # Determine project root (one level up from scraper/)
    project_root = Path(__file__).resolve().parent.parent

    # --- Step 1: Scrape clerk portal ---
    clerk = ClerkScraper(lookback_days=args.days)
    records = clerk.scrape()

    # --- Step 2: Scrape foreclosure GIS layer ---
    if not args.skip_foreclosure:
        fc_scraper = ForeclosureScraper()
        fc_records = fc_scraper.scrape()
        # Merge, de-duplicate by doc_num
        existing_nums = {r.doc_num for r in records}
        for fc in fc_records:
            if fc.doc_num not in existing_nums:
                records.append(fc)
                existing_nums.add(fc.doc_num)

    if not records:
        log.warning("No records found. Writing empty output files.")
        records = []

    # --- Step 3: Enrich with parcel data ---
    if not args.skip_parcel and records:
        enricher = ParcelEnricher()
        records = enricher.enrich(records)

    # --- Step 4: Score and flag ---
    records = score_all(records)

    # --- Step 5: Output ---
    # JSON files
    write_json(records, project_root / "dashboard" / "records.json", args.days)
    write_json(records, project_root / "data" / "records.json", args.days)

    # GHL CSV
    write_ghl_csv(records, project_root / "data" / "ghl_export.csv")

    # --- Summary ---
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("  Total records   : %d", len(records))
    log.info("  With address    : %d", sum(1 for r in records if r.prop_address))
    log.info("  Score >= 70     : %d", sum(1 for r in records if r.score >= 70))
    log.info("  Score >= 50     : %d", sum(1 for r in records if r.score >= 50))
    log.info("  Top categories  :")
    cat_counts: dict[str, int] = {}
    for r in records:
        cat_counts[r.cat_label] = cat_counts.get(r.cat_label, 0) + 1
    for label, count in sorted(cat_counts.items(), key=lambda x: -x[1])[:10]:
        log.info("    %-30s %d", label, count)
    log.info("=" * 60)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(1)
    except Exception as exc:
        log.error("Fatal error: %s", exc)
        traceback.print_exc()
        # Write empty output so the workflow doesn't fail on missing files
        project_root = Path(__file__).resolve().parent.parent
        for p in [
            project_root / "dashboard" / "records.json",
            project_root / "data" / "records.json",
        ]:
            if not p.exists():
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps({"fetched_at": datetime.now().isoformat(), "total": 0, "records": []}))
        sys.exit(1)

