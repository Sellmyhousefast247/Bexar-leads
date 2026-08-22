"""
Bexar County Motivated Seller Lead Scraper v2
Pipeline: County -> scrape -> normalize -> hash/dedupe -> identify NEW/CHANGED
State is persisted in data/state.json so every record carries first_seen
(the date it entered the system) and a status of NEW / CHANGED / EXISTING.
"""

import asyncio
import csv
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
REQUEST_TIMEOUT = 30
HEADLESS = True
CLERK_DIRECT = "https://bexar.tx.publicsearch.us"
ARCGIS_PARCEL_URL = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

STATE_PATH = Path("data/state.json")
OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]

# Motivated-seller doc types only, grouped like the dashboard sidebar
LEAD_TYPES = {
    "NOFC":     ("Notice of Foreclosure", "foreclosure"),
    "LP":       ("Lis Pendens",           "foreclosure"),
    "TAXDEED":  ("Tax Deed",              "tax_lien"),
    "LNIRS":    ("Federal Tax Lien",      "tax_lien"),
    "LNFED":    ("Federal Lien",          "tax_lien"),
    "LNCORPTX": ("State Tax Lien",        "tax_lien"),
    "LNMECH":   ("Mechanics Lien",        "tax_lien"),
    "LNHOA":    ("HOA Lien",              "tax_lien"),
    "MEDLN":    ("Medicaid Lien",         "tax_lien"),
    "JUD":      ("Judgment",              "judgment"),
    "CCJ":      ("Certified Judgment",    "judgment"),
    "DRJUD":    ("Domestic Judgment",     "judgment"),
    "PRO":      ("Probate Document",      "probate"),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── helpers ────────────────────────────────────────────────────────────────────

def safe_str(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def parse_amount(raw) -> Optional[float]:
    raw = safe_str(raw)
    digits = re.sub(r"[^\d.]", "", raw)
    if digits:
        try:
            return float(digits)
        except ValueError:
            pass
    return None


def name_variants(full_name: str):
    full_name = safe_str(full_name).upper()
    variants = {full_name}
    if "," in full_name:
        parts = [p.strip() for p in full_name.split(",", 1)]
        variants.add(f"{parts[1]} {parts[0]}")
        variants.add(f"{parts[0]} {parts[1]}")
    else:
        parts = full_name.split()
        if len(parts) >= 2:
            variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
            variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
    return {v for v in variants if v}


# ── STAGE 1: SCRAPE ──────────────────────────────────────────────────────────

class ClerkScraper:

    SEARCH_URL = "https://bexar.tx.publicsearch.us/search"

    def __init__(self, lookback_days=LOOKBACK_DAYS):
        self.lookback_days = lookback_days
        self.start_date = (date.today() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        self.end_date = date.today().strftime("%m/%d/%Y")
        self.records = []

    async def run(self) -> list:
        log.info("STAGE 1 SCRAPE | %s to %s", self.start_date, self.end_date)
        try:
            await self._playwright_scrape()
        except Exception as exc:
            log.error("Playwright failed: %s", exc)
            self._http_fallback()
        log.info("Scraped %d raw records", len(self.records))
        return self.records

    async def _playwright_scrape(self):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=HEADLESS)
            page = await browser.new_page()
            try:
                for code, (label, cat) in LEAD_TYPES.items():
                    try:
                        recs = await self._search(page, code, label, cat)
                        self.records.extend(recs)
                        log.info("  %-10s -> %d", code, len(recs))
                    except Exception as exc:
                        log.warning("Failed %s: %s", code, exc)
            finally:
                await browser.close()

    async def _search(self, page, code, label, cat) -> list:
        params = {
            "docTypeCode": code,
            "dateRange": "custom",
            "startDt": self.start_date,
            "endDt": self.end_date,
            "countyId": "15",
        }
        url = f"{self.SEARCH_URL}?{urlencode(params)}"
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)
        soup = BeautifulSoup(await page.content(), "lxml")
        out = []
        for row in soup.select("tr[data-id], .result-row, tbody tr"):
            try:
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue
                texts = [c.get_text(" ", strip=True) for c in cells]
                link_tag = row.find("a", href=True)
                link = urljoin(CLERK_DIRECT, link_tag["href"]) if link_tag else ""
                out.append({
                    "raw_cells": texts,
                    "link": link,
                    "doc_type": code,
                    "cat_label": label,
                    "cat": cat,
                })
            except Exception:
                continue
        return out

    def _http_fallback(self):
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        start = (date.today() - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
        end = date.today().strftime("%Y-%m-%d")
        for code, (label, cat) in LEAD_TYPES.items():
            try:
                url = f"{CLERK_DIRECT}/api/search?docTypeCode={code}&startDt={start}&endDt={end}&countyId=15"
                r = session.get(url, timeout=REQUEST_TIMEOUT)
                if r.status_code != 200:
                    continue
                try:
                    items = r.json()
                except ValueError:
                    continue
                if isinstance(items, dict):
                    items = items.get("results", items.get("records", []))
                for item in (items or []):
                    if not isinstance(item, dict):
                        continue
                    self.records.append({
                        "raw_cells": [
                            str(item.get("instrumentNumber") or item.get("id") or ""),
                            str(item.get("recordDate") or item.get("date") or ""),
                            str(item.get("grantor") or item.get("owner") or ""),
                            str(item.get("grantee") or ""),
                            str(item.get("legalDescription") or ""),
                            str(item.get("consideration") or item.get("amount") or ""),
                        ],
                        "link": str(item.get("url") or ""),
                        "doc_type": code,
                        "cat_label": label,
                        "cat": cat,
                    })
            except Exception as exc:
                log.warning("HTTP fallback failed %s: %s", code, exc)


# ── STAGE 2: NORMALIZE ───────────────────────────────────────────────────────

def normalize(raw_records: list) -> list:
    log.info("STAGE 2 NORMALIZE | %d records", len(raw_records))
    out = []
    for raw in raw_records:
        try:
            cells = raw.get("raw_cells", [])
            doc_num = safe_str(cells[0]) if len(cells) > 0 else ""
            filed_raw = safe_str(cells[1]) if len(cells) > 1 else ""
            grantor = safe_str(cells[2]) if len(cells) > 2 else ""
            grantee = safe_str(cells[3]) if len(cells) > 3 else ""
            legal = safe_str(cells[4]) if len(cells) > 4 else ""
            amount = parse_amount(cells[5]) if len(cells) > 5 else None

            if not doc_num and not filed_raw:
                continue

            filed_str = ""
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y-%m-%dT%H:%M:%S"):
                try:
                    filed_str = datetime.strptime(filed_raw[:19 if "T" in filed_raw else 10], fmt).strftime("%Y-%m-%d")
                    break
                except Exception:
                    continue
            if not filed_str:
                filed_str = filed_raw[:10]

            out.append({
                "doc_num": doc_num,
                "doc_type": raw.get("doc_type", ""),
                "cat_label": raw.get("cat_label", ""),
                "cat": raw.get("cat", ""),
                "filed": filed_str,
                "owner": grantor.upper(),
                "grantee": grantee.upper(),
                "legal": legal,
                "amount": amount,
                "clerk_url": raw.get("link") or f"{CLERK_DIRECT}/search?docType={raw.get('doc_type','')}",
            })
        except Exception:
            continue
    log.info("Normalized to %d records", len(out))
    return out


# ── STAGE 3: HASH / DEDUPE ───────────────────────────────────────────────────

def record_id(rec: dict) -> str:
    """Stable identity hash: county + doc number (or owner+filed+type if no doc num)."""
    base = rec.get("doc_num") or f"{rec.get('owner')}|{rec.get('filed')}|{rec.get('doc_type')}"
    return hashlib.sha1(f"bexar|{base}".encode()).hexdigest()[:16]


def content_hash(rec: dict) -> str:
    """Hash of the record's content - changes when any field changes."""
    fields = "|".join(safe_str(rec.get(k)) for k in
                      ("doc_num", "doc_type", "filed", "owner", "grantee", "legal", "amount"))
    return hashlib.sha1(fields.encode()).hexdigest()[:16]


def dedupe(records: list) -> list:
    log.info("STAGE 3 HASH/DEDUPE | %d records", len(records))
    seen = {}
    for rec in records:
        rid = record_id(rec)
        rec["id"] = rid
        rec["content_hash"] = content_hash(rec)
        if rid not in seen:
            seen[rid] = rec
    out = list(seen.values())
    log.info("Deduped to %d unique records", len(out))
    return out


# ── STAGE 4: NEW / CHANGED DETECTION ─────────────────────────────────────────

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(state: dict):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=1), encoding="utf-8")


def detect_changes(records: list, state: dict) -> list:
    log.info("STAGE 4 NEW/CHANGED | %d records vs %d known", len(records), len(state))
    today = date.today().isoformat()
    new_count = changed_count = existing_count = 0

    for rec in records:
        rid = rec["id"]
        prev = state.get(rid)
        if prev is None:
            rec["status"] = "NEW"
            rec["first_seen"] = today
            new_count += 1
        elif prev.get("content_hash") != rec["content_hash"]:
            rec["status"] = "CHANGED"
            rec["first_seen"] = prev.get("first_seen", today)
            changed_count += 1
        else:
            rec["status"] = "EXISTING"
            rec["first_seen"] = prev.get("first_seen", today)
            existing_count += 1

        state[rid] = {
            "content_hash": rec["content_hash"],
            "first_seen": rec["first_seen"],
            "last_seen": today,
        }

    log.info("NEW=%d CHANGED=%d EXISTING=%d", new_count, changed_count, existing_count)
    return records


# ── PARCEL ENRICHMENT ────────────────────────────────────────────────────────

class ParcelIndex:

    def __init__(self):
        self._by_owner = {}
        self._loaded = False

    def load(self):
        if self._loaded:
            return
        log.info("Loading parcel data from ArcGIS...")
        try:
            records = self._fetch()
            for rec in records:
                for key in name_variants(rec.get("owner", "")):
                    self._by_owner.setdefault(key, []).append(rec)
            log.info("Parcel index: %d records", len(records))
        except Exception as exc:
            log.error("Parcel load failed: %s", exc)
        self._loaded = True

    def lookup(self, owner_name: str) -> Optional[dict]:
        for key in name_variants(owner_name):
            hits = self._by_owner.get(key)
            if hits:
                return hits[0]
        return None

    def _fetch(self) -> list:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        records = []
        offset = 0
        while True:
            params = {
                "where": "1=1",
                "outFields": "Situs,SitusCity,SitusZip,OwnerName,MailAddr,MailCity,MailState,MailZip",
                "resultOffset": offset,
                "resultRecordCount": 1000,
                "returnGeometry": "false",
                "f": "json",
            }
            try:
                resp = session.get(ARCGIS_PARCEL_URL, params=params, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                if offset == 0:
                    raise
                break
            features = data.get("features", [])
            if not features:
                break
            for feat in features:
                a = feat.get("attributes") or {}
                owner = safe_str(a.get("OwnerName"))
                if not owner:
                    continue
                records.append({
                    "owner": owner,
                    "prop_address": safe_str(a.get("Situs")),
                    "prop_city": safe_str(a.get("SitusCity")),
                    "prop_state": "TX",
                    "prop_zip": safe_str(a.get("SitusZip")),
                    "mail_address": safe_str(a.get("MailAddr")),
                    "mail_city": safe_str(a.get("MailCity")),
                    "mail_state": safe_str(a.get("MailState")) or "TX",
                    "mail_zip": safe_str(a.get("MailZip")),
                })
            if not data.get("exceededTransferLimit", False):
                break
            offset += 1000
            time.sleep(0.5)
        return records


def enrich(records: list, parcels: ParcelIndex) -> list:
    for rec in records:
        try:
            parcel = parcels.lookup(rec.get("owner", ""))
            if parcel:
                for f in ("prop_address", "prop_city", "prop_state", "prop_zip",
                          "mail_address", "mail_city", "mail_state", "mail_zip"):
                    rec.setdefault(f, parcel.get(f, ""))
            for f in ("prop_address", "prop_city", "prop_state", "prop_zip",
                      "mail_address", "mail_city", "mail_state", "mail_zip"):
                rec.setdefault(f, "")
            # absentee flag: mailing address differs from property address
            rec["absentee"] = bool(
                rec["prop_address"] and rec["mail_address"]
                and rec["prop_address"].upper() != rec["mail_address"].upper()
            )
            rec["out_of_state"] = bool(rec["mail_state"] and rec["mail_state"].upper() != "TX")
        except Exception:
            continue
    return records


# ── SCORING ──────────────────────────────────────────────────────────────────

def score_records(records: list) -> list:
    for rec in records:
        flags = []
        doc_type = rec.get("doc_type", "").upper()
        cat = rec.get("cat", "")
        owner = rec.get("owner", "").upper()

        if doc_type == "LP":
            flags.append("Lis pendens")
        if doc_type == "NOFC" or cat == "foreclosure" and doc_type != "LP":
            flags.append("Pre-foreclosure")
        if cat == "judgment":
            flags.append("Judgment lien")
        if cat == "tax_lien":
            flags.append("Tax lien")
        if doc_type == "LNMECH":
            flags.append("Mechanic lien")
        if cat == "probate":
            flags.append("Probate / estate")
        for word in ("LLC", "INC", "CORP", "LTD", "TRUST", "HOLDINGS"):
            if word in owner:
                flags.append("LLC / corp owner")
                break
        if rec.get("absentee"):
            flags.append("Absentee owner")
        if rec.get("out_of_state"):
            flags.append("Out of state")
        if rec.get("status") == "NEW":
            flags.append("New this week")

        score = 30 + 10 * len(flags)
        if "Lis pendens" in flags and "Pre-foreclosure" in flags:
            score += 20
        amount = rec.get("amount")
        if amount:
            if amount > 100000:
                score += 15
            elif amount > 50000:
                score += 10
        if "New this week" in flags:
            score += 5
        if rec.get("prop_address"):
            score += 5

        rec["flags"] = list(dict.fromkeys(flags))
        rec["score"] = min(score, 100)
    records.sort(key=lambda r: (r.get("status") != "NEW", -r.get("score", 0)))
    return records


# ── OUTPUT ───────────────────────────────────────────────────────────────────

def build_output(records: list) -> dict:
    today = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    return {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "county": "Bexar",
        "source": "Bexar County Clerk / BCAD",
        "date_range": {
            "start": (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat(),
            "end": today,
        },
        "total": len(records),
        "new_7d": sum(1 for r in records if r.get("first_seen", "") >= week_ago),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records": records,
    }


def save_outputs(payload: dict):
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Saved -> %s", path)


def split_name(owner: str):
    owner = safe_str(owner)
    if "," in owner:
        p = owner.split(",", 1)
        return p[1].strip().title(), p[0].strip().title()
    parts = owner.split()
    first = parts[0].title() if parts else ""
    last = " ".join(parts[1:]).title() if len(parts) > 1 else ""
    return first, last


def export_ghl_csv(records: list, out_path=Path("data/ghl_export.csv")):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Date Entered System",
        "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Status", "Source", "Public Records URL",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for rec in records:
            first, last = split_name(rec.get("owner", ""))
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": rec.get("mail_address", ""),
                "Mailing City": rec.get("mail_city", ""),
                "Mailing State": rec.get("mail_state", "TX"),
                "Mailing Zip": rec.get("mail_zip", ""),
                "Property Address": rec.get("prop_address", ""),
                "Property City": rec.get("prop_city", ""),
                "Property State": rec.get("prop_state", "TX"),
                "Property Zip": rec.get("prop_zip", ""),
                "Lead Type": rec.get("cat_label", ""),
                "Document Type": rec.get("doc_type", ""),
                "Date Filed": rec.get("filed", ""),
                "Date Entered System": rec.get("first_seen", ""),
                "Document Number": rec.get("doc_num", ""),
                "Amount/Debt Owed": f"${rec['amount']:,.2f}" if rec.get("amount") else "",
                "Seller Score": str(rec.get("score", "")),
                "Motivated Seller Flags": " | ".join(rec.get("flags", [])),
                "Status": rec.get("status", ""),
                "Source": "Bexar County Clerk",
                "Public Records URL": rec.get("clerk_url", ""),
            })
    log.info("GHL CSV -> %s", out_path)


def export_skiptrace_csv(records: list, out_path=Path("data/skiptrace_export.csv")):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["First Name", "Last Name", "Mailing Address", "Mailing City",
               "Mailing State", "Mailing Zip", "Property Address", "Property City",
               "Property State", "Property Zip"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for rec in records:
            first, last = split_name(rec.get("owner", ""))
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": rec.get("mail_address", ""),
                "Mailing City": rec.get("mail_city", ""),
                "Mailing State": rec.get("mail_state", "TX"),
                "Mailing Zip": rec.get("mail_zip", ""),
                "Property Address": rec.get("prop_address", ""),
                "Property City": rec.get("prop_city", ""),
                "Property State": rec.get("prop_state", "TX"),
                "Property Zip": rec.get("prop_zip", ""),
            })
    log.info("Skip trace CSV -> %s", out_path)


# ── MAIN PIPELINE ────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("BEXAR COUNTY LEAD PIPELINE | lookback=%d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    # Stage 1: scrape
    scraper = ClerkScraper(lookback_days=LOOKBACK_DAYS)
    raw = await scraper.run()

    # Stage 2: normalize
    records = normalize(raw)

    # Stage 3: hash / dedupe
    records = dedupe(records)

    # Stage 4: NEW / CHANGED detection
    state = load_state()
    records = detect_changes(records, state)
    save_state(state)

    # Enrichment + scoring
    parcels = ParcelIndex()
    parcels.load()
    records = enrich(records, parcels)
    records = score_records(records)

    # Output
    payload = build_output(records)
    save_outputs(payload)
    export_ghl_csv(records)
    export_skiptrace_csv(records)

    log.info("DONE | total=%d | new_7d=%d | with_address=%d",
             payload["total"], payload["new_7d"], payload["with_address"])


if __name__ == "__main__":
    asyncio.run(main())
