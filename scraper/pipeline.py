#!/usr/bin/env python3
import sys, os, pathlib, json, csv, re, time, uuid, logging, argparse
from datetime import datetime, timezone
import requests

GHL_API_KEY=os.environ.get("GHL_API_KEY","")
GHL_LOCATION_ID=os.environ.get("GHL_LOCATION_ID","47v58a5xVmpgOdpajnj7")
GHL_API_BASE="https://services.leadconnectorhq.com"
XLEADS_EMAIL=os.environ.get("XLEADS_EMAIL","")
XLEADS_PASSWORD=os.environ.get("XLEADS_PASSWORD","")
XLEADS_URL="https://next.xleads.com/leads"
MIN_SCORE=int(os.environ.get("MIN_SCORE","40"))
DRY_RUN=os.environ.get("DRY_RUN","0")=="1"
ROOT=pathlib.Path(__file__).parent.parent
RECORDS_JSON=pathlib.Path(os.environ.get("RECORDS_JSON",str(ROOT/"dashboard"/"records.json")))
STATE_JSON=pathlib.Path(os.environ.get("STATE_JSON",str(ROOT/"state"/"pipeline_state.json")))
EXPORTS_DIR=pathlib.Path(os.environ.get("EXPORTS_DIR",str(ROOT/"exports"/"xleads")))
IMPORTS_DIR=pathlib.Path(os.environ.get("IMPORTS_DIR",str(ROOT/"imports"/"xleads")))
LOGS_DIR=pathlib.Path(os.environ.get("LOGS_DIR",str(ROOT/"logs")))
LOGS_DIR.mkdir(parents=True,exist_ok=True)
today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOGS_DIR/f"pipeline_{today}.log"),logging.StreamHandler(sys.stdout)])
log=logging.getLogger("pipeline")

def _n(s): return re.sub(r"[^a-z0-9 ]","",(s or "").lower().strip())
def get_key(r):
    if r.get("doc_num"): return f"doc:{r['doc_num']}"
    return f"addr:{_n(r.get('prop_address',''))}:{_n(r.get('owner',''))}"
def load_state():
    STATE_JSON.parent.mkdir(parents=True,exist_ok=True)
    return json.loads(STATE_JSON.read_text()) if STATE_JSON.exists() else {}
def save_state(s): STATE_JSON.parent.mkdir(parents=True,exist_ok=True); STATE_JSON.write_text(json.dumps(s,indent=2,default=str))
def parse_addr(r):
    raw=(r.get("prop_address") or "").strip()
    if "," in raw: parts=raw.split(",",1); st=parts[0].strip().title(); cf=parts[1].strip().title()
    else: st=raw.title(); cf=""
    city=(r.get("prop_city") or cf or "San Antonio").title()
    if city.upper() in ("TEXAS","TX"): city=cf or "San Antonio"
    return st,city,(r.get("prop_state") or "TX").upper(),(r.get("prop_zip") or "").strip()
def parse_owner(o):
    if not o: return "",""
    o=re.sub(r"\b(LLC|INC|TRUST|TRUSTEE|ET AL|ET UX|JR|SR|II|III|IV)\b","",o,flags=re.IGNORECASE).strip()
    p=o.split()
    if not p: return "",o.title()
    if len(p)==1: return "",p[0].title()
    return " ".join(p[1:]).title(),p[0].title()
def select_records(state):
    if not RECORDS_JSON.exists(): return []
    data=json.loads(RECORDS_JSON.read_text()); proc=state.get("processed_keys",{}); out=[]
    for r in data.get("records",[]):
        k=get_key(r)
        if k in proc and proc[k].get("ghl_contact_id"): continue
        if not r.get("prop_address"): continue
        if (r.get("score") or 0)<MIN_SCORE: continue
        out.append(r)
    log.info("Selected %d new records",len(out)); return out
def export_csv(records,batch_id):
    EXPORTS_DIR.mkdir(parents=True,exist_ok=True)
    ts=datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    cp=EXPORTS_DIR/f"xleads_export_{ts}_{batch_id[:8]}.csv"
    mp=EXPORTS_DIR/f"xleads_meta_{ts}_{batch_id[:8]}.json"
    xr,meta=[],[]
    for r in records:
        st,city,state,z=parse_addr(r); fi,la=parse_owner(r.get("owner","")); k=get_key(r)
        xr.append({"Street Address":st,"City":city,"State":state,"Zip":z})
        meta.append({"record_key":k,"batch_id":batch_id,"doc_num":r.get("doc_num",""),"owner":r.get("owner",""),"owner_first":fi,"owner_last":la,"street":st,"city":city,"state":state,"zip":z,"mail_address":r.get("mail_address","").title(),"mail_city":r.get("mail_city","").title(),"mail_state":r.get("mail_state","TX"),"mail_zip":r.get("mail_zip",""),"cat":r.get("cat",""),"cat_label":r.get("cat_label",""),"score":r.get("score",0),"flags":r.get("flags",[]),"filed":r.get("filed",""),"amount":r.get("amount",0),"clerk_url":r.get("clerk_url","")})
    with open(cp,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["Street Address","City","State","Zip"]); w.writeheader(); w.writerows(xr)
    mp.write_text(json.dumps(meta,indent=2)); log.info("Exported %d -> %s",len(xr),cp); return cp
def run_xleads(csv_path, batch_id):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("playwright not installed")
        return None
    log.info("S1: Navigate to XLeads")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True)
        pg = ctx.new_page()
        try:
            pg.goto(XLEADS_URL, timeout=60000)
            pg.wait_for_timeout(2000)
            if pg.locator("input[type='email']").count() > 0:
                log.info("Login required")
                pg.locator("input[type='email']").fill(XLEADS_EMAIL)
                pg.locator("input[type='password']").fill(XLEADS_PASSWORD)
                pg.get_by_role("button", name="Login").click()
                pg.wait_for_url("**/leads**", timeout=30000)
                pg.wait_for_timeout(2000)
            log.info("S2: Import file")
            pg.get_by_role("button", name="Import file").click()
            pg.wait_for_timeout(2000)
            log.info("S3: Upload " + str(csv_path))
            with pg.expect_file_chooser() as fc_info:
                pg.get_by_text("browse").click()
            fc_info.value.set_files(str(csv_path))
            pg.wait_for_timeout(2000)
            log.info("S4: Next")
            pg.get_by_role("button", name="Next").click()
            pg.wait_for_timeout(2000)
            log.info("S4.5: Import")
            pg.get_by_role("button", name="Import").click()
            pg.wait_for_timeout(8000)
            log.info("S4.6: Show properties")
            pg.get_by_role("button", name="Show properties").click()
            pg.wait_for_timeout(3000)
            log.info("S5: Wait for grid")
            pg.wait_for_selector("button:has-text('Select')", timeout=20000)
            pg.wait_for_timeout(2000)
            log.info("S5: Click Select dropdown")
            pg.locator("button").filter(has_text="Select").first.click()
            pg.wait_for_timeout(1500)
            log.info("S5: Click Select All")
            pg.get_by_role("menuitem", name="Select All").click()
            pg.wait_for_timeout(3000)
            log.info("S6: Click saved-list button")
            pg.locator("button[class*='bg-red'], button[class*='destructive']").first.click()
            pg.wait_for_timeout(2000)
            log.info("S7: Click Export")
            pg.get_by_role("button", name="Export").click()
            pg.wait_for_timeout(2000)
            log.info("S8: Select Lead Trace")
            pg.get_by_text("Lead Trace - Owner Contact Info", exact=False).click()
            pg.wait_for_timeout(1000)
            log.info("S9: Download")
            with pg.expect_download(timeout=120000) as dl_info:
                pg.get_by_role("button", name="Export").last.click()
            dl = dl_info.value
            dl_path = IMPORTS_DIR / dl.suggested_filename
            dl.save_as(str(dl_path))
            log.info(f"S9: Done -> {dl_path}")
            return str(dl_path)
        except Exception as e:
            log.error(f"XLeads error: {e}")
            pg.screenshot(path=str(LOGS_DIR/f"xleads_err_{today}.png"))
            return None
        finally:
            browser.close()


def run_pipeline():
    log.info("=== Pipeline start ===")
    log.info("Step 1: Running XLeads skip trace export")
    csv_files = sorted(EXPORTS_DIR.glob("xleads_export_*.csv"))
    if not csv_files:
        log.error("No export CSV found in exports/xleads/")
        sys.exit(2)
    latest_csv = csv_files[-1]
    batch_id = latest_csv.stem.split("_")[-1]
    log.info(f"Using CSV: {latest_csv.name}, batch_id: {batch_id}")
    enriched = run_xleads(latest_csv, batch_id)
    if enriched:
        log.info(f"XLeads export complete: {enriched}")
    else:
        log.error("XLeads export failed")
        sys.exit(2)
    log.info("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
