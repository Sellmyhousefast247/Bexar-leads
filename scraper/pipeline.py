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
def run_xleads(csv_path,batch_id):
    try: from playwright.sync_api import sync_playwright
    except ImportError: log.error("Playwright not installed"); return None
    IMPORTS_DIR.mkdir(parents=True,exist_ok=True)
    ts=datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    dl=IMPORTS_DIR/f"xleads_enriched_{ts}_{batch_id[:8]}.csv"
    with sync_playwright() as pw:
        br=pw.chromium.launch(headless=True,args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx=br.new_context(viewport={"width":1280,"height":800},accept_downloads=True); pg=ctx.new_page()
        try:
            log.info("S1: Navigate to XLeads directly")
            pg.goto(XLEADS_URL,wait_until="domcontentloaded",timeout=60000)
            pg.wait_for_timeout(3000)
            if pg.locator("input[type='email']").is_visible():
                log.info("Login required")
                pg.fill("input[type='email']",XLEADS_EMAIL)
                pg.fill("input[type='password']",XLEADS_PASSWORD)
                pg.click("button[type='submit']")
                pg.wait_for_url("**/leads**",timeout=30000)
                pg.wait_for_timeout(2000)
            fr=pg
            log.info("S2: Import file")
            fr.locator("button:has-text('Import file')").wait_for(state="visible",timeout=20000)
            fr.locator("button:has-text('Import file')").click(); fr.wait_for_timeout(1500)
            log.info("S3: Upload %s",csv_path)
            fi=fr.locator("input[type='file']")
            fi.wait_for(state="attached",timeout=10000)
            fi.set_input_files(str(csv_path)); fr.wait_for_timeout(1500)
            log.info("S4: Next")
            fr.locator("button:has-text('Next')").wait_for(state="visible",timeout=10000)
            fr.locator("button:has-text('Next')").click(); fr.wait_for_timeout(3000)
            # S4.5: Click Import on column mapping screen
            log.info("S4.5: Waiting for Import button")
            fr.get_by_role("button", name="Import").wait_for(state="visible", timeout=20000)
            fr.get_by_role("button", name="Import").click()
            fr.wait_for_timeout(8000)
            log.info("S4.5: Clicked Import")
            # S4.6: Click Show properties on import complete screen
            log.info("S4.6: Waiting for Show properties button")
            fr.get_by_role("button", name="Show properties").wait_for(state="visible", timeout=20000)
            fr.get_by_role("button", name="Show properties").click()
            fr.wait_for_timeout(5000)
            log.info("S4.6: Clicked Show properties")
            fr.wait_for_timeout(5000)
            # S5: Wait for grid then Select All via JS click on chevron
            log.info("S5: Waiting for property grid to load")
            fr.wait_for_selector("button:has-text('Select')", timeout=15000)
            fr.wait_for_timeout(2000)
            log.info("S5: Clicking Select dropdown chevron via JS")
            fr.evaluate("""() => {
                const btns = Array.from(document.querySelectorAll('button'));
                const selectBtn = btns.find(b => b.textContent.trim().startsWith('Select'));
                if (selectBtn) selectBtn.click();
            }""")
            fr.wait_for_timeout(1000)
            log.info("S5: Clicking Select All")
            fr.evaluate("""() => {
                const items = Array.from(document.querySelectorAll('li, [role="menuitem"], div'));
                const selectAll = items.find(el => el.textContent.trim().startsWith('Select All'));
                if (selectAll) selectAll.click();
            }""")
            fr.wait_for_timeout(2000)
            # S6: Click the red saved-list button (now shows "N selected")
            log.info("S6: Clicking saved-list button to open export panel")
            fr.evaluate("""() => {
                const btns = Array.from(document.querySelectorAll('button'));
                const redBtn = btns.find(b => /\d+\s*selected/i.test(b.textContent) || b.className.includes('bg-red') || b.className.includes('bg-destructive'));
                if (redBtn) redBtn.click();
                else {
                    // fallback: click last button in header area
                    const headerBtns = Array.from(document.querySelectorAll('header button, nav button, [class*="header"] button'));
                    if (headerBtns.length) headerBtns[headerBtns.length-1].click();
                }
            }""")
            fr.wait_for_timeout(2000)
            # S7: Click Export button in the panel
            log.info("S7: Clicking Export button")
            fr.get_by_role("button", name="Export").click()
            fr.wait_for_timeout(2000)
            # S8: Check Lead Trace - Owner Contact Info
            log.info("S8: Checking Lead Trace - Owner Contact Info")
            fr.get_by_text("Lead Trace - Owner Contact Info", exact=False).click()
            fr.wait_for_timeout(1000)
            # S9: Click final Export to download
            log.info("S9: Clicking final Export button")
            with page.expect_download(timeout=120000) as dl_info:
                fr.get_by_role("button", name="Export").last().click()
            dl = dl_info.value
            dl_path = IMPORTS_DIR / dl.suggested_filename
            dl.save_as(str(dl_path))
            log.info(f"S9: Downloaded enriched CSV to {dl_path}")
            return str(dl_path)
        except Exception as e:
            log.error(f"XLeads error: {e}")
            page.screenshot(path=str(LOGS_DIR/f"xleads_err_{today}.png"))
            return None
def run_pipeline():
    log.info("=== Pipeline start ===")
    state = load_state()
    records_data = json.loads(RECORDS_JSON.read_text())
    records = records_data.get("records", [])
    log.info(f"Loaded {len(records)} total records")

    # Filter to new records not yet imported
    new_recs = [r for r in records if get_key(r) not in state]
    log.info(f"{len(new_recs)} new records to import")

    if not new_recs:
        log.info("No new records Ã¢ÂÂ nothing to do")
        return

    # Import directly to GHL
    if not GHL_API_KEY:
        log.warning("GHL_API_KEY not set Ã¢ÂÂ skipping GHL import")
        return

    imported = 0
    for r in new_recs:
        try:
            import_ghl(r)
            state[get_key(r)] = True
            imported += 1
        except Exception as e:
            log.error(f"GHL import failed for {get_key(r)}: {e}")

    save_state(state)
    log.info(f"Done Ã¢ÂÂ imported {imported}/{len(new_recs)} records to GHL")

if __name__ == "__main__":
    run_pipeline()
