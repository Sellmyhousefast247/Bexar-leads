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
XLEADS_GHL_URL=f"https://login.xleads.com/v2/location/{GHL_LOCATION_ID}/custom-menu-link/f6d4da52-2aab-4c3e-8d35-7c1b51c86f46"
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
            pg.goto(XLEADS_GHL_URL,wait_until="networkidle",timeout=30000); pg.wait_for_timeout(3000)
            fr=next((f for f in pg.frames if "next.xleads.com" in f.url),None) or pg
            if fr is pg:
                pg.goto(XLEADS_URL,wait_until="networkidle",timeout=30000); pg.wait_for_timeout(3000)
                if "login" in pg.url.lower() or pg.locator("input[type='email']").is_visible():
                    pg.fill("input[type='email']",XLEADS_EMAIL); pg.fill("input[type='password']",XLEADS_PASSWORD)
                    pg.click("button[type='submit']"); pg.wait_for_url("**/leads**",timeout=15000)
            fr.locator("button:has-text('Import file')").wait_for(state="visible",timeout=15000); fr.locator("button:has-text('Import file')").click(); fr.wait_for_timeout(1500)
            fi=fr.locator("input[type='file']"); fi.wait_for(state="attached",timeout=10000); fi.set_input_files(str(csv_path)); fr.wait_for_timeout(1500)
            fr.locator("dialog button:has-text('Next')").wait_for(state="visible",timeout=10000); fr.locator("dialog button:has-text('Next')").click(); fr.wait_for_timeout(3000)
            fr.locator("button:has-text('Select')").first.wait_for(state="visible",timeout=15000); fr.locator("button:has-text('Select')").first.click(); fr.wait_for_timeout(1000)
            fr.locator("text=Select All").first.wait_for(state="visible",timeout=5000); fr.locator("text=Select All").first.click(); fr.wait_for_timeout(1500)
            rb=fr.locator("a[href*='new']").first
            if not rb.is_visible(): rb=fr.locator("button.bg-red-600").first
            rb.wait_for(state="visible",timeout=10000); rb.click(); fr.wait_for_timeout(3000)
            fr.locator("button:has-text('Export')").first.wait_for(state="visible",timeout=15000); fr.locator("button:has-text('Export')").first.click(); fr.wait_for_timeout(2000)
            cb=fr.locator("label:has-text('Lead Trace'),text=Lead Trace").first; cb.wait_for(state="visible",timeout=10000); cb.click(); fr.wait_for_timeout(1000)
            fr.locator("button:has-text('Export')").last.wait_for(state="visible",timeout=10000); fr.locator("button:has-text('Export')").last.click(); fr.wait_for_timeout(3000)
            for _ in range(30):
                fr.wait_for_timeout(10000)
                if any(s in fr.locator("body").inner_text() for s in ["Download","completed","export ready"]): break
            fr.goto(XLEADS_URL,wait_until="networkidle",timeout=30000); fr.wait_for_timeout(2000)
            fr.locator("button:has-text('Exports')").first.wait_for(state="visible",timeout=15000); fr.locator("button:has-text('Exports')").first.click(); fr.wait_for_timeout(2000)
            with fr.expect_download(timeout=30000) as dli:
                fr.locator("button:has-text('Download')").first.wait_for(state="visible",timeout=10000); fr.locator("button:has-text('Download')").first.click()
            dli.value.save_as(str(dl)); log.info("Downloaded -> %s",dl); return dl
        except Exception as e:
            log.error("XLeads err: %s",e,exc_info=True)
            try: pg.screenshot(path=str(LOGS_DIR/f"xleads_err_{today}.png"))
            except: pass
            return None
        finally: ctx.close(); br.close()
def parse_enriched(ep,mp):
    if not ep.exists(): return []
    mba={}
    if mp.exists():
        for m in json.loads(mp.read_text()): mba[_n(m.get("street","")+m.get("zip",""))]=m
    out=[]
    with open(ep,newline="",encoding="utf-8") as f:
        for r in csv.DictReader(f):
            key=_n(r.get("Street Address","")+r.get("Zip","")); meta=mba.get(key,{}); ph=[]; em=[]
            for c,v in r.items():
                if v and any(p in c.lower() for p in ["phone","mobile","cell","landline"]):
                    cl=re.sub(r"[^\d]","",v)
                    if len(cl)>=10: ph.append(cl)
                if v and "email" in c.lower() and "@" in v: em.append(v.strip().lower())
            ph=list(dict.fromkeys(ph))[:3]; em=list(dict.fromkeys(em))[:3]
            out.append({**meta,"raw":dict(r),"phones":ph,"emails":em,"has_mobile":bool(ph),"has_email":bool(em),"has_any_contact":bool(ph or em)})
    return out
def import_ghl(enriched,state,batch_id):
    stats={"created":0,"failed":0}; now=datetime.now(timezone.utc).isoformat()
    if "processed_keys" not in state: state["processed_keys"]={}
    for r in enriched:
        k=r.get("record_key",get_key(r))
        try:
            tags=["county-lead","bexar-county","skip-traced"]
            if r.get("has_mobile"): tags.append("has-mobile")
            if r.get("has_email"): tags.append("has-email")
            ph=r.get("phones",[]); em=r.get("emails",[])
            pl={"locationId":GHL_LOCATION_ID,"firstName":r.get("owner_first",""),"lastName":r.get("owner_last","") or r.get("owner","").title(),"name":r.get("owner","").title(),"address1":r.get("street",""),"city":r.get("city",""),"state":r.get("state","TX"),"postalCode":r.get("zip",""),"country":"US","source":"Bexar County Dashboard","tags":list(set(tags))}
            if ph: pl["phone"]=f"+1{ph[0]}" if not ph[0].startswith("+") else ph[0]
            if em: pl["email"]=em[0]
            if DRY_RUN: cid=f"dry_{uuid.uuid4().hex[:8]}"; log.info("[DRY] %s",pl.get("name","?"))
            else:
                r2=requests.post(f"{GHL_API_BASE}/contacts/",headers={"Authorization":f"Bearer {GHL_API_KEY}","Version":"2021-07-28","Content-Type":"application/json"},json=pl,timeout=15)
                cid=r2.json().get("contact",{}).get("id") if r2.status_code in (200,201) else None
            if cid: stats["created"]+=1; state["processed_keys"][k]={"ghl_contact_id":cid,"batch_id":batch_id,"imported_at":now}
            else: stats["failed"]+=1
            time.sleep(0.12)
        except Exception as e: log.error("Err %s: %s",k,e); stats["failed"]+=1
    return stats
def run_pipeline(dry_run=False,step="all"):
    global DRY_RUN; DRY_RUN=dry_run or DRY_RUN
    b=str(uuid.uuid4()); state=load_state(); sts={}; err=""; recs=[]
    log.info("="*60); log.info("Pipeline %s | dry=%s | step=%s",b,DRY_RUN,step); log.info("="*60)
    try:
        if step in ("all","export"):
            recs=select_records(state)
            if not recs: log.info("No new records"); save_state(state); return
            cp=export_csv(recs,b); state["current_batch"]={"batch_id":b,"csv_path":str(cp),"meta_path":str(cp).replace("xleads_export_","xleads_meta_").replace(".csv",".json"),"count":len(recs)}; save_state(state)
        if step in ("all","xleads"):
            bi=state.get("current_batch",{}); cp=pathlib.Path(bi.get("csv_path",""))
            ep=None if DRY_RUN else run_xleads(cp,b)
            if not DRY_RUN and not ep: log.error("XLeads failed"); save_state(state); return
            if ep: state["current_batch"]["enriched_path"]=str(ep); save_state(state)
        if step in ("all","ghl"):
            bi=state.get("current_batch",{}); ep=pathlib.Path(bi.get("enriched_path","")); mp=pathlib.Path(bi.get("meta_path",""))
            enr=parse_enriched(ep,mp)
            if not enr: return
            sts=import_ghl(enr,state,b); log.info("GHL: created=%d failed=%d",sts["created"],sts["failed"])
    except Exception as e: err=str(e); log.error("Pipeline err: %s",e,exc_info=True)
    finally: save_state(state); log.info("Done.")
if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--dry-run",action="store_true"); ap.add_argument("--step",choices=["all","export","xleads","ghl"],default="all"); args=ap.parse_args()
    run_pipeline(dry_run=args.dry_run,step=args.step)
