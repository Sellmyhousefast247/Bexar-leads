#!/usr/bin/env python3
"""One-time v2 patcher: restores the proven v1 clerk-scraper engine from git
history and grafts on the v2 pipeline (hash/dedupe identity, NEW/CHANGED
state tracking, dashboard field mapping, skip-trace CSV). Idempotent: no-op
once scraper/fetch.py already contains the v2 pipeline."""
import json, sys, urllib.request
from pathlib import Path

TARGET = Path(__file__).parent / "fetch.py"
V1_URL = "https://raw.githubusercontent.com/Sellmyhousefast247/Bexar-leads/f05e761/scraper/fetch.py"
SUBS = json.loads(Path(__file__).with_name("v2_patch_data.json").read_text(encoding="utf-8"))

def main():
    cur = TARGET.read_text(encoding="utf-8") if TARGET.exists() else ""
    if "CLERK_RESULTS" in cur and "detect_changes" in cur:
        print("fetch.py already v2 - nothing to do"); return
    src = urllib.request.urlopen(V1_URL, timeout=60).read().decode("utf-8")
    for old, new in SUBS:
        if src.count(old) != 1:
            print("PATCH ANCHOR FAILED:", old[:60]); sys.exit(1)
        src = src.replace(old, new)
    import ast; ast.parse(src)
    TARGET.write_text(src, encoding="utf-8")
    print("fetch.py rebuilt:", len(src), "bytes")

if __name__ == "__main__":
    main()
