Bexar County Motivated Seller Lead Scraper
Automated daily scraper that pulls distressed-property filings from the Bexar County Clerk portal, enriches them with parcel data, scores each lead, and exports GHL-ready CSV files.
Data sources
Source	URL	Data
Clerk portal	https://bexar.tx.publicsearch.us/	Lis pendens, foreclosure notices, judgments, liens, probate
Foreclosure GIS	https://maps.bexar.org/foreclosures/	Current-month mortgage & tax foreclosure notices
Parcel API	https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0	Owner name → property/mailing address
BCAD search	https://esearch.bcad.org/	Property appraisal data (manual lookup)
Lead types collected
(LP) Lis Pendens
(NOFC) Notice of Foreclosure / Notice of Trustee Sale
(TAXDEED) Tax Deed / Tax Sale
(JUD / CCJ / DRJUD) Judgments (Abstract, Certified, Domestic)
(LNCORPTX / LNIRS / LNFED) Tax Liens (Corp, IRS, Federal, State)
(LN / LNMECH / LNHOA) Liens (General, Mechanic, HOA)
(MEDLN) Medicaid / Hospital Liens
(PRO) Probate / Letters Testamentary / Affidavit of Heirship
(NOC) Notice of Commencement
(RELLP) Release of Lis Pendens
Seller score (0–100)
Condition	Points
Base score	30
Per distress flag	+10
LP + Foreclosure combo	+20
Amount > $100k	+15
Amount > $50k	+10
New this week	+5
Has property address	+5
Setup
1. Clone and install
```bash
git clone https://github.com/YOUR_USER/bexar-leads.git
cd bexar-leads
pip install -r scraper/requirements.txt
```
2. Run locally
```bash
# Default 7-day lookback
python scraper/fetch.py

# Custom lookback
python scraper/fetch.py --days 14

# Skip parcel enrichment (faster, no address lookup)
python scraper/fetch.py --skip-parcel
```
3. GitHub Actions (automated daily)
Push to GitHub and the workflow runs daily at 7 AM CST. You can also trigger manually from the Actions tab.
Enable GitHub Pages on the `gh-pages` branch to get a live dashboard.
4. Import to GoHighLevel
The scraper exports `data/ghl_export.csv` with columns mapped to GHL contact fields. Import directly into your CRM pipeline.
File structure
```
├── .github/workflows/scrape.yml   # Daily cron + GitHub Pages deploy
├── scraper/
│   ├── fetch.py                   # Main scraper script
│   └── requirements.txt           # Python dependencies
├── dashboard/
│   ├── index.html                 # Lead dashboard (deployed to GH Pages)
│   └── records.json               # Latest scrape results
├── data/
│   ├── records.json               # Latest scrape results (backup)
│   └── ghl_export.csv             # GHL-importable CSV
└── README.md
```
