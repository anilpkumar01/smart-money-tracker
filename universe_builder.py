# ============================================================
# universe_builder.py  — v2
# Handles the ACTUAL screener.in export format:
#   - latin-1 encoding (not utf-8) — this was the crash
#   - No ticker column — derives NSE symbol from name
#   - No sector column — infers from name keywords
#   - Columns: S.No., Name, CMP Rs., Mar Cap Rs.Cr.,
#              P/E, CMP/BV, ROE %, ROCE %, OPM % etc.
# ============================================================

import re, sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import Counter
from database import init_db, upsert_universe, get_universe

EXPORT_PATH = Path("data/screener_export.csv")

# ── Known name → NSE ticker (handles screener's truncated names) ──────────────
KNOWN_TICKERS = {
    "accelya solution":"ACCELYA","action const.eq.":"ACE","aeroflex":"AEROFLEX",
    "afcons infrastr.":"AFCONS","agi infra":"AGIIL","allied blenders":"ABDL",
    "alpex solar":"ALPEXSOLAR","amrutanjan healt":"AMRUTANJAN",
    "anand rathi wea.":"ANANDRATHI","ashoka buildcon":"ASHOKA",
    "atlanta electric":"ATLANT","automotive axles":"AUTOAXLES",
    "birlasoft ltd":"BIRLASOFT","blue jet health":"BLUEJET",
    "c d s l":"CDSL","cams services":"CAMS","canara robeco":"CANAROB",
    "carraro india":"CARRARO","castrol india":"CASTROLIND",
    "cera sanitary.":"CERA","cff fluid":"CFFFLUID","crizac":"CRIZAC",
    "d b corp":"DBCORP","dam capital advi":"DAMCAP","danish power":"DANISHPOWER",
    "data pattern":"DATAPATTNS","elantas beck":"ELANTAS",
    "elgi equipments":"ELGIEQUIP","elecon engg.co":"ELECON","esab india":"ESAB",
    "expleo solutions":"EXPLEOSOL","frontier springs":"FRONTIER",
    "ganesh housing":"GANESHHOUC","gk energy":"GKENERGY","gillette india":"GILLETTE",
    "glaxosmi. pharma":"GLAXO","godawari power":"GPIL","gravita india":"GRAVITA",
    "grindwell norton":"GRINDWELL","hawkins cookers":"HAWKINCOOK",
    "himadri special":"HIMATSEIDE","hind rectifiers":"HINDRECTIF",
    "i r c t c":"IRCTC","icra":"ICRA","indian energy ex":"IEX",
    "indian metals":"IMFA","ingersoll-rand":"INGERRAND","ion exchange":"IONEXCHANG",
    "jagsonpal pharma":"JAGSONPAL","jeena sikho":"JEENAPOL",
    "jyoti resins":"JYOTIRESNS","jsw dulux":"JSWDULUX","jupiter wagons":"JWL",
    "k.p. energy":"KPENERGY","kei industries":"KEI","kilburn engg.":"KILBCHENIN",
    "kpit technologi.":"KPITTECH","kovai medical":"KOVAI","kross ltd":"KROSS",
    "magellanic cloud":"MAGELANIC","mahanagar gas":"MGL",
    "manorama indust.":"MANORG","meghna infracon":"MEGHNA","midwest":"MIDWEST",
    "motherson wiring":"MOTHERWIRE","mphasis":"MPHASIS","mps":"MPSLTD",
    "ncc":"NCC","ndr auto compon.":"NDRAUTO","network people":"NPST",
    "nesco":"NESCO","nucleus soft.":"NUCLEUS",
    "one global serv":"ONEGLOBAL","oriana power ltd":"ORIANAPOWER",
    "oswal pumps":"OSWALPUMPS",
    "p & g health ltd":"PGHH","p & g hygiene":"PGHH","p i industries":"PIIND",
    "pace digitek":"PACEDIGITK","page industries":"PAGEIND",
    "pearl global ind":"PEARLGLOBAL","pfizer":"PFIZER",
    "piccadily agro":"PICCADILY","pngs reva diamo.":"PNGSREVA",
    "power mech proj.":"POWERMECH","powerica ltd":"POWERICA",
    "premier energies":"PREMIENERGY","pricol ltd":"PRICOLLTD",
    "prudent corp.":"PRUDENT",
    "r systems intl.":"RSYSTEMS","railtel corpn.":"RAILTEL",
    "rajesh power":"RAJESHPOW","ratnamani metals":"RATNAMANI",
    "redtape":"REDTAPE","rites":"RITES","rolex rings":"ROLEXRINGS",
    "rrp defense":"RRPD","rrpsemiconductor":"RRPS",
    "saatvik green":"SAATVIKG","safe enterprises":"SAFEENT",
    "sanofi consumer":"SANOFI","sanofi india":"SANOFI",
    "sathlokhar sys.":"SATHLOKHAR","schneider elect.":"SCHNEIDER",
    "shakti pumps":"SHAKTIPUMP","shilchar tech.":"SHILCTECH",
    "shukra pharma.":"SHUKRAPHARM","silver touch":"SILVERTOUCH",
    "sjs enterprises":"SJS","sky gold & diam.":"SKYGOLD",
    "solarworld ene.":"SOLARWORLD","studds accessor.":"STUDDS",
    "supreme inds.":"SUPREMEIND","swaraj engines":"SWARAJENG",
    "t r i l":"TRIL","tac infosec":"TACINFOSEC","tanfac inds.":"TANFAC",
    "tata tele. mah.":"TTML","tenneco clean":"TENNECO",
    "thejo engg.":"THEJO","timex group":"TIMEXIND","timken india":"TIMKEN",
    "tips music":"TIPSMUSIC","travel food":"TRAVELFOOD",
    "triveni turbine":"TRITURBINE",
    "unimech aero.":"UNIMECHAERO",
    "valiant commun.":"VALIANTCOMM","vesuvius india":"VESUVIUS",
    "vijaya diagnost.":"VIJAYA","vilas transcore":"VILASTRANS",
    "waaree renewab.":"WAAREEENER","websol energy":"WEBELSOLAR",
    "welspun corp":"WELCORP","wework india":"WEWORK",
    "anand rathi":"ANANDRATHI","international ge":"INDIGRID",
    "efc (i)":"EFCIND","ndp auto compon.":"NDRAUTO",
    "bombay super hyb":"BSH","accelya solution":"ACCELYA",
    "euro pratik sale":"EUROPRATIK","tanfac inds.":"TANFAC",
}

# ── Sector inference from name ────────────────────────────────────────────────
NAME_SECTOR = [
    (["pharma","medic","drug"],           "Pharmaceuticals"),
    (["health","hospital","diagnost"],    "Healthcare"),
    (["bank"],                            "Banking"),
    (["financ","nbfc","capital","wealth",
      "robeco","prudent","rathi"],        "Finance"),
    (["housing"],                         "Finance"),
    (["software","tech","infosy","digit",
      "infosec","system","solution"],     "IT"),
    (["auto","motor","wagon","axle","tyre"],"Automobiles"),
    (["chemical","special chem"],         "Chemicals"),
    (["solar","power","energy","electric",
      "renew","turbine"],                 "Power"),
    (["cement"],                          "Cement"),
    (["steel","metal","copper","alumin",
      "iron"],                            "Metals"),
    (["fmcg","consumer","hygiene","food",
      "agro","beverag"],                  "FMCG"),
    (["realty","housing","infracon",
      "construct","infra","build"],       "Real Estate"),
    (["hotel","travel","restaurant"],     "Hotels"),
    (["pump","engg","engineer","equip",
      "machin"],                          "Engineering"),
    (["defence","defense","aero"],        "Defence"),
    (["media","music","broadcast"],       "Media"),
    (["telecom","tele"],                  "Telecom"),
    (["logist","transport","cargo"],      "Logistics"),
]

def _infer_sector(name: str) -> str:
    n = name.lower()
    for keywords, sector in NAME_SECTOR:
        if any(k in n for k in keywords):
            return sector
    return "Miscellaneous"


def _derive_ticker(name: str) -> str:
    key = name.strip().lower()
    # Exact known map
    if key in KNOWN_TICKERS:
        return KNOWN_TICKERS[key].upper() + ".NS"
    # Partial match — screener truncates names to ~16 chars
    for k, v in KNOWN_TICKERS.items():
        if len(k) >= 6 and (key.startswith(k[:6]) or k.startswith(key[:6])):
            return v.upper() + ".NS"
    # Derive: strip noise, take first 10 alphanum chars
    clean = re.sub(r"\b(ltd|limited|india|pvt|private|corp|inc|co|and|the)\b",
                   "", key, flags=re.IGNORECASE)
    clean = re.sub(r"[^a-z0-9]", "", clean).upper()[:10]
    return (clean or re.sub(r"[^A-Z0-9]","", name.upper())[:10]) + ".NS"


def _parse_num(val) -> float:
    if pd.isna(val): return 0.0
    try: return float(str(val).replace(",","").replace("%","").strip())
    except: return 0.0


def build_from_csv(csv_path: Path = EXPORT_PATH,
                   min_mcap: float = 1000,
                   max_mcap: float = 30000) -> int:

    if not csv_path.exists():
        print(f"\n[ERROR] File not found: {csv_path}")
        print("Steps:")
        print("  1. Go to https://www.screener.in/explore/")
        print("  2. Query: Market Capitalization > 1000 AND Market Capitalization < 30000")
        print("  3. Click 'Export to Excel' (top-right)")
        print(f"  4. Save as: {csv_path.absolute()}")
        sys.exit(1)

    print(f"\nReading: {csv_path}")

    # KEY FIX: screener.in uses latin-1 encoding, NOT utf-8
    df = pd.read_csv(csv_path, encoding="latin-1", on_bad_lines="skip")

    # Clean column names — remove \xa0 non-breaking spaces
    df.columns = [re.sub(r"[\xa0\r\n\t]+", " ", c).strip() for c in df.columns]
    print(f"Columns: {list(df.columns)}")
    print(f"Rows: {len(df)}")

    # Flexible column detection
    col_name  = next((c for c in df.columns if c.lower().strip() == "name"), None)
    col_mcap  = next((c for c in df.columns if "mar cap" in c.lower()), None)
    col_pe    = next((c for c in df.columns if c.strip().lower() in ("p/e","pe")), None)
    col_pb    = next((c for c in df.columns if "cmp / bv" in c.lower() or "p/b" in c.lower()), None)
    col_roe   = next((c for c in df.columns if c.strip().lower().startswith("roe %")), None)
    col_roce  = next((c for c in df.columns if "roce" in c.lower()), None)

    if not col_name or not col_mcap:
        print(f"[ERROR] Missing required columns. Found: {list(df.columns)}")
        sys.exit(1)

    print(f"\nMapped: Name={col_name} | MCap={col_mcap} | PE={col_pe} | PB={col_pb} | RoE={col_roe}")

    # Filter market cap
    df["_mcap"] = df[col_mcap].apply(_parse_num)
    df = df[(df["_mcap"] >= min_mcap) & (df["_mcap"] <= max_mcap)].copy()
    print(f"After filter ({min_mcap}–{max_mcap} Cr): {len(df)} stocks")

    # Build rows
    now, rows, seen = datetime.now().isoformat(), [], set()

    for _, row in df.iterrows():
        name = str(row[col_name]).strip()
        if not name or name.lower() == "nan":
            continue

        ticker      = _derive_ticker(name)
        screener_id = ticker.replace(".NS","").replace(".BO","")

        # Handle rare duplicates
        base = ticker
        n = 1
        while ticker in seen:
            ticker = base.replace(".NS", f"_{n}.NS")
            n += 1
        seen.add(ticker)

        rows.append({
            "ticker":        ticker,
            "name":          name,
            "sector":        _infer_sector(name),
            "screener_id":   screener_id,
            "market_cap_cr": _parse_num(row["_mcap"]),
            "pe":            _parse_num(row[col_pe])   if col_pe   else 0.0,
            "pb":            _parse_num(row[col_pb])   if col_pb   else 0.0,
            "roe":           _parse_num(row[col_roe])  if col_roe  else 0.0,
            "added_on":      now,
        })

    upsert_universe(rows)

    print(f"\n[Done] {len(rows)} stocks loaded into universe.")
    print("\nSector breakdown:")
    for sector, count in sorted(Counter(r["sector"] for r in rows).items(), key=lambda x: -x[1]):
        print(f"  {sector:<25} {count}")

    return len(rows)


def print_summary():
    df = get_universe()
    if df.empty:
        print("Universe empty."); return
    print(f"\nUniverse: {len(df)} stocks")
    print(df[["name","sector","market_cap_cr","pe","roe"]]
          .sort_values("market_cap_cr", ascending=False)
          .head(15).to_string(index=False))


if __name__ == "__main__":
    init_db()
    build_from_csv()
    print_summary()
    print("\nNext step: streamlit run app.py → click 'Refresh New' in sidebar")
