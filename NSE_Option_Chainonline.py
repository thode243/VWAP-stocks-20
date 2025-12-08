# nifty_option_chain.py
import os
import sys
import pytz
import logging
import pandas as pd
import gspread
from datetime import datetime, time as dtime
from oauth2client.service_account import ServiceAccountCredentials
from unofficed import NseIndia  # This bypasses all NSE blocks forever

# ========================= CONFIG =========================
SHEET_ID = os.getenv("SHEET_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "service_account.json")

SHEET_CONFIG = [
    {"sheet_name": "Weekly",   "expiry_index": 0},   },  # Current week
    {"sheet_name": "NextWeek", "expiry_index": 1   },
    {"sheet_name": "Monthly",  "expiry_index": 2   },
    {"sheet_name": "NextMonth","expiry_index": 3   },
]

# ========================= LOGGING =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("option_chain.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# ========================= NSE Instance (Playwright under the hood – undefeated)
nse = NseIndia()

# ========================= HELPERS =========================
def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    t = now.time()
    d = now.date()
    if d.weekday() >= 5:  # Sat/Sun
        return False
    return dtime(9, 15) <= t <= dtime(15, 30)

def get_nifty_option_chain():
    try:
        data = nse.option_chain("NIFTY")
        log.info(f"Fetched full NIFTY chain | Strikes: {len(data['records']['data'])}")
        return data
    except Exception as e:
        log.error(f"NSE fetch failed: {e}")
        raise

def build_df_for_expiry(raw_data, expiry_date):
    rows = []
    for item: dict
    for item in raw_data["records"]["data"]:
        if item.get("expiryDate") != expiry_date:
            continue
        strike = item["strikePrice"]
        ce = item.get("CE", {})
        pe = item.get("PE", {})
        rows.append({
            "Strike": strike,
            "CE OI": ce.get("openInterest", 0),
            "CE Chng OI": ce.get("changeinOpenInterest", 0),
            "CE LTP": ce.get("lastPrice", 0),
            "CE Vol": ce.get("totalTradedVolume", 0),
            "PE LTP": pe.get("lastPrice", 0),
            "PE Chng OI": pe.get("changeinOpenInterest", 0),
            "PE OI": pe.get("openInterest", 0),
            "PE Vol": pe.get("totalTradedVolume", 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Strike").reset_index(drop=True)

def update_google_sheets(dfs_dict):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)

    for cfg in SHEET_CONFIG:
        sheet_name = cfg["sheet_name"]
        df = dfs_dict.get(sheet_name)
        if df is None or df.empty:
            log.warning(f"No data for {sheet_name}")
            continue

        try:
            ws = sh.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=20)

        ws.clear()
        ws.update("A1", [df.columns.tolist()] + df.values.tolist() )
        log.info(f"Updated → {sheet_name} | {len(df)} strikes | {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}")

# ========================= MAIN =========================
if __name__ == "__main__":
    log.info("NIFTY Option Chain → Google Sheets | Starting...")

    if not is_market_open():
        log.info("Market is closed right now. Exiting peacefully.")
        sys.exit(0)

    try:
        raw = get_nifty_option_chain()
        expiries = raw["records"]["expiryDates"]

        dfs = {}
        for cfg in SHEET_CONFIG:
            idx = cfg["expiry_index"]
            if idx >= len(expiries):
                log.warning(f"Expiry index {idx} not available")
                continue
            expiry = expiries[idx]
            df = build_df_for_expiry(raw, expiry)
            dfs[cfg["sheet_name"]] = df
            log.info(f"Built {cfg['sheet_name']} → {expiry} | {len(df)} strikes")

        update_google_sheets(dfs)
        log.info("All sheets updated successfully!")

    except Exception as e:
        log.error(f"Script failed: {e}")
        sys.exit(1)
