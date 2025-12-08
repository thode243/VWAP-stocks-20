# File: NIFTY_09Dec2025_Only.py   ← Save this name

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from nsepython import nse_optionchain_scrapper   # This one works best in Dec 2025
import logging
from time import sleep
from datetime import datetime, time
import pytz

# ========================= CONFIG =========================
SHEET_ID = "17YLthNpsymBOeDkBbRkb3eC8A3KZnKTXz4cSAGScx88"   # Your sheet
CREDS = r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"

TARGET_EXPIRY = "09-Dec-2025"        # ← FORCE THIS EXPIRY ONLY
SHEET_NAME = "Sheet1"                # Will update only this tab
# =========================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger()

def fetch_data():
    try:
        raw = nse_optionchain_scrapper("NIFTY")        # This function is currently the most reliable
        data = raw["records"]["data"]
        expiries = raw["records"]["expiryDates"]

        if TARGET_EXPIRY not in expiries:
            logger.warning(f"{TARGET_EXPIRY} not available today. Available: {expiries[:5]}")
            return None

        rows = []
        for item in data:
            if item.get("expiryDate") != TARGET_EXPIRY:
                continue
            strike = item["strikePrice"]
            CE = item.get("CE", {})
            PE = item.get("PE", {})

            rows.append({
                "Strike": int(strike),
                "CE OI": CE.get("openInterest", 0),
                "CE Chg OI": CE.get("changeinOpenInterest", 0),
                "CE LTP": CE.get("lastPrice", 0),
                "CE Vol": CE.get("totalTradedVolume", 0),
                "PE LTP": PE.get("lastPrice", 0),
                "PE Vol": PE.get("totalTradedVolume", 0),
                "PE Chg OI": PE.get("changeinOpenInterest", 0),
                "PE OI": PE.get("openInterest", 0),
            })

        df = pd.DataFrame(rows).sort_values("Strike").reset_index(drop=True)
        logger.info(f"Fetched {len(df)} strikes for NIFTY {TARGET_EXPIRY}")
        return df

    except Exception as e:
        logger.error(f"Fetch failed: {e}")
        return None

def update_sheet(df):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(S_ID)

    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=15)

    ws.clear()
    ws.update("A1", [df.columns.tolist()] + df.values.tolist())
    logger.info(f"Updated Google Sheet → {len(df)} rows @ {datetime.now(pytz.timezone('Asia/Kolkata'))}")

def market_open():
    ist = datetime.now(pytz.timezone("Asia/Kolkata"))
    return ist.weekday() < 5 and time(9, 15) <= ist.time() <= time(15, 30)

# ========================= MAIN =========================
if __name__ == "__main__":
    logger.info("NIFTY 09-Dec-2025 Live Updater Started")
    while True:
        if market_open():
            df = fetch_data()
            if df is not None:
                update_sheet(df)
        else:
            logger.info("Market closed / pre-open — waiting...")
        sleep(30)        # Updates every 30 seconds during market hours
