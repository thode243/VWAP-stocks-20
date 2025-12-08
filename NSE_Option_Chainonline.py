import os
import sys
import pytz
import logging
import gspread
import pandas as pd
from time import sleep
from datetime import datetime, time as dtime
from oauth2client.service_account import ServiceAccountCredentials
from nselib.derivatives import nse_optionchain_scrape


# ========= CONFIG ========
SHEET_ID = os.getenv("SHEET_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))

SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3}
]


# ========= LOGGING =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ========= FETCH OPTION CHAIN =========
def fetch_option_chain(index):
    """Fetch option chain using NSELIB (NO NSE WEBSITE REQUESTS)."""
    try:
        data = get_option_chain(index)
        return data
    except Exception as e:
        logger.error(f"Failed to fetch option chain for {index}: {e}")
        raise


def extract_by_expiry(data, expiry):
    rows = []
    for item in data["records"]["data"]:
        if item.get("expiryDate") != expiry:
            continue
        rows.append({
            "CE OI": item.get("CE", {}).get("openInterest", 0),
            "CE Chng OI": item.get("CE", {}).get("changeinOpenInterest", 0),
            "CE LTP": item.get("CE", {}).get("lastPrice", 0),
            "Strike Price": item.get("strikePrice", 0),
            "Expiry Date": expiry,
            "PE LTP": item.get("PE", {}).get("lastPrice", 0),
            "PE Chng OI": item.get("PE", {}).get("changeinOpenInterest", 0),
            "PE OI": item.get("PE", {}).get("openInterest", 0),
        })
    return pd.DataFrame(rows)


# ========= UPDATE GOOGLE SHEET =========
def update_google_sheet(sheet_dfs):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_PATH, 
        ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SHEET_ID)

    for cfg in SHEET_CONFIG:
        sheet_name = cfg["sheet_name"]
        df = sheet_dfs[sheet_name]

        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()

        data = [df.columns.tolist()] + df.values.tolist()
        ws.update("A1", data)

        logger.info(f"Updated {sheet_name} ({len(df)} rows)")


# ========= MARKET TIME CHECK =========
def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist).time()
    return dtime(9, 10) <= now <= dtime(15, 30)


# ========= MAIN =========
if __name__ == "__main__":
    logger.info("Starting NSE Option Chain Updater (using NSELIB)...")

    while True:
        try:
            if not is_market_open():
                logger.info("Market closed, skipping...")
                sleep(POLLING_INTERVAL_SECONDS)
                continue

            sheet_dfs = {}
            nifty_data = fetch_option_chain("NIFTY")

            expiries = nifty_data["expiryDates"]

            for cfg in SHEET_CONFIG:
                expiry = expiries[cfg["expiry_index"]]
                df = extract_by_expiry(nifty_data, expiry)
                sheet_dfs[cfg["sheet_name"]] = df

            update_google_sheet(sheet_dfs)

        except Exception as e:
            logger.error(f"Error: {e}")

        sleep(POLLING_INTERVAL_SECONDS)
