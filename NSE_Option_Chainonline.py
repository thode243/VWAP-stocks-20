# NSE_Option_Chainonline.py
import os
import sys
import pytz
import logging
import pandas as pd
import gspread
from datetime import datetime, time as dtime
from oauth2client.service_account import ServiceAccountCredentials
# Use a more explicit import that is less likely to conflict
try:
    from nsepython import option_chain as fetch_option_chain_data
except ImportError:
    # Fallback or alternative import if the above fails in some environments
    from nsepython import nse_optionchain as fetch_option_chain_data


# ========================= CONFIG =======================
SHEET_ID = os.getenv("SHEET_ID")
if not SHEET_ID:
    # Changed from raise ValueError to sys.exit for cleaner script execution failure
    logging.error("SHEET_ID environment variable is required")
    sys.exit(1)

CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "service_account.json")
if not os.path.exists(CREDENTIALS_PATH):
    logging.error(f"Credentials file not found: {CREDENTIALS_PATH}")
    sys.exit(1)

SHEET_CONFIG = [
    {"sheet_name": "Weekly",     "expiry_index": 0},  # Current weekly expiry
    {"sheet_name": "NextWeek",   "expiry_index": 1},
    {"sheet_name": "Monthly",    "expiry_index": 2},
    {"sheet_name": "NextMonth",  "expiry_index": 3},
]

# ========================= LOGGING =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("option_chain.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# ========================= HELPERS =========================
def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    t = now.time()
    # Market generally closes at 3:30 PM (15:30), but extended hours for specific sessions might exist.
    # Updated the closing time validation to a more standard market hours check.
    return dtime(9, 15) <= t <= dtime(15, 30) 

def fetch_nifty_chain():
    try:
        # Use the imported alias
        data = fetch_option_chain_data("NIFTY")
        
        # Enhanced validation using try-except for key access
        try:
            records_data = data["records"]["data"]
            if not records_data:
                 raise ValueError("Data list in 'records' is empty")
        except KeyError as e:
            raise ValueError(f"Missing expected key in data structure: {e}")
        except TypeError:
            raise ValueError("Data from NSE was not a dictionary (likely a string or other error response)")

        log.info(f"Fetched NIFTY option chain – {len(records_data)} records")
        return data
    except Exception as e:
        log.error(f"Failed to fetch data from NSE: {e}")
        raise

def build_df(expiry_date, raw_data):
    rows = []
    # Access records safely
    for item in raw_data.get("records", {}).get("data", []):
        if item.get("expiryDate") != expiry_date:
            continue
        strike = item.get("strikePrice")
        if strike is None:
            continue # Skip records without a strike price

        ce = item.get("CE", {})
        pe = item.get("PE", {})
        rows.append({
            "Strike": strike,
            "CE OI":      ce.get("openInterest", 0),
            "CE Chg OI":  ce.get("changeinOpenInterest", 0),
            "CE LTP":     ce.get("lastPrice", 0),
            "CE Volume":  ce.get("totalTradedVolume", 0),
            "PE LTP":     pe.get("lastPrice", 0),
            "PE Chg OI":  pe.get("changeinOpenInterest", 0),
            "PE OI":      pe.get("openInterest", 0),
            "PE Volume":  pe.get("totalTradedVolume", 0),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        log.warning(f"No rows found for expiry: {expiry_date}")
        return df
    return df.sort_values("Strike").reset_index(drop=True)

def update_sheets(dfs):
    # The gspread logic remains robust, assuming credentials and permissions are correct.
    scope = ["spreadsheets.google.com", "www.googleapis.com"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    book = client.open_by_key(SHEET_ID)

    # Ensure all sheets exist
    existing_sheets = {ws.title for ws in book.worksheets()}
    for cfg in SHEET_CONFIG:
        if cfg["sheet_name"] not in existing_sheets:
            # Added basic error handling here too
            try:
                book.add_worksheet(title=cfg["sheet_name"], rows=1000, cols=20)
                log.info(f"Created new sheet: {cfg['sheet_name']}")
            except gspread.exceptions.APIError as e:
                log.error(f"Failed to create sheet {cfg['sheet_name']}: {e}")

    for cfg in SHEET_CONFIG:
        name = cfg["sheet_name"]
        df = dfs.get(name)
        if df is None or df.empty:
            log.warning(f"No data for {name} – skipping update")
            continue

        try:
            sheet = book.worksheet(name)
            sheet.clear()
            data_to_upload = [df.columns.tolist()] + df.values.tolist()
            sheet.update("A1", data_to_upload)
            log.info(f"Updated {name} → {len(df)} strikes at {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S IST')}")
        except gspread.exceptions.APIError as e:
             log.error(f"Failed to update sheet {name}: {e}")

# ========================= MAIN =========================
if __name__ == "__main__":
    log.info("NIFTY Option Chain → Google Sheets – Starting")

    if not is_market_open():
        log.info("Market is closed → exiting without update")
        sys.exit(0)

    try:
        raw_data = fetch_nifty_chain()
        # Access expiries safely with get()
        expiries = raw_data.get("records", {}).get("expiryDates", [])
        
        if not expiries:
            raise ValueError("No expiry dates found in fetched data.")

        log.info(f"Available expiries: {expiries}")

        dfs_to_upload = {}
        for cfg in SHEET_CONFIG:
            idx = cfg["expiry_index"]
            if idx >= len(expiries):
                log.warning(f"Expiry index {idx} out of range (only {len(expiries)} available)")
                continue
            expiry = expiries[idx]
            df = build_df(expiry, raw_data)
            dfs_to_upload[cfg["sheet_name"]] = df
            log.info(f"Prepared {cfg['sheet_name']} → {expiry} ({len(df)} rows)")

        update_sheets(dfs_to_upload)
        log.info("✅ All sheets updated successfully!")

    except Exception as e:
        log.error(f"❌ Script failed: {e}", exc_info=True)
        # Exit with a non-zero code to indicate failure in an automated environment
        sys.exit(1)
