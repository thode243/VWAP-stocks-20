import requests
import pandas as pd
import gspread
import os
from time import sleep
from datetime import datetime, date
from datetime import time as dtime
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys

# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")

SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3},
    {"sheet_name": "Sheet5", "index": "BANKNIFTY", "expiry_index": None},
    # {"sheet_name": "Sheet6", "index": "MIDCPNIFTY", "expiry_index": None},
    # {"sheet_name": "Sheet7", "index": "FINNIFTY", "expiry_index": None},
]

POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))

CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"
)

BASE_URL = "https://www.nseindia.com"
OPTION_CHAIN_URL = f"{BASE_URL}/api/option-chain-indices?symbol={{index}}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{BASE_URL}/option-chain",
}

# ===== LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(stream=sys.stdout),
        logging.FileHandler("option_chain_updater.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)

# ===== FUNCTIONS =====

def create_session():
    """Create a requests session with retry logic and cookie initialization."""
    session = requests.Session()

    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        response = session.get(f"{BASE_URL}/option-chain", headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch cookies: {e}")
        raise

    return session


def get_latest_expiries(session, index, num_expiries=4):
    """Fetch next expiries for an index."""
    try:
        url = OPTION_CHAIN_URL.format(index=index)
        response = session.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        data = response.json()
        expiries = data.get("records", {}).get("expiryDates", [])

        if not expiries:
            raise ValueError(f"No expiry dates found for {index}")

        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist).date()

        valid = []
        for expiry in expiries:
            try:
                d = datetime.strptime(expiry, "%d-%b-%Y").date()
                if d >= today:
                    valid.append((d, expiry))
            except:
                continue

        valid.sort(key=lambda x: x[0])

        return [e[1] for e in valid[:num_expiries]]

    except Exception as e:
        logger.error(f"Failed to get expiries for {index}: {e}")
        raise


def fetch_option_chain():
    """Fetch & prepare option chain data."""
    try:
        session = create_session()
        sleep(1)

        indices = list({cfg["index"] for cfg in SHEET_CONFIG})
        expiry_map = {}

        # fetch expiries
        for index in indices:
            expiry_map[index] = get_latest_expiries(
                session, index,
                num_expiries=4 if index == "NIFTY" else 1
            )

        sheet_dfs = {cfg["sheet_name"]: None for cfg in SHEET_CONFIG}

        # fetch option chain data
        for index in indices:
            url = OPTION_CHAIN_URL.format(index=index)
            resp = session.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("records", {}).get("data", [])
            expiry_dfs = {}

            for row in items:
                expiry = row.get("expiryDate")
                if expiry not in expiry_map[index]:
                    continue

                strike = row.get("strikePrice")
                ce = row.get("CE", {})
                pe = row.get("PE", {})

                entry = {
                    "CE OI": ce.get("openInterest", 0),
                    "CE Chng OI": ce.get("changeinOpenInterest", 0),
                    "CE LTP": ce.get("lastPrice", 0),
                    "Strike Price": strike,
                    "Expiry Date": expiry,
                    "PE LTP": pe.get("lastPrice", 0),
                    "PE Chng OI": pe.get("changeinOpenInterest", 0),
                    "PE OI": pe.get("openInterest", 0),
                }

                expiry_dfs.setdefault(expiry, []).append(entry)

            for cfg in SHEET_CONFIG:
                if cfg["index"] != index:
                    continue

                expiry_index = cfg["expiry_index"]

                if expiry_index is not None:  # NIFTY sheets
                    expiry = expiry_map[index][expiry_index]
                else:  # BANKNIFTY etc.
                    expiry = expiry_map[index][0]

                if expiry in expiry_dfs:
                    sheet_dfs[cfg["sheet_name"]] = pd.DataFrame(expiry_dfs[expiry])

        return sheet_dfs

    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        raise


def update_google_sheet(dfs):
    """Write DataFrames to Google Sheets."""
    try:
        if not os.path.exists(CREDENTIALS_PATH):
            raise FileNotFoundError(f"Credentials not found: {CREDENTIALS_PATH}")

        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            CREDENTIALS_PATH, scope
        )
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(SHEET_ID)
        existing = {ws.title for ws in spreadsheet.worksheets()}

        # create missing sheets
        for cfg in SHEET_CONFIG:
            if cfg["sheet_name"] not in existing:
                spreadsheet.add_worksheet(cfg["sheet_name"], rows="200", cols="20")

        # update sheets
        for cfg in SHEET_CONFIG:
            name = cfg["sheet_name"]
            df = dfs.get(name)

            if df is None or df.empty:
                logger.warning(f"Skipping empty sheet: {name}")
                continue

            ws = spreadsheet.worksheet(name)
            ws.clear()

            data = [df.columns.tolist()] + df.values.tolist()
            ws.update("A1", data)

            logger.info(f"Updated {name} with {len(df)} rows")

    except Exception as e:
        logger.error(f"Sheet update failed: {e}")
        raise


def is_market_open():
    """Check NSE trading hours."""
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    t = now.time()

    start = dtime(9, 15)
    end = dtime(15, 30)

    return now.weekday() < 5 and start <= t <= end


# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("Starting updater...")

    while True:
        try:
            if is_market_open():
                dfs = fetch_option_chain()
                update_google_sheet(dfs)
            else:
                logger.info("Market closed, skipping.")

            sleep(POLLING_INTERVAL_SECONDS)

        except Exception as e:
            logger.error(f"Error: {e}")
            sleep(POLLING_INTERVAL_SECONDS)
