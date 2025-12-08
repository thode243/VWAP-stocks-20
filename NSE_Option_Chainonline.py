import os
import sys
import uuid
import pytz
import logging
import requests
import gspread
import pandas as pd
from time import sleep
from datetime import datetime, date, time as dtime
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ===== CONFIG =====
SHEET_ID = os.getenv(
    "SHEET_ID",
    "17YLthNpsymBOeDkBbRkb3eC8A3KZnKTXz4cSAGScx88"
)

SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3},
    {"sheet_name": "Sheet5", "index": "BANKNIFTY", "expiry_index": None}
]

POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))

CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5"
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
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ===== Logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("option_chain_updater.log", encoding="utf-8"),
    ],
)

class UnicodeSafeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            msg = msg.encode("ascii", errors="replace").decode("ascii")
            self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

logger = logging.getLogger(__name__)
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        logger.removeHandler(handler)
logger.addHandler(UnicodeSafeStreamHandler(stream=sys.stdout))


# ===== FUNCTIONS =====

def create_session():
    session = requests.Session()

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/15.5 Safari/605.1.15"
    ]

    import random
    ua = random.choice(USER_AGENTS)

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Host": "www.nseindia.com",
        "Referer": "https://www.nseindia.com/option-chain",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

    session.headers.update(headers)

    # --- very important retries ---
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # ---- try homepage 5 times ----
    for _ in range(5):
        try:
            r = session.get("https://www.nseindia.com", timeout=10)
            if r.status_code == 200:
                return session
        except:
            pass
        sleep(1)

    raise Exception("Unable to load NSE homepage even after retries")




def get_latest_expiries(session, index, num_expiries=4):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={index}"
    
    r = session.get(url, timeout=10)
    if r.status_code != 200:
        raise Exception("NSE returned no option-chain data")

    data = r.json()

    expiries = data.get("records", {}).get("expiryDates", [])
    if not expiries:
        raise Exception(f"No expiry dates returned for {index}")

    # Sort expiries to closest first
    def parse_date(d):
        return datetime.strptime(d, "%d-%b-%Y")

    expiries_sorted = sorted(expiries, key=parse_date)
    return expiries_sorted[:num_expiries]



def fetch_option_chain():
    try:
        session = create_session()
        sleep(1)

        indices = list(set(cfg["index"] for cfg in SHEET_CONFIG))
        expiry_map = {}

        for index in indices:
            if index == "NIFTY":
                expiry_map[index] = get_latest_expiries(session, index, 4)
            else:
                expiry_map[index] = get_latest_expiries(session, index, 1)

        logger.info(f"Expiry dates: {expiry_map}")

        sheet_dfs = {cfg["sheet_name"]: None for cfg in SHEET_CONFIG}

        # Fetch each index
        for index in indices:
            url = OPTION_CHAIN_URL.format(index=index)
            r = session.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()

            option_data = data.get("records", {}).get("data", [])
            if not option_data:
                raise ValueError(f"No option chain found for {index}")

            expiry_dfs = {}

            # ------------ FIXED INDENT BLOCK -------------
            for entry in option_data:
                expiry = entry.get("expiryDate")
                if expiry not in expiry_map[index]:
                    continue

                strike = entry.get("strikePrice")
                ce = entry.get("CE", {})
                pe = entry.get("PE", {})

                row = {
                    "CE OI": ce.get("openInterest", 0),
                    "CE Chng OI": ce.get("changeinOpenInterest", 0),
                    "CE LTP": ce.get("lastPrice", 0),
                    "Strike Price": strike,
                    "Expiry Date": expiry,
                    "PE LTP": pe.get("lastPrice", 0),
                    "PE Chng OI": pe.get("changeinOpenInterest", 0),
                    "PE OI": pe.get("openInterest", 0),
                    "CE Volume": ce.get("totalTradedVolume", 0),
                    "PE Volume": pe.get("totalTradedVolume", 0),
                }

                if expiry not in expiry_dfs:
                    expiry_dfs[expiry] = []

                expiry_dfs[expiry].append(row)
            # ----------------------------------------------

            # Assign sheets
            for cfg in SHEET_CONFIG:
                if cfg["index"] != index:
                    continue

                sheet_name = cfg["sheet_name"]
                expiry_index = cfg["expiry_index"]

                if expiry_index is not None:   # Nifty 4 expiries
                    exp = expiry_map[index][expiry_index]
                else:                          # BankNifty only 1
                    exp = expiry_map[index][0]

                if exp in expiry_dfs:
                    sheet_dfs[sheet_name] = pd.DataFrame(expiry_dfs[exp])

        return sheet_dfs

    except Exception as e:
        logger.error(f"Option chain fetch error: {e}")
        raise


def update_google_sheet(dfs):
    try:
        if not os.path.exists(CREDENTIALS_PATH):
            raise FileNotFoundError(f"Cred file not found: {CREDENTIALS_PATH}")

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(SHEET_ID)
        existing = {ws.title for ws in spreadsheet.worksheets()}

        for cfg in SHEET_CONFIG:
            if cfg["sheet_name"] not in existing:
                spreadsheet.add_worksheet(title=cfg["sheet_name"], rows="100", cols="20")

        for cfg in SHEET_CONFIG:
            name = cfg["sheet_name"]
            df = dfs.get(name)

            if df is None or df.empty:
                logger.warning(f"No data for {name}")
                continue

            ws = spreadsheet.worksheet(name)
            ws.clear()

            data = [df.columns.tolist()] + df.values.tolist()
            ws.update("A1", data)

            logger.info(f"Updated {name} with {len(df)} rows")

    except Exception as e:
        logger.error(f"Sheet update error: {e}")
        raise


def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    start = dtime(9, 10)
    end = dtime(17, 35)
    return now.weekday() < 5 and start <= now.time() <= end


# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("Starting option chain updater...")

    while True:
        try:
            if is_market_open():
                logger.info("Fetching...")
                dfs = fetch_option_chain()
                update_google_sheet(dfs)
            else:
                logger.info("Market closed.")

            sleep(POLLING_INTERVAL_SECONDS)

        except Exception as e:
            logger.error(f"Main loop error: {e}")
            sleep(POLLING_INTERVAL_SECONDS)
