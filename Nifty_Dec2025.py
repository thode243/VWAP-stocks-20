import requests
import pandas as pd
import gspread
import os
from time import sleep
from datetime import datetime, time as dtime
import pytz
import logging
import sys
from oauth2client.service_account import ServiceAccountCredentials

# Try cloudscraper first, fall back to requests if not available
try:
    import cloudscraper
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
        delay=10
    )
    USE_CLOUDSCRAPER = True
    logging.info("Using cloudscraper for bypassing Cloudflare")
except ImportError:
    import requests
    USE_CLOUDSCRAPER = False
    logging.warning("cloudscraper not installed. Falling back to requests (may fail with 403)")

# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},     # Weekly
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},     # Next weekly
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},     # Monthly
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3},     # Next monthly
    {"sheet_name": "Sheet5", "index": "BANKNIFTY", "expiry_index": 0},
    # Add more as needed
]

POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"  # Add .json!
)

BASE_URL = "https://www.nseindia.com"
OPTION_CHAIN_URL = BASE_URL + "/api/option-chain-indices?symbol={index}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": BASE_URL + "/option-chain",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("option_chain_updater.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ===== SESSION SETUP =====
def create_session():
    if USE_CLOUDSCRAPER:
        session = scraper
    else:
        session = requests.Session()
        retries = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount("https://", retries)

    session.headers.update(HEADERS)

    try:
        # Warm up session + get cookies
        resp = session.get(BASE_URL, timeout=15)
        resp = session.get(BASE_URL, timeout=15)
        resp = session.get(BASE_URL + "/option-chain", timeout=15)
        logger.info(f"Session initialized. Cookies: {list(session.cookies.keys())}")
    except Exception as e:
        logger.error(f"Failed to initialize session: {e}")
        raise
    return session

# ===== FETCH EXPIRIES =====
def get_expiries(session, index="NIFTY", count=4):
    try:
        url = OPTION_CHAIN_URL.format(index=index)
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        expiries = data["records"]["expiryDates"]
        today = datetime.now(pytz.timezone("Asia/Kolkata")).date()

        future_expiries = []
        for exp in expiries:
            try:
                exp_date = datetime.strptime(exp, "%d-%b-%Y").date()
                if exp_date >= today:
                    future_expiries.append(exp)
            except:
                continue

        future_expiries.sort(key=lambda x: datetime.strptime(x, "%d-%b-%Y"))
        return future_expiries[:count]
    except Exception as e:
        logger.error(f"Failed to fetch expiries for {index}: {e}")
        return []

# ===== FETCH OPTION CHAIN =====
def fetch_option_chain():
    session = create_session()
    sleep(2)

    all_expiries = {}
    for cfg in SHEET_CONFIG:
        idx = cfg["index"]
        if idx not in all_expiries:
            all_expiries[idx] = get_expiries(session, idx, 5)

    sheet_dfs = {}
    for cfg in SHEET_CONFIG:
        index = cfg["index"]
        expiry_idx = cfg["expiry_index"]
        sheet = cfg["sheet_name"]

        if not all_expiries[index]:
            logger.warning(f"No expiries found for {index}")
            continue

        target_expiry = all_expiries[index][expiry_idx if expiry_idx < len(all_expiries[index]) else 0]

        url = OPTION_CHAIN_URL.format(index=index)
        try:
            resp = session.get(url, timeout=15)
            data = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch {index}: {e}")
            continue

        rows = []
        for item in data["records"]["data"]:
            if item.get("expiryDate") != target_expiry:
                continue
            strike = item["strikePrice"]
            ce = item.get("CE", {})
            pe = item.get("PE", {})

            rows.append({
                "CE OI": ce.get("openInterest", 0),
                "CE Chng OI": ce.get("changeinOpenInterest", 0),
                "CE Volume": ce.get("totalTradedVolume", 0),
                "CE LTP": ce.get("lastPrice", 0),
                "Strike Price": strike,
                "Expiry": target_expiry,
                "PE LTP": pe.get("lastPrice", 0),
                "PE Volume": pe.get("totalTradedVolume", 0),
                "PE Chng OI": pe.get("changeinOpenInterest", 0),
                "PE OI": pe.get("openInterest", 0),
            })

        if rows:
            df = pd.DataFrame(rows)
            df = df.sort_values("Strike Price").reset_index(drop=True)
            sheet_dfs[sheet] = df
            logger.info(f"{sheet} → {len(df)} strikes | Expiry: {target_expiry}")
        else:
            logger.warning(f"No data for {sheet} ({index} - {target_expiry})")

    return sheet_dfs

# ===== GOOGLE SHEETS =====
def update_google_sheet(dfs):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)

    for sheet_name, df in dfs.items():
        try:
            if sheet_name not in [ws.title for ws in sh.worksheets()]:
                sh.add_worksheet(title=sheet_name, rows=1000, cols=20)

            ws = sh.worksheet(sheet_name)
            ws.clear()
            ws.update('A1', [df.columns.tolist()] + df.values.tolist())
            logger.info(f"Updated '{sheet_name}' → {len(df)} rows @ {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        except Exception as e:
            logger.error(f"Failed to update {sheet_name}: {e}")

# ===== MARKET HOURS =====
def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    t = now.time()
    d = now.date()

    if d.weekday() >= 5:  # Sat/Sun
        return False
    return dtime(9, 15) <= t <= dtime(15, 30)

# ===== MAIN =====
if __name__ == "__main__":
    logger.info("NSE Option Chain → Google Sheets Updater Started")
    session = None
    while True:
        try:
            if is_market_open():
                logger.info("Market open – fetching data...")
                dfs = fetch_option_chain()
                if dfs:
                    update_google_sheet(dfs)
                else:
                    logger.warning("No data fetched this cycle")
            else:
                logger.info("Market closed – waiting...")

            logger.info(f"Sleeping {POLLING_INTERVAL_SECONDS}s...\n")
            sleep(POLLING_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            sleep(POLLING_INTERVAL_SECONDS)
