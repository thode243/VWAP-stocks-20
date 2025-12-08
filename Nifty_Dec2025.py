import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import logging
from datetime import datetime, time as dtime
import pytz
import os

# ===== CONFIG =====
SHEET_ID = "17YLthNpsymBOeDkBbRkb3eC8A3KZnKTXz4cSAGScx88"
SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},  # Closest (e.g., 09-Dec-2025)
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3},
    {"sheet_name": "Sheet5", "index": "BANKNIFTY", "expiry_index": None},
]
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"  # Add .json if missing
)

# Logging (your Unicode-safe setup)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
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
logger.addHandler(UnicodeSafeStreamHandler())

def create_driver():
    """Stealth Chrome driver (bypasses NSE blocks)."""
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    driver = uc.Chrome(options=options, version_main=131)  # Update to your Chrome version
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def get_latest_expiries(driver, index):
    """Fetch expiry dates via browser."""
    driver.get("https://www.nseindia.com/option-chain")
    time.sleep(5)

    # Select index
    dropdown = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "underlyingSelect")))
    dropdown.click()
    option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f"//option[contains(text(), '{index}')]")))
    option.click()
    time.sleep(3)

    # Get expiries
    expiry_select = driver.find_element(By.ID, "expirySelect")
    expiries = [opt.text.strip() for opt in expiry_select.find_elements(By.TAG_NAME, "option") if opt.text.strip()]
    logger.info(f"Expiries for {index}: {expiries[:4]}")  # Includes 09-Dec-2025 if active
    return expiries

def fetch_option_chain_data(driver, index, target_expiry):
    """Fetch data for specific expiry."""
    # Load data
    view_btn = driver.find_element(By.ID, "viewOC")
    view_btn.click()
    time.sleep(5)

    # Select expiry
    expiry_dropdown = driver.find_element(By.ID, "expirySelect")
    expiry_dropdown.click()
    expiry_option = driver.find_element(By.XPATH, f"//option[contains(text(), '{target_expiry}')]")
    expiry_option.click()
    time.sleep(3)
    view_btn.click()
    time.sleep(5)

    # Extract JSON
    data = driver.execute_script("return window.optionChainData;")
    if not data:
        page_source = driver.page_source
        start = page_source.find('{"records"')
        end = page_source.find('}];', start) + 2 if start != -1 else -1
        if start != -1 and end > start:
            json_str = page_source[start:end]
            data = json.loads(json_str)
        else:
            raise ValueError("No data found in page source")
    return data.get("records", {}).get("data", [])

def fetch_option_chain():
    """Main fetch logic (replaces your requests-based function)."""
    try:
        driver = create_driver()
        indices = list(set(cfg["index"] for cfg in SHEET_CONFIG))
        expiry_map = {}
        sheet_dfs = {cfg["sheet_name"]: None for cfg in SHEET_CONFIG}

        for index in indices:
            expiries = get_latest_expiries(driver, index)
            if not expiries:
                raise ValueError(f"No expiries for {index}")
            expiry_map[index] = expiries

        logger.info(f"Expiry dates: {expiry_map}")

        for index in indices:
            expiries = expiry_map[index]
            for cfg in [c for c in SHEET_CONFIG if c["index"] == index]:
                sheet_name = cfg["sheet_name"]
                expiry_index = cfg["expiry_index"]
                if expiry_index is not None:
                    target_expiry = expiries[expiry_index]
                else:
                    target_expiry = expiries[0]

                option_data = fetch_option_chain_data(driver, index, target_expiry)
                if not option_data:
                    logger.warning(f"No data for {index} - {target_expiry}")
                    continue

                rows = []
                for entry in option_data:
                    if entry.get("expiryDate") != target_expiry:
                        continue
                    strike = entry.get("strikePrice")
                    ce = entry.get("CE", {})
                    pe = entry.get("PE", {})
                    rows.append({
                        "CE OI": ce.get("openInterest", 0),
                        "CE Chng OI": ce.get("changeinOpenInterest", 0),
                        "CE LTP": ce.get("lastPrice", 0),
                        "CE Volume": ce.get("totalTradedVolume", 0),
                        "Strike Price": strike,
                        "Expiry Date": target_expiry,
                        "PE LTP": pe.get("lastPrice", 0),
                        "PE Volume": pe.get("totalTradedVolume", 0),
                        "PE Chng OI": pe.get("changeinOpenInterest", 0),
                        "PE OI": pe.get("openInterest", 0),
                    })

                if rows:
                    df = pd.DataFrame(rows).sort_values("Strike Price").reset_index(drop=True)
                    sheet_dfs[sheet_name] = df
                    logger.info(f"Fetched {len(df)} rows for {sheet_name} ({target_expiry})")

        driver.quit()
        return sheet_dfs
    except Exception as e:
        if 'driver' in locals():
            driver.quit()
        logger.error(f"Option chain fetch error: {e}")
        raise

# Your existing update_google_sheet and is_market_open functions (unchanged)
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
                spreadsheet.add_worksheet(title=cfg["sheet_name"], rows="1000", cols="20")
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
    start = dtime(9, 15)  # Fixed to standard 9:15 AM
    end = dtime(15, 30)   # Fixed to 3:30 PM
    return now.weekday() < 5 and start <= now.time() <= end

# ===== MAIN LOOP (your structure) =====
if __name__ == "__main__":
    logger.info("Starting option chain updater (Stealth Mode - Dec 2025)...")
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
