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
CREDENTIALS_PATH = r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"  # Ensure .json extension!

SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},  # Closest expiry (e.g., 09-Dec-2025 if active)
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3},
    {"sheet_name": "Sheet5", "index": "BANKNIFTY", "expiry_index": 0},
]

POLLING_INTERVAL_SECONDS = 30

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def create_driver():
    """Create stealthy Chrome driver."""
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    driver = uc.Chrome(options=options, version_main=131)  # Match your Chrome version
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def fetch_option_chain_data(index="NIFTY"):
    """Fetch option chain via stealth browser."""
    driver = create_driver()
    try:
        driver.get("https://www.nseindia.com/option-chain")
        time.sleep(6)  # Wait for load

        # Select index
        dropdown = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "underlyingSelect")))
        dropdown.click()
        option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f"//option[contains(text(), '{index}')]")))
        option.click()
        time.sleep(3)

        # Get expiry dates
        expiry_select = driver.find_element(By.ID, "expirySelect")
        expiries = [opt.text.strip() for opt in expiry_select.find_elements(By.TAG_NAME, "option") if opt.text.strip()]
        logger.info(f"Expiries for {index}: {expiries[:4]}")  # e.g., includes 09-Dec-2025

        # Load data
        view_btn = driver.find_element(By.ID, "viewOC")
        view_btn.click()
        time.sleep(5)

        # Extract JSON
        data = driver.execute_script("return window.optionChainData;")
        if not data:
            # Fallback to page source parsing
            page_source = driver.page_source
            start_idx = page_source.find('{"records"')
            end_idx = page_source.find('}];', start_idx) + 2
            json_str = page_source[start_idx:end_idx]
            data = json.loads(json_str)

        return data.get("records", {}).get("data", []), expiries

    except Exception as e:
        logger.error(f"Fetch error for {index}: {e}")
        return [], []
    finally:
        driver.quit()

def process_data_to_dfs():
    """Process fetched data into DataFrames per sheet."""
    sheet_dfs = {}
    unique_indices = list(set(cfg["index"] for cfg in SHEET_CONFIG))

    for index in unique_indices:
        data, expiries = fetch_option_chain_data(index)
        if not data or not expiries:
            logger.warning(f"No data/expiries for {index}")
            continue

        # Get target expiry per config
        for cfg in [c for c in SHEET_CONFIG if c["index"] == index]:
            expiry_idx = cfg["expiry_index"] if cfg["expiry_index"] is not None else 0
            target_expiry = expiries[expiry_idx] if expiry_idx < len(expiries) else expiries[0]
            sheet_name = cfg["sheet_name"]

            rows = []
            for entry in data:
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
                logger.info(f"{sheet_name}: {len(df)} strikes for {target_expiry}")

    return sheet_dfs

def update_google_sheet(dfs):
    """Update sheets."""
    if not dfs:
        logger.warning("No data to upload")
        return

    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials not found: {CREDENTIALS_PATH}")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)

    # Add missing sheets
    existing_sheets = {ws.title for ws in spreadsheet.worksheets()}
    for cfg in SHEET_CONFIG:
        if cfg["sheet_name"] not in existing_sheets:
            spreadsheet.add_worksheet(title=cfg["sheet_name"], rows=1000, cols=15)

    # Update each
    for sheet_name, df in dfs.items():
        if df.empty:
            continue
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()
        data = [df.columns.tolist()] + df.values.tolist()
        ws.update("A1", data)
        logger.info(f"Updated {sheet_name} with {len(df)} rows @ {datetime.now(pytz.timezone('Asia/Kolkata'))}")

def is_market_open():
    """Check IST market hours."""
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    market_start = dtime(9, 15)
    market_end = dtime(15, 30)
    return now.weekday() < 5 and market_start <= now.time() <= market_end

# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("NSE Option Chain Updater (Stealth Mode) – Working Dec 2025")
    while True:
        try:
            if is_market_open():
                logger.info("Market open – Fetching...")
                dfs = process_data_to_dfs()
                update_google_sheet(dfs)
            else:
                logger.info("Market closed – Skipping...")
            time.sleep(POLLING_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Stopped by user")
            break
        except Exception as e:
            logger.error(f"Loop error: {e}")
            time.sleep(POLLING_INTERVAL_SECONDS)
