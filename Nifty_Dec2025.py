# File: nse_option_chain_live_working_dec2025.py

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import undetected_chromedriver as uc
import time
import json
import logging
from datetime import datetime
import pytz
import os

# ========================= CONFIG =========================
SHEET_ID = "17YLthNpsymBOeDkBbRkb3eC8A3KZnKTXz4cSAGScx88"
CREDENTIALS_PATH = r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"  # Add .json !!

SHEET_CONFIG = [
    {"sheet": "Sheet1", "symbol": "NIFTY",     "expiry_index": 0},   # Today/This Week → 09-Dec-2025
    {"sheet": "Sheet2", "symbol": "NIFTY",     "expiry_index": 1},   # Next Week
    {"sheet": "Sheet3", "symbol": "NIFTY",     "expiry_index": 2},   # Monthly
    {"sheet": "Sheet4", "symbol": "NIFTY",     "expiry_index": 3},   # Next Monthly
    {"sheet": "Sheet5", "symbol": "BANKNIFTY", "expiry_index": 0},
]

POLLING_INTERVAL = 30
# =========================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger()

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    driver = uc.Chrome(options=options, version_main=131)  # Force latest Chrome
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => false})"
    })
    return driver

def fetch_nse_data(symbol="NIFTY"):
    driver = get_driver()
    try:
        driver.get("https://www.nseindia.com/option-chain")
        time.sleep(6)

        # Select Index
        driver.execute_script(f"""
            document.querySelector('#underlyingSelect').value = '{symbol}';
            document.querySelector('#underlyingSelect').dispatchEvent(new Event('change'));
        """)
        time.sleep(3)

        # Get all expiry dates
        expiries = driver.execute_script("""
            return Array.from(document.querySelectorAll('#expirySelect option'))
                 .map(opt => opt.textContent.trim());
        """)

        # Trigger data load
        driver.execute_script("getOptionChainData();")
        time.sleep(5)

        # Extract full JSON from page
        data = driver.execute_script("return window.optionChainData || null;")
        if not data:
            # Fallback: extract from page source
            page = driver.page_source
            start = page.find('{"records"')
            end = page.find('}];', start) + 2
            raw = page[start:end]
            data = json.loads(raw)

        return data["records"]["data"], expiries

    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return [], []
    finally:
        driver.quit()

def update_sheets():
    all_dfs = {}
    for config in SHEET_CONFIG:
        symbol = config["symbol"]
        sheet_name = config["sheet"]
        exp_idx = config["expiry_index"]

        data, expiries = fetch_nse_data(symbol)
        if not data or not expiries:
            logger.warning(f"No data for {symbol}")
            continue

        target_expiry = expiries[exp_idx] if exp_idx < len(expiries) else expiries[0]
        logger.info(f"{sheet_name} → Using expiry: {target_expiry}")

        rows = []
        for item in data:
            if item.get("expiryDate") != target_expiry:
                continue
            strike = item["strikePrice"]
            ce = item.get("CE", {})
            pe = item.get("PE", {})

            rows.append({
                "Strike": int(strike),
                "CE OI": ce.get("openInterest", 0),
                "CE Chng OI": ce.get("changeinOpenInterest", 0),
                "CE Vol": ce.get("totalTradedVolume", 0),
                "CE LTP": ce.get("lastPrice", 0),
                "PE LTP": pe.get("lastPrice", 0),
                "PE Vol": pe.get("totalTradedVolume", 0),
                "PE Chng OI": pe.get("changeinOpenInterest", 0),
                "PE OI": pe.get("openInterest", 0),
            })

        if rows:
            df = pd.DataFrame(rows).sort_values("Strike").reset_index(drop=True)
            all_dfs[sheet_name] = df
            logger.info(f"Fetched {len(df)} strikes → {sheet_name} | Expiry: {target_expiry}")

    # Upload to Google Sheets
    if all_dfs:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        for name, df in all_dfs.items():
            try:
                if name not in [ws.title for ws in sh.worksheets()]:
                    sh.add_worksheet(title=name, rows=1000, cols=20)
                ws = sh.worksheet(name)
                ws.clear()
                ws.update("A1", [df.columns.tolist()] + df.values.tolist())
                logger.info(f"Uploaded {name} → {len(df.shape)} rows | {datetime.now(pytz.timezone('Asia/Kolkata'))}")
            except Exception as e:
                logger.error(f"Failed {name}: {e}")

if __name__ == "__main__":
    logger.info("Live NSE Option Chain → Google Sheets | Working Dec 2025")
    while True:
        ist_now = datetime.now(pytz.timezone("Asia/Kolkata"))
        is_trading_day = ist_now.weekday() < 5
        is_trading_hours = dtime(9, 15) <= ist_now.time() <= dtime(15, 30)

        if is_trading_day and is_trading_hours:
            logger.info("Market OPEN → Fetching live data...")
            update_sheets()
        else:
            logger.info(f"Market closed or pre-open → Next check in {POLLING_INTERVAL}s")

        time.sleep(POLLING_INTERVAL)
