# NSE_Option_Chainonline.py
# Live NIFTY Option Chain → Google Sheets (Playwright Browser Simulation – Dec 2025)
import os
import sys
import pytz
import logging
import pandas as pd
import gspread
from datetime import datetime, time as dtime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright  # Simulates real browser to bypass blocks

# ========================= CONFIG =========================
SHEET_ID = os.getenv("SHEET_ID")
if not SHEET_ID:
    raise ValueError("SHEET_ID env var required")

CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "service_account.json")
if not os.path.exists(CREDENTIALS_PATH):
    raise FileNotFoundError(f"Google creds missing: {CREDENTIALS_PATH}")

SHEET_CONFIG = [
    {"sheet_name": "Weekly",     "expiry_index": 0},
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
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 15) <= t <= dtime(18, 30)

def fetch_nifty_chain():
    """Use Playwright to load page, execute JS, and fetch API JSON (bypasses all blocks)."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Headless Chrome
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()

        try:
            # Step 1: Load option chain page (sets cookies, runs JS challenges)
            log.info("Loading NSE option chain page...")
            page.goto("https://www.nseindia.com/option-chain", timeout=30000)
            page.wait_for_load_state("networkidle")  # Wait for JS/API loads

            # Step 2: Select NIFTY if needed (mimics user interaction)
            page.select_option("select#symbol", "NIFTY")  # Dropdown for symbol
            page.wait_for_timeout(2000)  # Let it load

            # Step 3: Intercept and extract API response (like Network tab inspection)
            api_data = None
            def handle_response(response):
                nonlocal api_data
                if "option-chain-indices" in response.url and response.status == 200:
                    api_data = response.json()
                    log.info(f"Intercepted API: Keys = {list(api_data.keys()) if api_data else 'None'}")

            page.on("response", handle_response)
            page.reload()  # Trigger API call

            page.wait_for_timeout(5000)  # Wait for intercept

            if not api_data or "records" not in api_data:
                # Fallback: Evaluate JS to get data from page DOM (manual inspection style)
                log.info("Using JS evaluation fallback...")
                js_code = """
                return window.optionChainData || JSON.parse(document.querySelector('[data-api="option-chain"]').dataset.data || '{}');
                """
                api_data = page.evaluate(js_code)
                if not isinstance(api_data, dict) or "records" not in api_data:
                    raise ValueError(f"Invalid structure after fallback: {api_data}")

            records = api_data["records"]
            if not records.get("data"):
                raise ValueError(f"Empty data in records: {records}")

            log.info(f"✅ Fetched via browser: {len(records['data'])} records | Expiries: {len(records.get('expiryDates', []))}")
            return api_data

        except Exception as e:
            log.error(f"❌ Browser fetch failed: {e}")
            raise
        finally:
            browser.close()

def build_df_for_expiry(raw_data, expiry_date):
    rows = []
    for item in raw_data["records"]["data"]:
        if item.get("expiryDate") != expiry_date:
            continue
        strike = item["strikePrice"]
        ce = item.get("CE", {})
        pe = item.get("PE", {})
        rows.append({
            "Strike": strike,
            "CE OI": ce.get("openInterest", 0),
            "CE Chg OI": ce.get("changeinOpenInterest", 0),
            "CE LTP": ce.get("lastPrice", 0),
            "CE Volume": ce.get("totalTradedVolume", 0),
            "PE LTP": pe.get("lastPrice", 0),
            "PE Chg OI": pe.get("changeinOpenInterest", 0),
            "PE OI": pe.get("openInterest", 0),
            "PE Volume": pe.get("totalTradedVolume", 0),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        log.warning(f"No strikes for {expiry_date}")
        return df
    return df.sort_values("Strike").reset_index(drop=True)

def update_google_sheets(dfs_dict):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    book = client.open_by_key(SHEET_ID)

    existing = {ws.title for ws in book.worksheets()}
    for cfg in SHEET_CONFIG:
        if cfg["sheet_name"] not in existing:
            book.add_worksheet(title=cfg["sheet_name"], rows=1000, cols=20)
            log.info(f"Created sheet: {cfg['sheet_name']}")

    for cfg in SHEET_CONFIG:
        sheet_name = cfg["sheet_name"]
        df = dfs_dict.get(sheet_name)
        if df is None or df.empty:
            log.warning(f"Skipping {sheet_name}: No data")
            continue
        ws = book.worksheet(sheet_name)
        ws.clear()
        upload_data = [df.columns.tolist()] + df.values.tolist()
        ws.update("A1", upload_data)
        ist_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S IST")
        log.info(f"Updated {sheet_name}: {len(df)} strikes @ {ist_time}")

# ========================= MAIN =========================
if __name__ == "__main__":
    log.info("🚀 NIFTY Option Chain Updater – Starting (Playwright Simulation)")

    if not is_market_open():
        log.info("⏰ Market closed – Exiting gracefully")
        sys.exit(0)

    try:
        raw_data = fetch_nifty_chain()
        expiries = raw_data["records"]["expiryDates"]
        log.info(f"Expiries: {expiries}")

        dfs = {}
        for cfg in SHEET_CONFIG:
            idx = cfg["expiry_index"]
            if idx >= len(expiries):
                log.warning(f"Index {idx} invalid (only {len(expiries)} expiries)")
                continue
            expiry = expiries[idx]
            df = build_df_for_expiry(raw_data, expiry)
            dfs[cfg["sheet_name"]] = df
            log.info(f"Built {cfg['sheet_name']}: {expiry} ({len(df)} strikes)")

        update_google_sheets(dfs)
        log.info("🎉 All sheets updated!")

    except Exception as e:
        log.error(f"💥 Failure: {e}", exc_info=True)
        sys.exit(1)
