# vwap_data.py
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import time
#

# -------------------- CONFIG --------------------
SERVICE_ACCOUNT_FILE = "service_account.json"   # service account JSON file (added via GitHub Actions)
SHEET_ID = os.environ.get("SHEET_ID")           # GitHub Secret for Google Sheet ID
UPDATE_INTERVAL = 60                            # seconds refresh interval

# -------------------- NIFTY50 SYMBOLS --------------------
nifty50_map = {
    "Reliance": "RI",
    "TCS": "TCS",
    "HDFC Bank": "HDF01",
    "Bharti Airtel": "BTV",
    "ICICI Bank": "ICI02",
    "Infosys": "IT",
    "SBI": "SBI",
    "HUL": "HL",
    "Bajaj Finance": "BAF",
    "ITC": "ITC",
    "HCL Tech": "HCL02",
    "Larsen": "LT",
    "Sun Pharma": "SPI",
    "Kotak Mahindra": "KMF",
    "Maruti Suzuki": "MU01",
    "M&M": "MM",
    "UltraTech Cement": "UTC",
    "Wipro": "W",
    "NTPC": "NTP",
    "Axis Bank": "UTI10",
    "ONGC": "ONG",
    "Bajaj Finserv": "BF04",
    "Titan Company": "TI01",
    "Tata Motors": "TEL",
    "Adani Enterprises": "AE01",
    "Power Grid": "PGC",
    "JSW Steel": "JVS",
    "Bajaj Auto": "BA06",
    "Adani Ports": "MPS",
    "Coal India": "CI29",
    "Asian Paints": "API",
    "Nestle": "NI",
    "Bharat Elec": "BE03",
    "Trent": "L",
    "Tata Steel": "TIS",
    "Grasim": "GI01",
    "Tech Mahindra": "TM4",
    "SBI Life": "SLI03",
    "Hindalco": "H",
    "Eicher Motors": "EM",
    "HDFC Life": "HSL01",
    "Cipla": "C",
    "Britannia": "BI",
    "Shriram Finance": "STF",
    "BPCL": "BPC",
    "Tata Consumer": "TT",
    "Dr Reddys": "DRL",
    "Apollo Hospital": "AHE",
    "IndusInd Bank": "IIB",
    "Hero Motocorp": "HHM"
    }

# -------------------- API ENDPOINTS --------------------
ltp_url = "https://api.moneycontrol.com/mcapi/v1/stock/get-stock-price"
vwap_url_template = "https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/{}"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.moneycontrol.com",
    "Accept-Language": "en-US,en;q=0.9",
}

# -------------------- GOOGLE SHEETS AUTH --------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
# sheet = client.open_by_key(SHEET_ID).sheet1
sheet = client.open("NSE data").worksheet("Nifty50VWAP")

# Write headers if sheet is empty
if len(sheet.get_all_values()) == 0:
    headers_row = ["Timestamp", "Company", "Symbol Code", "LTP", "% Change", "VWAP/AVGP"]
    sheet.append_row(headers_row)

# -------------------- MAIN LOOP --------------------
while True:
    results = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for name, scId in nifty50_map.items():
        try:
            # LTP data
            params = {"scIdList": scId, "scId": scId}
            ltp_resp = requests.get(ltp_url, params=params, headers=headers, timeout=10).json()

            if "data" not in ltp_resp or not ltp_resp["data"]:
                print(f"[{timestamp}] ⚠️ No LTP for {name}")
                continue

            ltp_data = ltp_resp["data"][0]
            ltp = ltp_data.get("lastPrice", "-")
            change = ltp_data.get("perChange", "-")

            # VWAP data
            vwap = None
            try:
                vwap_resp = requests.get(vwap_url_template.format(scId), headers=headers, timeout=10).json()
                if vwap_resp.get("data"):
                    vwap = vwap_resp["data"].get("VWAP") or vwap_resp["data"].get("AVGP")
            except Exception as e:
                print(f"[{timestamp}] VWAP error {name}: {e}")

            results.append({
                "Timestamp": timestamp,
                "Company": name,
                "Symbol Code": scId,
                "LTP": ltp,
                "% Change": change,
                "VWAP/AVGP": vwap
            })

        except Exception as e:
            print(f"[{timestamp}] ❌ Error fetching {name}: {e}")

        time.sleep(0.5)  # avoid rate limiting

    # Convert to DataFrame
    df = pd.DataFrame(results)
    print(df.head())

# Overwrite old data (starting row=2, below headers)
    set_with_dataframe(
    sheet,
    df,
    row=2,                     # always start at row 2
    include_index=False,
    include_column_header=False
)

    print(f"[{timestamp}] ✅ Data refreshed in Google Sheet (overwritten old data)")
    time.sleep(UPDATE_INTERVAL)
