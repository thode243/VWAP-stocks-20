import requests
import json

# Session to get cookies (like browser)
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Referer': 'https://www.nseindia.com/option-chain'
})

# Get cookies from homepage
session.get('https://www.nseindia.com')

# Fetch NIFTY chain (inspect the JSON)
url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
response = session.get(url)
if response.status_code == 200:
    data = response.json()
    print(json.dumps(data['records']['expiryDates'], indent=2))  # Example: Expiries
    print(f"Strikes count: {len(data['records']['data'])}")
else:
    print(f"Error: {response.status_code} - {response.text[:200]}")
