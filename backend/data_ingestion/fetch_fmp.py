import requests
import json
FMP_API_KEY = "fh8yRadJqVm4w6CZkMEcLmdwAYvfFc1l"

BASE_URL = "https://financialmodelingprep.com/api/v3"

def fetch_fmp_data(symbol, statement_type):
    """
    Fetch financial statement data from FMP API.

    :param symbol: Stock ticker symbol (e.g., "AAPL").
    :param statement_type: "balance-sheet-statement", "income-statement", "cash-flow-statement".
    :return: JSON response with financial data.
    """
    endpoint_map = {
        "balance-sheet-statement": "balance-sheet-statement",
        "income-statement": "income-statement",
        "cash-flow-statement": "cash-flow-statement"
    }

    if statement_type not in endpoint_map:
        print(f"Invalid statement type: {statement_type}")
        return None

    url = f"{BASE_URL}/{endpoint_map[statement_type]}/{symbol}?apikey={FMP_API_KEY}"

    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching {statement_type} from FMP: {response.text}")
        return None

symbol = "AAPL"  # Apple Inc.
statement_types = ["balance-sheet-statement", "income-statement", "cash-flow-statement"]

for statement in statement_types:
    print(f"\nFetching {statement} for {symbol}...\n")
    data = fetch_fmp_data(symbol, statement)

    if data:
        print("Sample Data:", data[:2])  # Print first 2 records for preview
    else:
        print(f"Failed to fetch {statement}.")