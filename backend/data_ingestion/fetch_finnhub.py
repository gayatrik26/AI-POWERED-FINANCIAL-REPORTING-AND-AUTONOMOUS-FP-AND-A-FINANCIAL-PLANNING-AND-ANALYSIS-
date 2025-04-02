import requests
import json
import time
FINNHUB_API_KEY ="cvhrajpr01qgkck5o0jgcvhrajpr01qgkck5o0k0"

def fetch_finnhub_data(symbol, statement_type):
    """
    Fetch financial statements from Finnhub API.
    
    Parameters:
    - symbol (str): Stock ticker symbol (e.g., "AAPL").
    - statement_type (str): "bs" (Balance Sheet), "ic" (Income Statement), or "cf" (Cash Flow).
    
    Returns:
    - JSON data if successful, None otherwise.
    """
    base_url = "https://finnhub.io/api/v1/stock/financials"
    params = {
        "symbol": symbol,
        "statement": statement_type,
        "freq": "annual",
        "token": FINNHUB_API_KEY
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if "financials" in data:
            return data["financials"]
        else:
            print(f"⚠️ No data found for {symbol} - {statement_type}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching {statement_type} for {symbol}: {e}")
        return None

# Example usage
if __name__ == "__main__":
    test_symbol = "AAPL"
    for stmt in ["bs", "ic", "cf"]:
        print(f"\nFetching {stmt} for {test_symbol}...\n")
        result = fetch_finnhub_data(test_symbol, stmt)
        print(json.dumps(result[:2], indent=4) if result else "No data available.")
        time.sleep(1)  # Respect API rate limits
