import requests
import json
ALPHA_VANTAGE_API_KEY = "7I86ULXIXTCSQ4TI"

def fetch_alpha_vantage_data(symbol, function):
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": function,
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    # Debugging: Print the full response to check the actual structure
    print(f"\n🔍 Full API Response for {symbol} - {function}:\n", json.dumps(data, indent=4))

    return data  # Instead of extracting function data, return full response

# Test the function
if __name__ == "__main__":
    test_symbol = "IBM"
    functions = ["BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW"]

    for func in functions:
        print(f"\nFetching {func} for {test_symbol}...\n")
        result = fetch_alpha_vantage_data(test_symbol, func)