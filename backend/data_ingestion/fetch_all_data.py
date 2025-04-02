import requests
import json
from pymongo import MongoClient

# MongoDB setup
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "financial_data_2"
COLLECTION_NAME = "alpha_vantage"

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

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

    return data

def save_to_mongodb(data, symbol, statement_type):
    data_with_metadata = {
        "symbol": symbol,
        "statement_type": statement_type,
        "data": data
    }
    collection.insert_one(data_with_metadata)

def fetch_and_save(symbol):
    # Fetch and save balance sheet
    balance_sheet = fetch_alpha_vantage_data(symbol, "BALANCE_SHEET")
    save_to_mongodb(balance_sheet, symbol, "BALANCE_SHEET")

    # Fetch and save income statement
    income_statement = fetch_alpha_vantage_data(symbol, "INCOME_STATEMENT")
    save_to_mongodb(income_statement, symbol, "INCOME_STATEMENT")

    # Fetch and save cash flow statement
    cash_flow = fetch_alpha_vantage_data(symbol, "CASH_FLOW")
    save_to_mongodb(cash_flow, symbol, "CASH_FLOW")

# Example: dynamically fetching for a symbol (e.g., AAPL)
if __name__ == "__main__":
    symbol_input = input("Enter the symbol (e.g., AAPL): ").strip()
    fetch_and_save(symbol_input)
    print(f"Financial data for {symbol_input} saved to MongoDB.")
