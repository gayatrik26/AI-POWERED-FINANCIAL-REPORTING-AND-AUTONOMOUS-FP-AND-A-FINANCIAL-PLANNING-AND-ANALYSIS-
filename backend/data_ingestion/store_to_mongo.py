import pymongo
import json
from fetch_alpha_vantage import fetch_alpha_vantage_data
MONGO_URI = "mongodb://localhost:27017/"

# 🔗 Connect to MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client["financial_data"]

# 🔄 Function to store data
def store_to_mongo(symbol, function, collection_name):
    data = fetch_alpha_vantage_data(symbol, function)

    if "annualReports" in data:
        records = data["annualReports"]
    elif "quarterlyReports" in data:
        records = data["quarterlyReports"]
    else:
        print(f"⚠️ No valid data found for {symbol} - {function}")
        return

    # Reference to MongoDB collection
    collection = db[collection_name]

    # Insert data into MongoDB
    for record in records:
        record["symbol"] = symbol  # Add stock symbol to each entry
        collection.insert_one(record)

    print(f"✅ Stored {len(records)} records in '{collection_name}' for {symbol}.")

# 🔄 Store financial statements
if __name__ == "__main__":
    test_symbol = "IBM"
    
    store_to_mongo(test_symbol, "BALANCE_SHEET", "balance_sheets")
    store_to_mongo(test_symbol, "INCOME_STATEMENT", "income_statements")
    store_to_mongo(test_symbol, "CASH_FLOW", "cash_flows")
