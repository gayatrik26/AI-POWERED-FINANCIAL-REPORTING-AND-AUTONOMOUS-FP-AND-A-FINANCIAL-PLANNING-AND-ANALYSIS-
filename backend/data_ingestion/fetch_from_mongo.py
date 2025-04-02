from pymongo import MongoClient
import os

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "financial_data"

COLLECTIONS = ["balance_sheets", "income_statements", "cash_flows"]  # Relevant collections

def fetch_financial_data(symbol):
    """
    Fetch year-wise financial data from MongoDB for a given company symbol.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    company_data = {}

    for collection_name in COLLECTIONS:
        collection = db[collection_name]
        
        # Case-insensitive search for the given symbol
        data = list(collection.find({"symbol": {"$regex": f"^{symbol}$", "$options": "i"}}, {"_id": 0}))

        if not data:
            print(f"⚠️ No data found in {collection_name} for {symbol}")
        else:
            company_data[collection_name] = data  # Store results by collection

    if not company_data:
        print(f"❌ No financial data found for {symbol}")
        return None

    return company_data

# Test fetching data
if __name__ == "__main__":
    symbol = "AAPL"
    result = fetch_financial_data(symbol)

    if result:
        print("✅ Data fetched successfully!")
        for key, value in result.items():
            print(f"\n🔹 {key}: {len(value)} records")
            print(value[:2])  # Print first 2 records for preview
    else:
        print("❌ No data found.")
