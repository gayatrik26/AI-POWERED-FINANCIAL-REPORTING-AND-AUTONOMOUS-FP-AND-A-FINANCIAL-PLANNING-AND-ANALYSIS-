import pymongo
import json
import time
from fetch_alpha_vantage import fetch_alpha_vantage_data
MONGO_URI = "mongodb://localhost:27017/"
from apscheduler.schedulers.background import BackgroundScheduler

# 🔗 Connect to MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client["financial_data"]

# 🕒 Function to update MongoDB
def update_mongo(symbol, function, collection_name):
    print(f"🔄 Updating {collection_name} for {symbol}...")
    
    # Fetch new data
    data = fetch_alpha_vantage_data(symbol, function)

    if "annualReports" in data:
        records = data["annualReports"]
    elif "quarterlyReports" in data:
        records = data["quarterlyReports"]
    else:
        print(f"⚠️ No new data found for {symbol} - {function}")
        return

    collection = db[collection_name]

    for record in records:
        record["symbol"] = symbol  # Add stock symbol

        # Check if the record already exists in MongoDB
        existing_record = collection.find_one({"symbol": symbol, "fiscalDateEnding": record["fiscalDateEnding"]})

        if existing_record:
            print(f"✅ Record already exists for {symbol} - {record['fiscalDateEnding']}")
        else:
            collection.insert_one(record)
            print(f"✅ Inserted new record for {symbol} - {record['fiscalDateEnding']}")

# 🕒 Scheduling function
def schedule_updates():
    scheduler = BackgroundScheduler()
    
    symbols = ["IBM"]  # Add more stock symbols if needed
    
    for symbol in symbols:
        scheduler.add_job(update_mongo, "interval", seconds=10, args=[symbol, "BALANCE_SHEET", "balance_sheets"])
        scheduler.add_job(update_mongo, "interval", seconds=10, args=[symbol, "INCOME_STATEMENT", "income_statements"])
        scheduler.add_job(update_mongo, "interval", seconds=10, args=[symbol, "CASH_FLOW", "cash_flows"])

    scheduler.start()
    print("🚀 Real-time updates started! Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

# 🚀 Start real-time updates
if __name__ == "__main__":
    schedule_updates()
