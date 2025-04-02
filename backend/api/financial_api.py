from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import os
import sys

# Ensure backend is in the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_ingestion.mongo_connect import get_mongo_db  # Import MongoDB connection

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Financial Reporting API is running 🚀"}

@app.get("/companies")
def get_companies():
    """ Get a list of all unique company symbols in the database. """
    db = get_mongo_db()
    collection = db["transformed_financial_data"]
    companies = collection.distinct("symbol")  # Get unique company symbols
    return {"companies": companies}

@app.get("/financials/{symbol}")
def get_financial_data(symbol: str):
    """ Get the latest financial data for a given company symbol. """
    db = get_mongo_db()
    collection = db["transformed_financial_data"]
    
    data = collection.find({"symbol": symbol}).sort("reportDate", -1).limit(1)  # Get the latest report
    result = list(data)
    
    if not result:
        raise HTTPException(status_code=404, detail=f"No financial data found for {symbol}")
    
    return result[0]

@app.get("/ratios/{symbol}")
def get_financial_ratios(symbol: str):
    """ Get financial ratios for a given company symbol. """
    db = get_mongo_db()
    collection = db["transformed_financial_data"]
    
    data = collection.find_one({"symbol": symbol}, sort=[("reportDate", -1)])  # Get latest data
    
    if not data:
        raise HTTPException(status_code=404, detail=f"No financial ratios found for {symbol}")

    # Handle missing keys using `.get()`
    return {
        "symbol": symbol,
        "reportDate": data.get("reportDate", "N/A"),  # Default to "N/A" if missing
        "profit_margin": data.get("profit_margin", None),
        "return_on_assets": data.get("return_on_assets", None),
        "debt_to_equity": data.get("debt_to_equity", None),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
