import pymongo
import sys
import os

# Ensure the backend folder is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_ingestion.mongo_connect import get_mongo_db  # Fixed import

def calculate_ratios(financial_data):
    """ Calculate key financial ratios from the cleaned data. """
    try:
        revenue = financial_data.get("revenue", 0)
        net_income = financial_data.get("netIncome", 0)
        total_assets = financial_data.get("totalAssets", 0)
        total_liabilities = financial_data.get("totalLiabilities", 0)
        equity = financial_data.get("totalShareholderEquity", 0)

        return {
            "profit_margin": round((net_income / revenue) * 100, 2) if revenue else None,
            "return_on_assets": round((net_income / total_assets) * 100, 2) if total_assets else None,
            "debt_to_equity": round((total_liabilities / equity), 2) if equity else None,
        }
    except Exception as e:
        print(f"⚠️ Error calculating ratios: {e}")
        return {}

def transform_document(document, source_collection):
    """ Transform a single financial document and prepare for new storage. """
    transformed_data = document.copy()
    transformed_data["_id"] = str(document["_id"])  # Convert _id to string to avoid conflicts
    transformed_data["source_collection"] = source_collection  # Track original collection
    transformed_data.update(calculate_ratios(document))  # Add calculated ratios
    return transformed_data

def process_collection(collection_name):
    """ Apply transformations and store in a new collection. """
    db = get_mongo_db()
    source_collection = db[collection_name]
    transformed_collection = db["transformed_financial_data"]  # New collection

    print(f"🔄 Transforming data from {collection_name}...")

    for document in source_collection.find():
        transformed_data = transform_document(document, collection_name)
        transformed_collection.update_one({"_id": transformed_data["_id"]}, {"$set": transformed_data}, upsert=True)

    print(f"✅ Transformation completed for {collection_name} and stored in transformed_financial_data")

if __name__ == "__main__":
    collections_to_process = ["balance_sheets", "income_statements", "cash_flows"]
    for collection in collections_to_process:
        process_collection(collection)
