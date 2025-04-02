from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017/")
db = client["financial_data"]

# Input collections
collections_to_clean = ["balance_sheets", "income_statements", "cash_flows"]

# Output collection (cleaned data)
cleaned_collection = db["financial_reports"]

def clean_numeric(value):
    """Convert numeric values to float and handle missing data."""
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        return float(value) if value.replace(".", "").isdigit() else None
    return value if isinstance(value, (int, float)) else None

def clean_financial_data(document):
    """Clean a financial document while retaining important fields."""
    cleaned_data = {}

    for key, value in document.items():
        if key in ["symbol", "reportDate"]:  # Preserve important fields
            cleaned_data[key] = value
        else:
            cleaned_data[key] = clean_numeric(value)

    return cleaned_data

def process_collection(collection_name):
    """Process each collection, clean data, and store in cleaned collection."""
    source_collection = db[collection_name]
    
    print(f"🔄 Cleaning {collection_name} and storing in financial_reports...")

    for document in source_collection.find():
        cleaned_data = clean_financial_data(document)
        cleaned_data["source_collection"] = collection_name  # Keep track of origin
        
        # Ensure we don't insert duplicate keys
        cleaned_data.pop("_id", None)  # Remove `_id` before insertion

        # Insert into cleaned collection
        cleaned_collection.insert_one(cleaned_data)

    print(f"✅ Cleaning completed for {collection_name}")

if __name__ == "__main__":
    for collection in collections_to_clean:
        process_collection(collection)
