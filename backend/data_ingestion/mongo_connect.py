from pymongo import MongoClient

def get_mongo_db():
    """ Returns a connection to the MongoDB database. """
    client = MongoClient("mongodb://localhost:27017/")
    db = client["financial_data"]
    return db  # Returning the database instead of a collection

if __name__ == "__main__":
    db = get_mongo_db()
    print("✅ MongoDB Connection Successful!")
