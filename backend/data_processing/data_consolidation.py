from pymongo import MongoClient
import pandas as pd

# 🔹 Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Replace with your actual connection URI
db = client["financial_data_2"]  # Database name
collection = db["alpha_vantage"]  # Collection name

def fetch_financial_data(symbol, statement_type):
    """
    Fetches annual financial data for a given symbol and statement type (Balance Sheet, Income, Cash Flow)
    """
    # Query MongoDB
    document = collection.find_one({"symbol": symbol, "statement_type": statement_type}, {"_id": 0, "data.annualReports": 1})

    if not document:
        print(f"No data found for {symbol} - {statement_type}")
        return None

    # Extract annual reports
    annual_reports = document.get("data", {}).get("annualReports", [])

    # Convert to DataFrame
    df = pd.DataFrame(annual_reports)

    # Add symbol column
    df["symbol"] = symbol

    # Convert fiscal date to datetime format
    df.rename(columns={"fiscalDateEnding": "Year"}, inplace=True)
    df["Year"] = pd.to_datetime(df["Year"])

    return df


# Define the stock symbol
symbol = "AAPL"  # Change to any stock ticker you need

# Fetch financial statements
balance_sheet_df = fetch_financial_data(symbol, "BALANCE_SHEET")
income_statement_df = fetch_financial_data(symbol, "INCOME_STATEMENT")
cash_flow_df = fetch_financial_data(symbol, "CASH_FLOW")

# Save each DataFrame as a CSV file
if balance_sheet_df is not None:
    balance_sheet_df.to_csv(f"{symbol}_balance_sheet.csv", index=False)

if income_statement_df is not None:
    income_statement_df.to_csv(f"{symbol}_income_statement.csv", index=False)

if cash_flow_df is not None:
    cash_flow_df.to_csv(f"{symbol}_cash_flow.csv", index=False)

print("✅ Consolidation complete! Files saved.")
