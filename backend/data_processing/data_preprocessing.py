import pandas as pd
import glob
import os

# Ensure the cleaned data folder exists
cleaned_folder = "financial_reports/cleaned"
os.makedirs(cleaned_folder, exist_ok=True)

# Function to preprocess a financial CSV file
def preprocess_csv(file_path):
    df = pd.read_csv(file_path)

    # Convert column names to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Convert date columns to datetime format
    if "year" in df.columns:
        df["year"] = pd.to_datetime(df["year"], errors="coerce")

    # Convert financial data columns to numeric, forcing errors to NaN
    for col in df.columns:
        if col not in ["symbol", "year"]:  # Skip non-numeric columns
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Handle missing values:
    df.fillna(0, inplace=True)  # Replace NaN with 0 (or use df.fillna(df.mean()) for averages)

    # Construct cleaned file path
    cleaned_file_path = os.path.join(cleaned_folder, os.path.basename(file_path).replace(".csv", "_cleaned.csv"))

    # Save cleaned data
    df.to_csv(cleaned_file_path, index=False)

    print(f"✅ Cleaned file saved at: {os.path.abspath(cleaned_file_path)}")

# Get list of all CSV files in the "financial_reports" directory
csv_files = glob.glob("financial_reports/*.csv")

# Process each CSV file
if csv_files:
    for file in csv_files:
        preprocess_csv(file)
    print("✅ All files cleaned and saved successfully.")
else:
    print("⚠️ No CSV files found in 'financial_reports/'. Please check the folder path.")
