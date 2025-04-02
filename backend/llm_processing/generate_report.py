from transformers import TFGPT2LMHeadModel, GPT2Tokenizer
import sys
import os
import tensorflow as tf

# Ensure the backend folder is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_ingestion.fetch_from_mongo import fetch_financial_data

# Initialize the Hugging Face model and tokenizer (TensorFlow version)
model_name = "gpt2"  # You can use other models like "gpt2-medium" or "EleutherAI/gpt-neo-1.3B"
model = TFGPT2LMHeadModel.from_pretrained(model_name)
tokenizer = GPT2Tokenizer.from_pretrained(model_name)

# Function to generate a CFO-friendly report using LLM
def generate_cfo_report(symbol):
    """
    Fetches financial data from MongoDB and generates a CFO-friendly report using the LLM.
    """
    # Fetch financial data for the given symbol
    financial_data = fetch_financial_data(symbol)

    if not financial_data:
        return f"❌ No financial data found for {symbol}."

    # Prepare a structured text prompt for the model based on the fetched financial data
    report_prompt = f"""
    You are a CFO analyzing financial statements. Generate a professional financial summary 
    for {symbol} based on the following data:
    
    Income Statement:
    {financial_data.get("income_statements", "No data available")}
    
    Balance Sheet:
    {financial_data.get("balance_sheets", "No data available")}
    
    Cash Flow:
    {financial_data.get("cash_flows", "No data available")}
    
    Provide insights on revenue trends, net income, cash flow, and other key financial metrics.
    """

    # Tokenize the input data with truncation to ensure no exceeding max token length
    input_ids = tokenizer.encode(report_prompt, return_tensors='tf', truncation=True, max_length=1024)

    # Ensure the input length does not exceed the model's max token length
    if input_ids.shape[1] >= 1024:
        print(f"⚠️ Input length exceeds 1024 tokens, truncating.")
        input_ids = input_ids[:, :1024]  # Truncate to 1024 tokens

    # Calculate the maximum new tokens considering the total token limit of 1024
    max_generate_tokens = 1024 - input_ids.shape[1]

    # Create attention mask (to handle padding correctly)
    attention_mask = tf.ones(input_ids.shape, dtype=tf.int32)

    # Set the max_new_tokens to ensure that the model generates tokens within limits
    output = model.generate(input_ids, attention_mask=attention_mask, max_new_tokens=max_generate_tokens, num_return_sequences=1)

    # Decode the generated output into readable text
    generated_report = tokenizer.decode(output[0], skip_special_tokens=True)

    return generated_report

# Main block to test the report generation
if __name__ == "__main__":
    symbol = "IBM"  # Example symbol, can be replaced with dynamic input
    report = generate_cfo_report(symbol)
    print(report)
