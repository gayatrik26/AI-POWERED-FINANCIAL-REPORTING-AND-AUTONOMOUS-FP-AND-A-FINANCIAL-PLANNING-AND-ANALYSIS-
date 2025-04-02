import cudf  # GPU-accelerated Pandas
import cupy as cp  # GPU-accelerated NumPy
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from cuml.preprocessing import MinMaxScaler  # RAPIDS GPU scaler

# ✅ Load cleaned financial data (e.g., Balance Sheet)
df = cudf.read_csv("financial_reports/cleaned/AAPL_balance_sheet_cleaned.csv")

# ✅ Convert "Year" column to datetime and sort
df["year"] = cudf.to_datetime(df["year"])
df = df.sort_values("year")

# ✅ Select relevant financial metric for forecasting
target_col = "totalassets"  # Example: Predict total assets
data = df[[target_col]].to_pandas()  # Convert to pandas for scaling

# ✅ Normalize data using RAPIDS cuML MinMaxScaler
scaler = MinMaxScaler()
data_scaled = scaler.fit_transform(data)

# ✅ Convert data into sequences for LSTM
def create_sequences(data, seq_length):
    sequences, targets = [], []
    for i in range(len(data) - seq_length):
        sequences.append(data[i:i+seq_length])
        targets.append(data[i+seq_length])
    return np.array(sequences), np.array(targets)

seq_length = 5  # Look back 5 years
X, y = create_sequences(data_scaled, seq_length)

# ✅ Convert to PyTorch GPU tensors
X_tensor = torch.tensor(X, dtype=torch.float32).cuda()
y_tensor = torch.tensor(y, dtype=torch.float32).cuda()

# ✅ Define LSTM Model
class LSTMModel(nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim, num_layers):
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        return self.fc(lstm_out[:, -1, :])  # Take last output for forecasting

# ✅ Model Setup
input_dim = 1  # One financial metric
hidden_dim = 64
output_dim = 1
num_layers = 2
model = LSTMModel(input_dim, hidden_dim, output_dim, num_layers).cuda()

# ✅ Define Optimizer and Loss Function
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# ✅ Training Loop
epochs = 100
for epoch in range(epochs):
    model.train()
    optimizer.zero_grad()
    output = model(X_tensor)
    loss = criterion(output, y_tensor)
    loss.backward()
    optimizer.step()

    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss.item()}")

# ✅ Make Predictions
model.eval()
with torch.no_grad():
    predictions = model(X_tensor).cpu().numpy()

# ✅ Convert predictions back to original scale
predicted_values = scaler.inverse_transform(predictions)

# ✅ Print predicted values
print("Predicted future total assets:", predicted_values[-5:])  # Last 5 years' forecasts
