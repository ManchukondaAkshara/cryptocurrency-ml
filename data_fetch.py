import yfinance as yf
import pandas as pd
import requests
import datetime


# Fetch the data for Bitcoin (BTC-USD)
data = yf.download("BTC-USD", start="2018-02-01", end="2025-04-05")

# Display the original data to understand its structure
print("Original Bitcoin Data:")
print(data.head())

# Select only the desired columns: 'Close', 'High', 'Low', 'Open', 'Volume'
transformed_data = data[['Close', 'High', 'Low', 'Open', 'Volume']]

# Reset the index to get the 'Date' column as a regular column
transformed_data.reset_index(inplace=True)

# Display the transformed data
print("\nTransformed Bitcoin Data:")
print(transformed_data.head())
print(transformed_data.info())

# Save Bitcoin data to CSV
transformed_data.to_csv("BTC-USD_data.csv", index=False)

# Fetch Fear and Greed Index data
url = "https://api.alternative.me/fng/?limit=3000"  # Increase the limit to get more data (fetch around 2000 days of data)

# Fetch data
response = requests.get(url)
data = response.json()["data"]

# Convert to DataFrame
df = pd.DataFrame(data)

# Convert timestamps to readable dates
df["timestamp"] = df["timestamp"].astype(int)
df["date"] = pd.to_datetime(df["timestamp"], unit="s").dt.date

# Filter data from 2018-02-01 to 2025-04-05
start_date = datetime.date(2018, 2, 1)
end_date = datetime.date(2025, 4, 5)
df_filtered = df[(df["date"] >= start_date) & (df["date"] <= end_date)]

# Save to CSV
df_filtered.to_csv("fear_greed_2018_2025.csv", index=False)

print("✅ Fear & Greed data from 2018 to 2025 saved successfully.")

# Fetch US Dollar Index (DX-Y.NYB)
usdx_data = yf.download("DX-Y.NYB", start="2018-02-01", end="2025-04-05")

# Display the original USDX data
print("\nOriginal US Dollar Index Data:")
print(usdx_data.head())

# Select only the 'Close' column for US Dollar Index
usdx_data = usdx_data[['Close']]

# Reset index to get 'Date' column
usdx_data.reset_index(inplace=True)

# Rename the column to 'US Dollar Index'
usdx_data.rename(columns={'Close': 'US Dollar Index'}, inplace=True)

# Save US Dollar Index data to CSV
usdx_data.to_csv("USDX_data.csv", index=False)

# Step 3: Combine the Bitcoin, Fear & Greed, and USDX Data on the 'date' column
# Load both datasets
btc_data = pd.read_csv("BTC-USD_data.csv")
fear_greed_data = pd.read_csv("fear_greed_2018_2025.csv")
usdx_data = pd.read_csv("USDX_data.csv")

# Convert the 'date' column to datetime format in all dataframes to ensure proper merging
btc_data['Date'] = pd.to_datetime(btc_data['Date']).dt.date
fear_greed_data['date'] = pd.to_datetime(fear_greed_data['date']).dt.date
usdx_data['Date'] = pd.to_datetime(usdx_data['Date']).dt.date

# Merge the datasets on the 'Date' column
merged_data = pd.merge(btc_data, fear_greed_data, left_on='Date', right_on='date', how='inner')
merged_data = pd.merge(merged_data, usdx_data, on='Date', how='inner')

# Drop the extra 'date' column from fear_greed_data after merging
merged_data.drop(columns=['date'], inplace=True)

# Optional: Check for any missing values and handle them (optional)
merged_data.fillna(method='ffill', inplace=True)  # Forward fill missing values, or you can use other strategies

# Save the combined data to a new CSV file
merged_data.to_csv("combined_data_with_usd_2018_2025.csv", index=False)

print("✅ Combined data with USD Index saved successfully as 'combined_data_with_usd_2018_2025.csv'.")

