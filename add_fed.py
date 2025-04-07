import pandas as pd
import requests
from datetime import datetime, timedelta

# API Key and Base URL for fetching Federal Funds Rate data
api_key = "d96d0089f179de7002e6e96135cb2f5c"
start_date = (datetime.now() - timedelta(days=8 * 365)).strftime("%Y-%m-%d")  # Last 8 years
url = f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={api_key}&file_type=json&observation_start={start_date}"

# Fetch Federal Funds Rate data
response = requests.get(url)
data = response.json()

# Extract observations for Federal Funds Rate
observations = data.get("observations", [])
df = pd.DataFrame(observations)

# Rename and format columns
df.rename(columns={"date": "timestamp", "value": "Federal Funds Rate"}, inplace=True)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["date"] = df["timestamp"].dt.date  # Separate date column

# Save Federal Funds Rate data to CSV
df.to_csv("federal_funds_rate_2years.csv", index=False)

# Print a message confirming that the data is saved
print("Data saved to federal_funds_rate_2years.csv")

# Step 1: Load the combined data (df2)
df2 = pd.read_csv("combined_data_with_usd_2018_2025.csv")

# Convert 'Date' column in df2 to datetime format to ensure proper merging
df2['Date'] = pd.to_datetime(df2['Date']).dt.date

# Step 2: Merge the Federal Funds Rate data with the combined data (df2)
# We will do this by joining the Federal Funds Rate based on the month of the Date.
# Since Federal Funds Rate is monthly, we will match it with the corresponding date within the month.

# Merge df2 with the Federal Funds Rate (df) on 'Date'
merged_data = pd.merge(df2, df[['date', 'Federal Funds Rate']], left_on='Date', right_on='date', how='left')

# Drop the extra 'date' column from the Federal Funds Rate data after merging
merged_data.drop(columns=['date'], inplace=True)

# Optional: Check for any missing values in the merged data
# If any 'Federal Funds Rate' value is missing, you can forward fill or handle the missing data
merged_data['Federal Funds Rate'].fillna(method='ffill', inplace=True)  # Forward fill to ensure each month has a valid rate

# Save the final merged data to a new CSV file
merged_data.to_csv("combined_data_with_usd_fed_rate_2018_2025.csv", index=False)

# Print a message confirming that the data has been successfully combined and saved
print("✅ Combined data with USD Index and Federal Funds Rate saved successfully as 'combined_data_with_usd_fed_rate_2018_2025.csv'.")