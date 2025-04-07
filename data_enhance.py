import pandas as pd
import matplotlib.pyplot as plt

# Read CSV file (update with the actual path to your CSV)
df = pd.read_csv('combined_data_with_usd_fed_rate_2018_2025.csv')

# Convert 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Calculate Moving Averages (50-day and 200-day)
df['SMA50'] = df['Close'].rolling(window=50).mean()  # 50-day simple moving average
df['SMA200'] = df['Close'].rolling(window=200).mean()  # 200-day simple moving average

# Calculate the Relative Strength Index (RSI)
df['delta'] = df['Close'].diff()
df['gain'] = df['delta'].where(df['delta'] > 0, 0)
df['loss'] = -df['delta'].where(df['delta'] < 0, 0)
df['avg_gain'] = df['gain'].rolling(window=14).mean()
df['avg_loss'] = df['loss'].rolling(window=14).mean()
df['rs'] = df['avg_gain'] / df['avg_loss']
df['RSI'] = 100 - (100 / (1 + df['rs']))

# Calculate Bollinger Bands
df['MA20'] = df['Close'].rolling(window=20).mean()  # 20-day moving average
df['stddev'] = df['Close'].rolling(window=20).std()
df['UpperBand'] = df['MA20'] + (df['stddev'] * 2)
df['LowerBand'] = df['MA20'] - (df['stddev'] * 2)

# Calculate Daily Returns (percentage change)
df['Daily_Return'] = df['Close'].pct_change() * 100  # Percentage change in closing price

# Save the modified DataFrame to a new CSV file
df.to_csv('modified_data.csv', index=False)

print("Modified CSV file saved as 'modified_data.csv'")