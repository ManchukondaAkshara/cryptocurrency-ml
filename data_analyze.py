import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the dataset
df = pd.read_csv('modified_data.csv')

# Convert 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Set the 'Date' column as the index
df.set_index('Date', inplace=True)

# Set up the plotting style
sns.set(style='darkgrid')

# Plot Bitcoin Close Price over Time
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['Close'], label='BTC-USD Close Price', color='blue', linewidth=2)
plt.title('Bitcoin (BTC-USD) Closing Price Over Time')
plt.xlabel('Date')
plt.ylabel('Price (USD)')
plt.legend(loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot Fear & Greed Index vs Bitcoin Close Price
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['Close'], label='BTC-USD Close Price', color='blue', alpha=0.6)
plt.plot(df.index, df['value'], label='Fear & Greed Index', color='red', alpha=0.6)
plt.title('Bitcoin Close Price vs Fear & Greed Index')
plt.xlabel('Date')
plt.ylabel('Price / Fear & Greed Index')
plt.legend(loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot RSI Over Time
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['RSI'], label='RSI (Relative Strength Index)', color='green')
plt.axhline(70, color='red', linestyle='--', label='Overbought (70)')
plt.axhline(30, color='blue', linestyle='--', label='Oversold (30)')
plt.title('Relative Strength Index (RSI) Over Time')
plt.xlabel('Date')
plt.ylabel('RSI')
plt.legend(loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot Bollinger Bands
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['Close'], label='BTC-USD Close Price', color='blue', linewidth=2)
plt.plot(df.index, df['UpperBand'], label='Upper Bollinger Band', color='red', linestyle='--')
plt.plot(df.index, df['LowerBand'], label='Lower Bollinger Band', color='red', linestyle='--')
plt.fill_between(df.index, df['LowerBand'], df['UpperBand'], color='gray', alpha=0.2)
plt.title('Bitcoin Bollinger Bands')
plt.xlabel('Date')
plt.ylabel('Price (USD)')
plt.legend(loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot 50-day and 200-day Moving Averages
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['Close'], label='BTC-USD Close Price', color='blue', linewidth=2)
plt.plot(df.index, df['SMA50'], label='50-Day Moving Average', color='orange', linewidth=2)
plt.plot(df.index, df['SMA200'], label='200-Day Moving Average', color='green', linewidth=2)
plt.title('Bitcoin Moving Averages')
plt.xlabel('Date')
plt.ylabel('Price (USD)')
plt.legend(loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot Daily Returns
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['Daily_Return'], label='Daily Return (%)', color='purple')
plt.title('Bitcoin Daily Returns')
plt.xlabel('Date')
plt.ylabel('Daily Return (%)')
plt.legend(loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot Fear & Greed Index vs. RSI
plt.figure(figsize=(12, 6))

# Plot the Fear & Greed Index
plt.plot(df.index, df['value'], label='Fear & Greed Index', color='red', alpha=0.7)

# Plot the RSI
plt.plot(df.index, df['RSI'], label='RSI (Relative Strength Index)', color='blue', alpha=0.7)

# Adding overbought and oversold lines for RSI (70 and 30)
plt.axhline(70, color='green', linestyle='--', label='RSI Overbought (70)')
plt.axhline(30, color='orange', linestyle='--', label='RSI Oversold (30)')

# Title and labels
plt.title('Fear & Greed Index vs RSI')
plt.xlabel('Date')
plt.ylabel('Index Value / RSI')
plt.legend(loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot value against time (Fear & Greed Index)
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['value'], label='Fear & Greed Index', color='red')

# Adding title and labels
plt.title('Fear & Greed Index Over Time')
plt.xlabel('Date')
plt.ylabel('Fear & Greed Index Value')
plt.legend(loc='upper left')

# Rotate date labels for better readability
plt.xticks(rotation=45)

# Display the plot
plt.tight_layout()
plt.show()

# Plot Bitcoin Close Price vs US Dollar Index
plt.figure(figsize=(12, 6))

# Plot the Bitcoin Close Price
plt.plot(df.index, df['Close'], label='BTC-USD Close Price', color='blue', alpha=0.7)

# Plot the US Dollar Index
plt.plot(df.index, df['US Dollar Index'], label='US Dollar Index', color='green', alpha=0.7)

# Adding title and labels
plt.title('Bitcoin Close Price vs US Dollar Index')
plt.xlabel('Date')
plt.ylabel('Price / US Dollar Index')

# Adding the legend
plt.legend(loc='upper left')

# Rotate date labels for better readability
plt.xticks(rotation=45)

# Display the plot
plt.tight_layout()
plt.show()