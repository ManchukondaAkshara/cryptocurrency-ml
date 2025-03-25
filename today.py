import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta

# Define cryptocurrencies
crypto_symbols = ["BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "ADA-USD"]
macro_symbols = {
    "Gold": "GC=F",
    "US Dollar Index": "DX-Y.NYB"
}

# Fetch Macro Factors
macro_data = []
start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

print("Fetching macroeconomic data...")
for factor, ticker in macro_symbols.items():
    print(f"Fetching macro factor: {factor}...")
    macro_ticker = yf.Ticker(ticker)
    df = macro_ticker.history(period="1mo", interval="1h")[['Close']]
    
    if df.empty:
        print(f"No data found for {factor}")
        continue
    
    df = df.rename(columns={'Close': factor})
    df = df.reset_index()  # Reset index to include Datetime
    macro_data.append(df)

# Combine all macro data
macro_df = pd.concat(macro_data, ignore_index=True) if macro_data else pd.DataFrame()
if not macro_df.empty:
    macro_df = macro_df.pivot_table(index='Datetime', values=list(macro_symbols.keys())).reset_index()
    macro_df.fillna("N/A", inplace=True)

# Fetch Cryptocurrency Data
all_data = []
print("Fetching cryptocurrency data...")
for symbol in crypto_symbols:
    print(f"Fetching {symbol} data...")
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="1mo", interval="1h")
    
    if data.empty:
        print(f"No data found for {symbol}")
        continue
    
    data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
    data['Crypto'] = symbol
    data = data.reset_index()
    
    # Fetch Binance Order Book Data (Liquidity)
    binance_symbol = symbol.replace("-USD", "USDT")
    try:
        binance_response = requests.get(f"https://api.binance.com/api/v3/depth?symbol={binance_symbol}&limit=5").json()
        data['Bid-Ask Spread'] = float(binance_response['asks'][0][0]) - float(binance_response['bids'][0][0])
    except:
        data['Bid-Ask Spread'] = "N/A"
    
    all_data.append(data)

# Combine all crypto data
crypto_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
if not crypto_df.empty:
    crypto_df['Datetime'] = pd.to_datetime(crypto_df['Datetime'], utc=True)
    crypto_df.fillna("N/A", inplace=True)

# Merge Crypto and Macro Data
if not crypto_df.empty and not macro_df.empty:
    macro_df['Datetime'] = pd.to_datetime(macro_df['Datetime'], utc=True)
    final_df = pd.merge(crypto_df, macro_df, on="Datetime", how="left")
    final_df.fillna("N/A", inplace=True)
else:
    final_df = crypto_df if not crypto_df.empty else macro_df

# Reorder columns if data is available
if not final_df.empty:
    columns_order = ['Crypto', 'Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Bid-Ask Spread',
                     'Gold', 'US Dollar Index']
    final_df = final_df.reindex(columns=[col for col in columns_order if col in final_df.columns])

# Save to CSV
final_df.to_csv("today.csv", index=False)
print("Data saved to today.csv")
