import numpy as np
import pandas as pd
from numba import njit,jit
import yfinance as yf
from nsetools import Nse
from datetime import datetime
import time
from pandas import ExcelWriter

start_time = time.time()

# Step 1: Get NSE stock symbols
nse = Nse()
stock_codes = nse.get_stock_codes()[1:] # Skip header
tickers = [code + '.NS' for code in stock_codes[:50]]

start_date = "2020-01-01"
end_date = "2025-06-10"

print(tickers)


# Step 2: Download historical prices

data_combined=[]


# Download stock data
for i in range(len(tickers)):
    try:
        data= yf.download(tickers[i], start=start_date, end=end_date)
        if not data.empty:
            data_combined.append(data)
        else:
            print(f"Error fetching {data_combined[i]['Close'].columns.values[0]}")
    except Exception as e:
        pass
        #print(f"Error fetching {data_combined[i]['Close']}: {e}")
        #tickers.remove(tickers[i])
        
print(data_combined[0]['Close'].columns.values[0])
print(type(data_combined[0]['Close'].columns.values[0]))
print(len(data_combined))


# Step 3: Numba-accelerated return calculation
#@jit(nopython=True)
@njit
def calculate_returns_numba(prices):
    """
    Calculates percentage returns from a 1D NumPy array of prices.
    Returns a new array with returns, with the first element as NaN.
    """
    returns = np.empty_like(prices, dtype=np.float64)
    returns[0] = np.nan  # First return is undefined
    for i in range(1, len(prices)):
        returns[i] = (prices[i] - prices[i-1]) / prices[i-1]
    return returns


print(type(data_combined[1]['Close'].to_numpy()))
for i in range(len(data_combined)):
    data_combined[i]['Returns'] = calculate_returns_numba(data_combined[i]['Close'].to_numpy())
    

#print("data combined final")
print(len(data_combined))
print(data_combined)

# Write to different sheets in the same Excel file
with pd.ExcelWriter('50_sheet_output.xlsx', engine='xlsxwriter') as writer:
    for i in range(len(data_combined)):
        data_combined[i].to_excel(writer, sheet_name=data_combined[i]['Close'].columns.values[0])
    





end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

print(f"Execution time: {elapsed_time:.4f} seconds")
    



