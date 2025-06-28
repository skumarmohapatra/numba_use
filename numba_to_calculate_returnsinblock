import numpy as np
import pandas as pd
from numba import njit,jit
import yfinance as yf
from nsetools import Nse
from datetime import datetime
import time

start_time = time.time()

# Step 1: Get NSE stock symbols
nse = Nse()
stock_codes = nse.get_stock_codes()[1:] # Skip header
tickers = [code + '.NS' for code in stock_codes[:1000]]

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
@jit(nopython=True)
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


for i in range(len(data_combined)):
    try:
        data_combined[i]['Returns']=[None]*len(data_combined[i]['Close'])
        # Extract the price column as a NumPy array
        price_array = data_combined[i]['Close'][data_combined[i]].to_numpy()
        # Apply the Numba-jitted function
        returns_array = calculate_returns_numba(price_array) 
        if not data_combined[i].empty:
            # Add the returns as a new column to the DataFrame
            data_combined[i]['Returns'] = returns_array
        else:
            print(f"Error fetching {data_combined[i]['Close'].columns.values[0]}")
    except Exception as e:
        #print(f"Error fetching {data_combined[i]['Close']}: {e}")
        #data_combined.remove(data_combined[i])
        pass

#print("data combined final")
print(len(data_combined))
print(data_combined)

end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

print(f"Execution time: {elapsed_time:.4f} seconds")
    



