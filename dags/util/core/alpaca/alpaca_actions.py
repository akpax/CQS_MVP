"""
This module handles interactions with the Alpaca API.  It uses the read
watchlist helper function to get a list of all Alpaca-supported stocks to pull
data on. 
"""
import time
import random
import pandas as pd
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame


ALPACA_WATCHLIST_GCS_URI = (
    "gs://watchlist_alpaca_supported/20240609/alpaca_supported_tickers.csv"
)


def read_watchlist(path: str) -> list:
    """
    This function reads the watchlist csv and returns a list of tickers. The 
    watchlist .csv contains all Alpaca-supported tickers. 
    """
    tickers_df = pd.read_csv(path, na_filter=False)
    return tickers_df["Symbol"].to_list()


def pull_bars(start_date, end_date, header, tickers=None):
    """
    This function pulls equity data between the start date (non-inclusive) and
    end date (inclusive) for the given list of tickers.  If the returned DataFrame
    is empty, another attempt will be made for up to 5 attempts with 
    exponential backoff. If the returned DataFrame is still empty None will be
    returned. 
    """
    if not tickers:
        tickers = read_watchlist(ALPACA_WATCHLIST_GCS_URI)
    stock_client = StockHistoricalDataClient(*header)
    request_params = StockBarsRequest(
        symbol_or_symbols=tickers,
        timeframe=TimeFrame.Day,
        start=start_date,
        end=end_date,
        adjustment='split'
    )
    # do up to 5 retries if DataFrame returned is empty
    for i in range(1,6):
        bars_df = stock_client.get_stock_bars(request_params).df
        if len(bars_df) > 0:
            return bars_df.reset_index()
        # exponential backoff
        time.sleep((2 ** i) + (random.randint(0, 1000) / 1000))
    return None
