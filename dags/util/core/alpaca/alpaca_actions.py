from datetime import date, timedelta
import os
from tempfile import NamedTemporaryFile
import pandas as pd


from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame


ALAPCA_WATCHLIST_GCS_URI = (
    "gs://watchlist_alpaca_supported/20240609/alpaca_supported_tickers.csv"
)


def read_watchlist(path: str) -> list:
    tickers_df = pd.read_csv(path, na_filter=False)
    return tickers_df["Symbol"].to_list()


def pull_bars(start_date, end_date, header):
    tickers = read_watchlist(ALAPCA_WATCHLIST_GCS_URI)
    stock_client = StockHistoricalDataClient(*header)
    request_params = StockBarsRequest(
        symbol_or_symbols=tickers,
        timeframe=TimeFrame.Day,
        start=start_date,
        end=end_date,
    )
    # get bars and convert to pandas_df
    bars_df = stock_client.get_stock_bars(request_params).df
    return bars_df.reset_index()
