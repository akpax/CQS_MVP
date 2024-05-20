from datetime import date, timedelta
import os
from tempfile import NamedTemporaryFile


from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame


watchlist_path = "/Users/austinpaxton/Documents/dev/coastal_quant_strategies/CQS_MVP/stock_lists/watchlist/watchlist_.txt"


def read_watchlist(path: str) -> list:
    with open(path, "r") as f:
        watchlist = f.readlines()
    # remove newline characters at end
    return [ticker[:-1] for ticker in watchlist]


def pull_bars(start_date, end_date, header):
    tickers = read_watchlist(watchlist_path)
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
