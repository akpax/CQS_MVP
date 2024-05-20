from datetime import date, timedelta
import os
from tempfile import NamedTemporaryFile
from dotenv import load_dotenv

from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

load_dotenv()

watchlist_path = os.getenv("WATCHLIST_PATH")
print(watchlist_path)
headers = (os.getenv("ALPACA_API_KEY_ID"), os.getenv("ALPACA_API_SECRET_KEY"))


def read_watchlist(path: str) -> list:
    with open(path, "r") as f:
        watchlist = f.readlines()
    # remove newline characters at end
    return [ticker[:-1] for ticker in watchlist]


def pull_bars(start_date, end_date):
    tickers = read_watchlist(watchlist_path)
    stock_client = StockHistoricalDataClient(*headers)
    request_params = StockBarsRequest(
        symbol_or_symbols=tickers,
        timeframe=TimeFrame.Day,
        start=start_date,
        end=end_date,
    )
    # get bars and convert to pandas_df
    bars_df = stock_client.get_stock_bars(request_params).df
    return bars_df.reset_index()
