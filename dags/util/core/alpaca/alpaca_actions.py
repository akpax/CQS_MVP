from datetime import date, timedelta
import os
from tempfile import NamedTemporaryFile

from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

watchlist_path = os.getenv("WATCHLIST_PATH")
headers = (os.getenv("ALPACA_API_KEY_ID"),os.getenv("ALPACA_API_SECRET_KEY"))

today =  date.today()
yesterday = today - timedelta(days=1)


# stock_client = StockHistoricalDataClient(*headers)

# request_params = StockBarsRequest(
#     symbol_or_symbols=["BRK.B","SPY", "AMZN", "NFLX", "AAPL", "NVDA", "GOOG"],
#     timeframe=TimeFrame.Day,
#     start=yesterday,
#     end=today
# )

# bars = stock_client.get_stock_bars(request_params)
# print(bars.df)

def read_watchlist(path: str)-> list:
    with open(path, "r") as f:
        watchlist = f.readlines()
    # remove newline chracters at end
    return [ticker[:-1] for ticker in watchlist]

def pull_bars():
    tickers = read_watchlist(watchlist_path)
    stock_client = StockHistoricalDataClient(*headers)
    request_params = StockBarsRequest(
        symbol_or_symbols=tickers,
        timeframe=TimeFrame.Day,
        start=yesterday,
        end=today
    )
    # get bars and convert to pandas_df
    bars_df = stock_client.get_stock_bars(request_params).df

    temp_file = NamedTemporaryFile(delete=False, suffix=".csv")
    bars_df.to_csv(temp_file.name, index=False)
    return temp_file.name




if __name__ == "__main__":
    print(pull_bars())