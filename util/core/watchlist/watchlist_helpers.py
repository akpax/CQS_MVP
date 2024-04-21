"""
ALPACA currently supports NYSE and NASDAQ stock exchanges. 
These exchanges update which stocks they support and ALPACA may add other exchanges in future. Therefore, our watchlist must be periodically updated.
These helper functions aim to speed up the watchlist creation process.
The helper functions read these .csvs and create a text file to store our watchlist. 

.csvs sourced from: https://www.nasdaq.com/market-activity/stocks/screener 

"""

from os import listdir, getenv
from os.path import isfile, join
from datetime import date

import pandas as pd


def get_paths_to_exchange_csvs(dir_path: str) -> list:
    """
    Get all file paths of files inside specfified my path directory
    Inputs:
    *dir_path: path to directory that contains .csv from individual exchanges

    Output: list of file paths in dir_path directory
    """
    return [join(dir_path, f) for f in listdir(dir_path) if isfile(join(dir_path, f))]


def get_watchlist_tickers(path_to_csv_dir: str) -> list:
    """
    Converts a list of .csv files containing ticker symbols of unqique exchanges and converts to single watch list

    Input:
    *path_to_csv_dir: path to exchange csvs directory

    Output:
    *watchlist: list of symbols from .csvs
    """
    csvs = get_paths_to_exchange_csvs(path_to_csv_dir)
    tickers = []
    for csv in csvs:
        # assures stock with ticker "NA" not converted to nan
        df = pd.read_csv(csv, keep_default_na=False, na_values=["_"])
        tickers.extend(df["Symbol"].to_list())
    return list(set(tickers))


def create_watchlist_text_file(tickers: list, out_path: str):
    """
    Converts list of tickers intov text file and converts ticker to Alpaca format

    Input:
    *tickers: list of tickers
    *out: output path string
    """
    with open(out_path, "w") as f:
        _ = [
            f.write(convert_ticker_to_alpaca_format(symbol) + "\n")
            for symbol in tickers
        ]


def convert_ticker_to_alpaca_format(ticker: str) -> str:
    """
    Converts "^" in symbol to "."
    (Alpaca API expects "." for companies with multiple share classes and will throw an error
    example for Berkshire Hathaway Class B: BRK^B -> BRK.B
    """
    mapping_table = str.maketrans({"^": ".", "/": "."})
    tickers = ticker.translate(mapping_table)
    return tickers.strip()


if __name__ == "__main__":
    tickers = get_watchlist_tickers(r"stock_lists/exchanges")
    create_watchlist_text_file(tickers, out_path=getenv("WATCHLIST_PATH"))
