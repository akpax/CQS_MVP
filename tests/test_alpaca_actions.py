import pytest


import pandas as pd

from dags.util.core.alpaca.alpaca_actions import read_watchlist, pull_bars

WATCHLIST_CSV_PATH = "tests/files_for_testing/alpaca_supported_tickers.csv"


@pytest.fixture
def mock_read_watchlist_empty(mocker):
    """
    Mock read_watchlist to return an empty list.
    """
    mocker.patch("dags.util.core.alpaca.alpaca_actions.read_watchlist", return_value=[])


@pytest.fixture
def mock_read_watchlist_non_empty(mocker):
    """
    Mock read_watchlist to return a list with a single ticker.
    """
    mocker.patch(
        "dags.util.core.alpaca.alpaca_actions.read_watchlist", return_value=["AAPL"]
    )


@pytest.fixture
def mock_stock_client(mocker):
    """
    This is a mocker fixture for the StockHistoricalDataClient from Alpaca.
    The mock is necessary to test the logic in pull_bars without hitting the
    Alpaca endpoint.
    """
    mock_client = mocker.patch(
        "dags.util.core.alpaca.alpaca_actions.StockHistoricalDataClient"
    )
    return mock_client


def test_pull_bars_empty(mock_stock_client, mock_read_watchlist_empty):
    """
    Tests the pull bars function when an empty DataFrame is returned by the
    StockHistoricalDataClient.
    When the returned DataFrame is empty the pull_bars function should return none.

    An empty DataFrame occurs when the market is
    not open on the date requested or the ticker is incorrect.
    """
    mock_instance = mock_stock_client.return_value
    mock_df = pd.DataFrame(
        data=[],
        columns=[
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
        ],
    )

    mock_instance.get_stock_bars.return_value.df = mock_df

    start_date = "2023-01-01"
    end_date = "2023-01-31"
    header = ("api_key", "api_secret")

    result = pull_bars(start_date, end_date, header)

    assert result is None


def test_pull_bars_non_empty(mock_stock_client, mock_read_watchlist_non_empty):
    """
    Tests the pull bars function when an non-empty DataFrame is returned by the
    StockHistoricalDataClient.
    """
    mock_instance = mock_stock_client.return_value
    mock_df = pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "open": [150],
            "high": [155],
            "low": [148],
            "close": [152],
            "volume": [100000],
            "trade_count": [111111],
            "vwap": [150],
        }
    )
    mock_df.index = pd.to_datetime(["2023-01-01"])

    mock_instance.get_stock_bars.return_value.df = mock_df

    start_date = "2023-01-01"
    end_date = "2023-01-02"
    header = ("api_key", "api_secret")

    result = pull_bars(start_date, end_date, header)

    # pull_bars resets the index
    expected = mock_df.reset_index()

    pd.testing.assert_frame_equal(result, expected)


def test_read_watchlist():
    """
    Tests read_watchlist function with a local copy of the .csv watchlist.
    Asserts that a list is returned and that there are 6817 tickers.
    """
    watchlist = read_watchlist(WATCHLIST_CSV_PATH)
    assert type(watchlist) == list
    assert len(watchlist) == 6817
