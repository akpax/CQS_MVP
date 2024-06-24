"""
This module is used to check if stock exchange is open for trading. Used by 
Airflow in the short circuit operator. 
"""

from datetime import date
import pandas_market_calendars as mcal


def check_if_exchange_open(exchange_code: str, date: date) -> bool:
    """
    Checks if exchange is open for trading.
    Returns True if open and False otherwise
    """
    calendar = mcal.get_calendar(exchange_code)
    sched = calendar.schedule(date, date)
    if sched.empty:
        return False
    return True


if __name__ == "__main__":
    pass
