import pandas_market_calendars as mcal
from datetime import date, timedelta


def check_if_exchange_open(exchange_code, date: date):
    """
    Checks if exchange is open for trading.
    Returns True if open and False otherwise
    """
    calendar = mcal.get_calendar(exchange_code)
    sched = calendar.schedule(date, date)
    if sched.empty:
        return False
    else:
        return True


if __name__ == "__main__":
    pass
