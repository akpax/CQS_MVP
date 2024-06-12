import pytest

from ..dags.util.core.calendar.calendar_util import check_if_exchange_open
from datetime import date


EXCHANGE_CODE = "NYSE"


class Test_check_if_exchange_open:
    @pytest.mark.parametrize(
        "weekend_date, expected", [(date(2024, 6, 8), False), (date(2024, 6, 9), False)]
    )
    def test_weekend(self, weekend_date, expected):
        assert check_if_exchange_open(EXCHANGE_CODE, weekend_date) == expected

    @pytest.mark.parametrize(
        "weekday_date, expected",
        [
            (date(2024, 6, 3), True),
            (date(2024, 6, 4), True),
            (date(2024, 6, 5), True),
            (date(2024, 6, 6), True),
            (date(2024, 6, 7), True),
        ],
    )
    def test_weekdays(self, weekday_date, expected):
        assert check_if_exchange_open(EXCHANGE_CODE, weekday_date) == expected

    @pytest.mark.parametrize(
        "holiday_date, expected",
        [
            (date(2024, 1, 1), False),  # New Year's Day
            (date(2024, 1, 15), False),  # MLK Jr. Day
            (date(2024, 2, 19), False),  # Washington's Bday
            (date(2024, 3, 29), False),  # Good Friday
            (date(2024, 5, 27), False),  # Memorial Day
            (date(2024, 6, 19), False),  # Juneteenth
            (date(2024, 7, 4), False),  # Independence Day
            (date(2024, 9, 2), False),  # Labor Day
            (date(2024, 11, 28), False),
            # Thanksgiving
            (date(2024, 12, 25), False),  # Christmas
        ],
    )
    def test_holidays(self, holiday_date, expected):
        assert check_if_exchange_open(EXCHANGE_CODE, holiday_date) == False
