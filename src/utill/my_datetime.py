from datetime import date, datetime, timedelta
from enum import Enum


class Level(Enum):
    DAY = 1
    MONTH = 2


def get_current_date_str(use_separator: bool = False) -> str:
    return datetime.now().strftime('%Y-%m-%d' if use_separator else '%Y%m%d')


def current_datetime_str(use_separator: bool = False) -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S' if use_separator else '%Y%m%d%H%M%S')


def get_month_first_and_last_day(string: str) -> tuple:
    try:
        dt = datetime.strptime(string, '%Y-%m')
    except ValueError:
        dt = datetime.strptime(string, '%Y-%m-%d').replace(day=1)

    return (dt, (dt + timedelta(days=32)).replace(day=1) - timedelta(days=1))


def generate_dates(start_date: date | str, end_date: date | str, level: Level, is_output_strings: bool = False):
    # Auto convert strings
    if type(start_date) == str:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    if type(end_date) == str:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Auto convert datetime
    if type(start_date) == datetime:
        start_date = start_date.date()
    if type(end_date) == datetime:
        end_date = end_date.date()

    if start_date > end_date:
        raise ValueError(f'start_date \'{start_date}\' cannot be larger than end_date \'{end_date}\'')

    dates: list[date] = []

    match level:
        case Level.DAY:
            while end_date >= start_date:
                dates.append(end_date)
                end_date = end_date - timedelta(days=1)
        case Level.MONTH:
            start_date = start_date.replace(day=1)
            end_date = end_date.replace(day=1)
            while end_date >= start_date:
                end_date = end_date.replace(day=1)
                dates.append(end_date)
                end_date = end_date - timedelta(days=1)
        case _:
            raise ValueError(f'level \'{level}\' not recognized. available levels are: \'day\', \'month\'')

    if is_output_strings:
        return sorted([date.strftime('%Y-%m-%d') for date in dates])
    else:
        return sorted(dates)
