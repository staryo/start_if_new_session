from datetime import datetime


def date_to_week(input_date: str):
    return datetime.strptime(input_date, '%Y-%m-%d').strftime('%Y-%W')
