import datetime
from dfs_tools_mlb.utils.subclass import Map

time_frames = Map({'today': datetime.date.today(),
                   'tomorrow': datetime.date.today() + datetime.timedelta(days=1),
                   'seven_days': datetime.date.today() - datetime.timedelta(days=7),
                   'yesterday': datetime.date.today() - datetime.timedelta(days=1),
                   'thirty_days': datetime.date.today() - datetime.timedelta(days=30),
                   'one_year': datetime.date.today() - datetime.timedelta(days=365),
                   'two_years': datetime.date.today() - datetime.timedelta(days=730),
                   'fifteen_days': datetime.date.today() - datetime.timedelta(days=15)})

def month_end(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)
def mlb_months(year):
    mlb_months = {}
    for month in range(3, 11):
        last_day = month_end(datetime.date(year, month, 1))
        mlb_months[month] = []
        mlb_months[month].append(str(last_day.replace(day=1)))
        mlb_months[month].append(str(last_day))
    return mlb_months
