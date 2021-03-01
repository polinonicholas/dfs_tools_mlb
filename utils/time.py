import datetime
from functools import lru_cache
from dfs_tools_mlb.utils.subclass import Map
@lru_cache()
def time_frames():
	return Map({
		'today': datetime.date.today(),
		'yesterday': datetime.date.today() - datetime.timedelta(days=1),
		'thirty_days': datetime.date.today() - datetime.timedelta(days=30),
		'one_year': datetime.date.today() - datetime.timedelta(days=365),
		'two_years': datetime.date.today() - datetime.timedelta(days=730)})