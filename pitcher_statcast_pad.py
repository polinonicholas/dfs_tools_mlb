
from dfs_tools_mlb.compile.stats_mlb import get_statcast_p
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
p = get_statcast_p(621111, 2021)
recent_p = p[(p['date'] >= str(tf.thirty_days))].reset_index()
previous_p = p[((p['date'] <= '2021-06-20') & (p['date'] >= '2021-05-20'))].reset_index()
# recent_p.columns.tolist()
# recent_p['pitch'].unique()

print(previous_p['effectiveSpeed'].median())
print(recent_p['effectiveSpeed'].median())

print(previous_p['releaseSpinRate'].median())
print(recent_p['releaseSpinRate'].median())

print(previous_p['launchAngle'].median())
print(recent_p['launchAngle'].median())




# print(recent_p['effectiveSpeed'].max())
# print(recent_p['effectiveSpeed'].min())
# print(recent_p['effectiveSpeed'].std(ddof = 0))
# print(previous_p['effectiveSpeed'].max())
# print(previous_p['effectiveSpeed'].min())
# print(previous_p['effectiveSpeed'].std(ddof = 0))

# print(recent_p['releaseSpinRate'].max())
# print(recent_p['releaseSpinRate'].min())
# print(recent_p['releaseSpinRate'].std(ddof = 0))
# print(previous_p['releaseSpinRate'].max())
# print(previous_p['releaseSpinRate'].min())
# print(previous_p['releaseSpinRate'].std(ddof = 0))