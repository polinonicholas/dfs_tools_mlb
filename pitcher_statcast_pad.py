
from dfs_tools_mlb.compile.stats_mlb import get_statcast_p, get_statcast_h
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)




p = get_statcast_p(664034, 2021)
recent_p = p[(p['date'] >= '2021-06-20')].reset_index()
previous_p = p[((p['date'] <= '2021-06-20') & (p['date'] >= '2021-05-20'))].reset_index()
# recent_p.columns.tolist()

print(recent_p['result'].value_counts())
print(recent_p['pitch'].unique())

print(previous_p['effectiveSpeed'].median())
print(recent_p['effectiveSpeed'].median())

print(previous_p['releaseSpinRate'].median())
print(recent_p['releaseSpinRate'].median())

print(previous_p['launchAngle'].median())
print(recent_p['launchAngle'].median())


#lookup individual hitter statcast
hitter_mlb_id = 664034
hitter_statcast_time = str(tf.fifteen_days)
hitter_plays = get_statcast_h(hitter_mlb_id, 2021)
filtered_hitter_plays = hitter_plays[(hitter_plays['date'] >= hitter_statcast_time)].reset_index()
filtered_hitter_plays['distance'].count()
print(filtered_hitter_plays.describe())

hitter_statcast_time = str(tf.seven_days)
hitter_plays = get_statcast_h(hitter_mlb_id, 2021)
filtered_hitter_plays = hitter_plays[(hitter_plays['date'] >= hitter_statcast_time)].reset_index()
filtered_hitter_plays['distance'].count()
print(filtered_hitter_plays.describe())

hitter_statcast_time = str(tf.yesterday)
hitter_plays = get_statcast_h(hitter_mlb_id, 2021)
filtered_hitter_plays = hitter_plays[(hitter_plays['date'] >= hitter_statcast_time)].reset_index()
filtered_hitter_plays['distance'].count()
print(filtered_hitter_plays.describe())







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