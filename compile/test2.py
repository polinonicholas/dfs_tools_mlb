from dfs_tools_mlb.dataframes.game_data import game_data
from dfs_tools_mlb.dataframes.stat_splits import p_splits
from dfs_tools_mlb.dataframes.stat_splits import h_splits
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
p_splits.loc[p_splits['mlb_id'] == 571800, ['name','fd_wpa_b_vr', 'fd_wpa_b_vl', 'batters_faced_vl', 'pitches_start']]
p_splits.loc[p_splits['mlb_id'] == 571800, ['name','fd_wps_b_vr', 'fd_wps_b_vl', 'batters_faced_vl', 'pitches_start']]


venue = (game_data['venue_id'] == 3309)
temp_g =(game_data['temp'] > 80)
temp_l = (game_data['temp'] < 90)
wind_l = (game_data['wind_speed'] < 10)
wind_g = (game_data['wind_speed'] > 0)
wind_direction = (game_data['wind_direction'].str.contains("In"))
roof = (game_data['condition'].isin(mac.weather.roof_closed))
sp_filt = (game_data['away_sp'] == 605200)
filtered_venue = game_data[venue & (game_data['day_night'] == 'day')
                           
                   
                           

                           
                           ]
game_data['day_night']

filtered_venue['fd_points'].describe()
game_data['fd_points'].describe()


filtered_venue[['temp', 'home_score']]

filtered_venue.columns.tolist()

from dfs_tools_mlb.compile.historical_data import 
