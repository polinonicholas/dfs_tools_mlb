from dfs_tools_mlb.dataframes.game_data import game_data
from dfs_tools_mlb.dataframes.stat_splits import p_splits
from dfs_tools_mlb.dataframes.stat_splits import h_splits

game_data.columns.tolist()
h_splits.columns.tolist()
h_splits.loc[h_splits['hr'].idxmax()]

p_splits.loc[[205, 202, 184, 196, 137], 'name']


test = game_data[game_data['temp'] == 57]

dejong = game_data[(game_data['home_sp'] == 664045) | (game_data['away_sp'] == 640464)]

p_splits.loc[p_splits['mlb_id'] == 640444, ['name','fd_wpa_b_vr', 'fd_wpa_b_vl', 'batters_faced_vl', 'pitches_start']]

oviado['home_score']

allard = game_data[(game_data['away_sp'] == 663465)]

cease = game_data[(game_data['home_sp'] == 656302)]

zimmerman = game_data[(game_data['home_sp'] == 669145)]

ohtani = game_data[(game_data['away_sp'] == 660271)]

duplantier = game_data[(game_data['away_sp'] == 641541) | (game_data['home_sp'] == 641541)]

martinez = game_data[(game_data['away_sp'] == 593372)]
wood = game_data[(game_data['away_sp'] == 622072)]

bassit = game_data[(game_data['home_sp'] == 605135)]


white_sox_home = game_data[game_data['venue_id'] == 3289]
white_sox_home['fd_points'].median()


mariners_home = game_data[game_data['venue_id'] == 680]
mariners_home['fd_points'].median()

diamondbacks_home = game_data[game_data['venue_id'] == 15]
diamondbacks_home['fd_points'].median()

rockies_home = game_data[(game_data['venue_id'] == 2681) & (game_data['temp'] > 80) & (game_data['temp'] < 90)]
rockies_home['home_runs'].mean()
game_data['home_runs'].mean()
temp = game_data[(game_data['temp'] > 90)]
temp['fd_points'].describe()
game_data.columns.tolist()




dodgers_home = game_data[(game_data['venue_id'] == 7) & (game_data['wind_speed'] > 10) & (game_data['wind_direction'].str.contains("Out")) & (game_data['temp'] > 80)]
dodgers_home['fd_points'].median()

athletics_home = game_data[game_data['venue_id'] == 2392]
athletics_home['fd_points'].median()

 p_ppb = ((.5 * 4) + (.5 * 4)) * 9 
buffalo.columns.tolist()
buffalo = game_data[game_data['venue_id'] == 2756]
game_data['home_runs'].mean()
# test['fd_points'].median()
# twins.lineup
# test = p_splits[p_splits['name'] == 'Michael Wacha']

# test[['name','fd_wpa_b_vr', 'fd_wpa_b_vl', 'batters_faced_vl', 'pitches_start']]

# giants.clear_all_team_cache()
# , custom_lineup = [643565, 573262, 457763, 474832, 543105, 446334, 543063, 456781, 622072]