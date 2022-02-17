from dfs_tools_mlb.compile.fanduel import FDSlate
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.compile.stats_mlb import get_statcast_p, get_p_diff, get_statcast_h
from dfs_tools_mlb import settings
from dfs_tools_mlb.utils.storage import pickle_path
import pickle
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

s=FDSlate(
          p_fades = [],
          h_fades = [],
          salary_batting_order=5,
          stack_salary_factor = False,
          )

hitters = s.get_hitters()
pitchers = s.get_pitchers()
hitter_mlb_ids = hitters[['name', 'mlb_id']]
s.active_teams

lineups = s.build_lineups(
                      info_only = False,
                      index_track = 0, 
                      lus = 150, 
                      max_lu_total = 150,
                      secondary_stack_cut = 0,
                      no_surplus_cut = 0,                      
                      max_sal = 35000,
                      enforce_pitcher_surplus = True,
                      pitcher_surplus = 1,
                      always_replace_first = [],
                      #dicthere
                      remove_positions = {},
                      custom_stack_order = {},
                      always_pair_first = {},
                      always_pair_first_sec = {},
                      never_pair = {},
                      never_pair_sec = {},
                      max_order=9,
                      non_stack_max_order=7,
                      first_max_order = 5,
                      second_max_order = 7,
                      ss_max_order = 7,
                      third_max_order = 5,
                      of_max_order = 5,
                      util_max_order = 5,
                      min_avg_post_stack = 2650,
                      util_replace_filt = 300,
                      all_in_replace_filt = 300,
                      all_in_diff_filt = 0,
                      all_in_replace_order_stack = 7,
                      all_in_replace_order_sec = 7,
                      supreme_replace_order = 0,
                      replace_secondary_all_in = True,
                      replace_primary_all_in= True,
                      never_replace_primary_supreme = True,
                      #allinhere
                      complete_fade = [],
                      exempt=[],
                      all_in=[],
                      supreme_all_in=[''],
                      no_supreme_replace = [],
                      never_replace=[],
                      always_replace = [],
                      never_fill = [],
                      no_utility=[],
                      supreme_all_in_pos = ['1b', 'util', '2b', 'of', 'of.1', 'of.2', '3b', 'ss'],
                      no_stack_salary_decrease=[],
                      #avoid replacing stacked players with all ins.
                      never_replace_secondary=[],
                      never_replace_primary=[],
                      #for increasing stacks order/salary
                      no_secondary_replace = [],
                      no_stack_replace = [],
                      stack_only = [],
                      always_find_cheap = [],
                      custom_counts={},
                      #new
                      info_stack_key = 'exp_ps_sp_pa',
                      stack_info_order = 5,
                      stack_info_size = 4,
                      stack_info_salary_factor=False,
                      custom_pitchers = {3: 50, 9: 50, 19:50}
,
                      custom_stacks = {
 'brewers': 15,
 'dodgers': 15,
 'rays': 15,
 'phillies': 15,
 
 
 'giants': 15,
 
 
 
 'rockies': 15,
 'mariners': 15,

 'orioles': 15,
 
 'royals': 15,
 
 'rangers': 15},
                      custom_secondary = {
 'brewers': 15,
 'dodgers': 15,
 'rays': 15,
 'phillies': 15,
 
 
 'giants': 15,
 
 
 
 'rockies': 15,
 'mariners': 15,

 'orioles': 15,
 
 'royals': 15,
 
 'rangers': 15},
                      
                      )

s.stacks_df()['stacks'].to_dict()

s.p_lu_df()[['name', 'points', 'lus']]

# remove_positions  = {
#                           '':'1b',
#                           '':'1b',
#                           '':'3b',
#                           '':'of',
#                           '':'3b',
#                           '':'3b',
#                           '':'1b',
#                           '':'3b',
#                           '':'ss',
#                           '':'3b',
#                           '':'3b',
#                           '':'1b',
#                           '':'1b',
#                           '':'1b',
#                           '':'1b',
#                           '':'1b',
#                           '':'1b',
#                           '':'2b',
#                           '':'1b',
#                           }


{
 'brewers': 15,
 'dodgers': 15,
 'rays': 15,
 'phillies': 15,
 
 
 'giants': 15,
 
 
 
 'rockies': 15,
 'mariners': 15,

 'orioles': 15,
 
 'royals': 15,
 
 'rangers': 15}

#dict p_df.index: p_df: lineups to be in
default_pitcher = s.p_df()['points'].to_dict()
# # team: stacks to build
default_stacks = s.default_stack_dict
all_stacks = s.points_df()[["raw_points", "points","salary", "sp_mu", "raw_talent",
                            "ump_avg", "venue_avg", "env_avg", "sp_avg", "mz", 'z']].sort_values(by='z', ascending=False)

all_pitchers = s.p_df()[['name', 'team','points', 'fd_salary', 'pitches_start', 'mu','raw_mu', 'k_pred', 'k_pred_raw', 
                         'fd_id', 'venue_avg', 'ump_avg', 'venue_temp', 'exp_ps_raw', 'exp_inn', 'fav', 'env_points', 'mz', 'z']].sort_values(by='z', ascending=False)


#get pitcher statcast info for slate
pitcher_statcast_file = pickle_path(name=f"pitcher_statcast_{tf.today}_{s.slate_number}", directory=settings.FD_DIR)
pitcher_statcast_path = settings.FD_DIR.joinpath(pitcher_statcast_file)
if pitcher_statcast_path.exists():
    pitcher_statcast = pd.read_pickle(pitcher_statcast_path)
    
else:
    pitcher_statcast = pd.DataFrame(columns=['name',
                                             'spin',
                                             'pc',
                                             'speed',
                                             'launch',
                                             'speed_diff',
                                             'spin_diff',
                                             'launch_diff',
                                             'last_speed',
                                             'last_spin',
                                             'last_launch'])
    for i_d in pitchers['mlb_id'].values:
        try:
            p = get_statcast_p(int(i_d), 2021)
            p_diff = get_p_diff(int(i_d), 2020, 2021, filt1='', filt2='')
            last_game = p[p['date'] == p['date'].max()]
            
            row = [pitchers.loc[pitchers['mlb_id'] == i_d, 'name'].item(),
                   p['releaseSpinRate'].mean(),
                   len(p['pitch'].unique()),
                   p['effectiveSpeed'].max(),
                   p['launchAngle'].mean(),
                   p_diff.get('speed'),
                   p_diff.get('spin'),
                   p_diff.get('launch'),
                   last_game['effectiveSpeed'].max(),
                   last_game['releaseSpinRate'].mean(),
                   last_game['launchAngle'].mean(),
                   
                   ]
        except KeyError:
               continue
        to_append = row
        df_length = len(pitcher_statcast)
        pitcher_statcast.loc[df_length] = to_append
    pitcher_statcast = pitcher_statcast.set_index('name')
    with open(pitcher_statcast_file, "wb") as f:
        pickle.dump(pitcher_statcast, f)
pitcher_statcast['speed_last_speed'] = pitcher_statcast['last_speed'] - pitcher_statcast['speed']
pitcher_statcast

position_sort_key = 'points'
first = s.first_df[['name', 'points', 'exp_ps_sp_pa', 'sp_split', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
second = s.second_df[['name', 'points', 'exp_ps_sp_pa', 'sp_split', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
ss = s.ss_df[['name', 'points', 'exp_ps_sp_pa', 'sp_split', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
third = s.third_df[['name', 'points', 'exp_ps_sp_pa', 'sp_split', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
of = s.of_df[['name', 'points', 'exp_ps_sp_pa', 'sp_split', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
util = s.util_df[['name', 'points', 'exp_ps_sp_pa', 'sp_split', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
#lookup individual hitter statcast
player_name = 'brinson'
hitter_statcast_time = str(tf.fifteen_days)
hitter_mlb_ids[hitter_mlb_ids['name'].str.lower().str.contains(player_name)]

hitter_plays = get_statcast_h(621446, 2021)
filtered_hitter_plays = hitter_plays[(hitter_plays['date'] >= hitter_statcast_time)].reset_index()
filtered_hitter_plays['distance'].count()
filtered_hitter_plays.describe()


track_df = pd.DataFrame(lineups)
track_df.index
track_df.loc['combos', 'blue jays']


#AFTER BUILDING LINEUPS
pc_df = s.p_counts()
pc_index = pc_df['t_count'].nlargest(60).index
hc_df = s.h_counts()
hc_index = hc_df['t_count'].nlargest(60).index
pitcher_counts = pc_df.loc[pc_index, ['name', 't_count', 'fd_salary', 'team', 'fd_id','points','pitches_start', 'batters_faced_sp', 'exp_ps_raw']]
hitter_counts = hc_df.loc[hc_index, ['name', 't_count',  'fd_salary', 'order',  'fd_position', 'fd_id', 'team', 'points', 'fd_wps_pa' ]].set_index('name') 

s.finalize_entries()  

