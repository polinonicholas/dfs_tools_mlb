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
          )

hitters = s.get_hitters()
pitchers = s.get_pitchers()
hitter_mlb_ids = hitters[['name', 'mlb_id']]


lineups = s.build_lineups(
                      info_only = True,
                      custom_stacks_info = None,
                      median_pitcher_salary = None,
                      low_pitcher_salary = None,
                      high_pitcher_salary = None,
                      max_pitcher_salary = None,
                      median_team_salary = None,
                      low_stack_salary = None,
                      high_stack_salary = None,
                      max_stack_salary = None,
                      stack_salary_pairing_cutoff=0,
                      stack_max_pair = 0,
                      
                      
                      lus = 150, 
                      index_track = 0, 
                      non_random_stack_start=0,
                      
                      max_lu_total = 150,
                      variance = 0, 
                      below_avg_count = 0,
                      below_order_count = 135,
                      risk_limit = 125,
                      of_count_adjust = 0,
                      max_lu_stack = 50, 
                      
                      
                      stack_size = 4,
                      stack_sample = 4, 
                      
                      
                      fallback_stack_sample = 5,
                      stack_expand_limit = 10000,
                      
                      max_order=9,
                      non_stack_max_order=7,
                      
                      #new
                      first_max_order = 5,
                      second_max_order = 7,
                      ss_max_order = 7,
                      third_max_order = 5,
                      of_max_order = 5,
                      util_max_order = 5,
                      min_avg_post_stack = 2700,
                     
                      non_stack_quantile = .9, 
                      high_salary_quantile = .9,
                      
                      secondary_stack_cut = 0,
                      no_surplus_cut = 0,
                      
                      max_sal = 35000,
                      util_replace_filt = 300,
                      all_in_replace_filt = 300,
                      all_in_replace_order_stack = 5,
                      all_in_replace_order_sec = 5,
                      single_stack_surplus = 900,
                      double_stack_surplus = 900,
                      pitcher_surplus = 1,
                      #new
                      replace_secondary_all_in = True,
                      replace_primary_all_in= False,
                      never_replace_primary_supreme = True,
                      find_cheap_primary = False,
                      fill_high_salary_first = True,
                      select_max_pitcher = True,
                      no_surplus_secondary_stacks=True,
                      find_cheap_stacks = True,
                      always_pitcher_first=True,
                      enforce_pitcher_surplus = True,
                      enforce_hitter_surplus = False,
                      filter_last_replaced=False,
                      filter_last_lu = False,
                      always_replace_pitcher_lock=True,
                      p_sal_stack_filter = False,
                      expand_pitchers = False,
                      while_loop_secondary = True,
                      remove_positions = None,
                      
                      exempt=[],
                      all_in=[],
                      supreme_all_in=None,
                      supreme_all_in_pos = [],
                      no_supreme_replace = [],
                      never_replace=[],
                      no_stack_salary_decrease=[],
                      #avoid replacing stacked players with all ins.
                      never_replace_secondary=[],
                      never_replace_primary=[],
                      #for increasing stacks order/salary
                      no_secondary_replace = [],
                      no_stack_replace = [],
                      no_utility=[],
                      stack_only = [],
                      always_replace = [],
                      #pitcher
                      no_combos = [],
                      #position
                     
                      x_fallback = [],
                      limit_risk = [],
                      lock = [],
                      #stacks/h
                      no_combine = [],
                      always_replace_first = [],
                      #new
                      no_replace = [],
                      never_fill = [],
                      #new
                      always_find_cheap = [],
                      custom_counts={},
                      custom_pitchers = None,
                      custom_stacks = None,
                      custom_secondary = None,
                      locked_lineup = None,
                      custom_stack_order = {},
                      #new
                      always_pair_first = {},
                      never_pair = {},
                      info_stack_key = 'exp_ps_sp_pa',
                      stack_info_order = 5,
                      stack_info_size = 4,
                      stack_info_salary_factor=False,
                      stack_top_order=True
                      )



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


   


#dict p_df.index: p_df: lineups to be in
default_pitcher = s.p_df()['points'].to_dict()
# # team: stacks to build
default_stacks = s.default_stack_dict
all_stacks = s.points_df()[["raw_points", "points","salary", "sp_mu", "raw_talent",
                            "ump_avg", "venue_avg", "env_avg", "sp_avg", "mz", 'z']].sort_values(by='z', ascending=False)

all_pitchers = s.p_df()[['name', 'team','points', 'fd_salary', 'pitches_start', 'mu','raw_mu', 'k_pred', 'k_pred_raw', 
                         'fd_id', 'venue_avg', 'ump_avg', 'venue_temp', 'exp_ps_raw', 'exp_inn', 'fav', 'env_points', 'mz', 'z']].sort_values(by='k_pred', ascending=False)


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
first = s.first_df[['name', 'points', 'exp_ps_sp_pa', 'exp_ps_sp_raw', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
second = s.second_df[['name', 'points', 'exp_ps_sp_pa', 'exp_ps_sp_raw', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
ss = s.ss_df[['name', 'points', 'exp_ps_sp_pa', 'exp_ps_sp_raw', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
third = s.third_df[['name', 'points', 'exp_ps_sp_pa', 'exp_ps_sp_raw', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
of = s.of_df[['name', 'points', 'exp_ps_sp_pa', 'exp_ps_sp_raw', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
util = s.util_df[['name', 'points', 'exp_ps_sp_pa', 'exp_ps_sp_raw', 'fd_salary']].sort_values(by=position_sort_key, ascending=False)
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

