from dfs_tools_mlb.compile.fanduel import FDSlate
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.compile.stats_mlb import get_statcast_p, get_p_diff, get_statcast_h
from dfs_tools_mlb import settings
from dfs_tools_mlb.utils.storage import pickle_path
import pickle
clean_fd_dir = False
get_pitcher_statcast = False
if clean_fd_dir:
    from dfs_tools_mlb.utils.storage import clean_directory
    clean_directory(settings.FD_DIR, force_delete = True)
    
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
cols_of_interest = ['name', 'points', 'exp_ps_sp_pa', 'sp_split', 'fd_salary']
position_sort_key = 'sp_split'
s=FDSlate(   slate_number = 1, 
             lineups = 150,
             p_fades = [],
             h_fades=[],
             no_stack=[],
             stack_threshold = 150, 
             pitcher_threshold = 150,
             heavy_weight_stack=True, 
             heavy_weight_p=True,
             stack_salary_factor = False,
             salary_batting_order=5,
             default_stack_count = 15,
             name_merge_ratio = .8
          )

    
hitters = s.get_hitters()
pitchers = s.get_pitchers()
h_ids = hitters[['name', 'mlb_id', 'fd_id']]
p_ids = pitchers[['name', 'mlb_id', 'fd_id']]
h_ids[h_ids['name'].str.contains('Ohtani')]
p_ids[p_ids['name'].str.contains('Ohtani')]
hitters.columns.tolist()
h_x_stats = hitters[['name', 'points_salary', 'fd_id', 'fd_ps_pa']]
p_x_stats = pitchers[['name', 'pitches_inning', 'fd_wps_b_sp', 'fd_wpa_b_sp','pitches_start']]

# pitchers.columns.tolist()
lineups = s.build_lineups(
    no_secondary = [], info_only = False,
                  lus = 150, 
                  index_track = 0, 
                  non_random_stack_start=0,
                  max_lu_total = 70,
                  variance = 0, 
                  below_avg_count = 25,
                  below_order_count = 60,
                  risk_limit = 125,
                  of_count_adjust = 5,
                  max_lu_stack = 150, 
                  max_sal = 35000,
                  min_avg_post_stack = 2700,
                  stack_size = 4,
                  stack_sample = 4, 
                  stack_salary_pairing_cutoff=4,
                  stack_max_pair = 4,
                  fallback_stack_sample = 5,
                  stack_expand_limit = 10000,
                  max_order=9,
                  all_in_replace_order_stack = 7,
                  all_in_replace_order_sec = 7,
                  supreme_replace_order = 4,
                  non_stack_max_order=9,
                  first_max_order = 9,
                  second_max_order = 9,
                  ss_max_order = 9,
                  third_max_order = 9,
                  of_max_order = 9,
                  util_max_order = 9,
                  util_replace_filt = 300,
                  all_in_replace_filt = 300,
                  all_in_diff_filt = 300,
                  non_stack_quantile = .9, 
                  high_salary_quantile = .9,
                  secondary_stack_cut = 0,
                  no_surplus_cut = 0,
                  single_stack_surplus = 500,
                  double_stack_surplus = 1000,
                  pitcher_surplus = 1000,
                  median_pitcher_salary = None,
                  low_pitcher_salary = None,
                  high_pitcher_salary = None,
                  max_pitcher_salary = None,
                  median_team_salary = None,
                  low_stack_salary = None,
                  high_stack_salary = None,
                  max_stack_salary = None,
                  no_surplus_secondary_stacks=True,
                  find_cheap_stacks = True,
                  always_pitcher_first=True,
                  enforce_pitcher_surplus = True,
                  enforce_hitter_surplus = False,
                  filter_last_replaced=False,
                  always_replace_pitcher_lock=False,
                  p_sal_stack_filter = False,
                  replace_secondary_all_in = True,
                  replace_primary_all_in = True,
                  never_replace_primary_supreme = False,
                  expand_utility = False,
                  decrease_stack_salary = True,
                  factor_salary_secondary = False,
                  increase_stack_salary = True,
                  increase_secondary_salary = True,
                  exempt=[],
                  all_in=[],
                  supreme_all_in=[],
                  no_supreme_replace=[],
                  supreme_all_in_pos=['1b', 'util', '2b', 'of', 'of.1', 'of.2', '3b', 'ss'],
                  lock = [],
                  no_combos = [],
                  never_replace=[],
                  never_replace_secondary=[],
                  never_replace_primary=[],
                  x_fallback = [],
                  stack_only = [],
                  no_stack_salary_decrease = [],
                  limit_risk = [],
                  no_combine = [],
                  always_replace_first = [],
                  no_replace = [],
                  custom_counts={},
                  always_find_cheap = [],
                  no_secondary_replace = [],
                  no_stack_replace = [],
                  always_replace = [],
                  never_fill = [],
                  no_utility = [],
                  complete_fade = [],
                  remove_positions = None,
                  custom_stacks = None,
                  custom_pitchers = None,
                  custom_secondary = None,
                  custom_stacks_info = None,
                  fill_high_salary_first = True,
                  select_max_pitcher = True,
                  last_lu = [],
                  filter_last_lu = False,
                  select_max_stack = True,
                  select_non_random_stack = False,
                  shuffle_positions_stack = True,
                  shuffle_positions_non_stack = True,
                  all_in_auto = True,
                  custom_stack_order = {},
                  always_pair_first = {},
                  always_pair_first_sec = {},
                  never_pair = {},
                  never_pair_sec = {},
                  locked_lineup = None,
                  expand_pitchers = False,
                  find_cheap_primary = False,
                  while_loop_secondary = True,
                  stack_top_order = False,
                  info_stack_key = 'points',
                  stack_info_order = 7,
                  stack_info_size = 4,
                  stack_info_salary_factor = False,
                  dupe_lu_replacement_pos = ['1b', 'util', '2b', 'of', 'of.1', 'of.2', '3b', 'ss'])



default_pitcher = s.p_df()['points'].to_dict()
# # team: stacks to build
default_stacks = s.default_stack_dict
all_stacks = s.points_df()[["raw_points", "points","salary", "sp_mu", "raw_talent",
                            "ump_avg", "venue_avg", "env_avg", "sp_avg", "mz", 'z']].sort_values(by='points', ascending=False)
all_stacks['raw_talent']
all_pitchers = s.p_df()[['name', 'team','points', 'fd_salary', 'pitches_start', 'mu','raw_mu', 'k_pred', 'k_pred_raw', 
                         'fd_id', 'venue_avg', 'ump_avg', 'venue_temp', 'exp_ps_raw', 'exp_inn', 'fav', 'env_points', 'mz', 'z']].sort_values(by='z', ascending=False)
all_stacks['total'] = all_stacks['mz'] + all_stacks['z']
all_stacks['total'].nlargest(100)
#get pitcher statcast info for slate
if get_pitcher_statcast:
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


first = s.first_df[cols_of_interest].sort_values(by=position_sort_key, ascending=False)
second = s.second_df[cols_of_interest].sort_values(by=position_sort_key, ascending=False)
ss = s.ss_df[cols_of_interest].sort_values(by=position_sort_key, ascending=False)
third = s.third_df[cols_of_interest].sort_values(by=position_sort_key, ascending=False)
of = s.of_df[cols_of_interest].sort_values(by=position_sort_key, ascending=False)
util = s.util_df[cols_of_interest].sort_values(by=position_sort_key, ascending=False)
#lookup individual hitter statcast

def get_by_name(name, df = h_ids):
    return df[df['name'].str.lower().str.contains(name.lower())]
def possible_combos(iterable, combo_len = 2):
    from itertools import combinations
    return len(list(combinations(list(iterable), combo_len)))
def h_ind(i_id, year=2022, hitter_statcast_time = str(tf.fifteen_days)):
    hitter_plays = get_statcast_h(i_id, year)
    filtered_hitter_plays = hitter_plays[(hitter_plays['date'] >= hitter_statcast_time)].reset_index()
    filtered_hitter_plays['distance'].count()
    return filtered_hitter_plays.describe()

track_df = pd.DataFrame(lineups)
track_df.index
try:
    track_df.loc['combos', 'twins']
except:
    pass
#AFTER BUILDING LINEUPS
pc_df = s.p_counts()
pc_index = pc_df['t_count'].nlargest(60).index
hc_df = s.h_counts()
hc_index = hc_df['t_count'].nlargest(60).index
pitcher_counts = pc_df.loc[pc_index, ['name', 't_count', 'fd_salary', 'team', 'fd_id','points','pitches_start', 'batters_faced_sp', 'exp_ps_raw']]
hitter_counts = hc_df.loc[hc_index, ['name', 't_count',  'fd_salary', 'order',  'fd_position', 'fd_id', 'team', 'points', 'fd_wps_pa' ]].set_index('name') 

default_pitcher
all_pitchers
default_stacks
all_stacks
s.active_teams
s.stacks_df()['stacks'].to_dict()
s.p_lu_df()[['name', 'points', 'lus']]
get_by_name('brett', h_ids)
get_by_name('brett', p_ids)
h_ind(621433)
s.finalize_entries() 
