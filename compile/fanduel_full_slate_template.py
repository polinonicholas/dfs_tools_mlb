from dfs_tools_mlb.compile.fanduel import FDSlate
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.compile.stats_mlb import get_statcast_p, get_p_diff, get_statcast_h
from dfs_tools_mlb import settings
from dfs_tools_mlb.utils.storage import pickle_path
import pickle
pd.set_option('display.max_columns', None)

# def __init__(self, 
#                  entries_file=config.get_fd_file(), 
#                  slate_number = 1, 
#                  lineups = 150,
#                  p_fades = [],
#                  h_fades=[],
#                  no_stack=[],
#                  stack_points_weight = 2, 
#                  stack_threshold = 50, 
#                  pitcher_threshold = 75,
#                  heavy_weight_stack=False, 
#                  heavy_weight_p=False, 
#                  salary_batting_order=5):
        
s=FDSlate(
          p_fades = [],
          h_fades = [],
          salary_batting_order=5,
          )

s.active_teams

# for team in s.team_instances:
#     if team.name in s.active_teams:
#         team.del_next_game()



hitters = s.get_hitters()
pitchers = s.get_pitchers()
hitter_mlb_ids = hitters[['name', 'mlb_id']]

# p_auto = s.p_lu_df()['lus'].to_dict()
# stack_auto = s.stacks_df()
# stack_count = stack_auto['stacks'].nlargest(60)
# stack_info = stack_auto[['s_z', 'p_z', 'z', 'points', 'salary', 'stacks', 'sp_mu']].sort_values(by='points', ascending=False)


  # def build_lineups(self, no_secondary = [], info_only = False,
  #                     lus = 150, 
  #                     index_track = 0, 
  #                     non_random_stack_start=0,
  #                     max_lu_total = 150,
  #                     variance = 0, 
  #                     below_avg_count = 0,
  #                     below_order_count = 135,
  #                     risk_limit = 125,
  #                     of_count_adjust = 0,
  #                     max_lu_stack = 50, 
  #                     max_sal = 35000, 
  #                     stack_size = 4,
  #                     stack_sample = 5, 
  #                     stack_salary_pairing_cutoff=0,
  #                     stack_max_pair = 5,
  #                     fallback_stack_sample = 6,
  #                     stack_expand_limit = 20,
  #                     max_order=7,
  #                     non_stack_max_order=6, 
  #                     util_replace_filt = 200,
  #                     non_stack_quantile = .9, 
  #                     high_salary_quantile = .9,
  #                     secondary_stack_cut = 0,
  #                     no_surplus_cut = 0,
  #                     single_stack_surplus = 400,
  #                     double_stack_surplus = 400,
  #                     pitcher_surplus = 1000,
  #                     median_pitcher_salary = None,
  #                     low_pitcher_salary = None,
  #                     high_pitcher_salary = None,
  #                     max_pitcher_salary = None,
  #                     median_team_salary = None,
  #                     low_stack_salary = None,
  #                     high_stack_salary = None,
  #                     max_stack_salary = None,
  #                     no_surplus_secondary_stacks=True,
  #                     find_cheap_stacks = True,
  #                     always_pitcher_first=True,
  #                     enforce_pitcher_surplus = True,
  #                     enforce_hitter_surplus = True,
  #                     filter_last_replaced=True,
  #                     always_replace_pitcher_lock=False,
  #                     p_sal_stack_filter = True,
  #                     exempt=[],
  #                     all_in=[],
  #                     lock = [],
  #                     no_combos = [],
  #                     never_replace=[],
  #                     x_fallback = [],
  #                     stack_only = [],
  #                     limit_risk = [],
  #                     no_combine = [],
  #                     always_replace_first = [],
  #                     custom_counts={},
  #                     remove_positions = None,
  #                     custom_stacks = None,
  #                     custom_pitchers = None,
  #                     custom_secondary = None,
  #                     custom_stacks_info = None):
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
                      stack_max_pair = 5,
                      
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
                      stack_sample = 5, 
                      
                      
                      fallback_stack_sample = 6,
                      stack_expand_limit = 20,
                      
                      max_order=7,
                      non_stack_max_order=6, 
                     
                      non_stack_quantile = .9, 
                      high_salary_quantile = .9,
                      
                      secondary_stack_cut = 0,
                      no_surplus_cut = 0,
                      
                      max_sal = 35000,
                      util_replace_filt = 200,
                      single_stack_surplus = 400,
                      double_stack_surplus = 400,
                      pitcher_surplus = 1000,
                      
                      
                      fill_high_salary_first = False,
                      select_max_pitcher = True,
                      no_surplus_secondary_stacks=True,
                      find_cheap_stacks = True,
                      always_pitcher_first=True,
                      enforce_pitcher_surplus = True,
                      enforce_hitter_surplus = True,
                      filter_last_replaced=True,
                      always_replace_pitcher_lock=False,
                      p_sal_stack_filter = True,
                      remove_positions = None,
                      
                      exempt=[],
                      all_in=[],
                      lock = [],
                      #pitcher
                      no_combos = [],
                      #position
                      never_replace=[],
                      x_fallback = [],
                      stack_only = [],
                      limit_risk = [],
                      #stacks/h
                      no_combine = [],
                      always_replace_first = [],
                      custom_counts={},
                      custom_pitchers = None,
                      custom_stacks = None,
                      custom_secondary = None,
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
all_stacks = s.points_df()[["raw_points", "venue_points", "temp_points", 
                            "points", "salary", "sp_mu", "raw_talent",
                            "ump_avg", "venue_avg", "venue_temp"]].sort_values(by='points', ascending=False)
for team in s.team_instances:
    if team.ppd and team.name in s.active_teams:
        filt = all_stacks[(all_stacks.index == team.name) | (all_stacks.index == team.name)].index
        all_stacks.drop(filt, inplace = True)

all_stacks['u_z'] = (all_stacks['ump_avg'] - all_stacks['ump_avg'].mean()) / all_stacks['ump_avg'].std()
all_stacks['v_z'] = (all_stacks['venue_avg'] - all_stacks['venue_avg'].mean()) / all_stacks['venue_avg'].std()
all_stacks['vt_z'] = (all_stacks['venue_temp'] - all_stacks['venue_temp'].mean()) / all_stacks['venue_temp'].std()

all_stacks['p_z'] = (all_stacks['raw_points'] - all_stacks['raw_points'].mean()) / all_stacks['raw_points'].std()
all_stacks['s_z'] = ((all_stacks['salary'] - all_stacks['salary'].mean()) / all_stacks['salary'].std()) * -1
all_stacks['mu_z'] = (all_stacks['sp_mu'] - all_stacks['sp_mu'].mean()) / all_stacks['sp_mu'].std()
all_stacks['t_z'] = (all_stacks['raw_talent'] - all_stacks['raw_talent'].mean()) / all_stacks['raw_talent'].std()
all_stacks['z'] = ((all_stacks['p_z'] * 2) + (all_stacks['s_z'] * 2) + (all_stacks['mu_z'] * 2) + (all_stacks['t_z'] * 1.75)  + (all_stacks['u_z'] * 0) + (all_stacks['v_z'] * 2.25)) / 10
all_stacks['mz'] = ((all_stacks['p_z'] * 2.5) + (all_stacks['s_z'] * 0) + (all_stacks['mu_z'] * 2.5) + (all_stacks['t_z'] * 2.25) + (all_stacks['u_z'] * 0) + (all_stacks['v_z'] * 2.75)) / 10
all_pitchers = s.p_df()[['name', 'team','points', 'fd_salary', 'pitches_start', 'mu','raw_mu', 'k_pred', 'k_pred_raw', 
                         'fd_id', 'venue_avg', 'ump_avg', 'venue_temp', 'exp_ps_raw', 'exp_inn', 'fav']].sort_values(by='points', ascending=False)


for team in s.team_instances:
    if team.ppd and team.name in s.active_teams:
        filt = all_pitchers[(all_pitchers['team'] == team.opp_name) | (all_pitchers['team'] == team.name)].index
        all_pitchers.drop(filt, inplace = True)



all_pitchers.loc

#################################
vt_filt = (all_pitchers['venue_temp'] == '')
all_pitchers.loc[vt_filt, 'venue_temp'] = 72
all_pitchers['i_z'] = (all_pitchers['exp_inn'] - all_pitchers['exp_inn'].mean()) / all_pitchers['exp_inn'].std()
all_pitchers['f_z'] = (all_pitchers['fav'] - all_pitchers['fav'].mean()) / all_pitchers['fav'].std()
all_pitchers['vt_z'] = ((all_pitchers['venue_temp'] - all_pitchers['venue_temp'].mean()) / all_pitchers['venue_temp'].std()) * -1
all_pitchers['v_z'] = ((all_pitchers['venue_avg'] - all_pitchers['venue_avg'].mean()) / all_pitchers['venue_avg'].std()) * -1
all_pitchers['u_z'] = ((all_pitchers['ump_avg'] - all_pitchers['ump_avg'].mean()) / all_pitchers['ump_avg'].std()) * -1
all_pitchers['p_z'] = (all_pitchers['exp_ps_raw'] - all_pitchers['exp_ps_raw'].mean()) / all_pitchers['exp_ps_raw'].std()
all_pitchers['rmu_z'] = (all_pitchers['raw_mu'] - all_pitchers['raw_mu'].mean()) / all_pitchers['raw_mu'].std()
all_pitchers['mu_z'] = (all_pitchers['mu'] - all_pitchers['mu'].mean()) / all_pitchers['mu'].std()
all_pitchers['kp_z'] = (all_pitchers['k_pred'] - all_pitchers['k_pred'].mean()) / all_pitchers['k_pred'].std()
all_pitchers['rk_z'] = (all_pitchers['k_pred_raw'] - all_pitchers['k_pred_raw'].mean()) / all_pitchers['k_pred_raw'].std()
all_pitchers['s_z'] = ((all_pitchers['fd_salary'] - all_pitchers['fd_salary'].mean()) / all_pitchers['fd_salary'].std()) * -1
all_pitchers['pps_z'] = (all_pitchers['pitches_start'] - all_pitchers['pitches_start'].mean()) / all_pitchers['pitches_start'].std()
all_pitchers['z'] = ((all_pitchers['f_z'] * .5) + (all_pitchers['i_z'] * 1.5) + (all_pitchers['p_z'] * 1.25) + (all_pitchers['rmu_z'] * .75) + (all_pitchers['mu_z'] * 1.5) + (all_pitchers['kp_z'] * 1) + (all_pitchers['rk_z'] * .75) + (all_pitchers['s_z'] * 1) + (all_pitchers['pps_z'] * 0) + (all_pitchers['v_z'] * 1.5) + (all_pitchers['u_z'] * .25)) / 10
all_pitchers['mz'] = ((all_pitchers['f_z'] * .75) + (all_pitchers['i_z'] * 1.5) + (all_pitchers['p_z'] * 1.5) + (all_pitchers['rmu_z'] * .75) + (all_pitchers['mu_z'] * 1.5) + (all_pitchers['kp_z'] * 1) + (all_pitchers['rk_z'] * .75) + (all_pitchers['s_z'] * 0) + (all_pitchers['pps_z'] * .25) + (all_pitchers['v_z'] * 1.5) + (all_pitchers['u_z'] * .5)) / 10

pitcher_z =  all_pitchers[['name',
                           'p_z',
                           'i_z',
                           'f_z',
                           'mu_z',
                           'rmu_z',
                           'kp_z',
                           'rk_z',
                           's_z',
                           'pps_z',
                           'u_z',
                           'v_z',
                           'vt_z',
                           'z',
                           'mz']].sort_values(by='mz', ascending=False).set_index('name')
stack_z = all_stacks[['p_z',
                      's_z',
                      'mu_z',
                      't_z',
                      'u_z',
                      'v_z',
                      'vt_z',
                      'z', 
                      'mz']].sort_values(by='mz', ascending=False)

pitcher_points = all_pitchers[['name','points']].set_index('name')
pitcher_mz = pitcher_z[['mz', 'z', 'u_z', 'vt_z']]
pitcher_adj_z = pitcher_z[['z', 'mz']].sort_values(by='z', ascending=False)
stack_mz = stack_z[['mz', 'z', 'u_z', 'vt_z']].sort_values(by='mz', ascending=False)
stack_adj_z = stack_z[['z', 'mz', 'u_z', 'vt_z']].sort_values(by='z', ascending=False)


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

