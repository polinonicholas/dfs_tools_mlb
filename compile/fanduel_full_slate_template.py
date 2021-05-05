from dfs_tools_mlb.compile.fanduel import FDSlate
import pandas as pd
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
#                  max_batting_order=7):
        
s=FDSlate(
    
          p_fades = [],
          h_fades = [],
          no_stack = [],
          max_batting_order=7,
          stack_threshold = 35, 
          
          
          )

pitchers = s.get_pitchers()
hitters = s.get_hitters()

p_auto = s.p_lu_df()['lus'].to_dict()
stack_auto = s.stacks_df()
stack_count = stack_auto['stacks'].nlargest(60)
stack_info = stack_auto[['s_z', 'p_z', 'z', 'points', 'salary', 'stacks', 'sp_mu']].sort_values(by='points', ascending=False)


# def build_lineups(self, lus = 150, 
#                       index_track = 0, 
#                       max_lu_total = 75,
#                       max_lu_stack = 50, 
#                       max_sal = 35000, 
#                       stack_sample = 5, 
#                       util_replace_filt = 300,
#                       variance = 25, 
#                       non_stack_quantile = .80, 
#                       high_salary_quantile = .80,
#                       enforce_pitcher_surplus = True,
#                       enforce_hitter_surplus = True, 
#                       non_stack_max_order=5, 
#                       custom_counts={},
#                       fallback_stack_sample = 6,
#                       custom_stacks = None,
#                       custom_pitchers = None,
#                       custom_secondary = None,
#                       x_fallback = [],
#                       stack_only = [],
#                       below_avg_count = 15,
#                       stack_expand_limit = 20,
#                       of_count_adjust = 6,
#                       limit_risk = [],
#                       risk_limit = 25,
#                       exempt=[],
#                       stack_size = 4,
#                       secondary_stack_cut = 50,
#                       no_surplus_cut = 100,
#                       single_stack_surplus = 600,
#                       double_stack_surplus = 1200,
#                       pitcher_surplus = 1000,
#                       no_secondary = [],
#                       lock = [],
#                       no_surplus_secondary_stacks=True,
#                       find_cheap_stacks = False,
#                       all_in=[]):
lineups = s.build_lineups(
                      lus = 150, 
                      index_track = 0, 
                      max_lu_total = 75,
                      variance = 25,
                      below_avg_count = 15,
                      of_count_adjust = 6,
                      max_sal = 35000, 
                      stack_sample = 5,
                      fallback_stack_sample = 6,
                      stack_expand_limit = 20,
                      non_stack_max_order=5, 
                      enforce_pitcher_surplus = True,
                      enforce_hitter_surplus = True, 
                      custom_counts={},
                      x_fallback = [],
                      stack_only = [],
                      all_in=[],
                      exempt=[],
                      secondary_stack_cut = 50,
                      no_surplus_cut = 100,
                      single_stack_surplus = 600,
                      double_stack_surplus = 1200,
                      pitcher_surplus = 1000,
                      lock = [],
                      find_cheap_stacks = False,
                      custom_stacks = None,
                      custom_pitchers = None,
                      custom_secondary = None,
                      )

s.finalize_entries()     

#dict p_df.index: p_df: lineups to be in
default_pitcher = s.p_df()['points'].to_dict()
# # team: stacks to build
default_stacks = s.default_stack_dict

all_stacks = s.points_df()[['points', 'salary', 'sp_mu', 'raw_talent']].sort_values(by='points', ascending=False)
all_stacks['p_z'] = (all_stacks['points'] - all_stacks['points'].mean()) / all_stacks['points'].std()
all_stacks['s_z'] = ((all_stacks['salary'] - all_stacks['salary'].mean()) / all_stacks['salary'].std()) * -1
all_stacks['mu_z'] = (all_stacks['sp_mu'] - all_stacks['sp_mu'].mean()) / all_stacks['sp_mu'].std()
all_stacks['t_z'] = (all_stacks['raw_talent'] - all_stacks['raw_talent'].mean()) / all_stacks['raw_talent'].std()
all_stacks['z'] = (all_stacks['p_z'] * 1) + (all_stacks['s_z'] * 1) + (all_stacks['mu_z'] * 1) + (all_stacks['t_z'] * 1) 
stack_z = all_stacks[['p_z', 's_z', 'mu_z', 't_z', 'z']].sort_values(by='z', ascending=False)


all_pitchers = s.p_df()[['name', 'points', 'fd_salary', 'pitches_start']].sort_values(by='points', ascending=False)
all_pitchers['p_z'] = (all_pitchers['points'] - all_pitchers['points'].mean()) / all_pitchers['points'].std()
all_pitchers['rmu_z'] = (all_pitchers['raw_mu'] - all_pitchers['raw_mu'].mean()) / all_pitchers['raw_mu'].std()
all_pitchers['mu_z'] = (all_pitchers['mu'] - all_pitchers['mu'].mean()) / all_pitchers['mu'].std()
all_pitchers['kp_z'] = (all_pitchers['k_pred'] - all_pitchers['k_pred'].mean()) / all_pitchers['k_pred'].std()
all_pitchers['rk_z'] = (all_pitchers['k_pred_raw'] - all_pitchers['k_pred_raw'].mean()) / all_pitchers['k_pred_raw'].std()
all_pitchers['s_z'] = ((all_pitchers['fd_salary'] - all_pitchers['fd_salary'].mean()) / all_pitchers['fd_salary'].std()) * -1
all_pitchers['pps_z'] = (all_pitchers['pitches_start'] - all_pitchers['pitches_start'].mean()) / all_pitchers['pitches_start'].std()
all_pitchers['z'] = (all_pitchers['p_z'] * 1) + (all_pitchers['rmu_z'] * 1) + (all_pitchers['mu_z'] * 1) + (all_pitchers['kp_z'] * 1) + (all_pitchers['rk_z'] * 1) + (all_pitchers['s_z'] * 1) + (all_pitchers['pps_z'] * 1)
pitcher_z =  all_pitchers[['name', 'p_z','mu_z', 'rmu_z', 'kp_z', 'name','rk_z', 's_z', 'pps_z', 'z']].sort_values(by='z', ascending=False)

#AFTER BUILDING LINEUPS
pc_df = s.p_counts()
pc_index = pc_df['t_count'].nlargest(60).index
hc_df = s.h_counts()
hc_index = hc_df['t_count'].nlargest(60).index
pitcher_counts = pc_df.loc[pc_index, ['name', 't_count', 'fd_salary', 'team', 'fd_id','points','pitches_start', 'batters_faced_sp', 'exp_ps_raw']]
hitter_counts = hc_df.loc[hc_index, ['name', 't_count', 'team', 'fd_salary', 'points', 'fd_wps_pa','order',  'fd_position', 'fd_id']]  




