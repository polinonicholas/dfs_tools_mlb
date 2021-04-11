from dfs_tools_mlb.compile.fanduel import FDSlate





s=FDSlate(
    slate_number=2,
          p_fades = [],
          h_fades = [],
          no_stack = []
          )


s.get_pitchers()
s.get_hitters()


x = s.build_lineups(
                    variance = 15,
                    util_replace_filt = 100,
                    custom_stacks = {
  'nationals': 10,
  'diamondbacks': 35,
  'reds': 20,
  'braves': 20,
  'phillies': 0,
  'blue jays': 0,
  'angels': 0,
  'orioles': 20,
  'red sox': 30,
  'rangers': 0,
  'padres': 15},
                    custom_pitchers = {6: 30, 4:50 , 1: 10, 0: 20, 3:20, 9: 20},
                    x_fallback = [],
                    stack_only = [],
                    below_avg_count = 30,
                    enforce_pitcher_surplus = False,
                    limit_risk = [],
                    risk_limit = 30
                    )






s.finalize_entries()                      
                      
#hitter counts
hc_df = s.h_counts()
hc_index = hc_df['t_count'].nlargest(60).index
import pandas as pd
pd.set_option('display.max_columns', None)
hitter_counts = hc_df.loc[hc_index, ['name', 't_count', 'team', 'fd_salary', 'points', 'fd_wps_pa','order',  'fd_position', 'fd_id']]  
hc_df['fd_wps_pa'].describe()
hc_splits = hc_df.loc[hc_index, ['name', 'fd_wps_pa','fd_wps_pa_vr', 'fd_wps_pa_vl','fd_id']]

#final stack information
h_stack_df = s.stacks_df()
stack_count = h_stack_df['stacks'].nlargest(60)
stack_info = h_stack_df[['s_z', 'p_z', 'z', 'points', 'salary', 'stacks']].sort_values(by='points', ascending=False)                
                
#used pitcher information 
pc_df = s.p_counts()
pc_index = pc_df['t_count'].nlargest(60).index
pitcher_counts = pc_df.loc[pc_index, ['name', 't_count', 'fd_salary', 'team', 'points','pitches_start', 'batters_faced_sp', 'games_sp', 'k_sp']]
#dict p_df.index: p_df: lineups to be in
initial_pitcher = s.p_lu_df()['lus'].to_dict()
# team: stacks to build
initial_stacks = s.stacks_df()['stacks'].to_dict()

# hitter_counts['fd_salary'].quantile(.85)
all_stacks = s.points_df()[['points', 'salary']].sort_values(by='points')
all_pitchers = s.p_df()[['name', 'points', 'fd_salary', 'pitches_start']].sort_values(by='points')



# {'marlins': 5,
#   'mets': 25,
#   'yankees': 30,
#   'rays': 5,
#   'mariners': 15,
#   'twins': 40,
#   'brewers': 15,
#   'cardinals': 15}

# {3: 100, 0:30 , 9: 20}





# def __init__(self, 
#              entries_file=config.get_fd_file(), 
#              slate_number = 1, 
#              lineups = 150,
                  
#              p_fades = [],
                  
#              h_fades=[],
                  
#              no_stack=[],
                  
#              stack_points_weight = 2, 
#              stack_threshold = 50, 
#              pitcher_threshold = 75,
#                   heavy_weight_stack=False, 
#                   heavy_weight_p=False, 
#                   max_batting_order=7):

    
# (self, 
#   lus = 150, 
#   index_track = 0, 
#   max_surplus = 600, 
#   max_lu_total = 75,                     
#   max_lu_stack = 50, 
#   max_sal = 35000, 
#   stack_sample = 5, 
#   util_replace_filt = 0,                     
#   variance = 25, 
#   non_stack_quantile = .80, 
#   high_salary_quantile = .80,                     
#   enforce_pitcher_surplus = False,                     
#   enforce_hitter_surplus = True,                      
#   non_stack_max_order=6,                       
#   custom_counts={},                     
#   fallback_stack_sample = 6,                      
#   custom_stacks = None,                     
#   custom_pitchers = None,                     
#   x_fallback = [],                      
#   stack_only = [],
#   below_avg_count = 20,  
#   stack_expand_limit = 15,
#   of_count_adjust = 10,
#   limit_risk = [],
#   risk_limit = 30):
    
    
    
    
    
    
    
                      