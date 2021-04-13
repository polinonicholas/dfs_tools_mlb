from dfs_tools_mlb.compile.fanduel import FDSlate
import pandas as pd
pd.set_option('display.max_columns', None)

s=FDSlate(
   
          p_fades = [],
          h_fades = [],
          no_stack = []
          )


s.get_pitchers()
s.get_hitters()


x = s.build_lineups(
                    variance = 25,
                    util_replace_filt = 100,
                    custom_stacks = None,
                    custom_pitchers = None,
                    x_fallback = [],
                    stack_only = [],
                    limit_risk=[],
                    enforce_pitcher_surplus=False
                    )
s.finalize_entries()     

#dict p_df.index: p_df: lineups to be in
default_pitcher = s.p_df()['points'].to_dict()
# # team: stacks to build
default_stacks = s.default_stack_dict

all_stacks = s.points_df()[['points', 'salary']].sort_values(by='points')
all_pitchers = s.p_df()[['name', 'points', 'fd_salary', 'pitches_start', 'team', 'opp']].sort_values(by='points')

#AFTER BUILDING LINEUPS
pc_df = s.p_counts()
pc_index = pc_df['t_count'].nlargest(60).index
hc_df = s.h_counts()
hc_index = hc_df['t_count'].nlargest(60).index
h_stack_df = s.stacks_df()
stack_count = h_stack_df['stacks'].nlargest(60)

pitcher_counts = pc_df.loc[pc_index, ['name', 't_count', 'fd_salary', 'team', 'fd_id','points','pitches_start', 'batters_faced_sp', 'exp_ps_raw']]
hitter_counts = hc_df.loc[hc_index, ['name', 't_count', 'team', 'fd_salary', 'points', 'fd_wps_pa','order',  'fd_position', 'fd_id']]  
stack_info = h_stack_df[['s_z', 'p_z', 'z', 'points', 'salary', 'stacks', 'sp_mu']].sort_values(by='points', ascending=False)             



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
#   below_avg_count = 25,  
#   stack_expand_limit = 15,
#   of_count_adjust = 12,
#   limit_risk = [],
#   risk_limit = 25):
    
    
    
    # hitter_counts['fd_salary'].quantile(.85)
    
    
    
                      