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
          no_stack = []
          )

pitchers = s.get_pitchers()
hitters = s.get_hitters()

p_auto = s.p_lu_df()['lus'].to_dict()

stack_auto.index

stack_auto = s.stacks_df()
stack_count = stack_auto['stacks'].nlargest(60)
stack_info = stack_auto[['s_z', 'p_z', 'z', 'points', 'salary', 'stacks', 'sp_mu']].sort_values(by='points', ascending=False)


# def build_lineups(self, lus = 150, 
#                       index_track = 0, 
#                       max_lu_total = 75,
#                       max_lu_stack = 50, 
#                       max_sal = 35000, 
#                       stack_sample = 5, 
#                       util_replace_filt = 0,
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
#                       x_fallback = [],
#                       stack_only = [],
#                       below_avg_count = 25,
#                       stack_expand_limit = 15,
#                       of_count_adjust = 6,
#                       limit_risk = [],
#                       risk_limit = 25,
#                       exempt=[],
#                       stack_size = 4,
#                       secondary_stack_cut = 75,
#                       single_stack_surplus = 600,
#                       double_stack_surplus = 900,
#                       no_secondary = []):
lineups = s.build_lineups(

                    variance = 30,
                    util_replace_filt = 200,
                    custom_stacks = None,
                    custom_pitchers = None,
                    x_fallback = [],
                    secondary_stack_cut = 0,
                    stack_expand_limit = 15,
                    single_stack_surplus = 300,
                    double_stack_surplus = 1200,
                    no_secondary = [],
                    limit_risk = [],
                    below_avg_count = 15,
                    exempt=[],
                    
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
pitcher_counts = pc_df.loc[pc_index, ['name', 't_count', 'fd_salary', 'team', 'fd_id','points','pitches_start', 'batters_faced_sp', 'exp_ps_raw']]
hitter_counts = hc_df.loc[hc_index, ['name', 't_count', 'team', 'fd_salary', 'points', 'fd_wps_pa','order',  'fd_position', 'fd_id']]  
             





