from dfs_tools_mlb.compile.fanduel_single import FDSlateSingle
import pandas as pd
pd.set_option('display.max_columns', None)

# def __init__(self, 
#                  entries_file=config.get_fd_file(), 
#                  slate_number = 1, 
#                  lineups = 150,
#                  stack_points_weight = 2, 
#                  stack_threshold = 120, 
#                  heavy_weight_stack=False, 
#                  max_batting_order=7,
#                  h_fades=[]):
s=FDSlateSingle(lineups = 51,
    
          
          )


hitters = s.get_hitters()

# p_auto = s.p_lu_df()['lus'].to_dict()

# stack_auto.index

# stack_auto = s.stacks_df()
# stack_count = stack_auto['stacks'].nlargest(60)
# stack_info = stack_auto[['s_z', 'p_z', 'z', 'points', 'salary', 'stacks', 'sp_mu']].sort_values(by='points', ascending=False)


# def build_lineups(self, 
#                       lus = 150, 
#                       index_track = 0, 
#                       max_lu_total = 90,
#                       max_sal = 35000, 
#                       stack_sample = 4, 
#                       util_replace_filt = 1000,
#                       variance = 0, 
#                       non_stack_max_order=5, 
#                       custom_counts={},
#                       custom_stacks = None,
#                       below_avg_count = 30,
#                       stack_expand_limit = 30,
#                       exempt=[],
#                       full_stack_cutoff = 50
#                       ):
lineups = s.build_lineups(

                    
                    lus=51,
                    custom_stacks=None,
                    full_stack_cutoff = 150
                    
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
             





