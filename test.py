from dfs_tools_mlb.compile.fanduel import FDSlate

s=FDSlate(
          slate_number = 2,
          lineups = 150,
          p_fades = ['red sox','dodgers', 'giants', 'cubs', 'brewers',
                     'padres', 'mariners', 'angels', 'astros', 'phillies',
                     'athletics', 'rays'],
          h_fades = ['phillies', 'mariners'],
          no_stack = ['mariners', 'phillies','astros'],
          stack_points_weight = 1.4,
          stack_threshold = 50,
          pitcher_threshold = 130)


# s.get_pitchers()
# s.get_hitters()

x = s.build_lineups(index_track = 0,
                    lus = 150,
                    max_order = 7,
                    max_surplus = 400,
                    max_lu_total = 75,
                    variance = 15,
                    max_lu_stack = 50,
                    max_sal = 35000,
                    stack_sample = 6,
                    util_replace_filt = 0,
                    non_stack_quantile = .9,
                    high_salary_quantile = .95,
                    enforce_hitter_surplus = True,
                    enforce_pitcher_surplus = True)
#hitter counts
hc_df = s.h_counts()
hc_index = hc_df['t_count'].nlargest(60).index
hitter_counts = hc_df.loc[hc_index, ['name', 't_count', 'team', 'fd_salary', 'points',  'order',  'fd_position']]             

#final stack information
h_stack_df = s.stacks_df()
stack_count = h_stack_df['stacks'].nlargest(60)
stack_info = h_stack_df[['s_z', 'p_z', 'z', 'points', 'salary', 'stacks']].sort_values(by='points', ascending=False)                
                
#used pitcher information 
pc_df = s.p_counts()
pc_index = pc_df['t_count'].nlargest(60).index
pitcher_counts = pc_df.loc[pc_index, ['name', 't_count', 'fd_salary', 'team', 'points', 'pitches_start', 'batters_faced_sp']]
                
                
#dict p_df.index: p_df: lineups to be in
initial_pitcher = s.p_lu_df()['lus'].to_dict()
# team: stacks to build
initial_stacks = s.stacks_df()['stacks'].to_dict()


# s.finalize_entries()

# hitter_counts['fd_salary'].quantile(.90)
