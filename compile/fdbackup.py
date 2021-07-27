import pickle
import pandas
import numpy as np
from functools import cached_property
from dfs_tools_mlb import settings
from dfs_tools_mlb import config
from dfs_tools_mlb.utils.storage import pickle_path
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.compile import teams
from dfs_tools_mlb.utils.pd import modify_team_name
from dfs_tools_mlb.utils.pd import sm_merge_single
import random
from itertools import combinations 

class FDSlate:
    def __init__(self, 
                 entries_file=config.get_fd_file(), 
                 slate_number = 1, 
                 lineups = 150,
                 p_fades = [],
                 h_fades=[],
                 no_stack=[],
                 stack_points_weight = 2, 
                 stack_threshold = 150, 
                 pitcher_threshold = 150,
                 heavy_weight_stack=False, 
                 heavy_weight_p=False,
                 stack_salary_factor = True,
                 salary_batting_order=5):
        
        self.entry_csv = entries_file
        if not self.entry_csv:
            raise TypeError("There are no fanduel entries files in specified DL_FOLDER, obtain one at fanduel.com/upcoming")
        no_stack.extend(h_fades)            
        self.slate_number = slate_number
        self.lineups = lineups
        self.p_fades = p_fades
        self.h_fades = h_fades
        # daily_info = teams.Team.daily_info()
        # self.p_fades.extend(daily_info['rain'])
        self.points_weight = stack_points_weight
        self.stack_threshold = stack_threshold
        self.pitcher_threshold = pitcher_threshold
        self.no_stack = no_stack
        self.heavy_weight_stack = heavy_weight_stack
        self.heavy_weight_p = heavy_weight_p
        self.salary_batting_order = salary_batting_order
        self.stack_salary_factor = stack_salary_factor
        
    def entries_df(self, reset=False):
        df_file = pickle_path(name=f"lineup_entries_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if path.exists() and not reset:
            df = pandas.read_pickle(path)
        else:
            cols = ['entry_id', 'contest_id', 'contest_name', 'P', 'C/1B', '2B', '3B', 'SS', 'OF' ,'OF.1', 'OF.2', 'UTIL']
            csv_file = self.entry_csv
            with open(csv_file, 'r') as f:
                df = pandas.read_csv(f, usecols = cols)
            df = df[~df['entry_id'].isna()]
            df = df.astype({'entry_id': np.int64})
            with open(df_file, "wb") as f:
                pickle.dump(df, f)
        return df
    @cached_property
    def player_info_df(self):
        df_file = pickle_path(name=f"player_info_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if path.exists():
            df = pandas.read_pickle(path)
        else:   
            cols = ['Player ID + Player Name', 'Id', 'Position', 'First Name', 'Nickname', 'Last Name', 'FPPG', 'Salary', 'Game', 'Team', 'Opponent', 'Injury Indicator', 'Injury Details', 'Roster Position']
            csv_file = self.entry_csv
            with open(csv_file, 'r') as f:
                df = pandas.read_csv(f, skiprows = lambda x: x < 6, usecols=cols)
            df.rename(columns = {'Id': 'fd_id', 'Player ID + Player Name': 'fd_id_name', 'Nickname': 'name',
                                      'Position': 'fd_position', 'Roster Position': 'fd_r_position',
                                      'Injury Indicator': 'fd_injury_i', 'Injury Details': 'fd_injury_d',
                                      'Opponent': 'opp', 'First Name': 'f_name', 'Last Name': 'l_name',
                                      'Salary': 'fd_salary'}, inplace = True)
            
            
            df.columns = df.columns.str.lower()
            df = modify_team_name(df, columns = ['team', 'opp'])
            df.loc[df['fd_position'].isna(), 'fd_position'] = 'xxxx'
            df.loc[df['fd_r_position'].isna(), 'fd_r_position'] = 'xxxx'
            df['fd_position'] = df['fd_position'].str.replace('C', '1B').str.lower().str.split('/')
            df['fd_r_position'] = df['fd_r_position'].str.lower().str.split('/')
            
            p_idx = df[df['fd_position'].apply(lambda x: 'p' in x)].index
            df.loc[p_idx, 'is_p'] = True
            h_idx = df[df['fd_r_position'].apply(lambda x: 'util' in x)].index
            df.loc[h_idx, 'is_h'] = True
            with open(df_file, "wb") as f:
                pickle.dump(df, f)
        return df
    @cached_property
    def active_teams(self):
        return self.player_info_df['team'].unique()
    @cached_property
    def default_stack_dict(self):
        team_dict = {}
        for team in self.active_teams:
            team_dict[team] = 0
        return team_dict
    
    def insert_lineup(self, idx, lineup):
        cols = ['P', 'C/1B', '2B', '3B', 'SS', 'OF', 'OF.1', 'OF.2', 'UTIL']
        df = self.entries_df()
        df.loc[idx, cols] = lineup
        file = pickle_path(name=f"lineup_entries_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        with open(file, "wb") as f:
            pickle.dump(df, f)
        return f"Inserted {lineup} at index {idx}."
    def finalize_entries(self):
        df = self.entries_df()
        csv = self.entry_csv
        df.rename(columns = {'OF.1': 'OF', 'OF.2': 'OF'}, inplace=True)
        df.to_csv(csv, index=False)
        return f"Stored lineups at {csv}"
   
    @cached_property
    def team_instances(self):
        instances = set()
        for team in teams.Team:
            if team.name in self.active_teams:
                instances.add(team)
        return instances
    def get_hitters(self):
        df_file = pickle_path(name=f"all_h_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        for team in self.team_instances:
            hitters = team.lineup_df()
            merge = self.player_info_df[(self.player_info_df['team'] == team.name) & (self.player_info_df['is_h'] == True)]
            hitters = sm_merge_single(hitters, merge, ratio=.75, suffixes=('', '_fd'))     
            hitters.drop_duplicates(subset='mlb_id', inplace=True)
            order_filter = (hitters['order'] <= self.salary_batting_order)
            hitters['points_salary'] = hitters['points'] - (hitters['fd_salary'] / 1000)
            hitters_order = hitters[order_filter]
            team.salary = hitters_order['fd_salary'].sum() / len(hitters_order.index)
            cols = ["raw_points", "venue_points", "temp_points", "points", "salary", "sp_mu", "raw_talent",
                    'ump_avg', 'venue_temp', 'venue_avg', 'env_avg', 'sp_avg']
            points_file = pickle_path(name=f"team_points_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
            points_path = settings.FD_DIR.joinpath(points_file)
            if points_path.exists():
                p_df = pandas.read_pickle(points_path)
            else:
                p_df = pandas.DataFrame(columns = cols)
            p_df.loc[team.name, cols] = [team.raw_points, team.venue_points, team.temp_points, 
                                         team.points, team.salary, team.sp_mu, team.lu_talent_sp,
                                         team.ump_avg, team.venue_temp, team.venue_avg, team.env_avg,
                                         team.sp_avg()]
            with open(points_file, 'wb') as f:
                pickle.dump(p_df, f)
            if path.exists():
                df = pandas.read_pickle(path)
                df.drop(index = df[df['team'] == team.name].index, inplace=True)
                df = pandas.concat([df, hitters], ignore_index=True)
            else:
                df = hitters
            with open(df_file, "wb") as f:
                 pickle.dump(df, f)
        
        return df
    def get_pitchers(self):
        df_file = pickle_path(name=f"all_p_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        for team in self.team_instances:
            #incase of PPD, sometimes a team's game will register vs. another team in next game.
            try:
                pitcher = team.sp_df()
                pitcher['fav'] = team.points - team.opp_instance.points
                merge = self.player_info_df[(self.player_info_df['team'] == team.name) & (self.player_info_df['is_p'] == True)]
                try:
                    pitcher = sm_merge_single(pitcher, merge, ratio=.75, suffixes=('', '_fd')) 
                except KeyError:
                    continue
                pitcher.drop_duplicates(subset='mlb_id', inplace=True)
                if path.exists():
                    df = pandas.read_pickle(path)
                    df.drop(index = df[df['team'] == team.name].index, inplace=True)
                    df = pandas.concat([df, pitcher], ignore_index=True)
                else:
                    df = pitcher
                with open(df_file, "wb") as f:
                     pickle.dump(df, f)
            except AttributeError:
                continue
        # df['p_z'] = (df['points'] - df['points'].mean()) / df['points'].std()
        # df['rmu_z'] = (df['raw_mu'] - df['raw_mu'].mean()) / df['raw_mu'].std()
        # df['mu_z'] = (df['mu'] - df['mu'].mean()) / df['mu'].std()
        # df['kp_z'] = (df['k_pred'] - df['k_pred'].mean()) / df['k_pred'].std()
        # df['rk_z'] = (df['k_pred_raw'] - df['k_pred_raw'].mean()) / df['k_pred_raw'].std()
        # df['s_z'] = ((df['fd_salary'] - df['fd_salary'].mean()) / df['fd_salary'].std()) * -1
        # df['pps_z'] = (df['pitches_start'] - df['pitches_start'].mean()) / df['pitches_start'].std()
        # df['z'] = (df['p_z'] * 1) + (df['rmu_z'] * 1) + (df['mu_z'] * 1) + (df['kp_z'] * 1) + (df['rk_z'] * 1) + (df['s_z'] * 1) + (df['pps_z'] * 1)
            
        return df
    def h_df(self, refresh = False):
        df_file = pickle_path(name=f"all_h_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if not path.exists() or refresh:
            df = self.get_hitters()
        else:
            df = pandas.read_pickle(path)
        return df
    def p_df(self, refresh = False):
        df_file = pickle_path(name=f"all_p_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if not path.exists() or refresh:
            df = self.get_pitchers()
        else:
            df = pandas.read_pickle(path)
        df = df[df['is_p'] == True]
        for team in self.team_instances:
            if team.ppd and team.name in self.active_teams:
                filt = df[(df['team'] == team.opp_name) | (df['team'] == team.name)].index
                df.drop(filt, inplace = True)
        df['i_z'] = FDSlate.df_z_score(df, 'exp_inn')
        df['f_z'] = FDSlate.df_z_score(df, 'fav')
        # df['vt_z'] = FDSlate.df_z_score(df, 'venue_temp', mult = -1)
        df['v_z'] = FDSlate.df_z_score(df, 'venue_avg', mult = -1)
        df['u_z'] = FDSlate.df_z_score(df, 'ump_avg', mult = -1)
        df['p_z'] = FDSlate.df_z_score(df, 'exp_ps_raw')
        df['rmu_z'] = FDSlate.df_z_score(df, 'raw_mu')
        df['mu_z'] = FDSlate.df_z_score(df, 'mu')
        df['kp_z'] = FDSlate.df_z_score(df, 'k_pred')
        df['rk_z'] = FDSlate.df_z_score(df, 'k_pred_raw')
        df['s_z'] = FDSlate.df_z_score(df, 'fd_salary', mult = -1)
        df['pps_z'] = FDSlate.df_z_score(df, 'pitches_start')
        df['e_z'] = FDSlate.df_z_score(df, 'env_points', mult = -1)
        df['z'] = ((df['f_z'] * .5) + (df['rmu_z'] * 2) + (df['i_z'] * 1) + (df['p_z'] * 2) + (df['kp_z'] * 2) + (df['s_z'] * 1) + (df['e_z'] * 1.5)) / 10
        df['mz'] = ((df['f_z'] * 1) + (df['rmu_z'] * 2) + (df['i_z'] * 1) + (df['p_z'] * 2) + (df['kp_z'] * 2) + (df['e_z'] * 2)) / 10       
        return df
    
    def points_df(self, refresh = False):
        file = pickle_path(name=f"team_points_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(file)
        if not path.exists() or refresh:
            self.get_hitters()
        df = pandas.read_pickle(path).apply(pandas.to_numeric)
        for team in self.team_instances:
            if team.ppd and team.name in self.active_teams:
                filt = df[(df.index == team.name) | (df.index == team.name)].index
                df.drop(filt, inplace = True)
        df['p_z'] = FDSlate.df_z_score(df, 'points')
        df['v_z'] = FDSlate.df_z_score(df, 'venue_avg')
        df['s_z'] = FDSlate.df_z_score(df, 'salary', mult = -1)
        df['mu_z'] = FDSlate.df_z_score(df, 'sp_mu')
        df['t_z'] = FDSlate.df_z_score(df, 'raw_talent')
        df['u_z'] = FDSlate.df_z_score(df, 'ump_avg')
        df['e_z'] = FDSlate.df_z_score(df, 'env_avg')
        df['sa_z'] = FDSlate.df_z_score(df, 'sp_avg')
        df['z'] = (((df['p_z'] * 3) + (df['s_z'] * 2) + (df['sa_z'] * 1) + (df['mu_z'] * 3) + (df['t_z'] * 1) + (df['v_z'] * 0)) / 10)
        df['mz'] = (((df['p_z'] * 4) + (df['sa_z'] * 1) + (df['mu_z'] * 4) + (df['t_z'] * 1) + (df['v_z'] * 0)) / 10)
        return df
    
    @cached_property
    def util_df(self):
        df = self.h_df()
        return df[df['fd_r_position'].apply(lambda x: 'util' in x)]
    @cached_property
    def first_df(self):
        df = self.h_df()
        return df[df['fd_position'].apply(lambda x: '1b' in x or 'c' in x)]
    @cached_property
    def second_df(self):
        df = self.h_df()
        return df[df['fd_position'].apply(lambda x: '2b' in x)]
    @cached_property
    def ss_df(self):
        df = self.h_df()
        return df[df['fd_position'].apply(lambda x: 'ss' in x)]
    @cached_property
    def third_df(self):
        df = self.h_df()
        return df[df['fd_position'].apply(lambda x: '3b' in x)]
    @cached_property
    def of_df(self):
        df = self.h_df()
        return df[df['fd_position'].apply(lambda x: 'of' in x)]
    @staticmethod
    def df_z_score(df, column, mult = 1):
        return ((df[column] - df[column].mean()) / df[column].std(ddof = 0)) * mult
        
    def stacks_df(self):
        lineups = self.lineups
        df = self.points_df()
        i = df[(df.index.isin(self.no_stack))].index
        df.drop(index = i, inplace=True)
        df['p_z'] = FDSlate.df_z_score(df, 'raw_points')
        df['v_z'] = FDSlate.df_z_score(df, 'venue_avg')
        df['s_z'] = FDSlate.df_z_score(df, 'salary', mult=-1)
        df['mu_z'] = FDSlate.df_z_score(df, 'sp_mu')
        df['t_z'] = FDSlate.df_z_score(df, 'raw_talent')
        df['u_z'] = FDSlate.df_z_score(df, 'ump_avg')
        df['e_z'] = FDSlate.df_z_score(df, 'env_avg')
        df['sa_z'] = FDSlate.df_z_score(df, 'sp_avg')
        df['stacks'] = 1000
        increment = 0
        for team in self.team_instances:
            if team.ppd and team.name in self.active_teams:
                filt = df[(df.index == team.name) | (df.index == team.name)].index
                df.drop(filt, inplace = True)
        while df['stacks'].max() > self.stack_threshold:
            df_c = df.copy()
            if self.stack_salary_factor:
                df_c['z'] = (((df_c['p_z'] * 2.75) + (df_c['s_z'] * 2) + (df_c['sa_z'] * 2.5) + (df_c['e_z'] * 2.75)) / 10) + increment
            else:
                df_c['z'] = (((df['p_z'] * 3.75) + (df['sa_z'] * 2.75) + (df['e_z'] * 3.5)) / 10) + increment
            df_c = df_c[df_c['z'] > 0]
            lu_base = lineups / len(df_c.index)
            df_c['stacks'] = lu_base * df_c['z']
            if df_c['stacks'].max() > self.stack_threshold:
                increment += .01
                continue
            diff = lineups - df_c['stacks'].sum()
            df_c['stacks'] = round(df_c['stacks'])
            while df_c['stacks'].sum() < lineups:
                if self.heavy_weight_stack:
                    df_c['stacks'] = df_c['stacks'] + np.ceil(((diff / len(df.index)) * df_c['z']))
                else:
                    df_c['stacks'] = df_c['stacks'] + np.ceil((diff / len(df.index)))
                df_c['stacks'] = round(df_c['stacks'])
            while df_c['stacks'].sum() > lineups:
                for idx in df_c.index:
                    if df_c['stacks'].sum() == lineups:
                        break
                    else:
                        df_c.loc[idx, 'stacks'] -= 1
            if df_c['stacks'].max() > self.stack_threshold:
                increment += .01
                continue
            df = df_c
        
        i = df[df['stacks'] == 0].index
        df.drop(index = i, inplace=True)
        return df
    
    def p_lu_df(self):
        lineups = self.lineups
        df = self.p_df()
        i = df[(df['points'] == 0) | (df['team'].isin(self.p_fades))].index
        df.drop(index = i, inplace=True)
        # p_drops = df.sort_values(by=['fd_salary'], ascending = False)
        # if df['fd_salary'].mean() > df['fd_salary'].median():
        #     filt_sal = df['fd_salary'].mean()
        # else:
        #     filt_sal = df['fd_salary'].median()
        # p_drops = p_drops[p_drops['fd_salary'] > filt_sal]
        # mp = p_drops['points'].max()
        # for x in p_drops.index:
        #     if p_drops.loc[x, 'points'] == mp:
        #         break
        #     else:
        #         df.drop(index=x, inplace=True)
        df['lus'] = 1000
        increment = 0
        while df['lus'].max() > self.pitcher_threshold:
            df_c = df.copy()
            df_c['p_z'] = ((df_c['points'] - df_c['points'].mean()) / df_c['points'].std()) + increment
            i = df_c[df_c['p_z'] <= 0].index
            df_c.drop(index = i, inplace=True)
            lu_base = lineups / len(df_c.index)
            df_c['lus'] = lu_base * df_c['p_z']
            if df_c['lus'].max() > self.pitcher_threshold:
                increment += .01
                continue
            diff = lineups - df_c['lus'].sum()
            df_c['lus'] = round(df_c['lus'])
            while df_c['lus'].sum() < lineups:
                if self.heavy_weight_p:
                    df_c['lus'] = df_c['lus'] + np.ceil(((diff / len(df_c.index)) * df_c['p_z']))
                else:
                    df_c['lus'] = df_c['lus'] + np.ceil((diff / len(df_c.index)))
                df_c['lus'] = round(df_c['lus']) 
            # if df_c['lus'].max() > self.pitcher_threshold:
            #     increment += .01
            #     continue
            while df_c['lus'].sum() > lineups:
                for idx in df_c.index:
                    if df_c['lus'].sum() == lineups:
                        break
                    else:
                        df_c.loc[idx, 'lus'] -= 1
            if df_c['lus'].max() > self.pitcher_threshold:
                increment += .01
                continue
            df = df_c
        i = df[df['lus'] == 0].index
        df.drop(index = i, inplace=True)
        return df
    def p_counts(self):
        file = pickle_path(name=f"p_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(file)
        if path.exists():
            df = pandas.read_pickle(path)
            return df
        else:
            return "No pitchers stored yet."
    def h_counts(self):
        file = pickle_path(name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(file)
        if path.exists():
            df = pandas.read_pickle(path)
            return df
        else:
            return "No lineups stored yet."
        
    
    def build_lineups(self, no_secondary = [], info_only = False,
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
                      max_sal = 35000,
                      min_avg_post_stack = 2700,
                      stack_size = 4,
                      stack_sample = 5, 
                      stack_salary_pairing_cutoff=0,
                      stack_max_pair = 5,
                      fallback_stack_sample = 6,
                      stack_expand_limit = 20,
                      max_order=7,
                      all_in_replace_order_stack = 6,
                      all_in_replace_order_sec = 5,
                      non_stack_max_order=6,
                      first_max_order = 6,
                      second_max_order = 6,
                      ss_max_order = 6,
                      third_max_order = 6,
                      of_max_order = 6,
                      util_max_order =6,
                      util_replace_filt = 200,
                      all_in_replace_filt = 200,
                      non_stack_quantile = .9, 
                      high_salary_quantile = .9,
                      secondary_stack_cut = 0,
                      no_surplus_cut = 0,
                      single_stack_surplus = 400,
                      double_stack_surplus = 400,
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
                      enforce_hitter_surplus = True,
                      filter_last_replaced=True,
                      always_replace_pitcher_lock=False,
                      p_sal_stack_filter = True,
                      replace_secondary_all_in = True,
                      replace_primary_all_in = False,
                      exempt=[],
                      all_in=[],
                      supreme_all_in=None,
                      no_supreme_replace=[],
                      supreme_all_in_pos=[],
                      lock = [],
                      no_combos = [],
                      never_replace=[],
                      never_replace_secondary=[],
                      never_replace_primary=[],
                      x_fallback = [],
                      stack_only = [],
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
                      remove_positions = None,
                      custom_stacks = None,
                      custom_pitchers = None,
                      custom_secondary = None,
                      custom_stacks_info = None,
                      fill_high_salary_first = False,
                      select_max_pitcher = True,
                      last_lu = [],
                      filter_last_lu = False,
                      select_max_stack = True,
                      select_non_random_stack = False,
                      custom_stack_order = {},
                      always_pair_first = {},
                      never_pair = {},
                      never_comine = {},
                      locked_lineup = None,
                      expand_pitchers = False,
                      find_cheap_primary = False,
                      while_loop_secondary = True,
                      stack_top_order = False,
                      info_stack_key = 'points',
                      stack_info_order = 7,
                      stack_info_size = 4,
                      stack_info_salary_factor = True):
        
        o_cheap_primary = find_cheap_primary
        o_cheap_stacks = find_cheap_stacks
        # all hitters in slate
        h = self.h_df()
        #dropping faded, platoon, and low order (max_order) players.
        h_fade_filt = ((h['team'].isin(self.h_fades)) | (h['fd_id'].isin(self.h_fades)))
        h_order_filt = (h['order'] > max_order)
        h_exempt_filt = (~h['fd_id'].isin(exempt))
        hfi = h[(h_fade_filt | h_order_filt) & h_exempt_filt].index
        h.drop(index=hfi,inplace=True)
        no_utility_filt = (~h['fd_id'].isin(no_utility))
        #count each players entries
        h['t_count'] = 0
        #count non_stack
        h['ns_count'] = 0
        h_count_df = h.copy()
        #risk_limit should always be >= below_avg_count
        h.loc[(h['sp_split'] < h['sp_split'].median()),'t_count'] = below_avg_count
        h.loc[(h['team'].isin(stack_only)) | (h['fd_id'].isin(stack_only)), 't_count'] = 1000
        exempt_filt = (h['fd_id'].isin(exempt))
        h.loc[exempt_filt, 't_count'] = 0
        #in the case a player's batting order is above the max_order, but is exempted from drop.
        h.loc[h_order_filt, 't_count'] = below_order_count
        #allowing outfields, as there are 3 of in each LU, to be in more lineups than non-of
        h.loc[(h['fd_position'].apply(lambda x: 'of' in x)), 't_count'] -= of_count_adjust
        #if a game is a threat to be postponed, limit exposure to that team
        h.loc[(h['team'].isin(limit_risk)), 't_count'] = risk_limit
        #ids or teams in all_in have no limit to allowed number of lineups.
        h.loc[((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in))), 't_count'] = -1000
        #limit a player by fanduel id in custom_counts dict {'id': original count}
        for k,v in custom_counts.items():
            h.loc[h['fd_id'] == k, 't_count'] = v
        #all pitchers...
        p = self.p_df()
        #drop pitchers in class p_fades (__init__)
        p_fade_filt = (p['team'].isin(self.p_fades))
        pfi = p[p_fade_filt].index
        p.drop(index=pfi,inplace=True)
        #count a pitcher's entries
        p_count_df = p.copy()
        p_count_df['t_count'] = 0
        #dict p_df.index: p_df: lineups to be in
        if not custom_pitchers:
            pd = self.p_lu_df()['lus'].to_dict()
        else:
            pd = custom_pitchers
        # team: stacks to build
        if not custom_stacks:
            s = self.stacks_df()['stacks'].to_dict()
        else:
            s = custom_stacks
        if not custom_secondary:
            secondary = self.stacks_df()['stacks'].to_dict()
        else:
            secondary = custom_secondary
        stack_list = list(s)
       
        #keep track of each team's pairing with another team, pitcher, both.
        stack_track = {}
        for team in self.active_teams:
            stack_track[team] = {}
            stack_track[team]['pitchers'] = {}
            stack_track[team]['secondary'] = {}
            stack_track[team]['salary'] = self.points_df().loc[team, 'salary'].item()
            stack_track[team]['combos'] = []
        #append each completed lineup, sorted, as to assure unique each time.
        sorted_lus = []
        #keys are positions, values are indexes that correspond to those positions.
        p_map = {
            'p': 0,
            '1b': 1,
            '2b': 2,
            '3b': 3,
            'ss': 4,
            'of': 5,
            'of.1': 6,
            'of.2': 7,
            'util': 8
            }
        """
        if filter_last_replaced == True:
            each player that is inserted in a lineup, that is not part
            of a stack, will be filtered the next time that position
            is needed.
        """
        last_replaced = p_map.copy()
        """
        if playing a player only at a specific position, 
        use remove_position dict.
        in {'id1': '1b'}, player with 'id1' will no-longer be eligible at 1b.
        Will throw error if all player's positions are removed. Or if player
        isn't eligible at specified position.'
        """
        if remove_positions:
            for k,v in remove_positions.items():
                position_id_filt = (h['fd_id'] == k)
                h.loc[position_id_filt, 'fd_position'].apply(lambda x: x.remove(v))
                
                
        """
        if not specified as KW args., filter out team's teams not
        being used, and gather salary information for them. This can
        be used to pair team's with high salary with lower one's
        """
        points_df_filt = ((self.points_df().index.isin(s.keys())) | (self.points_df().index.isin(secondary.keys())))
        filtered_points_df = self.points_df()[points_df_filt]
        pitcher_points_filt = (self.p_df().index.isin(pd))
        filtered_pitcher_df = self.p_df()[pitcher_points_filt]
        if not median_team_salary:
            median_team_salary = filtered_points_df['salary'].median()
        if not low_stack_salary:
            low_stack_salary = filtered_points_df['salary'].quantile(.25)
        if not high_stack_salary:
            high_stack_salary = filtered_points_df['salary'].quantile(.75)
        if not max_stack_salary:
            max_stack_salary = filtered_points_df['salary'].max() 
        if not median_pitcher_salary:       
            median_pitcher_salary = filtered_pitcher_df['fd_salary'].median()
        if not high_pitcher_salary:
            high_pitcher_salary = filtered_pitcher_df['fd_salary'].quantile(.75)
        if not low_pitcher_salary:
            low_pitcher_salary = filtered_pitcher_df['fd_salary'].quantile(.25)
        if not max_pitcher_salary:
            max_pitcher_salary = filtered_pitcher_df['fd_salary'].max()
        """
        if info_only == True, return a dict with some information regarding
        salaries and possible combinations of provided teams. Also,
        print stack rankings based on talent/salary, selecting 
        stack size(stack_info_size), maximum stack order (stack_info_order),
        and stack filter(info_stack_key).
        Half the weight will be applied to salary if stack_info_salary_factor == True
        """
            
        if info_only:
            information = {}
            information['low_stack_salary'] = low_stack_salary
            information['median_team_salary'] = median_team_salary
            information['high_stack_salary'] = high_stack_salary
            information['max_stack_salary'] = max_stack_salary
            information['low_pitcher_salary'] = low_pitcher_salary
            information['median_pitcher_salary'] = median_pitcher_salary
            information['high_pitcher_salary'] = high_pitcher_salary
            information['max_pitcher_salary'] = max_pitcher_salary
            information['high_salary_quantile'] = h['fd_salary'].quantile(high_salary_quantile)
            information['avg_stack_per_team'] = lus / len(list(s))
            stack_info_columns = ['salary', 'points', 'points_std', 'sp_split', 'sp_split_std', 'hr_weight', 'hr_weight_std',
                                  'stack_exp_pa']
            stack_info_df = pandas.DataFrame(columns = stack_info_columns)
            for team in self.active_teams:
                if team not in self.h_fades:
                    team_order_filt = (h['order'] <= stack_info_order)
                    x_team_order_filt = (h['order'] <= stack_info_order + 10)
                    is_team_filt = (h['team'] == team)
                    # plat_filt = (h['is_platoon'] != True)
                    min_abs_filt = (h['pa'] > 100)
                    team_copy = h[is_team_filt & team_order_filt & min_abs_filt]
                    if len(team_copy.index) != stack_info_order:
                        team_copy = h[is_team_filt & x_team_order_filt & min_abs_filt]
                        
                    team_copy = team_copy[['name','points', 'fd_salary', 'order', 'sp_split', 'exp_ps_sp_pa',
                                           'fd_hr_weight', 'is_platoon']]
                    indices = team_copy[info_stack_key].nlargest(stack_info_size).index
                    team_high_points = team_copy.loc[indices]
                    stack_info_salary = team_high_points['fd_salary'].sum()
                    stack_info_points = team_high_points['points'].sum()
                    stack_info_split = team_high_points['sp_split'].sum()
                    stack_info_salary_std = team_high_points['fd_salary'].std(ddof = 0)
                    stack_info_points_std = team_high_points['points'].std(ddof = 0)
                    stack_info_split_std = team_high_points['sp_split'].std(ddof = 0)
                    hr_weight = team_high_points['fd_hr_weight'].sum()
                    hr_weight_std = team_high_points['fd_hr_weight'].std(ddof = 0)
                    stack_exp_pa = team_high_points['exp_ps_sp_pa'].sum()
                    stack_info_df.loc[team, stack_info_columns] = [
                        stack_info_salary,
                        stack_info_points,
                        stack_info_points_std,
                        stack_info_split,
                        stack_info_split_std,
                        hr_weight,
                        hr_weight_std,
                        stack_exp_pa
                        
                        ]
                    print(team)
                    print(f"{team}: {stack_info_points}")
                    print(team_high_points)
            stack_info_df['s_z'] = FDSlate.df_z_score(stack_info_df, 'salary', mult = -1)
            stack_info_df['p_z'] = FDSlate.df_z_score(stack_info_df, 'points')
            stack_info_df['p_z_s'] = FDSlate.df_z_score(stack_info_df, 'points_std', mult = -1)
            stack_info_df['sp_z'] = FDSlate.df_z_score(stack_info_df, 'sp_split')
            stack_info_df['sp_z_s'] = FDSlate.df_z_score(stack_info_df, 'sp_split_std', mult = -1)
            stack_info_df['hr_z'] = FDSlate.df_z_score(stack_info_df, 'hr_weight')
            stack_info_df['hr_z_s'] = FDSlate.df_z_score(stack_info_df, 'hr_weight_std', mult = -1)
            stack_info_df['exp_p_z'] = FDSlate.df_z_score(stack_info_df, 'stack_exp_pa')
            if stack_info_salary_factor:
                stack_info_df['score'] = ((stack_info_df['s_z'] * 5) +
                                      (stack_info_df['p_z'] * 0) +
                                      (stack_info_df['p_z_s'] * 0) +
                                      (stack_info_df['sp_z'] * 5) +
                                      (stack_info_df['sp_z_s'] * 0) +
                                      (stack_info_df['hr_z'] * 0) +
                                      (stack_info_df['hr_z_s'] * 0) + 
                                      (stack_info_df['exp_p_z'] * 0)
                ) / 10
            
            
            else:
                stack_info_df['score'] = ((stack_info_df['s_z'] * 0) +
                                          (stack_info_df['p_z'] * 0) +
                                          (stack_info_df['p_z_s'] * 0) +
                                          (stack_info_df['sp_z'] * 10) +
                                          (stack_info_df['sp_z_s'] * 0) +
                                          (stack_info_df['hr_z'] * 0) +
                                          (stack_info_df['hr_z_s'] * 0) + 
                                          (stack_info_df['exp_p_z'] * 0)
                    ) / 10
                
            
            
            if not custom_stacks_info:
                information['stack_combos'] = len(list(combinations(list(s), 2)))
                
            else:
                information['stack_combos'] = len(list(combinations(list(custom_stacks_info), 2)))
            information['pitcher_combos'] = len(list(combinations(list(s), 2))) * len(list(pd))
            # print(h.loc[h['exp_ps_sp_pa'].nlargest(60).index, ['name', 'points', 'exp_ps_sp_pa', 'exp_ps_sp_raw','fd_salary', 'order']])
            print(p.loc[p.index.isin(pd.keys()), 'name'])
            # print(h.columns.tolist())
            stack_info_df = stack_info_df.apply(pandas.to_numeric, errors = 'ignore')
            print(stack_info_df.loc[stack_info_df['score'].nlargest(30).index, ['score', 'points', 'salary']])
            # print(h[h['team'] == 'orioles'])
            
            return information

       
        #lineups to build
        while lus > 0:
            non_random_stack = stack_list[non_random_stack_start]
            #filter for not inserting
            if filter_last_lu:
                last_lu_filt = (~h['fd_id'].isin(last_lu))
            else:
                last_lu_filt = (h['fd_id'] != 1)
            
            #if lineup fails requirements, reset will be set to true.
            reset = False
            
            ##drop players already in max_lu_total/max_lu_stack lineups
            # total_filt = (h['t_count'] >= max_lu_total)
            # count_filt = (h['ns_count'] >= max_lu_stack)
            # c_idx = h[total_filt | count_filt].index
            # h.drop(index=c_idx,inplace=True)
            #pitchers expected lineups will be reduced by 1 for each successful lineup insertion
            if select_max_pitcher:
                pitchers = {k:v for k,v in pd.items() if v == max(pd.values())}
            else:
                pitchers = {k:v for k,v in pd.items() if v > 0}
            #randomly select a pitcher in pool, keys are == p.index
            pi = random.choice(list(pitchers.keys()))
            #lookup required pitcher info. by index (pi)
            p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
            #start with the pitchers salary
            salary = 0 + p_info[1]
            if p_info[0] in always_find_cheap:
                # find_cheap_stacks = True
                find_cheap_primary = True
            else:
                find_cheap_primary = o_cheap_primary
                find_cheap_stacks = o_cheap_stacks
            #choose a stack that hasn't exceed its insertion limit and is not playing the current pitcher
            stacks = {}
            if select_max_stack:
                if always_pair_first.get(p_info[0]):
                    stacks = {k:v for k,v in s.items() if v > 0 and p_info[0] not in stack_track[k]['pitchers'] and k in always_pair_first[p_info[0]]}
                    if len(stacks.keys()) == 0:
                        {k:v for k,v in s.items() if v > 0 and k in always_pair_first[p_info[0]]}
                if len(stacks.keys()) == 0:
                    filtered_stacks = {k:v for k,v in s.items() if k != p_info[2]}
                    stacks = {k:v for k,v in filtered_stacks.items() if v == max(filtered_stacks.values()) and k != p_info[2] and p_info[0] not in stack_track[k]['pitchers']}
                    if len(stacks.keys()) == 0:
                        stacks = {k:v for k,v in filtered_stacks.items() if v > 0 and k != p_info[2] and p_info[0] not in stack_track[k]['pitchers']} 
                    if len(stacks.keys()) == 0:
                        stacks = {k:v for k,v in filtered_stacks.items() if v == max(filtered_stacks.values()) and k != p_info[2]}
                    
            if select_non_random_stack:    
                stacks = {k:v for k,v in s.items() if v > 0 and k != p_info[2] and k == non_random_stack}
                if len(stacks.keys()) > 0:
                    non_random_stack_start += 1
                    if non_random_stack_start > len(stack_list) - 1:
                            non_random_stack_start = 0
                else:
                    # original_non_random_stack_start = non_random_stack_start
                    non_random_stack_attempts = 0
                    while len(stacks.keys()) < 1 and non_random_stack_attempts < len(stack_list):
                        non_random_stack_start += 1
                        if non_random_stack_start > len(stack_list) - 1:
                            non_random_stack_start = 0
                        non_random_stack = stack_list[non_random_stack_start]
                        stacks = {k:v for k,v in s.items() if v > 0 and k != p_info[2] and k == non_random_stack}
                        non_random_stack_attempts += 1
                    if non_random_stack_attempts >= len(stack_list):
                        stacks = {k:v for k,v in s.items() if v > 0 and k != p_info[2]}
                    
                    non_random_stack_start += 1
                    if non_random_stack_start > len(stack_list) - 1:
                        non_random_stack_start = 0
                
            #randomly select an eligible stack. keys == team names.
            try:
                stack = random.choice(list(stacks.keys()))
            except IndexError:
                print("Error....")
                print(lus)
                continue
            if stack in always_find_cheap:
                # find_cheap_stacks = True
                find_cheap_primary = True
            elif p_info[0] not in always_find_cheap:
                find_cheap_primary = o_cheap_primary
                find_cheap_stacks = o_cheap_stacks
            remaining_stacks = stacks[stack]
            #lookup players on the team for the selected stack
            stack_df = h[h['team'] == stack]
            
            if remaining_stacks % 2 == 0:
                stack_key = 'sp_split'
            # elif remaining_stacks % 3 == 0 and find_cheap_stacks:
            #     stack_key = 'points_salary'
            # elif remaining_stacks % 3 == 0:
            #     stack_key = 'fd_hr_weight'
            # elif remaining_stacks % 2 == 0:
            #     stack_key = 'fd_hr_weight'
            else:
                stack_key = 'sp_split'
            if lus % 2 == 0:
                non_stack_key ='points'
                pitcher_replace_key = 'points'
            else:
                non_stack_key ='points'
                pitcher_replace_key = 'points'
            #filter the selected stack by stack_sample arg.
            if custom_stack_order.get(stack):
                if type(custom_stack_order[stack][0]) == list:
                    custom_stack_sample = random.choice(custom_stack_order[stack])
                else:
                    custom_stack_sample = custom_stack_order[stack].copy()
                highest = stack_df[stack_df['order'].isin(custom_stack_sample)]
            else:
                if find_cheap_primary:
                    highest = stack_df.loc[stack_df['fd_salary'].nsmallest(stack_sample).index]
                elif stack_top_order:
                    highest = stack_df.loc[stack_df['order'].nsmallest(stack_sample).index]
                    
                else:
                    if remaining_stacks > stack_expand_limit:
                        highest = stack_df.loc[stack_df[stack_key].nlargest(stack_sample+1).index]
                    else:
                        highest = stack_df.loc[stack_df[stack_key].nlargest(stack_sample).index]
                        
            #array of fanduel ids of the selected hitters
            stack_ids = highest['fd_id'].values
            #initial empty lineup, ordered by fanduel structed and mapped by p_map
            lineup = [None, None, None, None, None, None, None, None, None]
            #insert current pitcher into lineup
            lineup[0] = p_info[0]
            #try to create a 4-man stack that satifies position requirements 5 times, else 3-man stack.
            a = 0
            pl2 = []
            while len(pl2) != stack_size:
                a +=1
                if a > 10:
                    break
                #empty the list of positions each mapped player will fill
                pl2 = []
                #take an initial random sample of 4
                try:
                    #must sort array to avoid TypeError
                    samp = random.sample(sorted(stack_ids), stack_size)
                except ValueError:
                    a = 1000
                    continue
                #list of lists of the sample's eligible positions
                pl1 = [x for x in h.loc[h['fd_id'].isin(samp), 'fd_position'].values]
                
                
                # for s in lineup:
                #     if s:
                #         locked_player = h[h['fd_id'] == s]
                #         locked_position = p_map[]
                #         if s in samp:
                #             lineup[p_map[locked_player]]
                #             lineup = [None, None, None, None, None, None, None, None, None]
                #         else:
                            
                #             if locked_player['team'].item() == stack:
                                
               
                for x in pl1:
                    #iterate each player's list of positions, append if not already filled
                    if x[0] not in pl2 and not lineup[p_map.get(x[0])]:
                        pl2.append(x[0])
                    elif x[-1] not in pl2 and not lineup[p_map.get(x[-1])]:
                        pl2.append(x[-1])
                    #index 1 because if len == 2, would've already checked
                    elif len(x) > 2 and x[1] not in pl2 and not lineup[p_map.get(x[1])]:
                        pl2.append(x[1])
                    #if player is eligible outfielder and first of slot taken
                    elif 'of' in x and pl2.count('of') == 1 and 'of.1' not in pl2 and not lineup[p_map['of.1']]:
                        pl2.append('of.1')
                    #if already 2 outfielders and player is eligible outfielder
                    elif 'of' in x and pl2.count('of.1') == 1 and 'of.2' not in pl2 and not lineup[p_map['of.2']]:
                        pl2.append('of.2')
                    elif 'util' not in pl2 and not lineup[p_map['util']]:
                         pl2.append('util')
                
                        
            #if couldn't create a 4-man stack, create a 3-man stack        
            if a > 10:
                a = 0
                print(f"Using fallback_stack_sample for {stack}.")
                while len(pl2) != stack_size:
                    a +=1
                    if a > 10:
                        raise Exception(f"Could not create 4-man stack for {stack}.")
                    pl2 = []
                    if find_cheap_primary:
                        highest = stack_df.loc[stack_df['fd_salary'].nsmallest(fallback_stack_sample).index]
                    elif stack_top_order:
                        highest = stack_df.loc[stack_df['order'].nsmallest(fallback_stack_sample).index] 
                    else:
                        if stack in x_fallback:
                            highest = stack_df.loc[stack_df[stack_key].nlargest(fallback_stack_sample+1).index]
                        else:
                            highest = stack_df.loc[stack_df[stack_key].nlargest(fallback_stack_sample).index]
                    #array of fanduel ids of the selected hitters
                    stack_ids = highest['fd_id'].values
                    samp = random.sample(sorted(stack_ids), stack_size)
                    pl1 = [x for x in h.loc[h['fd_id'].isin(samp), 'fd_position'].values]                
                    for x in pl1:
                        if x[0] not in pl2:
                            pl2.append(x[0])
                        elif x[-1] not in pl2:
                            pl2.append(x[-1])
                        elif len(x) > 2 and x[1] not in pl2:
                            pl2.append(x[1])
                        elif 'of' in x and pl2.count('of.1') == 1 and 'of.2' not in pl2:
                            pl2.append('of.2')
                        elif 'of' in x and pl2.count('of') == 1 and 'of.1' not in pl2:
                            pl2.append('of.1')
                        elif 'util' not in pl2:
                            pl2.append('util')
                            
            
            #put required information for stack in lists of lists   
            stack_info = h.loc[h['fd_id'].isin(samp), ['fd_salary', 'fd_id']].values
            #zip position (keys) and player info into a dict (values - list salary, fanduel ID)
            stack_map = dict(zip(pl2, stack_info))
            # calculate used salary and remaining salary after selecting pitcher/stack
            for v in stack_map.values():
                salary += v[0]
            rem_sal = max_sal - salary
            #use position of each player to find corresponding value/index in p_map dict to insert fanduel ID.
            for k, v in stack_map.items():
                idx = p_map[k]
                lineup[idx] = v[1]
            #create list of lineup indexes that need to be filled.
            np_list = [idx for idx, spot in enumerate(lineup) if not spot]
            s_list = [idx for idx, spot in enumerate(lineup) if spot and idx != 0]
            stacked_pos = [x for x, y in p_map.items() if y in s_list]
            #create list of positions that need to be filled, util always empty here.
            needed_pos = [x for x, y in p_map.items() if y in np_list]
            #get the ammount of roster spots that need filling.
            npl = len([idx for idx, spot in enumerate(lineup) if not spot])
            #the avgerage salary remaining for each empty lineup spot
            avg_sal = rem_sal / npl
            if avg_sal < min_avg_post_stack:
                random.shuffle(stacked_pos)
                for ps in stacked_pos:
                    print(avg_sal)
                    if avg_sal >= min_avg_post_stack:
                        break
                    idx = p_map[ps]
                    if lineup[idx] not in never_replace:
                        selected = h[h['fd_id'] == lineup[idx]]
                        s_sal = selected['fd_salary'].item()
                        sal_filt = (h['fd_salary'] < s_sal)
                        dupe_filt = (~h['fd_id'].isin(lineup))
                        stack_only_filt = (h['team'] == stack)
                        if 'of' in ps:
                            pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                        elif 'util' in ps:
                            pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                        else:
                            pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                        hitters = h[pos_filt & dupe_filt & sal_filt & stack_only_filt]
                        if len(hitters.index) > 0:
                            hitter = hitters.loc[hitters['fd_salary'].idxmin()]
                            selected_name = selected['name'].max()
                            hitter_name = hitter['name']
                            print(f"Replacing {selected_name} with {hitter_name}.(salary)")
                            n_sal = hitter['fd_salary'].item()
                            salary -= s_sal
                            salary += n_sal
                            rem_sal = max_sal - salary
                            lineup[idx] = hitter['fd_id']
                            avg_sal = rem_sal / npl
                            
                
            #shuffle the list of positions to encourage variance of best players at each position
            random.shuffle(needed_pos)
            #filter out hitters going against selected pitcher
            opp_filt = (h['opp'] != p_info[3])
            #filter out hitters on the team of the current stack, as they are already in the lineup.
            stack_filt = (h['team'] != stack)
            #filter out players hitting below specified lineup spot
            order_filt = ((h['order'] <= non_stack_max_order) | exempt_filt)
            #filter out players not on a team being stacked on slate and proj. points not in 90th percentile.
            fade_filt = ((h['team'].isin(stacks.keys())) | ((h[non_stack_key] >= h[non_stack_key].quantile(non_stack_quantile)) & order_filt))
            #filter players out with a current count above the highest value of remaining stacks
            max_stack = max(stacks.values())
            #variance default is 0
            count_filt = (h['t_count'] < ((max_lu_total - max_stack) - variance))
            plat_filt = (h['is_platoon'] != True)
            value_filt = ((h['fd_salary'] <= h['fd_salary'].median()) & (h['points'] >= h['points'].median()))
            new_combo = False
            if lus > secondary_stack_cut:
                if no_surplus_secondary_stacks and lus > no_surplus_cut:
                    enforce_hitter_surplus = False
                else:
                    enforce_hitter_surplus = True
                stack_salary = stack_track[stack]['salary']
                secondary_stacks = {}
                if always_pair_first.get(p_info[0]):
                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                        and k != p_info[2] 
                                        and k != stack 
                                        and [p_info[0], k] not in stack_track[stack]['combos']
                                        and [p_info[0], stack] not in stack_track[k]['combos'] 
                                        and k in always_pair_first[p_info[0]] }
                    
                if stack not in no_combine and p_info[0] not in no_combos and len(list(secondary_stacks)) == 0:
                
                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                        and k != p_info[2] 
                                        and k != stack 
                                        and [p_info[0], k] not in stack_track[stack]['combos']
                                        and [p_info[0], stack] not in stack_track[k]['combos'] }
                if len(list(secondary_stacks)) > 0:
                    new_combo = True
                    
                
                else:
                    new_combo = False
                    if stack_salary >= median_team_salary:
                        if p_sal_stack_filter and p_info[1] >= high_pitcher_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] <= low_stack_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] <= low_stack_salary }
                        elif p_sal_stack_filter and p_info[1] <= low_pitcher_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] >= median_team_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] >= median_team_salary }
                            
                            
                        elif stack_salary >= high_stack_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] <= low_stack_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] <= low_stack_salary }
                        elif stack_salary < high_stack_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] <= median_team_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] <= median_team_salary }
                    else:
                        ####
                        if p_sal_stack_filter and p_info[1] >= high_pitcher_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] <= median_team_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] <= median_team_salary }
                        elif p_sal_stack_filter and p_info[1] <= low_pitcher_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] > median_team_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] > median_team_salary }
                            
                        elif stack_salary <= low_stack_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] >= high_stack_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] >= high_stack_salary }
                        
                        elif stack_salary > low_stack_salary:
                            secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                            and k != p_info[2] 
                                            and k != stack 
                                            and k not in stack_track[stack]['secondary']
                                            and stack_track[k]['salary'] >= median_team_salary }
                            if len(list(secondary_stacks)) < 1:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                and stack_track[k]['salary'] >= median_team_salary }
                            
                    
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                        and k != p_info[2] 
                                        and k != stack 
                                        and [p_info[0], k] not in stack_track[stack]['combos']}
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info[2] 
                                    and k != stack 
                                    and k not in stack_track[stack]['secondary']}
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info[2] 
                                    and k != stack 
                                    and stack_track[stack]['secondary'].get(k, 0) < stack_max_pair}
                        
                    
                        
                    if len(list(secondary_stacks)) < 1:
                        if while_loop_secondary:
                            secondary_count = 1
                            stop_while = 0
                            while len(list(secondary_stacks)) < 1 and stop_while < 10:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info[2] 
                                    and k != stack 
                                    and stack_track[stack]['secondary'].get(k, 0) < secondary_count}
                                secondary_count += 1
                                stop_while += 1
                                
                        else:
                             secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info[2] 
                                    and k != stack 
                                    }
                            
                                
                                
                            
                        
                        
                        
                        
                        
                        
                        
                       
                    
                    # if len(list(secondary_stacks)) < 1:
                    #          print('default')
                    #          secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                    #                             and k != p_info[2] 
                    #                             and k != stack 
                    #                             }
                    # if len(list(secondary_stacks)) < 1:
                    #         print('-1')
                    #         secondary_stacks = {k:v for k,v in secondary.items() if v > -1
                    #                                 and k != p_info[2] 
                    #                                 and k != stack 
                    #                                 }
                    # if len(list(secondary_stacks)) < 1:
                    #         print('-2')
                    #         secondary_stacks = {k:v for k,v in secondary.items() if v > -2
                    #                                 and k != p_info[2] 
                    #                                 and k != stack 
                    #                                 }
                if len(list(secondary_stacks)) != 0:
                    
                    secondary_stack = random.choice(list(secondary_stacks.keys()))
                    max_surplus = double_stack_surplus
                    
                else:
                    enforce_hitter_surplus = True
                    secondary_stack = None
                    max_surplus = single_stack_surplus
                    
            else:
                enforce_hitter_surplus = True
                secondary_stack = None
                max_surplus = single_stack_surplus
            secondary_stack_filt = (h['team'] == secondary_stack)
            
            
            for ps in needed_pos:
                if 'of' in ps:
                    order_filt = ((h['order'] <= of_max_order) | exempt_filt)
                elif '1b' in ps:
                    order_filt = ((h['order'] <= first_max_order) | exempt_filt)
                elif '2b' in ps:
                    order_filt = ((h['order'] <= second_max_order) | exempt_filt)
                elif 'ss' in ps:
                    order_filt = ((h['order'] <= ss_max_order) | exempt_filt)
                elif '3b' in ps:
                    order_filt = ((h['order'] <= third_max_order) | exempt_filt)
                elif 'util' in ps:
                    order_filt = ((h['order'] <= util_max_order) | exempt_filt)
                    
                if filter_last_replaced:
                     if 'of' in ps:
                         last_replaced_filt = (h['fd_id'] != last_replaced['of'])
                         order_filt = ((h['order'] <= of_max_order) | exempt_filt)
                     else:
                         last_replaced_filt = (h['fd_id'] != last_replaced[ps])
                else:
                    last_replaced_filt = (h['fd_id'] != 1)
                    
               
                #filter out players already in lineup, lineup will change with each iteration
                dupe_filt = (~h['fd_id'].isin(lineup))
                #filter out players not eligible for the current position being filled.
                if 'of' in ps:
                    pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                elif 'util' in ps:
                    pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                else:
                    pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                #get the ammount of roster spots that need filling.
                npl = len([idx for idx, spot in enumerate(lineup) if not spot])
                #the avgerage salary remaining for each empty lineup spot
                avg_sal = rem_sal / npl
                #filter out players with a salary greater than the average avg_sal above
                if npl > 1 and avg_sal >= min_avg_post_stack:
                    sal_filt = (h['fd_salary'] <= (avg_sal + (npl * 100)))
                sal_filt = (h['fd_salary'] <= avg_sal)
                try:
                    update_last_replaced = False
                    if not find_cheap_stacks and npl > 1 or ((npl == 4 and avg_sal > h['fd_salary'].median())
                                                             or (npl == 3 and avg_sal > h['fd_salary'].quantile(.75))
                                                             or (npl == 2 and avg_sal > h['fd_salary'].quantile(.90))):
                         hitters = h[pos_filt & dupe_filt & secondary_stack_filt]
                    else:
                        hitters = h[pos_filt & dupe_filt & secondary_stack_filt & sal_filt]
                        
                    hitter = hitters.loc[hitters[stack_key].idxmax()]
                    
                # except (KeyError, ValueError):
                #     try:
                #         hitters = h[pos_filt & dupe_filt & count_filt & secondary_stack_filt]
                #         hitter = hitters.loc[hitters[stack_key].idxmax()]
                except (KeyError, ValueError):
                    try:
                        update_last_replaced = True
                        hitters = h[pos_filt & stack_filt & dupe_filt & (fade_filt | value_filt) & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    except (KeyError, ValueError):
                        try:
                            hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                            hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                        except (KeyError, ValueError):
                            try:
                                hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                            except (KeyError, ValueError):
                                try:
                                    hitters = h[pos_filt & stack_filt & dupe_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                except:
                                    hitters = h[pos_filt & stack_filt & dupe_filt & sal_filt & count_filt & order_filt & plat_filt]
                                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                        
                if update_last_replaced and filter_last_replaced:
                    
                    if 'of' in ps:
                        last_replaced['of'] = hitter['fd_id']
                    else:
                        last_replaced[ps] = hitter['fd_id']
                        
                salary += hitter['fd_salary'].item()
                rem_sal = max_sal - salary
                #if the selected hitter's salary put the lineup over the max. salary, try to find replacement.
                if rem_sal < 0:
                     r_sal = hitter['fd_salary'].item()
                     try:
                         
                         salary_df = hitters[((hitters['fd_salary'] <= (r_sal + rem_sal)) & hitters['team'] == secondary_stack)]
                         hitter = salary_df.loc[hitters[non_stack_key].idxmax()]
                         salary += hitter['fd_salary']
                         salary -= r_sal
                         rem_sal = max_sal - salary
                     except(ValueError,KeyError):
                         try:
                             salary_df = hitters[(hitters['fd_salary'] <= (r_sal + rem_sal)) ]
                             hitter = salary_df.loc[hitters[non_stack_key].idxmax()]
                             salary += hitter['fd_salary']
                             salary -= r_sal
                             rem_sal = max_sal - salary
                             if filter_last_replaced:
                                if 'of' in ps:
                                    last_replaced['of'] = hitter['fd_id']
                                else:
                                    last_replaced[ps] = hitter['fd_id']
                         
                         #if could not find replacment hitter, swap the pitcher for a lower salaried one.
                         except(ValueError,KeyError):
                             try:
                                 new_combo = False
                                 current_pitcher = p.loc[pi]
                                 cp_sal = current_pitcher['fd_salary'].item()
                                 #filter out pitchers too expensive for current lineup
                                 p_sal_filt = (p['fd_salary'] <= (rem_sal + cp_sal))
                                 #filter out pitchers not being used for slate. 
                                 p_teams = p.loc[pd.keys(), 'team'].values
                                 p_team_filt = ((p['team'].isin(p_teams)) & (~p['fd_id'].isin(no_replace)))
                                 #filter out pitchers going against any team in current lineup
                                 h_df = h[h['fd_id'].isin(lineup)]
                                 used_teams=h_df['team'].unique()
                                 p_opp_filt = (~p['opp'].isin(used_teams))
                                 #potential replacement pitchers
                                 replacements = p[p_sal_filt & p_team_filt & p_opp_filt]
                                 #use the pitcher with the most projected points in above rps
                                 new_pitcher = replacements.loc[replacements[pitcher_replace_key].idxmax()]
                                 #reset the index and information for the pi/p_info variables
                                 pi = replacements[pitcher_replace_key].idxmax()
                                 p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
                                 np_id = new_pitcher['fd_id']
                                 np_sal = new_pitcher['fd_salary']
                                 #replacement found, subtract replaced pitchers salary and add in new p's.
                                 salary -= cp_sal
                                 salary += np_sal
                                 rem_sal = max_sal - salary
                                 #insert new pitcher into the static lineup pitcher spot
                                 lineup[0] = np_id
                             #same as above exception, but try with pitchers not being used in slate.   
                             except(ValueError,KeyError):
                                 if not expand_pitchers:
                                     reset = True
                                     print(f"Problem combining stack: {stack} secondary: {secondary_stack} pitcher:{p_info[3]}")
                                     break
                                 else:
                                     try:
                                         current_pitcher = p.loc[pi]
                                         cp_sal = current_pitcher['fd_salary'].item()
                                         p_sal_filt = (p['fd_salary'] <= (rem_sal + cp_sal))
                                         h_df = h[h['fd_id'].isin(lineup)]
                                         used_teams=h_df['team'].unique()
                                         p_opp_filt = (~p['opp'].isin(used_teams))
                                         replacements = p[p_sal_filt & p_opp_filt] 
                                         new_pitcher = replacements.loc[replacements[pitcher_replace_key].idxmax()]
                                         pi = replacements[pitcher_replace_key].idxmax()
                                         p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
                                         np_id = new_pitcher['fd_id']
                                         np_sal = new_pitcher['fd_salary']
                                         salary -= cp_sal
                                         salary += np_sal
                                         rem_sal = max_sal - salary
                                         lineup[0] = np_id
                                         if not pitchers.get(pi):
                                             pd[pi] = -1
                                     except(KeyError, ValueError):
                                         reset = True
                                         print(f"Problem combining stack: {stack} secondary: {secondary_stack} pitcher:{p_info[3]}")
                                         break
                #at this point the selected hitter's salary has not put the salary over max_sal
                h_id = hitter['fd_id']
                idx = p_map[ps]
                lineup[idx] = h_id
            if secondary_stack and not reset and secondary_stack not in no_secondary_replace:
                random.shuffle(needed_pos)
                for ps in needed_pos:
                    if rem_sal == 0:
                        break
                    idx = p_map[ps]
                    if lineup[idx] not in all_in and lineup[idx] not in never_replace:
                        selected = h[h['fd_id'] == lineup[idx]]
                        s_sal = selected['fd_salary'].item()
                        
                        s_team = selected['team'].item()
                        if lineup[idx] in always_replace or s_team != secondary_stack:
                            sal_filt = (h['fd_salary'] <= (rem_sal + s_sal))
                        else:
                            sal_filt = ((h['fd_salary'] >= s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                        dupe_filt = (~h['fd_id'].isin(lineup))
                        opp_filt = (h['opp'] != p_info[3])
                        s_order = selected['order'].item()
                        s_points = selected['sp_split'].item()
                        higher_points_filt = (h['sp_split'] > s_points)
                        higher_order_filt = (h['order'] < s_order)
                        all_in_filt = ((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in)))
                        never_fill_filt = (~h['fd_id'].isin(never_fill))
                        if 'of' in ps:
                            pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                        elif 'util' in ps:
                            pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                        else:
                            pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                        hitters = h[pos_filt & dupe_filt & sal_filt & secondary_stack_filt & all_in_filt]
                        if len(hitters.index) == 0:
                            if s_team != secondary_stack:
                                hitters = h[pos_filt & dupe_filt & sal_filt & secondary_stack_filt]
                            else:   
                                hitters = h[pos_filt & dupe_filt & sal_filt & secondary_stack_filt & (higher_order_filt | higher_points_filt) & never_fill_filt]
                        
                        if len(hitters.index) > 0:
                            hitter = hitters.loc[hitters['fd_salary'].idxmax()]
                            selected_name = selected['name'].max()
                            hitter_name = hitter['name']
                            print(f"Replacing {selected_name} with {hitter_name}.(sec. up)")
                            
                            n_sal = hitter['fd_salary'].item()
                            salary -= s_sal
                            salary += n_sal
                            rem_sal = max_sal - salary
                            lineup[idx] = hitter['fd_id']
            if not reset and stack not in no_stack_replace:
                random.shuffle(stacked_pos)
                for ps in stacked_pos:
                    if rem_sal == 0:
                        break
                    idx = p_map[ps]
                    if lineup[idx] not in all_in and lineup[idx] not in never_replace:
                        selected = h[h['fd_id'] == lineup[idx]]
                        
                        s_sal = selected['fd_salary'].item()
                        s_order = selected['order'].item()
                        s_points = selected['sp_split'].item()
                        higher_points_filt = (h['sp_split'] > s_points)
                        higher_order_filt = (h['order'] < s_order)
                        if lineup[idx] in always_replace:
                            sal_filt = (h['fd_salary'] <= (rem_sal + s_sal))
                        else:
                            sal_filt = ((h['fd_salary'] >= s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                        dupe_filt = (~h['fd_id'].isin(lineup))
                        stack_only_filt = (h['team'] == stack)
                        all_in_filt = ((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in)))
                        never_fill_filt = (~h['fd_id'].isin(never_fill))
                        if 'of' in ps:
                            pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                        elif 'util' in ps:
                            pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                        else:
                            pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                        hitters = h[pos_filt & dupe_filt & sal_filt & stack_only_filt & all_in_filt]
                        if len(hitters.index) == 0:
                            hitters = h[pos_filt & dupe_filt & sal_filt & stack_only_filt & (higher_order_filt | higher_points_filt) & never_fill_filt]
                        if len(hitters.index) > 0:
                            hitter = hitters.loc[hitters['fd_salary'].idxmax()]
                            selected_name = selected['name'].max()
                            hitter_name = hitter['name']
                            print(f"Replacing {selected_name} with {hitter_name}.(stack-up)")
                            n_sal = hitter['fd_salary'].item()
                            salary -= s_sal
                            salary += n_sal
                            rem_sal = max_sal - salary
                            lineup[idx] = hitter['fd_id']
                random.shuffle(needed_pos)
            if replace_secondary_all_in and secondary_stack not in never_replace_secondary:
                for ps in needed_pos:
                    if rem_sal == 0 or len(all_in) == 0:
                        break
                    idx = p_map[ps]
                    if lineup[idx] not in all_in and lineup[idx] not in never_replace:
                        idx = p_map[ps]
                        selected = h[h['fd_id'] == lineup[idx]]
                        s_team = selected['team'].item()
                        s_order = selected['order'].item()
                        if s_order > all_in_replace_order_sec or s_team != secondary_stack or lineup[idx] in always_replace:
                            s_sal = selected['fd_salary'].item()
                            sal_filt = ((h['fd_salary'] >= s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            dupe_filt = (~h['fd_id'].isin(lineup))
                            opp_filt = (h['opp'] != p_info[3])
                            all_in_filt = ((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in)))
                            if 'of' in ps:
                                pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                            elif 'util' in ps:
                                pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                            else:
                                pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                            hitters = h[pos_filt & dupe_filt & sal_filt & all_in_filt & opp_filt & stack_filt]
                            if len(hitters.index) > 0:
                                qualified_sal = hitters['fd_salary'].max() - all_in_replace_filt
                                qualified_sal_hitters = hitters[hitters['fd_salary'] >= qualified_sal]
                                hitter = qualified_sal_hitters.loc[qualified_sal_hitters['t_count'].idxmin()]
                                selected_name = selected['name'].max()
                                hitter_name = hitter['name']
                                print(f"Replacing {selected_name} with {hitter_name}.(all in sec.)")
                                
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']
                    
                        
            if replace_primary_all_in and stack not in never_replace_primary:
                for ps in stacked_pos:
                    if rem_sal == 0 or len(all_in) == 0:
                        break
                    idx = p_map[ps]
                    h_df = h[h['fd_id'].isin(lineup)]
                    team_counts = dict(h_df['team'].value_counts())
                    team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                    if lineup[idx] not in all_in and lineup[idx] not in never_replace:
                        idx = p_map[ps]
                        selected = h[h['fd_id'] == lineup[idx]]
                        s_order = selected['order'].item()
                        if s_order > all_in_replace_order_stack or lineup[idx] in always_replace:
                            s_sal = selected['fd_salary'].item()
                            sal_filt = ((h['fd_salary'] >= s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            # dupe_filt = (~h['fd_id'].isin(lineup))
                            dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(team_counts_list)))
                            opp_filt = (h['opp'] != p_info[3])
                            all_in_filt = ((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in)))
                            if 'of' in ps:
                                pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                            elif 'util' in ps:
                                pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                            else:
                                pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                            hitters = h[pos_filt & dupe_filt & sal_filt & all_in_filt & opp_filt]
                            if len(hitters.index) > 0:
                                qualified_sal = hitters['fd_salary'].max() - all_in_replace_filt
                                qualified_sal_hitters = hitters[hitters['fd_salary'] >= qualified_sal]
                                hitter = qualified_sal_hitters.loc[qualified_sal_hitters['t_count'].idxmin()]
                                selected_name = selected['name'].max()
                                hitter_name = hitter['name']
                                print(f"Replacing {selected_name} with {hitter_name}.(all in stack.)")
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']
                                    
                                    
                                    
            if supreme_all_in:
                all_positions = stacked_pos + needed_pos
                for ps in all_positions:
                    if ps in supreme_all_in_pos:
                        if rem_sal == 0:
                            break
                        idx = p_map[ps]
                        h_df = h[h['fd_id'].isin(lineup)]
                        team_counts = dict(h_df['team'].value_counts())
                        team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                        if lineup[idx] not in no_supreme_replace:
                            idx = p_map[ps]
                            selected = h[h['fd_id'] == lineup[idx]]
                            s_sal = selected['fd_salary'].item()
                            sal_filt = ((h['fd_salary'] >= s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            # dupe_filt = (~h['fd_id'].isin(lineup))
                            dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(team_counts_list)))
                            opp_filt = (h['opp'] != p_info[3])
                            all_in_filt = ((h['fd_id'].isin(supreme_all_in)) | (h['team'].isin(supreme_all_in)))
                            if 'of' in ps:
                                pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                            elif 'util' in ps:
                                pos_filt = ((h['fd_r_position'].apply(lambda x: ps in x)) & (~h['fd_id'].isin(no_utility)))
                            else:
                                pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                            hitters = h[pos_filt & dupe_filt & sal_filt & all_in_filt & opp_filt]
                            if len(hitters.index) > 0:
                                qualified_sal = hitters['fd_salary'].max() - all_in_replace_filt
                                qualified_sal_hitters = hitters[hitters['fd_salary'] >= qualified_sal]
                                hitter = qualified_sal_hitters.loc[qualified_sal_hitters['t_count'].idxmin()]
                                selected_name = selected['name'].max()
                                hitter_name = hitter['name']
                                print(f"Replacing {selected_name} with {hitter_name}.(supreme all in.)")
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']        
                    
                        
            #if remaining salary is greater than specified arg max_surplus
            if lus % 2 == 0 and not always_pitcher_first:
                if not reset and (enforce_hitter_surplus or (p_info[0] in lock and always_replace_pitcher_lock)) and rem_sal > max_surplus and (not new_combo or p_info[0] in no_combos or not no_surplus_secondary_stacks):
                    random.shuffle(needed_pos)    
                    for ps in needed_pos:
                        if rem_sal <= max_surplus:
                            break
                        if 'of' in ps:
                            order_filt = ((h['order'] <= of_max_order) | exempt_filt)
                        elif '1b' in ps:
                            order_filt = ((h['order'] <= first_max_order) | exempt_filt)
                        elif '2b' in ps:
                            order_filt = ((h['order'] <= second_max_order) | exempt_filt)
                        elif 'ss' in ps:
                            order_filt = ((h['order'] <= ss_max_order) | exempt_filt)
                        elif '3b' in ps:
                            order_filt = ((h['order'] <= third_max_order) | exempt_filt)
                        elif 'util' in ps:
                            order_filt = ((h['order'] <= util_max_order) | exempt_filt)
                        idx = p_map[ps]
                        selected = h[h['fd_id'] == lineup[idx]]
                        s_sal = selected['fd_salary']
                        if s_sal < h['fd_salary'].quantile(high_salary_quantile) and ps not in never_replace and lineup[idx] not in all_in:
                            #filter out players already in lineup, lineup will change with each iteration
                            dupe_filt = (~h['fd_id'].isin(lineup))
                            #filter out players going against current lineup's pitcher
                            opp_filt = (h['opp'] != p_info[3])
                            if filter_last_replaced:
                                if 'of' in ps:
                                    last_replaced_filt = (h['fd_id'] != last_replaced['of'])
                                else:
                                    last_replaced_filt = (h['fd_id'] != last_replaced[ps])
                            else:
                                last_replaced_filt = (h['fd_id'] != 1)
                            #filter out players not eligible for the current position being filled.
                            if 'of' in ps:
                                pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                            elif 'util' in ps:
                                pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                            else:
                                pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                            #filter out players with a salary less than high_salary_quantile arg
                            sal_filt = ((h['fd_salary'] > s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & secondary_stack_filt]
                            if len(hitters.index) == 0:
                                if fill_high_salary_first:
                                    sal_filt = ((h['fd_salary'] >= np.floor(h['fd_salary'].quantile(high_salary_quantile))) & (h['fd_salary'] <= (rem_sal + s_sal)))
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                            if len(hitters.index) > 0:
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']
                            else:
                                sal_filt = ((h['fd_salary'] > s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & order_filt & secondary_stack_filt]
                                if len(hitters.index) == 0:
                                    hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                if len(hitters.index) > 0:
                                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                    n_sal = hitter['fd_salary'].item()
                                    salary -= s_sal
                                    salary += n_sal
                                    rem_sal = max_sal - salary
                                    lineup[idx] = hitter['fd_id']
                            if len(hitters.index) > 0:
                                if 'of' in ps:
                                     last_replaced['of'] = hitter['fd_id']
                                else:
                                    last_replaced[ps] = hitter['fd_id']
                    
                if not reset and enforce_pitcher_surplus and rem_sal > pitcher_surplus and p_info[0] not in lock and (not new_combo or p_info[0] in no_combos):
                    #filter out pitchers who would put the salary over the max_sal if inserted
                    current_pitcher = p.loc[pi]
                    cp_sal = current_pitcher['fd_salary'].item()
                    p_sal_filt = (p['fd_salary'] <= max_sal - (salary - cp_sal))
                    #filter out pitchers not being used for slate.
                    p_teams = p.loc[pd.keys(), 'team'].values
                    p_team_filt = (p['team'].isin(p_teams))
                    #filter out pitchers going against teams already in lineup
                    h_df = h[h['fd_id'].isin(lineup)]
                    used_teams=h_df['team'].unique()
                    p_opp_filt = (~p['opp'].isin(used_teams))
                    top_pitcher_filt = (p['fd_id'].isin(always_replace_first))
                    replacements = p[p_sal_filt & p_team_filt & p_opp_filt & top_pitcher_filt]
                    if len(replacements.index) == 0:
                        replacements = p[p_sal_filt & p_team_filt & p_opp_filt]
                    #if a pitcher meeting parameters exists, insert in lineup, else leave lineup as is.
                    if len(replacements.index) > 1:
                        new_pitcher = replacements.loc[replacements['fd_salary'].idxmax()]
                        pi = replacements['fd_salary'].idxmax()
                        p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
                        np_id = new_pitcher['fd_id']
                        np_sal = new_pitcher['fd_salary']
                        #subtract replaced pitcher's salary, add new pitcher's salary
                        salary -= cp_sal
                        salary += np_sal
                        rem_sal = max_sal - salary
                        lineup[0] = np_id
            else:
                
                if not reset and enforce_pitcher_surplus and rem_sal > pitcher_surplus and p_info[0] not in lock and (not new_combo or p_info[0] in no_combos or not no_surplus_secondary_stacks):
                    #filter out pitchers who would put the salary over the max_sal if inserted
                    current_pitcher = p.loc[pi]
                    cp_sal = current_pitcher['fd_salary'].item()
                    p_sal_filt = (p['fd_salary'] <= max_sal - (salary - cp_sal))
                    #filter out pitchers not being used for slate.
                    p_teams = p.loc[pd.keys(), 'team'].values
                    p_team_filt = (p['team'].isin(p_teams))
                    #filter out pitchers going against teams already in lineup
                    h_df = h[h['fd_id'].isin(lineup)]
                    used_teams=h_df['team'].unique()
                    p_opp_filt = (~p['opp'].isin(used_teams))
                    top_pitcher_filt = (p['fd_id'].isin(always_replace_first))
                    replacements = p[p_sal_filt & p_team_filt & p_opp_filt & top_pitcher_filt]
                    if len(replacements.index) == 0:
                        replacements = p[p_sal_filt & p_team_filt & p_opp_filt]
                    #if a pitcher meeting parameters exists, insert in lineup, else leave lineup as is.
                    if len(replacements.index) > 1:
                        new_pitcher = replacements.loc[replacements['fd_salary'].idxmax()]
                        pi = replacements['fd_salary'].idxmax()
                        p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
                        np_id = new_pitcher['fd_id']
                        np_sal = new_pitcher['fd_salary']
                        #subtract replaced pitcher's salary, add new pitcher's salary
                        salary -= cp_sal
                        salary += np_sal
                        rem_sal = max_sal - salary
                        lineup[0] = np_id
                if not reset and (enforce_hitter_surplus or (p_info[0] in lock and always_replace_pitcher_lock)) and rem_sal > max_surplus and (not new_combo or p_info[0] in no_combos):
                    random.shuffle(needed_pos)    
                    for ps in needed_pos:
                        if rem_sal <= max_surplus:
                            break
                        if 'of' in ps:
                            order_filt = ((h['order'] <= of_max_order) | exempt_filt)
                        elif '1b' in ps:
                            order_filt = ((h['order'] <= first_max_order) | exempt_filt)
                        elif '2b' in ps:
                            order_filt = ((h['order'] <= second_max_order) | exempt_filt)
                        elif 'ss' in ps:
                            order_filt = ((h['order'] <= ss_max_order) | exempt_filt)
                        elif '3b' in ps:
                            order_filt = ((h['order'] <= third_max_order) | exempt_filt)
                        elif 'util' in ps:
                            order_filt = ((h['order'] <= util_max_order) | exempt_filt)
                        idx = p_map[ps]
                        selected = h[h['fd_id'] == lineup[idx]]
                        s_sal = selected['fd_salary'].item()
                        if s_sal < h['fd_salary'].quantile(high_salary_quantile) and ps not in never_replace and lineup[idx] not in all_in:
                            #filter out players already in lineup, lineup will change with each iteration
                            dupe_filt = (~h['fd_id'].isin(lineup))
                            #filter out players going against current lineup's pitcher
                            opp_filt = (h['opp'] != p_info[3])
                            if filter_last_replaced:
                                if 'of' in ps:
                                    last_replaced_filt = (h['fd_id'] != last_replaced['of'])  
                                else:
                                    last_replaced_filt = (h['fd_id'] != last_replaced[ps])
                            else:
                                last_replaced_filt = (h['fd_id'] != 1)
                            #filter out players not eligible for the current position being filled.
                            if 'of' in ps:
                                pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                            elif 'util' in ps:
                                pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                            else:
                                pos_filt = (h['fd_position'].apply(lambda x: ps in x))

                            #filter out players with a salary less than high_salary_quantile arg
                            sal_filt = ((h['fd_salary'] > s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & secondary_stack_filt]
                            if len(hitters.index) == 0:
                                if fill_high_salary_first:
                                    sal_filt = ((h['fd_salary'] >= np.floor(h['fd_salary'].quantile(high_salary_quantile))) & (h['fd_salary'] <= (rem_sal + s_sal)))
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt] 
                            if len(hitters.index) > 0:
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']
                            else:
                                sal_filt = ((h['fd_salary'] > s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & secondary_stack_filt]
                                if len(hitters.index) == 0:
                                    hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                if len(hitters.index) > 0:
                                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                    n_sal = hitter['fd_salary'].item()
                                    salary -= s_sal
                                    salary += n_sal
                                    rem_sal = max_sal - salary
                                    lineup[idx] = hitter['fd_id']
                            if len(hitters.index) > 0:
                                if 'of' in ps:
                                     last_replaced['of'] = hitter['fd_id']
                                else:
                                    last_replaced[ps] = hitter['fd_id']
                
                
            #if there aren't enough teams being used, replace the utility player with a team not in use.    
            h_df = h[h['fd_id'].isin(lineup)]
            used_teams=h_df['team'].unique()
            if not reset and (len(used_teams) < 3 and p_info[3] in used_teams):
                try:
                    if filter_last_replaced:
                        last_replaced_filt = (h['fd_id'] != last_replaced['util'])
                    else:
                        last_replaced_filt = (h['fd_id'] != 1)
                        
                    #filter out players on teams already in lineup
                    dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(used_teams)))
                    utility = h[h['fd_id'] == lineup[8]]
                    r_salary = utility['fd_salary'].item()
                    order_filt = ((h['order'] <= util_max_order) | exempt_filt)
                    
                    #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = (h['fd_salary'].between((r_salary-util_replace_filt), (rem_sal + r_salary)))
                    #don't use players on team against current pitcher
                    opp_filt = (h['opp'] != p_info[3])
                    #check for all in player first
                    all_in_filt = ((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in)))
                    hitters = h[dupe_filt & sal_filt & opp_filt & all_in_filt & no_utility_filt]
                    if len(hitters.index) == 0:
                        hitters = h[dupe_filt & sal_filt & opp_filt & fade_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt & no_utility_filt]
                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    salary += hitter['fd_salary'].item()
                    salary -= utility['fd_salary'].item()
                    rem_sal = max_sal - salary
                    lineup[8] = hitter['fd_id']
                #if player wasn't avaiable, try with players on non-stacked teams and decrease salary min-threshold by 100
                except(KeyError, ValueError):
                    dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(used_teams)))
                    utility = h[h['fd_id'] == lineup[8]]
                    r_salary = utility['fd_salary'].item()
                    #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = (h['fd_salary'].between((r_salary-(util_replace_filt+100)), (rem_sal + r_salary)))
                    opp_filt = (h['opp'] != p_info[3])
                    hitters = h[dupe_filt & sal_filt & opp_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    salary += hitter['fd_salary'].item()
                    salary -= utility['fd_salary'].item()
                    rem_sal = max_sal - salary
                    lineup[8] = hitter['fd_id']
                if filter_last_replaced:
                    last_replaced['util'] = hitter['fd_id']
       
            #don't insert lineup unless it's not already inserted        
            used_players = []
            h_df = h[h['fd_id'].isin(lineup)]
            used_teams = h_df['team'].unique()
            update_last_replaced = False
            while not reset and (sorted(lineup) in sorted_lus or (len(used_teams) < 3 and p_info[3] in used_teams)):
                try:
                    if filter_last_replaced:
                        update_last_replaced = True
                        last_replaced_filt = (h['fd_id'] != last_replaced['util'])
                    else:
                        last_replaced_filt = (h['fd_id'] != 1)
                    #redeclare use_teams each pass
                    h_df = h[h['fd_id'].isin(lineup)]
                    team_counts = dict(h_df['team'].value_counts())
                    team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                    used_teams = h_df['team'].unique()
                    #append players already attempted to used_players and filter them out each loop
                    used_filt = (~h['fd_id'].isin(used_players))
                    dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(team_counts_list)))
                    utility = h[h['fd_id'] == lineup[8]]
                    used_players.append(utility['fd_id'])
                    r_salary = utility['fd_salary'].item()
                    order_filt = ((h['order'] <= util_max_order) | exempt_filt)
                    #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = (h['fd_salary'].between((r_salary-util_replace_filt), (rem_sal + r_salary)))
                    #don't use players on team against current pitcher
                    opp_filt = (h['opp'] != p_info[3])
                    all_in_filt = ((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in)))
                    hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & all_in_filt & no_utility_filt]
                    if len(hitters.index) == 0:
                        hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & (fade_filt | value_filt) & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt & no_utility_filt]
                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    used_players.append(hitter['fd_id'])
                    salary += hitter['fd_salary'].item()
                    salary -= utility['fd_salary'].item()
                    rem_sal = max_sal - salary
                    lineup[8] = hitter['fd_id']
                    h_df = h[h['fd_id'].isin(lineup)]
                    used_teams = h_df['team'].unique()
                #same as above, but no fade_filt and increasing minimum thresehold by 100.
                except (KeyError, ValueError):
                    try:
                        h_df = h[h['fd_id'].isin(lineup)]
                        team_counts = dict(h_df['team'].value_counts())
                        team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                        used_teams = h_df['team'].unique()
                        used_filt = (~h['fd_id'].isin(used_players))
                        dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(team_counts_list)))
                        utility = h[h['fd_id'] == lineup[8]]
                        used_players.append(utility['fd_id'])
                        r_salary = utility['fd_salary'].item()
                        sal_filt = (h['fd_salary'].between((r_salary-(util_replace_filt+100)), (rem_sal + r_salary)))
                        opp_filt = (h['opp'] != p_info[3])
                        hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt & no_utility_filt]
                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                        used_players.append(hitter['fd_id'])
                        salary += hitter['fd_salary'].item()
                        salary -= utility['fd_salary'].item()
                        rem_sal = max_sal - salary
                        lineup[8] = hitter['fd_id']
                        h_df = h[h['fd_id'].isin(lineup)]
                        used_teams = h_df['team'].unique()
                    except (KeyError, ValueError):
                        reset = True
                        print('resetting, no utility available.')
                        break
                
            if reset == True:
                continue
            if update_last_replaced:
                last_replaced['util'] = lineup[8]
            #!!the lineup meets all parameters at this point.!!
            #decrease lus arg, loop ends at 0.
            lus -=1
            if secondary_stack:
                secondary[secondary_stack] -= 1
            #append the new lineup to the sorted lus list for next loop.
            sorted_lus.append(sorted(lineup))
            last_lu = lineup.copy()
            #insert the lineup in the lineup df
            self.insert_lineup(index_track, lineup)
            #increase index for next lineup insertion.
            index_track += 1
            #decrease the current stack's value, so it won't be attempted once it reaches 0.
            s[stack] -= 1
            #decrease the pitcher's pd value, so it won't be used once expected lus filled.
            pd[pi] -= 1
            #keep track of total insertions and stack insertions, players exceeding max_lu_total will be dropped next loop.
            lu_filt = (h['fd_id'].isin(lineup))
            h.loc[lu_filt, 't_count'] += 1
            h.loc[lu_filt & stack_filt, 'ns_count'] += 1
            # print(lus)
            # print(index_track)
            print(salary)
            
            if not stack_track[stack]['secondary'].get(secondary_stack):
                stack_track[stack]['secondary'][secondary_stack] = 1
            else:
                stack_track[stack]['secondary'][secondary_stack] += 1
            if not stack_track[stack]['pitchers'].get(p_info[0]):
                stack_track[stack]['pitchers'][p_info[0]] = 1
            else:
                stack_track[stack]['pitchers'][p_info[0]] += 1
            p_stack_combo = [p_info[0], secondary_stack]
            if p_stack_combo not in stack_track[stack]['combos'] and secondary_stack:
                stack_track[stack]['combos'].append(p_stack_combo)
            #keep track of players counts, regardless if they're eventually dropped.
            h_count_df.loc[(h_count_df['fd_id'].isin(lineup)), 't_count'] += 1
            p_count_df.loc[(p_count_df['fd_id']) == lineup[0], 't_count'] += 1
        #dump the counts into pickled df for analysis    
        h_count_file = pickle_path(name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)   
        p_count_file = pickle_path(name=f"p_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        with open(h_count_file, "wb") as f:
                pickle.dump(h_count_df, f)
        with open(p_count_file, "wb") as f:
                pickle.dump(p_count_df, f)
        print(secondary)

        return stack_track