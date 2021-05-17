import pickle
import pandas as pd
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
                 stack_threshold = 50, 
                 pitcher_threshold = 75,
                 heavy_weight_stack=False, 
                 heavy_weight_p=False, 
                 max_batting_order=7):
        
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
        self.max_batting_order = max_batting_order
        
    def entries_df(self, reset=False):
        df_file = pickle_path(name=f"lineup_entries_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if path.exists() and not reset:
            df = pd.read_pickle(path)
        else:
            cols = ['entry_id', 'contest_id', 'contest_name', 'P', 'C/1B', '2B', '3B', 'SS', 'OF' ,'OF.1', 'OF.2', 'UTIL']
            csv_file = self.entry_csv
            with open(csv_file, 'r') as f:
                df = pd.read_csv(f, usecols = cols)
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
            df = pd.read_pickle(path)
        else:   
            cols = ['Player ID + Player Name', 'Id', 'Position', 'First Name', 'Nickname', 'Last Name', 'FPPG', 'Salary', 'Game', 'Team', 'Opponent', 'Injury Indicator', 'Injury Details', 'Roster Position']
            csv_file = self.entry_csv
            with open(csv_file, 'r') as f:
                df = pd.read_csv(f, skiprows = lambda x: x < 6, usecols=cols)
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
            order_filter = (hitters['order'] <= self.max_batting_order)
            hitters['points_salary'] = hitters['points'] - (hitters['fd_salary'] / 1000)
            hitters_order = hitters[order_filter]
            team.salary = hitters_order['fd_salary'].sum() / len(hitters_order.index)
            cols = ["raw_points", "venue_points", "temp_points", "points", "salary", "sp_mu", "raw_talent"]
            points_file = pickle_path(name=f"team_points_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
            points_path = settings.FD_DIR.joinpath(points_file)
            if points_path.exists():
                p_df = pd.read_pickle(points_path)
            else:
                p_df = pd.DataFrame(columns = cols)
            p_df.loc[team.name, cols] = [team.raw_points, team.venue_points, team.temp_points, team.points, team.salary, team.sp_mu, team.lu_talent_sp]
            with open(points_file, 'wb') as f:
                pickle.dump(p_df, f)
            if path.exists():
                df = pd.read_pickle(path)
                df.drop(index = df[df['team'] == team.name].index, inplace=True)
                df = pd.concat([df, hitters], ignore_index=True)
            else:
                df = hitters
            with open(df_file, "wb") as f:
                 pickle.dump(df, f)
        
        return df
    def get_pitchers(self):
        df_file = pickle_path(name=f"all_p_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        for team in self.team_instances:
            pitcher = team.sp_df()
            merge = self.player_info_df[(self.player_info_df['team'] == team.name) & (self.player_info_df['is_p'] == True)]
            try:
                pitcher = sm_merge_single(pitcher, merge, ratio=.75, suffixes=('', '_fd')) 
            except KeyError:
                continue
            pitcher.drop_duplicates(subset='mlb_id', inplace=True)
            if path.exists():
                df = pd.read_pickle(path)
                df.drop(index = df[df['team'] == team.name].index, inplace=True)
                df = pd.concat([df, pitcher], ignore_index=True)
            else:
                df = pitcher
            with open(df_file, "wb") as f:
                 pickle.dump(df, f)
        # df['p_z'] = (df['points'] - df['points'].mean()) / df['points'].std()
        # df['rmu_z'] = (df['raw_mu'] - df['raw_mu'].mean()) / df['raw_mu'].std()
        # df['mu_z'] = (df['mu'] - df['mu'].mean()) / df['mu'].std()
        # df['kp_z'] = (df['k_pred'] - df['k_pred'].mean()) / df['k_pred'].std()
        # df['rk_z'] = (df['k_pred_raw'] - df['k_pred_raw'].mean()) / df['k_pred_raw'].std()
        # df['s_z'] = ((df['fd_salary'] - df['fd_salary'].mean()) / df['fd_salary'].std()) * -1
        # df['pps_z'] = (df['pitches_start'] - df['pitches_start'].mean()) / df['pitches_start'].std()
        # df['z'] = (df['p_z'] * 1) + (df['rmu_z'] * 1) + (df['mu_z'] * 1) + (df['kp_z'] * 1) + (df['rk_z'] * 1) + (df['s_z'] * 1) + (df['pps_z'] * 1)
            
        return df
    def h_df(self):
        df_file = pickle_path(name=f"all_h_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if not path.exists():
            df = self.get_hitters()
        else:
            df = pd.read_pickle(path)
        return df
    def p_df(self):
        df_file = pickle_path(name=f"all_p_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if not path.exists():
            df = self.get_pitchers()
        else:
            df = pd.read_pickle(path)
        df = df[df['is_p'] == True]
        return df
    
    def points_df(self):
        file = pickle_path(name=f"team_points_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(file)
        if not path.exists():
            self.get_hitters()
        df = pd.read_pickle(path).apply(pd.to_numeric)
        return df
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
    def stacks_df(self):
        lineups = self.lineups
        df = self.points_df()
        i = df[(df.index.isin(self.no_stack))].index
        df.drop(index = i, inplace=True)
        df['p_z'] = ((df['points'] - df['points'].mean()) / df['points'].std()) * self.points_weight
        df['s_z'] = ((df['salary'] - df['salary'].mean()) / df['salary'].std()) * -(2 - self.points_weight)
        df['stacks'] = 1000
        increment = 0
        while df['stacks'].max() > self.stack_threshold:
            df_c = df.copy()
            df_c['z'] = ((df_c['p_z'] + df_c['s_z']) / 2) + increment
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
            df = pd.read_pickle(path)
            return df
        else:
            return "No pitchers stored yet."
    def h_counts(self):
        file = pickle_path(name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(file)
        if path.exists():
            df = pd.read_pickle(path)
            return df
        else:
            return "No lineups stored yet."
        
    
    def build_lineups(self, no_secondary = [], info_only = False,
                      lus = 150, 
                      index_track = 0, 
                      non_random_stack_start=0,
                      max_lu_total = 75,
                      variance = 25, 
                      below_avg_count = 15,
                      risk_limit = 25,
                      of_count_adjust = 6,
                      max_lu_stack = 50, 
                      max_sal = 35000, 
                      stack_size = 4,
                      stack_sample = 5, 
                      stack_salary_pairing_cutoff=3,
                      stack_max_pair = 6,
                      fallback_stack_sample = 6,
                      stack_expand_limit = 20,
                      non_stack_max_order=6, 
                      util_replace_filt = 200,
                      non_stack_quantile = .9, 
                      high_salary_quantile = .9,
                      secondary_stack_cut = 0,
                      no_surplus_cut = 75,
                      single_stack_surplus = 600,
                      double_stack_surplus = 1200,
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
                      find_cheap_stacks = False,
                      always_pitcher_first=False,
                      enforce_pitcher_surplus = True,
                      enforce_hitter_surplus = True,
                      filter_last_replaced=True,
                      always_replace_pitcher_lock=False,
                      p_sal_stack_filter = True,
                      exempt=[],
                      all_in=[],
                      lock = [],
                      no_combos = [],
                      never_replace=[],
                      x_fallback = [],
                      stack_only = [],
                      limit_risk = [],
                      custom_counts={},
                      remove_positions = None,
                      custom_stacks = None,
                      custom_pitchers = None,
                      custom_secondary = None,):
        
        max_order = self.max_batting_order
        # all hitters in slate
        h = self.h_df()
        #dropping faded, platoon, and low order (max_order) players.
        h_fade_filt = ((h['team'].isin(self.h_fades)) | (h['fd_id'].isin(self.h_fades)))
        h_order_filt = (h['order'] > max_order)
        h_exempt_filt = (~h['fd_id'].isin(exempt))
        hfi = h[(h_fade_filt | h_order_filt) & h_exempt_filt].index
        h.drop(index=hfi,inplace=True)
        #count each players entries
        h['t_count'] = 0
        #count non_stack
        h['ns_count'] = 0
        h_count_df = h.copy()
        #risk_limit should always be >= below_avg_count
        h.loc[(h['sp_split'] < h['sp_split'].median()),'t_count'] = below_avg_count
        h.loc[(h['team'].isin(stack_only)), 't_count'] = 1000
        exempt_filt = (h['fd_id'].isin(exempt))
        h.loc[exempt_filt, 't_count'] = 0
        h.loc[h_order_filt, 't_count'] = below_avg_count
        h.loc[(h['fd_position'].apply(lambda x: 'of' in x)), 't_count'] -= of_count_adjust
        h.loc[(h['team'].isin(limit_risk)), 't_count'] = risk_limit
        h.loc[((h['fd_id'].isin(all_in)) | (h['team'].isin(all_in))), 't_count'] = -1000
        
        for k,v in custom_counts.items():
            h.loc[h['fd_id'] == k, 't_count'] = v
        #all pitchers...
        p = self.p_df()
        p_fade_filt = (p['team'].isin(self.p_fades))
        pfi = p[p_fade_filt].index
        p.drop(index=pfi,inplace=True)
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
       
        
        stack_track = {}
        
        for team in stack_list:
            stack_track[team] = {}
            stack_track[team]['pitchers'] = {}
            stack_track[team]['secondary'] = {}
            stack_track[team]['salary'] = self.points_df().loc[team, 'salary'].item()
            stack_track[team]['combos'] = []
        sorted_lus = []
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
        last_replaced = p_map.copy()
        
        if remove_positions:
            for k,v in remove_positions.items():
                position_id_filt = (h['fd_id'] == k)
                h.loc[position_id_filt, 'fd_position'].apply(lambda x: x.remove(v))
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
            information['stack_combos'] = len(list(combinations(list(s), 2)))
            information['pitcher_combos'] = len(list(combinations(list(s), 2))) * len(list(pd))                            
            return information

        
        #lineups to build
        while lus > 0:
            non_random_stack = stack_list[non_random_stack_start]
            
            #if lineup fails requirements, reset will be set to true.
            reset = False
            
            #drop players already in max_lu_total/max_lu_stack lineups
            # total_filt = (h['t_count'] >= max_lu_total)
            # count_filt = (h['ns_count'] >= max_lu_stack)
            # c_idx = h[total_filt | count_filt].index
            # h.drop(index=c_idx,inplace=True)
            #pitchers expected lineups will be reduced by 1 for each successful lineup insertion
            pitchers = {k:v for k,v in pd.items() if v > 0}
            #randomly select a pitcher in pool, keys are == p.index
            pi = random.choice(list(pitchers.keys()))
            #lookup required pitcher info. by index (pi)
            p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
            #start with the pitchers salary
            salary = 0 + p_info[1]
            #choose a stack that hasn't exceed its insertion limit and is not playing the current pitcher
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
                if non_random_stack_attempts > len(stack_list):
                    stacks = {k:v for k,v in s.items() if v > 0 and k != p_info[2]}
                
                non_random_stack_start += 1
                if non_random_stack_start > len(stack_list) - 1:
                    non_random_stack_start = 0
            
            #randomly select an eligible stack. keys == team names.
            try:
                stack = random.choice(list(stacks.keys()))
            except IndexError:
                print(f"Trying to stack {stack} against pitcher for {p_info[2]}.")
                continue
            remaining_stacks = stacks[stack]
            #lookup players on the team for the selected stack
            stack_df = h[h['team'] == stack]
            if remaining_stacks % 4 == 0:
                stack_key = 'sp_split'
            # elif remaining_stacks % 3 == 0 and find_cheap_stacks:
            #     stack_key = 'points_salary'
            elif remaining_stacks % 3 == 0 and not find_cheap_stacks:
                stack_key = 'fd_hr_weight'
            elif remaining_stacks % 2 == 0:
                stack_key = 'points'
            else:
                stack_key = 'exp_ps_sp_pa'
            if lus % 2 == 0:
                non_stack_key='exp_ps_sp_pa'
                pitcher_replace_key = 'k_pred'
            else:
                non_stack_key='points'
                pitcher_replace_key = 'points'
            #filter the selected stack by stack_sample arg.

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
                for x in pl1:
                    #iterate each player's list of positions, append if not already filled
                    if x[0] not in pl2:
                        pl2.append(x[0])
                    elif x[-1] not in pl2:
                        pl2.append(x[-1])
                    #index 1 because if len == 2, would've already checked
                    elif len(x) > 2 and x[1] not in pl2:
                        pl2.append(x[1])
                    #if player is eligible outfielder and first of slot taken
                    elif 'of' in x and pl2.count('of') == 1 and 'of.1' not in pl2:
                        pl2.append('of.1')
                    #if already 2 outfielders and player is eligible outfielder
                    elif 'of' in x and pl2.count('of.1') == 1 and 'of.2' not in pl2:
                        pl2.append('of.2')
            #if couldn't create a 4-man stack, create a 3-man stack        
            if a > 10:
                a = 0
                print(f"Using fallback_stack_sample for {stack}.")
                while len(pl2) != stack_size:
                    a +=1
                    if a > 10:
                        raise Exception(f"Could not create 4-man stack for {stack}.")
                    pl2 = []
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
            #create list of positions that need to be filled, util always empty here.
            needed_pos = [x for x, y in p_map.items() if y in np_list]
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
                
                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info[2] 
                                    and k != stack 
                                    and [p_info[0], k] not in stack_track[stack]['combos']}
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
                                    and k not in stack_track[stack]['secondary']}
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                        and k != p_info[2] 
                                        and k != stack 
                                        and [p_info[0], k] not in stack_track[stack]['combos']}
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info[2] 
                                    and k != stack 
                                    and stack_track[stack]['secondary'].get(k, 0) < stack_max_pair}
                    if len(list(secondary_stacks)) < 1:
                             print('default')
                             secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info[2] 
                                                and k != stack 
                                                }
                    if len(list(secondary_stacks)) < 1:
                            print('-1')
                            secondary_stacks = {k:v for k,v in secondary.items() if v > -1
                                                    and k != p_info[2] 
                                                    and k != stack 
                                                    }
                    if len(list(secondary_stacks)) < 1:
                            print('-2')
                            secondary_stacks = {k:v for k,v in secondary.items() if v > -2
                                                    and k != p_info[2] 
                                                    and k != stack 
                                                    }

                secondary_stack = random.choice(list(secondary_stacks.keys()))
                max_surplus = double_stack_surplus
                secondary[secondary_stack] -= 1
                    
            else:
                enforce_hitter_surplus = True
                secondary_stack = None
                max_surplus = single_stack_surplus
            secondary_stack_filt = (h['team'] == secondary_stack)
            
            
            for ps in needed_pos:
                
                if filter_last_replaced:
                     if 'of' in ps:
                         last_replaced_filt = (h['fd_id'] != last_replaced['of'])
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
                sal_filt = (h['fd_salary'] <= avg_sal)
                try:
                    update_last_replaced = False
                    if not find_cheap_stacks or (p_info[1] <= low_pitcher_salary and p_sal_stack_filter):
                         raise ValueError("Not using salary filter for secondary stacks.")
                    hitters = h[pos_filt & dupe_filt & count_filt & secondary_stack_filt & sal_filt]
                    hitter = hitters.loc[hitters[stack_key].idxmax()]
                    
                except (KeyError, ValueError):
                    try:
                        hitters = h[pos_filt & dupe_filt & count_filt & secondary_stack_filt]
                        hitter = hitters.loc[hitters[stack_key].idxmax()]
                    except (KeyError, ValueError):
                        try:
                            update_last_replaced = True
                            hitters = h[pos_filt & stack_filt & dupe_filt & (fade_filt | value_filt) & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
                            hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                        except (KeyError, ValueError):
                            try:
                                hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                            except (KeyError, ValueError):
                                try:
                                    hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
                                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                except (KeyError, ValueError):
                                    try:
                                        hitters = h[pos_filt & stack_filt & dupe_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
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
                                 p_team_filt = (p['team'].isin(p_teams))
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
                                     print('resetting')
                                     break
                #at this point the selected hitter's salary has not put the salary over max_sal
                h_id = hitter['fd_id']
                idx = p_map[ps]
                lineup[idx] = h_id
            #if remaining salary is greater than specified arg max_surplus
            if lus % 2 == 0 and not always_pitcher_first:
                if not reset and (enforce_hitter_surplus or (p_info[0] in lock and always_replace_pitcher_lock)) and rem_sal > max_surplus and (not new_combo or p_info[0] in no_combos):
                    random.shuffle(needed_pos)    
                    for ps in needed_pos:
                        if rem_sal < max_surplus:
                            break
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
                            sal_filt = ((h['fd_salary'] >= np.floor(h['fd_salary'].quantile(high_salary_quantile))) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & secondary_stack_filt]
                            if len(hitters.index) == 0:
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
                            if len(hitters.index) > 0:
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']
                            else:
                                sal_filt = ((h['fd_salary'] > s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & secondary_stack_filt]
                                if len(hitters.index) == 0:
                                    hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
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
                        if rem_sal < max_surplus:
                            break
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
                            sal_filt = ((h['fd_salary'] >= np.floor(h['fd_salary'].quantile(high_salary_quantile))) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & secondary_stack_filt]
                            if len(hitters.index) == 0:
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
                            if len(hitters.index) > 0:
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']
                            else:
                                sal_filt = ((h['fd_salary'] > s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                                hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & secondary_stack_filt]
                                if len(hitters.index) == 0:
                                    hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
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
                    #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = (h['fd_salary'].between((r_salary-util_replace_filt), (rem_sal + r_salary)))
                    #don't use players on team against current pitcher
                    opp_filt = (h['opp'] != p_info[3])
                    hitters = h[dupe_filt & sal_filt & opp_filt & fade_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
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
                    hitters = h[dupe_filt & sal_filt & opp_filt & count_filt & order_filt & plat_filt & last_replaced_filt]
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
                    used_teams = h_df['team'].unique()
                    #append players already attempted to used_players and filter them out each loop
                    used_filt = (~h['fd_id'].isin(used_players))
                    dupe_filt = (~h['fd_id'].isin(lineup))
                    utility = h[h['fd_id'] == lineup[8]]
                    used_players.append(utility['fd_id'])
                    r_salary = utility['fd_salary'].item()
                    #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = (h['fd_salary'].between((r_salary-util_replace_filt), (rem_sal + r_salary)))
                    #don't use players on team against current pitcher
                    opp_filt = (h['opp'] != p_info[3])
                    hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & (fade_filt | value_filt) & count_filt & stack_filt & order_filt & plat_filt & last_replaced_filt]
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
                        used_teams = h_df['team'].unique()
                        used_filt = (~h['fd_id'].isin(used_players))
                        dupe_filt = (~h['fd_id'].isin(lineup))
                        utility = h[h['fd_id'] == lineup[8]]
                        used_players.append(utility['fd_id'])
                        r_salary = utility['fd_salary'].item()
                        sal_filt = (h['fd_salary'].between((r_salary-(util_replace_filt+100)), (rem_sal + r_salary)))
                        opp_filt = (h['opp'] != p_info[3])
                        hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & count_filt & stack_filt & order_filt & plat_filt & last_replaced_filt]
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
                        print('resetting')
                        break
                
            if reset == True:
                continue
            if update_last_replaced:
                last_replaced['util'] = lineup[8]
            #!!the lineup meets all parameters at this point.!!
            #decrease lus arg, loop ends at 0.
            lus -=1
            #append the new lineup to the sorted lus list for next loop.
            sorted_lus.append(sorted(lineup))
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
            print(lus)
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

        return stack_track