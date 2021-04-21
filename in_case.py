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
            hitters_order = hitters[order_filter]
            team.salary = hitters_order['fd_salary'].sum() / len(hitters_order.index)
            cols = ["raw_points", "venue_points", "temp_points", "points", "salary", "sp_mu"]
            points_file = pickle_path(name=f"team_points_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
            points_path = settings.FD_DIR.joinpath(points_file)
            if points_path.exists():
                p_df = pd.read_pickle(points_path)
            else:
                p_df = pd.DataFrame(columns = cols)
            p_df.loc[team.name, cols] = [team.raw_points, team.venue_points, team.temp_points, team.points, team.salary, team.sp_mu]
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
        
    
    def build_lineups(self, lus = 150, 
                      index_track = 0, 
                      max_lu_total = 75,
                      max_lu_stack = 50, 
                      max_sal = 35000, 
                      stack_sample = 5, 
                      util_replace_filt = 0,
                      variance = 25, 
                      non_stack_quantile = .80, 
                      high_salary_quantile = .80,
                      enforce_pitcher_surplus = True,
                      enforce_hitter_surplus = True, 
                      non_stack_max_order=5, 
                      custom_counts={},
                      fallback_stack_sample = 6,
                      custom_stacks = None,
                      custom_pitchers = None,
                      x_fallback = [],
                      stack_only = [],
                      below_avg_count = 25,
                      stack_expand_limit = 15,
                      of_count_adjust = 6,
                      limit_risk = [],
                      risk_limit = 25,
                      exempt=[],
                      non_stack_key='exp_ps_sp_pa',
                      stack_size = 4,
                      secondary_stack_cut = 90,
                      single_stack_surplus = 600,
                      double_stack_surplus = 900):
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
        h.loc[(h['exp_ps_sp_pa'] < h['exp_ps_sp_pa'].median()),'t_count'] = below_avg_count
        h.loc[(h['team'].isin(stack_only)), 't_count'] = 1000
        exempt_filt = (h['fd_id'].isin(exempt))
        h.loc[exempt_filt, 't_count'] = 0
        h.loc[(h['fd_position'].apply(lambda x: 'of' in x)), 't_count'] -= of_count_adjust
        h.loc[(h['team'].isin(limit_risk)), 't_count'] = risk_limit
        
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
        
        #lineups to build
        while lus > 0:
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
            stacks = {k:v for k,v in s.items() if v > 0 and k != p_info[2]}
            #randomly select an eligible stack. keys == team names.
            try:
                stack = random.choice(list(stacks.keys()))
            except IndexError:
                print('continuing')
                print(stack)
                print(p_info)
                continue
            remaining_stacks = stacks[stack]
            #lookup players on the team for the selected stack
            stack_df = h[h['team'] == stack]
            if remaining_stacks % 4 == 0:
                stack_key = 'total_pitches'
            elif remaining_stacks % 3 == 0:
                stack_key = 'fd_hr_weight'
            elif remaining_stacks % 2 == 0:
                stack_key = 'points'
            else:
                stack_key = 'exp_ps_sp_pa'
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
            if lus > secondary_stack_cut:
                secondary_stacks = {k:v for k,v in s.items() if v > 0 and k != p_info[2] and k != stack}
                secondary_stack = random.choice(list(secondary_stacks.keys()))
                max_surplus = double_stack_surplus
            else:
                secondary_stack = None
                max_surplus = single_stack_surplus
            secondary_stack_filt = (h['team'] == secondary_stack)
            
            for ps in needed_pos:
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
                    hitters = h[pos_filt & dupe_filt & count_filt & secondary_stack_filt]
                    hitter = hitters.loc[hitters[stack_key].idxmax()]
                except (KeyError, ValueError):
                    try:
                        hitters = h[pos_filt & stack_filt & dupe_filt & (fade_filt | value_filt) & opp_filt & sal_filt & count_filt & order_filt & plat_filt]
                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    except (KeyError, ValueError):
                        try:
                            hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt]
                            hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                        except (KeyError, ValueError):
                            try:
                                hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & count_filt & order_filt & plat_filt]
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                            except (KeyError, ValueError):
                                hitters = h[pos_filt & stack_filt & dupe_filt & sal_filt & count_filt & order_filt & plat_filt]
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                
                salary += hitter['fd_salary'].item()
                rem_sal = max_sal - salary
                #if the selected hitter's salary put the lineup over the max. salary, try to find replacement.
                if rem_sal < 0:
                     r_sal = hitter['fd_salary'].item()
                     try:
                         salary_df = hitters[(hitters['fd_salary'] <= (r_sal + rem_sal)) & secondary_stack_filt]
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
                         
                         #if could not find replacment hitter, swap the pitcher for a lower salaried one.
                         except(ValueError,KeyError):
                             try:
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
                                 new_pitcher = replacements.loc[replacements['points'].idxmax()]
                                 #reset the index and information for the pi/p_info variables
                                 pi = replacements['points'].idxmax()
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
                                     new_pitcher = replacements.loc[replacements['points'].idxmax()]
                                     pi = replacements['points'].idxmax()
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
            if not reset and enforce_hitter_surplus and rem_sal > max_surplus:
                random.shuffle(needed_pos)    
                for ps in needed_pos:
                    if rem_sal < max_surplus:
                        break
                    idx = p_map[ps]
                    selected = h[h['fd_id'] == lineup[idx]]
                    s_sal = selected['fd_salary'].item()
                    if s_sal < h['fd_salary'].quantile(high_salary_quantile):
                        #filter out players already in lineup, lineup will change with each iteration
                        dupe_filt = (~h['fd_id'].isin(lineup))
                        #filter out players going against current lineup's pitcher
                        opp_filt = (h['opp'] != p_info[3])
                        #filter out players not eligible for the current position being filled.
                        if 'of' in ps:
                            pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                        elif 'util' in ps:
                            pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                        else:
                            pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                        #filter out players with a salary less than high_salary_quantile arg
                        sal_filt = ((h['fd_salary'] >= np.floor(h['fd_salary'].quantile(high_salary_quantile))) & (h['fd_salary'] <= (rem_sal + s_sal)))
                        hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt]
                        if len(hitters.index) > 0:
                            hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                            n_sal = hitter['fd_salary'].item()
                            salary -= s_sal
                            salary += n_sal
                            rem_sal = max_sal - salary
                            lineup[idx] = hitter['fd_id']
                        else:
                            sal_filt = ((h['fd_salary'] > s_sal) & (h['fd_salary'] <= (rem_sal + s_sal)))
                            hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt]
                            if len(hitters.index) > 0:
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                rem_sal = max_sal - salary
                                lineup[idx] = hitter['fd_id']
                
            if not reset and enforce_pitcher_surplus and rem_sal > max_surplus:
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
                
            #if there aren't enough teams being used, replace the utility player with a team not in use.    
            h_df = h[h['fd_id'].isin(lineup)]
            used_teams=h_df['team'].unique()
            if not reset and (len(used_teams) < 3 and p_info[3] in used_teams):
                try:
                    #filter out players on teams already in lineup
                    dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(used_teams)))
                    utility = h[h['fd_id'] == lineup[8]]
                    r_salary = utility['fd_salary'].item()
                    #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = (h['fd_salary'].between((r_salary-util_replace_filt), (rem_sal + r_salary)))
                    #don't use players on team against current pitcher
                    opp_filt = (h['opp'] != p_info[3])
                    hitters = h[dupe_filt & sal_filt & opp_filt & fade_filt & count_filt & order_filt & plat_filt]
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
                    hitters = h[dupe_filt & sal_filt & opp_filt & count_filt & order_filt & plat_filt]
                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    salary += hitter['fd_salary'].item()
                    salary -= utility['fd_salary'].item()
                    rem_sal = max_sal - salary
                    lineup[8] = hitter['fd_id']
                    
            #don't insert lineup unless it's not already inserted        
            used_players = []
            h_df = h[h['fd_id'].isin(lineup)]
            used_teams = h_df['team'].unique()
            while not reset and (sorted(lineup) in sorted_lus or (len(used_teams) < 3 and p_info[3] in used_teams)):
                try:
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
                    hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & (fade_filt | value_filt) & count_filt & stack_filt & order_filt & plat_filt]
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
                        hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & count_filt & stack_filt & order_filt & plat_filt]
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
        return sorted_lus
    
import statsapi
from functools import cached_property, lru_cache
import re
import pandas as pd
from dfs_tools_mlb.utils.pd import sm_merge
from dfs_tools_mlb.compile import current_season as cs
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac
from dfs_tools_mlb.utils.statsapi import full_schedule
from dfs_tools_mlb.utils.storage import json_path
import json
from dfs_tools_mlb.compile.static_mlb import team_info
from json import JSONDecodeError
from dfs_tools_mlb.utils.strings import plural
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.dataframes.game_data import game_data
from dfs_tools_mlb.utils.storage import pickle_path
from dfs_tools_mlb.utils.strings import ids_string
import numpy as np
import pickle
import datetime
from dfs_tools_mlb import settings
from math import floor, ceil
from pathlib import Path
from dfs_tools_mlb.dataframes.stat_splits import (h_splits, p_splits, h_q, h_q_vr,
                                                  h_q_vl, p_q_rp, p_q_sp, p_q_vl,
                                                  p_q_vr, p_q, p_q_l_vl,p_q_r_vl,p_q_l_vr,
                                                  p_q_r_vr, h_l_vl,h_l_vr,h_r_vr,h_r_vl,hp_q)


p_q_rp['ra-_b_rp'].mean()






class IterTeam(type):
    def __iter__(cls):
        return iter(cls._all_teams)
    
class Team(metaclass=IterTeam):
    _all_teams = []
    def __init__(self, mlb_id, name=''):
        self._all_teams.append(self)
        self.id = mlb_id
        self.name = name
    @cached_property
    def depth(self):
        file = pickle_path(name=f"{self.name}_depth_{tf.today}", directory=settings.DEPTH_DIR)
        path = settings.DEPTH_DIR.joinpath(file)
        if path.exists():
            depth = pd.read_pickle(path)
            return depth
        hydrate = 'person'
        call = statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'depthChart', 'hydrate': hydrate})['roster']
        depth = {
            'starters': [],
            'bullpen': [],
            'hitters': []
            }
        for p in call:
            
            player = {'mlb_id': p['person']['id'],
                      'name': p['person']['fullName'],
                      'mlb_api': p['person'].get('link', ''),
                      'number': p['person'].get('primaryNumber', ''),
                      'born': p['person'].get('birthCity', '') + ', ' + p['person'].get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', p['person'].get('height', ''))),
                      'weight': p['person'].get('weight', ''),
                      'nickname': p['person'].get('nickName', ''),
                      'debut': p['person'].get('mlbDebutDate', ''),
                      'bat_side': p['person'].get('batSide', {}).get('code', ''),
                      'pitch_hand': p['person'].get('pitchHand', {}).get('code', ''),
                      'age': p['person'].get('currentAge', ''),
                      'note': p.get('note', ''),
                      'position_type': p['position']['type'],
                      'position': p['position']['code'],
                      'status': p.get('status', {}).get('code', ''),
                      'team': self.name
                      }

            position = str(player['position'])
            if position in mac.players.h:
                depth['hitters'].append(player)
            elif position in mac.players.sp:
                depth['starters'].append(player)
            elif position in mac.players.bullpen:
                depth['bullpen'].append(player)
        with open(file, "wb") as f:
            pickle.dump(depth, f)
        return depth
    @cached_property
    def full_roster(self):
        file = pickle_path(name=f"{self.name}_roster_{tf.today}", directory=settings.ROSTER_DIR)
        path = settings.ROSTER_DIR.joinpath(file)
        if path.exists():
            roster = pd.read_pickle(path)
            return roster
        hydrate = 'person'
        call = statsapi.get('team_roster', {'teamId': self.id, 'rosterType': '40Man', 'hydrate': hydrate})['roster']
        roster = []
        for p in call:
            p_info = p['person']
            player = {'mlb_id': p_info['id'],
                      'name': p_info.get('fullName', ''),
                      'mlb_api': p_info.get('link', ''),
                      'number': p_info.get('primaryNumber', ''),
                      'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                      'weight': p_info.get('weight', ''),
                      'nickname': p_info.get('nickName', ''),
                      'debut': p_info.get('mlbDebutDate', ''),
                      'bat_side': p_info.get('batSide', {}).get('code', ''),
                      'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                      'age': p_info.get('currentAge', ''),
                      'position_type': p['position']['type'],
                      'position': p['position']['code'],
                      'status': p.get('status', {}).get('code', ''),
                      'team': self.name
                      }
            position = str(player['position'])
            if position in mac.players.h:
                player['h'] = True
            elif position in mac.players.sp:
                player['sp'] = True
            elif position in mac.players.bullpen:
                player['bp'] = True
            elif position in mac.players.twp:
                player['p'] = True
                player['h'] = True
            roster.append(player)
        roster.extend(self.nri)
        with open(file, "wb") as f:
            pickle.dump(roster, f)
        return roster
    @cached_property
    def nri(self):
        file = pickle_path(name=f"{self.name}_nri_{tf.today}", directory=settings.NRI_DIR)
        path = settings.NRI_DIR.joinpath(file)
        if path.exists():
            nri = pd.read_pickle(path)
            return nri
        hydrate = 'person'
        try:
            call = statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'nonRosterInvitees', 'hydrate': hydrate})['roster']
        except KeyError:
            return []
        nri = []
        if call:
            for p in call:
                p_info = p['person']
                player = {'mlb_id': p_info['id'],
                          'name': p_info.get('fullName', ''),
                          'mlb_api': p_info.get('link', ''),
                          'number': p_info.get('primaryNumber', ''),
                          'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                          'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                          'weight': p_info.get('weight', ''),
                          'nickname': p_info.get('nickName', ''),
                          'debut': p_info.get('mlbDebutDate', ''),
                          'bat_side': p_info.get('batSide', {}).get('code', ''),
                          'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                          'age': p_info.get('currentAge', ''),
                          'position_type': p['position']['type'],
                          'position': p['position']['code'],
                          'status': p.get('status', {}).get('code', ''),
                          'team': self.name
                          }
                position = str(player['position'])
                if position in mac.players.h:
                    player['h'] = True
                elif position in mac.players.sp:
                    player['sp'] = True
                    player['p'] = True
                elif position in mac.players.bullpen:
                    player['bp'] = True
                    player['p'] = True
                elif position in mac.players.twp:
                    player['p'] = True
                    player['h'] = True
                nri.append(player)
        with open(file, "wb") as f:
            pickle.dump(nri, f)
        return nri
    #list of integers representing ids of every affiliated player.
    @cached_property
    def all_player_ids(self):
        player_ids = []
        for player in self.full_roster:
            player_ids.append(player['mlb_id'])
        return player_ids
    @lru_cache
    def batters(self, active=False):
        roster = pd.DataFrame(self.full_roster)
        batters = roster[roster['h'] == True]
        if active:
            batters = batters[batters['status'] == 'A']
        batters = batters.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        return batters
   
    @cached_property
    def hitter(self):
        hitters = pd.DataFrame(self.depth['hitters']).apply(pd.to_numeric, errors = 'ignore')
        hitters['h'] = True
        hitters = hitters.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        return hitters
    
    @cached_property
    def starter(self):
        starters = pd.DataFrame(self.depth['starters']).apply(pd.to_numeric, errors = 'ignore')
        starters = starters.join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        starters['p'] = True
        return starters
    
    @cached_property
    def bullpen(self):
        bullpen = pd.DataFrame(self.depth['bullpen']).apply(pd.to_numeric, errors = 'ignore')
        bullpen = bullpen.join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        bullpen['p'] = True
        return bullpen
    
    @cached_property
    def catcher(self):
        return self.hitter[self.hitter['position'].astype('int') == 2]
    
    @cached_property
    def first_base(self):
        return self.hitter[self.hitter['position'].astype('int') == 3]
    
    @cached_property
    def second_base(self):
        return self.hitter[self.hitter['position'].astype('int') == 4]
    
    @cached_property
    def third_base(self):
        return self.hitter[self.hitter['position'].astype('int') == 5]
    
    @cached_property
    def shorstop(self):
        return self.hitter[self.hitter['position'].astype('int') == 6]
    
    @cached_property
    def left_field(self):
        return self.hitter[self.hitter['position'].astype('int') == 7]
    
    @cached_property
    def center_field(self):
        return self.hitter[self.hitter['position'].astype('int') == 8]
    
    @cached_property
    def right_field(self):
        return self.hitter[self.hitter['position'].astype('int') == 9]
    
    @cached_property
    def dh(self):
        return self.hitter[self.hitter['position'].astype('int') == 10]
    
    @cached_property
    def infield(self):
        return self.hitter[self.hitter['position_type'].str.casefold().str.contains('infield')]
    
    @cached_property
    def outfield(self):
        return self.hitter[self.hitter['position_type'].str.casefold().str.contains('outfield')]
    
    @cached_property
    def pitcher(self):
        pitchers = self.starter.combine_first(self.bullpen)
        return pitchers
    
    @cached_property
    def coaches(self):
        print(f"Getting {self.name.capitalize()}' coaches.")
        data = statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'coach'})['roster']
        coaches = []
        for c in data:
            coach = {'mlb_id': c.get('person', {}).get('id', ''),
                     'name': c.get('person', {}).get('fullName', ''),
                     'mlb_api': c.get('person', {}).get('link', ''),
                     'job': c.get('job', '')
                }
            coaches.append(coach)
        return pd.DataFrame(coaches).set_index('mlb_id')
    
    @cached_property
    def manager(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('manage')]
    
    @cached_property
    def bench_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('bench')]
    
    @cached_property
    def hitting_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('hitting')]
    
    @cached_property
    def pitching_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('pitch')]
    
    @cached_property
    def first_base_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('first')]
    
    @cached_property
    def third_base_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('third')]
    
    @cached_property
    def bullpen_staff(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('bullpen')]
    
    @cached_property
    def all_games(self):
        file = pickle_path(name=f"{self.name}_schedule_{tf.today}", directory=settings.SCHED_DIR)
        path = settings.SCHED_DIR.joinpath(file)
        if path.exists():
            schedule = pd.read_pickle(path)
        else:
            schedule = full_schedule(team=self.id, start_date=cs.spring_start, end_date=cs.playoff_end)
            with open(file, "wb") as f:
                pickle.dump(schedule, f)
        return schedule
    def clear_team_cache(self, directories):
        for d in directories:
            for file in Path.iterdir(d):
                if not file.is_dir() and self.name in str(file):
                    file.unlink()
        return f"Cleared cache for {self.name}."
            
        
    
    @cached_property
    def future_games(self):
        return [x for x in self.all_games if x['games'][0]['status']['codedGameState'] in mac.game_status.pre]
    
    @cached_property
    def past_games(self):
        return [x for x in self.all_games if x['games'][0]['status']['codedGameState'] in mac.game_status.post]
    
    @cached_property
    def next_game_pk(self):
        try:
            return self.future_games[0]['games'][0]['gamePk']
        except IndexError:
            print("No games on schedule.")
            return False
    
    @cached_property
    def last_game_pk(self):
        try:
            return self.past_games[-1]['games'][0]['gamePk']
        except IndexError:
            print(f'The {self.name.capitalize()} have yet to play in {cs.season_id}.')
            return False
    
    @cached_property
    def next_game(self):
        if self.next_game_pk:
            file = pickle_path(name=f"{self.name}_next_game_{tf.today}", directory=settings.GAME_DIR)
            path = settings.GAME_DIR.joinpath(file)
            if path.exists():
                game = pd.read_pickle(path) 
            else:
                print(f"Getting boxscore for {self.name.capitalize()}' next game.")
                game = statsapi.get('game', {'gamePk': self.next_game_pk})
            return game
        return self.next_game_pk
    
    @cached_property
    def last_game(self):
        file = pickle_path(name=f"{self.name}_last_game_{tf.today}", directory=settings.GAME_DIR)
        path = settings.GAME_DIR.joinpath(file)
        if path.exists():
            game = pd.read_pickle(path)
            return game
            if not game['gameData']['game']['doubleHeader'] == 'Y' and game['gameData']['gameNumber'] == 1:
                return game
        if self.last_game_pk:
            print(f"Getting boxscore {self.name.capitalize()}' last game.")
            game =  statsapi.get('game', {'gamePk': self.last_game_pk})
            with open(file, "wb") as f:
                pickle.dump(game, f)
            return game
        return self.last_game_pk
    
    @cached_property
    def is_home(self):
        if self.next_game_pk:
            return int(self.next_game['gameData']['teams']['home']['id']) == self.id
        return self.next_game_pk
    
    @cached_property
    def was_home(self):
        if self.last_game_pk:
            return int(self.last_game['gameData']['teams']['home']['id']) == self.id
        return self.last_game_pk
    
    @cached_property
    def ha(self):
        if self.next_game_pk:
            if self.is_home:
                return 'home'
            else:
                return 'away'
        return self.next_game_pk
    
    @cached_property
    def opp_ha(self):
        if self.next_game_pk:
            if self.ha == 'home':
                return 'away'
            elif self.ha == 'away':
                return 'home'
        return self.next_game_pk
    
    @cached_property
    def was_ha(self):
        if self.last_game_pk:
            if self.was_home:
                return 'home'
            else:
                return 'away'
        return self.last_game_pk
    
    @cached_property
    def opp_was_ha(self):
        if self.last_game_pk:
            if self.was_ha == 'home':
                return 'away'
            elif self.was_ha == 'away':
                return 'home'
        return self.last_game_pk
    @cached_property
    def opp_sp(self):
        if self.next_game_pk:
            daily_info = Team.daily_info()
            if self.opp_name not in daily_info['confirmed_sp']:
                self.del_next_game()
            try:
                sp_id = 'ID' + str(self.next_game['gameData']['probablePitchers'][self.opp_ha]['id'])
                sp_info = self.next_game['gameData']['players'][sp_id]
                daily_info = Team.daily_info()
                if self.opp_name not in daily_info["confirmed_sp"]:
                    daily_info["confirmed_sp"].append(self.opp_name)
                    file = json_path(name=f"daily_info_{tf.today}", directory=settings.STORAGE_DIR)
                    with open(file, "w+") as f:
                        json.dump(daily_info, f)
                self.opp_instance.cache_next_game()
                return sp_info
            except KeyError:
                starters = self.opp_instance.rested_sp
                try:
                    projected_sp = starters.loc[starters['fd_ps_s'].idxmax()]
                except (KeyError, ValueError):
                    bullpen = self.opp_instance.rested_bullpen
                    try:
                        projected_sp = bullpen.loc[bullpen['batters_faced_sp'].idxmax()]
                    except (KeyError, ValueError):
                        projected_sp = bullpen.loc[bullpen['ppa'].idxmax()]
                i_d = projected_sp['mlb_id'].item()
                return statsapi.get('people', {'personIds': i_d})['people'][0]
        return self.next_game_pk
    def projected_sp(self):
        if self.next_game_pk:
            daily_info = Team.daily_info()
            if self.name not in daily_info['confirmed_sp']:
                self.del_next_game()
            try:
                sp_id = 'ID' + str(self.next_game['gameData']['probablePitchers'][self.ha]['id'])
                sp_info = self.next_game['gameData']['players'][sp_id]
                if self.name not in daily_info["confirmed_sp"]:
                    daily_info["confirmed_sp"].append(self.name)
                    file = json_path(name=f"daily_info_{tf.today}", directory=settings.STORAGE_DIR)
                    with open(file, "w+") as f:
                        json.dump(daily_info, f)
                self.cache_next_game()
                return sp_info
            except KeyError:
                starters = self.rested_sp
                try:
                    projected_sp = starters.loc[starters['fd_ps_s'].idxmax()]
                except (KeyError, ValueError):
                    bullpen = self.rested_bullpen
                    try:
                        projected_sp = bullpen.loc[bullpen['batters_faced_sp'].idxmax()]
                    except (KeyError, ValueError):
                        projected_sp = bullpen.loc[bullpen['ppa'].idxmax()]
                i_d = projected_sp['mlb_id'].item()
                return statsapi.get('people', {'personIds': i_d})['people'][0]
        return self.next_game_pk
    @cached_property
    def last_opp_sp(self):
        if self.last_game_pk:
            sp_id = 'ID' + str(self.last_game['gameData']['probablePitchers'][self.opp_was_ha]['id'])
            sp_info = self.last_game['gameData']['players'][sp_id]
            return sp_info
        return self.last_game_pk
    
    @cached_property
    def opp_sp_hand(self):
        try:
            return self.opp_sp['pitchHand']['code']
        except TypeError:
            print(f"{self.opp_sp} returning default 'R'")
            return 'R'
    @cached_property
    def o_split(self):
        if self.opp_sp_hand == 'R':
            return 'vr'
        else:
            return 'vl'
    
    @cached_property
    def last_opp_sp_hand(self):
        if self.last_game_pk:
            return self.last_opp_sp['pitchHand']['code']
        return self.last_game_pk
    
    @cached_property
    def opp_name(self):
        if self.next_game_pk:
            name = self.next_game['gameData']['teams'][self.opp_ha]['teamName'].lower()
            if 'backs' in name:
                name = 'diamondbacks'
            return name
        return self.next_game_pk
    
    @staticmethod
    def lineups():
        path = json_path(name='team_lineups')
        try:
            with open(path) as file:
                team_lineups = json.load(file)
                file.close()
            return team_lineups
        except (FileNotFoundError, JSONDecodeError):
            team_lineups = {}
            for team in team_info.keys():
                team_lineups[team] = {'L': [], 'R': []}
            with open(path, "w+") as file:
                json.dump(team_lineups, file)
                file.close()
            return team_lineups
    @staticmethod
    def daily_info():
        file = json_path(name=f"daily_info_{tf.today}", directory=settings.STORAGE_DIR)
        try:
            with open(file) as f:
                daily_info = json.load(f)
        except (FileNotFoundError, JSONDecodeError):
            daily_info = {
                "confirmed_lu": [],
                "confirmed_sp": [],
                "rain": [],
                }
            with open(file, "w+") as f:
                json.dump(daily_info, f)
        return daily_info
    @staticmethod
    def drop(df,filt):
        df = df.loc[~df.duplicated(subset=['mlb_id'])]
        df = df[df['mlb_id'].isin(filt)]
        return df
    @staticmethod
    def default_sp():
        empty = p_q_sp[p_q_sp['mlb_id'] == 0]
        defaults = p_q_sp.median()
        empty = empty.append(defaults, ignore_index = True)
        empty['name'] = 'Unknown'
        empty['pitch_hand'] = 'R'
        return empty
    
    @cached_property
    def lineup(self):
        if self.next_game_pk:
            daily_info = Team.daily_info()
            if self.name in daily_info["confirmed_lu"]:
                lineup =  Team.lineups()[self.name][self.opp_sp_hand]
                if len(lineup) == 9:
                    return lineup
            if self.name not in daily_info["confirmed_lu"]:
                self.del_next_game()
            lineup = self.next_game['liveData']['boxscore']['teams'][self.ha]['battingOrder']
            if len(lineup) == 9:
                self.update_lineup(lineup, self.opp_sp_hand)
                if self.name not in daily_info["confirmed_lu"]:
                    daily_info["confirmed_lu"].append(self.name)
                    file = json_path(name=f"daily_info_{tf.today}", directory=settings.STORAGE_DIR)
                    with open(file, "w+") as f:
                        json.dump(daily_info, f)
                self.cache_next_game()
                return lineup
            else:
                return self.projected_lineup
        return self.next_game_pk
    
    @cached_property
    def projected_lineup(self):
        print(f"Getting projected lineup for {self.name}")
        team_lineups = Team.lineups()
        try:
            if len(team_lineups[self.name][self.opp_sp_hand]) == 9:
                return team_lineups[self.name][self.opp_sp_hand]
            else:
                lineup = []
                plays = self.last_game['liveData']['plays']['allPlays']
                if self.was_home:
                    h = False
                else:
                    h = True
                for play in plays:
                    if play['about']['isTopInning'] == h:
                        lineup.append(play['matchup']['batter']['id'])
                        if len(lineup) == 9:
                            break
                self.update_lineup(lineup, self.last_opp_sp_hand)
                return lineup
        except (KeyError, TypeError):
            return self.last_game_pk + "projected lineup will be available after first spring training game."
    
    def update_lineup(self, lineup, hand):
        team_lineups = Team.lineups()
        team_lineups[self.name][hand] = lineup
        with open(json_path(name="team_lineups"), "w+") as file:
            json.dump(team_lineups, file)
            file.close()
        return lineup
    
    def get_replacement(self, position, position_type, excluded):
        roster = pd.DataFrame(self.full_roster)
        roster = roster.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        h_key = 'fd_wps_pa_' + self.o_split
        if str(position) in mac.players.p:
            if not self.is_dh:
                projected_sp = roster[roster['mlb_id'] == self.projected_sp()['id']]
                if len(projected_sp.index) != 0:
                    return projected_sp.loc[projected_sp['age'].idxmax()]
                else:
                    bullpen = self.rested_bullpen[~self.rested_bullpen['mlb_id'].isin(excluded)]
                    try:
                        return bullpen.loc[bullpen['batters_faced_sp'].idxmax()]
                    except KeyError:
                        return bullpen.loc[bullpen['ppa'].idxmax()]
            else: 
                dh = self.dh[~self.dh['mlb_id'].isin(excluded)]
                try:
                    return dh.loc[dh[h_key].idxmax()]
                except (KeyError, ValueError):
                    hitters = self.batters(active=True)
                    hitters = hitters[(~hitters['mlb_id'].isin(excluded)) & (hitters['pa_' + self.o_split] > 100)]
                    try:
                        return hitters.loc[hitters[h_key].idxmax()]
                    except (KeyError, ValueError):
                        hitters = self.batters(active=True)
                        return hitters.loc[hitters[h_key].idxmax()]
        else:
            hitters = self.batters(active=True)
            hitters = hitters[~hitters['mlb_id'].isin(excluded)]
            hitters = hitters[hitters['position'].astype(str) == position]
            try:
                return hitters.loc[hitters[h_key].idxmax()]
            except (KeyError, ValueError):
                hitters = self.batters(active=True)
                hitters = hitters[~hitters['mlb_id'].isin(excluded)]
                hitters = hitters[hitters['position_type'].astype(str) == position_type]
                try:
                    return hitters.loc[hitters[h_key].idxmax()]
                except (KeyError, ValueError):
                    hitters = self.batters(active=True)
                    hitters = hitters[~hitters['mlb_id'].isin(excluded)]
                    return hitters.loc[hitters[h_key].idxmax()]
                    
    def lineup_df(self):
        file = pickle_path(name=f"{self.name}_lu_{tf.today}", directory=settings.LINEUP_DIR)
        path = settings.LINEUP_DIR.joinpath(file)
        daily_info = Team.daily_info()
        if self.name not in daily_info["confirmed_lu"]:
            self.del_props()
        if not path.exists() or self.name not in daily_info["confirmed_lu"]:
            lineup_ids = self.lineup
            lineup = pd.DataFrame(lineup_ids, columns= ["mlb_id"])
            roster = pd.DataFrame(self.full_roster)
            merged = pd.merge(lineup, roster, on="mlb_id")
            h_df = merged.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            roster = roster.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            if settings.use_fangraphs:
                def drop(df):
                    df = df.loc[~df.duplicated(subset=['mlb_id'])]
                    df = df[df['mlb_id'].isin(lineup_ids)]
                    return df
                from dfs_tools_mlb.compile.stats_fangraphs import Stats
                fg_stats = Stats.current_stats()
                h_df = sm_merge(h_df, fg_stats, columns=['name', 'team'], ratios=[.63, 1], prefix='m_', reset_index=False, post_drop=True, suffixes=('', '_fg'))
                h_df = drop(h_df)
            h_df = h_df[((~h_df['position'].astype(str).isin(mac.players.p)) & (h_df['mlb_id'] != 660271))]
            if not self.is_dh and len(h_df.index) == 9:
                h_key = 'fd_wps_pa_' + self.o_split
                i_d = h_df.loc[h_df[h_key].idxmin(), 'mlb_id'].item()
                h_df.drop(h_df[h_key].idxmin(), inplace=True)
                new_row = self.get_replacement('S', 'Pitcher', lineup_ids)
                h_df = h_df.append(new_row, ignore_index = True)
                lineup_ids.remove(i_d)
                lineup_ids.append(int(new_row['mlb_id']))
            if len(h_df.index) < 9:
                for i_d in lineup_ids:
                    if i_d not in h_df['mlb_id'].values:
                        new_row = roster[roster['mlb_id'] == i_d]
                        new_row = new_row[~new_row['position'].astype(str).isin(mac.players.p)]
                        if len(new_row.index) == 0:
                            player = statsapi.get('people', {'personIds': i_d})
                            position = player['people'][0]['primaryPosition']['code']
                            position_type = player['people'][0]['primaryPosition']['type']
                            new_row = self.get_replacement(position, position_type, lineup_ids)
                            index = lineup_ids.index(i_d)
                            lineup_ids[index] = int(new_row['mlb_id'])
                            self.update_lineup(lineup_ids, self.opp_sp_hand)
                        h_df = h_df.append(new_row, ignore_index = True)
            sorter = dict(zip(lineup_ids, range(len(lineup_ids))))
            h_df['order'] = h_df['mlb_id'].map(sorter)
            h_df.sort_values(by='order',inplace=True, kind='mergesort')
            h_df['order'] = h_df['order'] + 1
            h_df.reset_index(inplace=True, drop=True)
            if self.opp_sp_hand == 'R':
                h_df.loc[h_df['bat_side'] == 'S', 'bat_side'] = 'L'
            else:
                h_df.loc[h_df['bat_side'] == 'S', 'bat_side'] = 'R'
                
            lefties = (h_df['bat_side'] == 'L')
            righties = (h_df['bat_side'] == 'R')
            # adjustments for pitchers in lineup
            h_df.loc[((h_df['pa'] < 50) | (h_df['pitches_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa'] = hp_q['pitches_pa'].median()
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['pitches_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa_vr'] = hp_q['pitches_pa'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['pitches_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa_vl'] = hp_q['pitches_pa'].median()
            h_df.loc[(h_df['pa'] < 50) | (h_df['pitches_pa'].isna()), 'pitches_pa'] = hp_q['pitches_pa'].median()
            h_df.loc[((h_df['pa'] < 50) | (h_df['fd_wps_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa'] = hp_q['fd_wps_pa'].median()
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['fd_wps_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa_vr'] = hp_q['fd_wps_pa'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['fd_wps_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa_vl'] = hp_q['fd_wps_pa'].median()
            h_df.loc[(h_df['pa'] < 50) | (h_df['fd_wps_pa'].isna()), 'fd_wps_pa'] = hp_q['fd_wps_pa'].median()
            h_df.loc[((h_df['pa'] < 50) | (h_df['fd_wpa_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa'] = hp_q['fd_wpa_pa'].median()
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['fd_wpa_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa_vr'] = hp_q['fd_wpa_pa'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['fd_wpa_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa_vl'] = hp_q['fd_wpa_pa'].median()
            h_df.loc[(h_df['pa'] < 50) | (h_df['fd_wpa_pa'].isna()), 'fd_wpa_pa'] = hp_q['fd_wpa_pa'].median()

            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['pitches_pa_vr'].isna())) & righties, 'pitches_pa_vr'] = h_r_vr['pitches_pa_vr'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['pitches_pa_vl'].isna())) & lefties, 'pitches_pa_vl'] = h_l_vl['pitches_pa_vl'].median()
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['fd_wps_pa_vr'].isna())) & righties, 'fd_wps_pa_vr'] = h_r_vr['fd_wps_pa_vr'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['fd_wps_pa_vl'].isna())) & lefties, 'fd_wps_pa_vl'] = h_l_vl['fd_wps_pa_vl'].median()
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['fd_wpa_pa_vr'].isna())) & righties, 'fd_wpa_pa_vr'] = h_r_vr['fd_wpa_pa_vr'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['fd_wpa_pa_vl'].isna())) & lefties, 'fd_wpa_pa_vl'] = h_l_vl['fd_wpa_pa_vl'].median()
            
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['pitches_pa_vr'].isna())) & lefties, 'pitches_pa_vr'] = h_l_vr['pitches_pa_vr'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['pitches_pa_vl'].isna())) & righties, 'pitches_pa_vl'] = h_r_vl['pitches_pa_vl'].median()
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['fd_wps_pa_vr'].isna())) & lefties, 'fd_wps_pa_vr'] = h_l_vr['fd_wps_pa_vr'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['fd_wps_pa_vl'].isna())) & righties, 'fd_wps_pa_vl'] = h_r_vl['fd_wps_pa_vl'].median()
            h_df.loc[((h_df['pa_vr'] < 25) | (h_df['fd_wpa_pa_vr'].isna())) & lefties, 'fd_wpa_pa_vr'] = h_l_vr['fd_wpa_pa_vr'].median()
            h_df.loc[((h_df['pa_vl'] < 25) | (h_df['fd_wpa_pa_vl'].isna())) & righties, 'fd_wpa_pa_vl'] = h_r_vl['fd_wpa_pa_vl'].median()
            
            
            try:
                p_info = self.opp_sp
                player = {'mlb_id': p_info['id'],
                          'name': p_info.get('fullName', ''),
                          'mlb_api': p_info.get('link', ''),
                          'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                          'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                          'weight': p_info.get('weight', ''),
                          'nickname': p_info.get('nickName', ''),
                          'debut': p_info.get('mlbDebutDate', ''),
                          'bat_side': p_info.get('batSide', {}).get('code', ''),
                          'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                          'age': p_info.get('currentAge', ''),
                          'team': self.opp_name
                              }
                p_df = pd.DataFrame([player]).join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            except TypeError:
                p_df = Team.default_sp()
            
            l_len = len(h_df[lefties].index)
            r_len = len(h_df[righties].index)
            l_weight = l_len / 9
            r_weight = r_len / 9
            p_df.loc[(p_df['batters_faced_sp'] < 50) | (p_df['ppb_sp'].isna()), 'ppb_sp'] = p_q['ppb_sp'].median()
            if p_df['pitch_hand'].item() == 'L':
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['ppb_vl'].isna()), 'ppb_vl'] = p_q_l_vl['ppb_vl'].median()
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['ppb_vr'].isna()), 'ppb_vr'] = p_q_l_vr['ppb_vr'].median()
            else:
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['ppb_vl'].isna()), 'ppb_vl'] = p_q_r_vl['ppb_vl'].median()
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['ppb_vr'].isna()), 'ppb_vr'] = p_q_r_vr['ppb_vr'].median()
                
            p_ppb = ((l_weight * p_df['ppb_vl'].max()) + (r_weight * p_df['ppb_vr'].max())) * 9 
            p_df['pitches_start'].fillna(p_q_sp['pitches_start'].median(), inplace = True)
            key = 'pitches_pa_' + self.o_split
            p_df['exp_x_lu'] = p_df['pitches_start'] / ((h_df[key].sum() + p_ppb) / 2)
            p_df['exp_bf'] = round((p_df['exp_x_lu'] * 9))
            sp_rollover = floor((p_df['exp_x_lu'] % 1) * 9)
            h_df.loc[h_df['order'] <= sp_rollover, 'exp_pa_sp'] = ceil(p_df['exp_x_lu'])
            h_df.loc[h_df['order'] > sp_rollover, 'exp_pa_sp'] = floor(p_df['exp_x_lu'])
            if p_df['pitch_hand'].item() == 'L':
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['fd_wpa_b_vr'].isna()), 'fd_wpa_b_vr'] = p_q_l_vr['fd_wpa_b_vr'].median()
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['fd_wpa_b_vl'].isna()), 'fd_wpa_b_vl'] = p_q_l_vl['fd_wpa_b_vl'].median()
            else:
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['fd_wpa_b_vr'].isna()), 'fd_wpa_b_vr'] = p_q_r_vr['fd_wpa_b_vr'].median()
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['fd_wpa_b_vl'].isna()), 'fd_wpa_b_vl'] = p_q_r_vl['fd_wpa_b_vl'].median()
            
            key = 'fd_wps_pa_' + self.o_split
            h_df.loc[lefties, 'exp_ps_sp_pa'] = (((p_df['fd_wpa_b_vl'].max() * 1.1) + (h_df[key] *.9)) / 2)
            h_df.loc[righties, 'exp_ps_sp_pa'] = (((p_df['fd_wpa_b_vr'].max()  * 1.1) + (h_df[key] *.9)) / 2)
            h_df.loc[lefties, 'exp_ps_sp'] = (((p_df['fd_wpa_b_vl'].max()  * 1.1) + (h_df[key] *.9)) / 2) * h_df['exp_pa_sp']
            h_df.loc[righties, 'exp_ps_sp'] = (((p_df['fd_wpa_b_vr'].max()  * 1.1) + (h_df[key] *.9)) / 2) * h_df['exp_pa_sp']
            h_df.loc[lefties, 'sp_mu'] = p_df['fd_wpa_b_vl'].max()
            h_df.loc[righties, 'sp_mu'] = p_df['fd_wpa_b_vr'].max()
            self.sp_mu = h_df['sp_mu'].sum()
            #points conceded
            key = 'fd_wpa_pa_' + self.o_split
            
            if p_df['pitch_hand'].item() == 'L':
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['fd_wps_b_vr'].isna()), 'fd_wps_b_vr'] = p_q_l_vr['fd_wps_b_vr'].median()
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['fd_wps_b_vl'].isna()), 'fd_wps_b_vl'] = p_q_l_vl['fd_wps_b_vl'].median()
            else:
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['fd_wps_b_vr'].isna()), 'fd_wps_b_vr'] = p_q_r_vr['fd_wps_b_vr'].median()
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['fd_wps_b_vl'].isna()), 'fd_wps_b_vl'] = p_q_r_vl['fd_wps_b_vl'].median()
                
            h_df.loc[lefties, 'exp_pc_sp'] = (((p_df['fd_wps_b_vl'].max() * 1.1) + (h_df[key] * .9)) / 2) * h_df['exp_pa_sp']
            h_df.loc[righties, 'exp_pc_sp'] = (((p_df['fd_wps_b_vr'].max() * 1.1) + (h_df[key] * .9)) / 2) * h_df['exp_pa_sp']
            h_df['exp_pc_sp_raw'] = h_df[key] * h_df['exp_pa_sp']
            if p_df['pitch_hand'].item() == 'L':
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['ra-_b_vr'].isna()), 'ra-_b_vr'] = p_q_l_vr['ra-_b_vr'].median()
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['ra-_b_vl'].isna()), 'ra-_b_vl'] = p_q_l_vl['ra-_b_vl'].median()
            else:
                p_df.loc[(p_df['batters_faced_vr'] < 25) | (p_df['ra-_b_vr'].isna()), 'ra-_b_vr'] = p_q_r_vr['ra-_b_vr'].median()
                p_df.loc[(p_df['batters_faced_vl'] < 25) | (p_df['ra-_b_vl'].isna()), 'ra-_b_vl'] = p_q_r_vl['ra-_b_vl'].median()
            exp_pa_r_sp = h_df.loc[righties, 'exp_pa_sp'].sum()
            exp_pa_l_sp = h_df.loc[lefties, 'exp_pa_sp'].sum()
            p_df['exp_ra'] = floor((exp_pa_r_sp * p_df['ra-_b_vr'].max()) + (exp_pa_l_sp * p_df['ra-_b_vl'].max()))
            p_df['exp_inn'] = (p_df['exp_bf'].max() - p_df['exp_ra'].max()) / 3
            if self.is_home:
                exp_bp_inn = 9 - p_df['exp_inn'].max()
            else:
                exp_bp_inn = 9 - p_df['exp_inn'].max()
            bp = self.proj_opp_bp
            l_filt = (bp['pitch_hand'] == 'L')
            r_filt = (bp['pitch_hand'] == 'R')
            bp.loc[((bp['batters_faced_vr'] < 25) | (bp['fd_wpa_b_vr'].isna())) & r_filt, 'fd_wpa_b_vr'] = p_q_r_vr['fd_wpa_b_vr'].median()
            bp.loc[((bp['batters_faced_vl'] < 25) | (bp['fd_wpa_b_vl'].isna())) & l_filt, 'fd_wpa_b_vl'] = p_q_l_vl['fd_wpa_b_vl'].median()
            bp.loc[((bp['batters_faced_vr'] < 25) | (bp['fd_wpa_b_vr'].isna())) & l_filt, 'fd_wpa_b_vr'] = p_q_l_vr['fd_wpa_b_vr'].median()
            bp.loc[((bp['batters_faced_vl'] < 25) | (bp['fd_wpa_b_vl'].isna())) & r_filt, 'fd_wpa_b_vl'] = p_q_r_vl['fd_wpa_b_vl'].median()
            bp.loc[((bp['batters_faced_vr'] < 25) | (bp['ra-_b_vr'].isna())) & r_filt, 'ra-_b_vr'] = p_q_r_vr['ra-_b_vr'].median()
            bp.loc[((bp['batters_faced_vl'] < 25) | (bp['ra-_b_vl'].isna())) & l_filt, 'ra-_b_vl'] = p_q_l_vl['ra-_b_vl'].median()
            bp.loc[((bp['batters_faced_vr'] < 25) | (bp['ra-_b_vr'].isna())) & l_filt, 'ra-_b_vr'] = p_q_l_vr['ra-_b_vr'].median()
            bp.loc[((bp['batters_faced_vl'] < 25) | (bp['ra-_b_vl'].isna())) & r_filt, 'ra-_b_vl'] = p_q_r_vl['ra-_b_vl'].median()
            bp.loc[(bp['batters_faced_rp'] < 25) | (bp['fd_wpa_b_rp'].isna()), 'fd_wpa_b_rp'] = p_q_rp['fd_wpa_b_rp'].median()
            bp.loc[(bp['batters_faced_rp'] < 25) | (bp['ra-_b_rp'].isna()), 'ra-_b_rp'] = p_q_rp['ra-_b_rp'].median()
            try:
                exp_bf_bp = round((exp_bp_inn * 3) + ((exp_bp_inn * 3) * bp['ra-_b_rp'].mean()))
            except ValueError:
                print(f'USING DEFAULT BP RA- for {self.name}')
                exp_bf_bp = round((exp_bp_inn * 3) + ((exp_bp_inn * 3) * p_q_rp['ra-_b_rp'].median()))
                
            first_bp_pa = h_df.loc[(h_df['exp_pa_sp'] == floor(p_df['exp_x_lu'])), 'order'].idxmin()
            order = h_df.loc[first_bp_pa, 'order'].item()
            h_df['exp_pa_bp'] = 0
            while exp_bf_bp > 0:
                if order == 10:
                    order = 1
                h_df.loc[h_df['order'] == order, 'exp_pa_bp'] += 1
                order += 1
                exp_bf_bp -= 1
            h_df['exp_ps_bp'] = h_df['exp_pa_bp'] * (((bp['fd_wpa_b_rp'].mean() * 1.1) + (h_df['fd_wps_pa'] * .9)) / 2)
            h_df['exp_ps_raw'] = h_df['exp_ps_sp'] + h_df['exp_ps_bp']
            
            self.raw_points = h_df['exp_ps_raw'].sum()
            self.venue_points = (self.raw_points * self.next_venue_boost) - self.raw_points
            self.temp_points = (self.raw_points * self.temp_boost) - self.raw_points
            self.ump_points = (self.raw_points * self.ump_boost) - self.raw_points
            self.points = self.venue_points + self.temp_points + self.ump_points + self.raw_points
            h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp']
            h_df['venue_points'] = (h_df['exp_ps_raw'] * self.next_venue_boost) - h_df['exp_ps_raw']
            h_df['temp_points'] = (h_df['exp_ps_raw'] * self.temp_boost) - h_df['exp_ps_raw']
            h_df['ump_points'] = (h_df['exp_ps_raw'] * self.ump_boost) - h_df['exp_ps_raw']
            h_df['points'] = h_df['venue_points'] + h_df['temp_points'] + h_df['ump_points'] + h_df['exp_ps_raw']
            
            with open(file, "wb") as f:
                pickle.dump(h_df, f)
        else:
            h_df = pd.read_pickle(path)
            h_df['venue_points'] = (h_df['exp_ps_raw'] * self.next_venue_boost) - h_df['exp_ps_raw']
            h_df['temp_points'] = (h_df['exp_ps_raw'] * self.temp_boost) - h_df['exp_ps_raw']
            h_df['ump_points'] = (h_df['exp_ps_raw'] * self.ump_boost) - h_df['exp_ps_raw']
            h_df['points'] = h_df['venue_points'] + h_df['temp_points'] + h_df['ump_points'] + h_df['exp_ps_raw']
            h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp'] + h_df['exp_ps_bp']
            self.raw_points = h_df['exp_ps_raw'].sum()
            self.venue_points = (self.raw_points * self.next_venue_boost) - self.raw_points
            self.temp_points = (self.raw_points * self.temp_boost) - self.raw_points
            self.ump_points = (self.raw_points * self.ump_boost) - self.raw_points
            self.points = self.venue_points + self.temp_points + self.ump_points + self.raw_points
            self.sp_mu = h_df['sp_mu'].sum()
            h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp']
        return h_df
    def sp_df(self):
        daily_info = Team.daily_info()
        projected_sp = self.projected_sp()
        slug = projected_sp['nameSlug']
        file = pickle_path(name=f"{slug}_{tf.today}", directory=settings.SP_DIR)
        path = settings.SP_DIR.joinpath(file)
        if path.exists():
            p_df = pd.read_pickle(path)
            p_df['venue_points'] = (p_df['exp_ps_raw'] * (-self.next_venue_boost % 2)) - p_df['exp_ps_raw']
            p_df['temp_points'] = (p_df['exp_ps_raw'] * (-self.temp_boost % 2)) - p_df['exp_ps_raw']
            p_df['ump_points'] = (p_df['exp_ps_raw'] * (-self.ump_boost % 2)) - p_df['exp_ps_raw']
            p_df['points'] = p_df['exp_ps_raw'] + p_df['venue_points'] + p_df['temp_points'] + p_df['ump_points']
            return p_df
        try:
            p_info = projected_sp
            player = {'mlb_id': p_info['id'],
                      'name': p_info.get('fullName', ''),
                      'mlb_api': p_info.get('link', ''),
                      'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                      'weight': p_info.get('weight', ''),
                      'nickname': p_info.get('nickName', ''),
                      'debut': p_info.get('mlbDebutDate', ''),
                      'bat_side': p_info.get('batSide', {}).get('code', ''),
                      'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                      'age': p_info.get('currentAge', ''),
                      'team': self.name
                          }
            p_df = pd.DataFrame([player]).join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        except TypeError:
            print('Getting default SP stats.')
            p_df = Team.default_sp()
        h_df = self.opp_instance.lineup_df()
        p_df['exp_ps_raw'] = h_df['exp_pc_sp'].sum() + h_df['exp_pc_sp_raw'].sum()
        p_df['venue_points'] = (p_df['exp_ps_raw'] * (-self.next_venue_boost % 2)) - p_df['exp_ps_raw']
        p_df['temp_points'] = (p_df['exp_ps_raw'] * (-self.temp_boost % 2)) - p_df['exp_ps_raw']
        p_df['ump_points'] = (p_df['exp_ps_raw'] * (-self.ump_boost % 2)) - p_df['exp_ps_raw']
        p_df['points'] = p_df['exp_ps_raw'] + p_df['venue_points'] + p_df['temp_points'] + p_df['ump_points']
        
        if self.name in daily_info['confirmed_sp']:
            with open(file, "wb") as f:
                pickle.dump(p_df, f)
        return p_df
    
    def live_game(self):
        try:
            game = next(x for x in self.all_games if x['games'][0]['status']['codedGameState'] in mac.game_status.live)['games'][0]
            game_pk = game['gamePk']
            print(game_pk)
            game_box = statsapi.get('game', {'gamePk': game_pk})
        except(StopIteration, TypeError):
            return f"The {self.name.capitalize()} are not currently playing."
        all_plays = game_box['liveData']['plays']['allPlays']
        scoring_plays = game_box['liveData']['plays']['scoringPlays'] #integer_list
        current_play = game_box['liveData']['plays']['currentPlay']
        home_name = game_box['gameData']['teams']['home']['teamName']
        away_name = game_box['gameData']['teams']['away']['teamName']
        game_line = game_box['liveData']['linescore']
        home_score = game_line['teams']['home']['runs']
        away_score = game_line['teams']['away']['runs']
        inning_pre = game_line['inningHalf']
        current_inning = game_line['currentInning']
        home_hits = game_line['teams']['home']['hits']
        away_hits = game_line['teams']['away']['hits']
        total_hits = home_hits + away_hits
        if not scoring_plays:
            pass
        else:
            print('All Scoring Plays:')
            for play in all_plays:
                play_index = play.get('atBatIndex', 1000)
                if play_index > scoring_plays[-1]:
                    break
                elif play_index in scoring_plays:
                    score_event = play.get('result', {}).get('description', "No description.")
                    print("-" + re.sub(' +', ' ', score_event))
        current_batter = current_play['matchup']['batter']['fullName']
        current_pitcher = current_play['matchup']['pitcher']['fullName']
        runners = current_play['runners']
        while not runners:
            try:
                flag = -2
                runners = all_plays[flag]['runners']
                flag -= 1
            except IndexError:
                break
        count = current_play['count']
        outs = count['outs']
        out_string = plural(outs,'out')
        strikes = count['strikes']
        balls = count['balls']
        print(f"At bat: {current_batter}")
        print(f"Pitching: {current_pitcher}")
        print(f"Count: {balls}-{strikes}, {outs} {out_string}")
        if runners:
            runner_d = {}
            for runner in runners:
                runner_name = runner['details']['runner']['fullName']
                runner_base = runner['movement']['end']
                runner_d[runner_name] = runner_base
            for k, v in runner_d.items():
                if v:
                    if str(v) == 'score':
                        print(f"{k} {v}d.")
                    else:
                        print((f"{k} is on {v}."))
        print(f"{away_name}: {away_score}")
        print(f"{home_name}: {home_score}")
        print(f"{inning_pre} {current_inning}")
        print(f"Total hits: {total_hits}")
        try:
            print('Last update:')
            last_event = next(x for x in reversed(all_plays) if x['result'].get('description'))['result']['description']
            print(re.sub(' +', ' ', last_event))
            
        except StopIteration:
            pass
        return None
    
    @cached_property
    def is_new_series(self):
        if self.next_game_pk:
            return self.future_games[0]['games'][0]['seriesGameNumber'] == 1
        return self.next_game_pk     
    
    @cached_property
    def projected_ump(self):
        def get_official(game,ump_type):
            try:
                officials = game['liveData']['boxscore']['officials']
                projected_official = next(x for x in officials if x['officialType'] == ump_type)
                return projected_official['official']['id']
            except StopIteration:
                return 'Unable to project ump.'
        if not self.is_new_series:
            return (get_official(self.last_game, 'First Base'))
        elif self.next_game['liveData']['boxscore']['officials']:
            return (get_official(self.next_game,'Home Plate'))
        else:
            return 'Unable to project ump.'
    
    @cached_property
    def projected_ump_data(self):
        return game_data[game_data['umpire'] == self.projected_ump]
    
    @cached_property
    def next_venue(self):
        if self.next_game_pk:
            v_id =  self.next_game['gameData']['venue']['id']
            return v_id
        return self.next_game_pk
    
    @cached_property
    def last_venue(self):
        if self.last_game_pk:
            return self.next_game['gameData']['venue']['id']
        return self.last_game_pk
    
    @cached_property
    def home_venue(self):
        return team_info[self.name]['venue']['id']
            
    
    @cached_property
    def next_venue_data(self):
        if self.next_game_pk:
            return game_data[game_data['venue_id'] == self.next_venue]
        return self.next_game_pk
   
    @cached_property
    def last_venue_data(self):
        if self.last_game_pk:
            return game_data[game_data['venue_id'] == self.next_venue]
        return self.next_game_pk
    
    @cached_property
    def home_venue_data(self):
        return game_data[game_data['venue_id'] == self.home_venue]
    
    @cached_property
    def weather(self):
        if self.next_game_pk:
            return self.next_game['gameData']['weather']
        return self.next_game_pk
    
    @cached_property
    def wind_speed(self):
        if self.roof_closed:
            return 0
        elif not self.next_has_roof:
            avg_wind = self.next_venue_data['wind_speed'].mean()
        else:
            avg_wind = 0
        if self.weather:
            wind_description = self.weather.get('wind')
            if wind_description:
                try:
                    return int(wind_description.split(' ')[0])
                except (TypeError, ValueError):
                    if not np.isnan(avg_wind):
                        return avg_wind
                    return 0
            else:
                if not np.isnan(avg_wind):
                    return avg_wind
                return 0
        else:
            if not np.isnan(avg_wind):
                return avg_wind
            return 0
                
    @cached_property
    def wind_direction(self):
        wind_direction = ''
        if self.weather:
            wind_description = self.weather.get('wind')
            if wind_description:
                return wind_description.split(', ')[-1]
        return wind_direction
    
    @cached_property
    def venue_condition(self):
        condition = ''
        if self.weather:
            condition = self.weather.get('condition')
            if condition in mac.weather.rain:
                file = json_path(name=f"daily_info_{tf.today}", directory=settings.STORAGE_DIR)
                daily_info = Team.daily_info()
                if self.name not in daily_info["rain"]:
                    daily_info["rain"].append(self.name)
                    with open(file, "w+") as f:
                        json.dump(daily_info, f)
        return condition
    @cached_property
    def venue_temp(self):
        if self.weather:
            temp = self.weather.get('temp')
            if temp and not self.roof_closed:
                try:
                    return int(temp)
                except ValueError:
                    return ''
        if self.roof_closed:
            return 72
        return ''
    @cached_property
    def roof_closed(self):
        return ((self.next_has_roof and not self.venue_condition) or self.venue_condition in mac.weather.roof_closed)
    
    @cached_property
    def home_has_roof(self):
        return any(i in mac.weather.roof_closed for i in self.home_venue_data['condition'].unique())
    
    @cached_property
    def next_has_roof(self):
        return any(i in mac.weather.roof_closed for i in self.next_venue_data['condition'].unique())
    
    @cached_property
    def last_has_roof(self):
        return any(i in mac.weather.roof_closed for i in self.last_venue_data['condition'].unique())
    
    @cached_property
    def no_rest(self):
        if self.last_game_pk:
            date = self.last_game['gameData']['datetime']['originalDate']
            if datetime.date.fromisoformat(date) >= tf.yesterday:
                return True
            return False
        return self.last_game_pk
    
    @cached_property
    def used_rp(self):
        if self.no_rest:
            return self.last_game['liveData']['boxscore']['teams'][self.was_ha]['pitchers'][1:]
        return []
    
    @cached_property
    def no_rest_travel(self):
        if self.no_rest and self.last_venue != self.next_venue:
            return True
        return False
    
    @cached_property
    def is_dh(self):
        return self.next_game['gameData']['teams']['home']['league']['id'] == 103
    
    @cached_property
    def was_dh(self):
        return self.last_game['gameData']['teams']['home']['league']['id'] == 103
    
    @cached_property
    def next_venue_boost(self):
        if (self.wind_direction in mac.weather.wind_out or self.wind_direction in mac.weather.wind_in) and not self.roof_closed:
            wind_in = self.next_venue_data[self.next_venue_data['wind_direction'].isin(mac.weather.wind_in)]
            wind_out = self.next_venue_data[self.next_venue_data['wind_direction'].isin(mac.weather.wind_out)]
            if len(wind_in.index) >= 50 and len(wind_out.index) >= 50:
                if ((wind_out['fd_points'].mean() - wind_in['fd_points'].mean()) / game_data['fd_points'].mean()) > 0:
                    if self.wind_speed >= (game_data['wind_speed'].median() - 1):
                        if self.wind_direction in mac.weather.wind_out:
                            return wind_out['fd_points'].mean() / game_data['fd_points'].mean()
                        if self.wind_direction in mac.weather.wind_in:
                            return wind_in['fd_points'].mean() / game_data['fd_points'].mean()
        if len(self.next_venue_data.index) < 100:
            return 1
        return self.next_venue_data['fd_points'].mean() / game_data['fd_points'].mean()
    @cached_property
    def temp_boost(self):
        if self.venue_temp:
            count = 0
            mult = .01
            temp  = self.venue_temp
            data = game_data[game_data['condition'].isin(mac.weather.roof_open)]
            if len(data.index) < 10000:
                stop = len(data.index) * .1
            else:
                stop = 1000
            while count < stop:
                df = data[data['temp'].between(temp - mult, temp + mult, inclusive=False)]
                count = len(df.index)
                mult += 1
            return df['fd_points'].mean() / game_data['fd_points'].mean()
        return 1
    @cached_property
    def ump_boost(self):
        if len(self.projected_ump_data.index) >= 100:
            return self.projected_ump_data['fd_points'].mean() / game_data['fd_points'].mean()
        return 1

    @cached_property
    def opp_bullpen(self):
        file = pickle_path(name=f"{self.opp_name}_bp_{tf.today}", directory=settings.BP_DIR)
        path = settings.BP_DIR.joinpath(file)
        if path.exists():
            bp = pd.read_pickle(path)
            return bp
        bp = self.opp_instance.bullpen
        bp = bp[(~bp['mlb_id'].isin(self.opp_instance.used_rp)) & (bp['status'] == 'A')]
        if len(bp.index) < 4:
            bp = self.opp_instance.bullpen
            bp = bp[(bp['status'] == 'A')]
        with open(file, "wb") as f:
            pickle.dump(bp, f)
        return bp
    @cached_property
    def rested_bullpen(self):
        file = pickle_path(name=f"{self.name}_bp_{tf.today}", directory=settings.BP_DIR)
        path = settings.BP_DIR.joinpath(file)
        if path.exists():
            bp = pd.read_pickle(path)
            return bp
        bp = self.bullpen
        bp = bp[(~bp['mlb_id'].isin(self.used_rp)) & (bp['status'] == 'A')]
        with open(file, "wb") as f:
            pickle.dump(bp, f)
        return bp
        
    @cached_property
    def proj_opp_bp(self):
        return self.opp_bullpen.loc[self.opp_bullpen['batters_faced_rp'].nlargest(4).index]
    @cached_property
    def opp_instance(self):
        for team in Team:
            if team.name == self.opp_name:
                return team
    
    @cached_property
    def rested_sp(self):
        if self.last_game_pk:
            try:
                games = self.past_games[-4:]
            except IndexError:
                games = self.past_games
            game_ids = []
            for game in games:
                print()
                game_ids.append(game['games'][0]['gamePk'])
            game_ids = ids_string(game_ids)
            call = statsapi.get('schedule', {'gamePks': game_ids, 'sportId': 1, 'hydrate': 'probablePitcher'})
            recent_sp = set()
            for g in call['dates']:
                home_sp = g['games'][0]['teams']['home'].get('probablePitcher', {}).get('id')       
                away_sp = g['games'][0]['teams']['away'].get('probablePitcher', {}).get('id') 
                recent_sp.add(home_sp)
                recent_sp.add(away_sp)
            starters = self.starter
            return starters[~starters['mlb_id'].isin(recent_sp)]
        return set()
    def del_props(self):
        try:
            del self.next_game
        except AttributeError:
            pass
        try:
            del self.lineup
        except AttributeError:
            pass
        
        return None
    def del_next_game(self):
        try:
            del self.next_game
        except AttributeError:
            pass  
        return None
    def cache_next_game(self):
        file = pickle_path(name=f"{self.name}_next_game_{tf.today}", directory=settings.GAME_DIR)
        path = settings.GAME_DIR.joinpath(file)
        if not path.exists() and self.weather:
            di = Team.daily_info()
            n = self.name
            if n in di["confirmed_lu"] and n in di["confirmed_sp"]:
                game = self.next_game
                with open(file, "wb") as f:
                    pickle.dump(game, f)
                print(f"Cached {self.name} next game.")
        return None
    
    def clear_all_team_cache(self):
        self.clear_team_cache(directories = [settings.BP_DIR, settings.SCHED_DIR,settings.LINEUP_DIR, settings.GAME_DIR])
        return f"Cleared vital directories for {self.name}."

athletics = Team(mlb_id = 133, name = 'athletics')
pirates = Team(mlb_id = 134, name = 'pirates')
padres = Team(mlb_id = 135, name = 'padres')
mariners = Team(mlb_id = 136, name = 'mariners')
giants = Team(mlb_id = 137, name = 'giants')
cardinals = Team(mlb_id = 138, name = 'cardinals')
rays = Team(mlb_id = 139, name = 'rays')
rangers = Team(mlb_id = 140, name = 'rangers')
blue_jays = Team(mlb_id = 141, name = 'blue jays')
twins = Team(mlb_id = 142, name = 'twins')
phillies = Team(mlb_id = 143, name = 'phillies')
braves = Team(mlb_id = 144, name = 'braves')
white_sox = Team(mlb_id = 145, name = 'white sox')
marlins = Team(mlb_id = 146, name = 'marlins')
yankees = Team(mlb_id = 147, name = 'yankees')
brewers = Team(mlb_id = 158, name = 'brewers')
angels = Team(mlb_id = 108, name = 'angels')
diamondbacks = Team(mlb_id = 109, name = 'diamondbacks')
orioles = Team(mlb_id = 110, name = 'orioles')
red_sox = Team(mlb_id = 111, name = 'red sox')
cubs = Team(mlb_id = 112, name = 'cubs')
reds = Team(mlb_id = 113, name = 'reds')
indians = Team(mlb_id = 114, name = 'indians')
rockies = Team(mlb_id = 115, name = 'rockies')
tigers = Team(mlb_id = 116, name = 'tigers')
astros = Team(mlb_id = 117, name = 'astros')
royals = Team(mlb_id = 118, name = 'royals')
dodgers = Team(mlb_id = 119, name = 'dodgers')
nationals = Team(mlb_id = 120, name = 'nationals')
mets = Team(mlb_id = 121, name = 'mets')