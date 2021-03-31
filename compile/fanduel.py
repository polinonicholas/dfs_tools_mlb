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
    def __init__(self, entries_file=config.get_fd_file(), slate_number = 1, lineups = 150, max_entries = 50, p_fades = [], h_fades=[]):
        
        self.entry_csv = entries_file
        if not self.entry_csv:
            raise TypeError("There are no fanduel entries files in specified DL_FOLDER, obtain one at fanduel.com/upcoming")
        self.slate_number = slate_number
        self.lineups = lineups
        self.max_entries = max_entries
        self.p_fades = p_fades
        self.h_fades = h_fades
        daily_info = teams.Team.daily_info()
        self.p_fades.extend(daily_info['rain'])
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
            hitters = sm_merge_single(hitters, merge, ratio=.63, suffixes=('', '_fd'))     
            hitters.drop_duplicates(subset='mlb_id', inplace=True)
            team.salary = hitters['fd_salary'].sum() / len(hitters.index)
            cols = ["raw_points", "venue_points", "temp_points", "points", "salary"]
            points_file = pickle_path(name=f"team_points_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
            points_path = settings.FD_DIR.joinpath(points_file)
            if points_path.exists():
                p_df = pd.read_pickle(points_path)
            else:
                p_df = pd.DataFrame(columns = cols)
            p_df.loc[team.name, cols] = [team.raw_points, team.venue_points, team.temp_points, team.points, team.salary]
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
                pitcher = sm_merge_single(pitcher, merge, ratio=.63, suffixes=('', '_fd')) 
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
        i = df[(df.index.isin(self.h_fades))].index
        df.drop(index = i, inplace=True)
        df['p_z'] = (df['points'] - df['points'].mean()) / df['points'].std()
        df['s_z'] = ((df['salary'] - df['salary'].mean()) / df['salary'].std()) * -1
        df['z'] = (df['p_z'] + df['s_z']) / 2
        df = df[df['z'] > 0]
        lu_base = lineups / len(df.index)
        df['stacks'] = lu_base * df['z']
        diff = lineups - df['stacks'].sum()
        df['stacks'] = round(df['stacks'])
        while df['stacks'].sum() < lineups:
            df['stacks'] = df['stacks'] + np.ceil(((diff / len(df.index)) * df['z']))
            df['stacks'] = round(df['stacks'])
        while df['stacks'].sum() > lineups:
            for idx in df.index:
                if df['stacks'].sum() == lineups:
                    break
                else:
                    df.loc[idx, 'stacks'] -= 1
        i = df[df['stacks'] == 0].index
        df.drop(index = i, inplace=True)
        return df
    
    def p_lu_df(self):
        lineups = self.lineups
        df = self.p_df()
        i = df[(df['points'] == 0) | (df['team'].isin(self.p_fades))].index
        df.drop(index = i, inplace=True)
        p_drops = df.sort_values(by=['fd_salary'], ascending = False)
        p_drops = p_drops[p_drops['fd_salary'] > df['fd_salary'].mean()]
        print(df['fd_salary'].quantile(.75))
        mp = p_drops['points'].max()
        for x in p_drops.index:
            if p_drops.loc[x, 'points'] == mp:
                break
            else:
                df.drop(index=x, inplace=True)
        df['p_z'] = (df['points'] - df['points'].mean()) / df['points'].std()
        i = df[df['p_z'] <= 0].index
        df.drop(index = i, inplace=True)
        lu_base = lineups / len(df.index)
        df['lus'] = lu_base * df['p_z']
        diff = lineups - df['lus'].sum()
        df['lus'] = round(df['lus'])
        while df['lus'].sum() < lineups:
            df['lus'] = df['lus'] + np.ceil(((diff / len(df.index)) * df['p_z']))
            df['lus'] = round(df['lus'])
        while df['lus'].sum() > lineups:
            for idx in df.index:
                if df['lus'].sum() == lineups:
                    break
                else:
                    df.loc[idx, 'lus'] -= 1
        i = df[df['lus'] == 0].index
        df.drop(index = i, inplace=True)
        return df
    
    def build_lineups(self):
        # all hitters in slate
        h = self.h_df()
        #count each players entries
        h['t_count'] = 0
        #count non_stack
        h['ns_count'] = 0
        #all pitchers...
        p = self.p_df()
        #dict p_df.index: p_df: lineups to be in
        pd = self.p_lu_df()['lus'].to_dict()
        # team: stacks to build
        s = self.stacks_df()['stacks'].to_dict()
        
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
        max_sal = 35000
        #lineups to build
        lus = 150
        index_track = 0
        while lus > 0:
            lus -=1
            total_filt = (h['t_count'] >= 75)
            count_filt = (h['ns_count'] >= 50)
            c_idx = h[total_filt | count_filt].index
            h.drop(index=c_idx,inplace=True)
            
            pitchers = {k:v for k,v in pd.items() if v > 0 }
            pi = random.choice(list(pitchers.keys()))
            # pi = 2
            p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
            salary = 0 + p_info[1]
            stacks = {k:v for k,v in s.items() if v > 0 and k != p_info[2] }
            stack = random.choice(list(stacks.keys()))
            stack_df = h[h['team'] == stack]
            highest = stack_df.loc[stack_df['points'].nlargest(6).index]
            stack_ids = highest['fd_id'].values
            lineup = [None, None, None, None, None, None, None, None, None]
            lineup[0] = p_info[0]
            a = 0
            pl2 = []
            while len(pl2) != 4:
                a +=1
                if a > 5:
                    break
                pl2 = []
                samp = random.sample(sorted(stack_ids), 4)
                pl1 = [x for x in h.loc[h['fd_id'].isin(samp), 'fd_position'].values]
                for x in pl1:
                    if x[0] not in pl2:
                        pl2.append(x[0])
                    elif x[-1] not in pl2:
                        pl2.append(x[-1])
                    elif len(x) > 2 and x[1] not in pl2:
                        pl2.append(x[1])
                    elif 'of' in x and pl2.count('of') == 1 and 'of.1' not in pl2:
                        pl2.append('of.1')
                    elif 'of' in x and pl2.count('of.1') == 1 and 'of.2' not in pl2:
                        pl2.append('of.2')
                    
            if a > 5:
                pl2 = []
                samp = random.sample(sorted(stack_ids), 3)
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
                
            stack_info = h.loc[h['fd_id'].isin(samp), ['fd_salary', 'fd_id']].values
            d = dict(zip(pl2, stack_info))
            stack_map = {}
            for k,v in d.items():
                if not stack_map.get(k):
                    stack_map[k] = v
                    salary += v[0]
                    rem_sal = max_sal - salary
            for k, v in stack_map.items():
                idx = p_map[k]
                lineup[idx] = v[1]  
           
        
            np = [idx for idx, spot in enumerate(lineup) if not spot]
            needed_pos = [x for x, y in p_map.items() if y in np]
            random.shuffle(needed_pos)
            rem_sal = max_sal - salary
            # avg_sal = rem_sal / len(needed_pos)
            
            opp_filt = (h['opp'] != p_info[3])
            stack_filt = (h['team'] != stack)
            fade_filt = ((h['team'].isin(stacks.keys())) | (h['points'] > h['points'].quantile(.90)))
            
            for ps in needed_pos:
                npl = len([idx for idx, spot in enumerate(lineup) if not spot])
                if rem_sal / npl > h['fd_salary'].median():
                    sal_filt = (h['fd_salary'] > h['fd_salary'].mean())
                else:
                    sal_filt = (h['fd_salary'] <= h['fd_salary'].mean())
                
                dupe_filt = (~h['fd_id'].isin(lineup))
                if 'of' in ps:
                    pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                    
                elif 'util' in ps:
                    pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                else:
                    pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                try:
                    hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt]
                    hitter = hitters.loc[hitters['points'].idxmax()]
                except (KeyError, ValueError):
                    try:
                        hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & sal_filt]
                        hitter = hitters.loc[hitters['points'].idxmax()]
                    except (KeyError, ValueError):
                        hitters = h[pos_filt & stack_filt & dupe_filt & sal_filt]
                        hitter = hitters.loc[hitters['points'].idxmax()]
                salary += hitter['fd_salary']
                rem_sal = max_sal - salary
                if rem_sal < 0:
                     r_sal = hitter['fd_salary']
                     try:
                         sal_filt = hitters[hitters['fd_salary'] <= (r_sal + rem_sal)]
                         hitter = sal_filt.loc[hitters['points'].idxmax()]
                         salary += hitter['fd_salary']
                         salary -= r_sal
                         rem_sal = max_sal - salary
                    
                     except(ValueError,KeyError):
                         try:
                             cp = p.loc[pi]
                             cp_sal = cp['fd_salary']
                             p_sal_filt = (p['fd_salary'] <= (rem_sal + cp_sal))
                             p_teams = p.loc[pd.keys(), 'team'].values
                             p_team_filt = (p['team'].isin(p_teams))
                             h_df = h[h['fd_id'].isin(lineup)]
                             used_teams=h_df['team'].unique()
                             p_opp_filt = ((p['opp'] != stack) & (~p['opp'].isin(used_teams)))
                             rps = p[p_sal_filt & p_team_filt & p_opp_filt] 
                             n_p = rps.loc[rps['points'].idxmax()]
                             pi = rps['points'].idxmax()
                             p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
                             n_p_id = n_p['fd_id']
                             n_p_sal = n_p['fd_salary']
                             salary -= cp_sal
                             salary += n_p_sal
                             rem_sal = max_sal - salary
                             lineup[0] = n_p_id
                         except(ValueError,KeyError):
                             try:
                                 cp = p.loc[pi]
                                 cp_sal = cp['fd_salary'].item()
                                 p_sal_filt = (p['fd_salary'] <= (rem_sal + cp_sal))
                                 print(cp_sal)
                                 print(cp)
                                 print(rem_sal)
                                 print(salary)
                                 print(used_teams)
                                 
                                 h_df = h[h['fd_id'].isin(lineup)]
                                 used_teams=h_df['team'].unique()
                                 p_opp_filt = ((p['opp'] != stack) & (~p['opp'].isin(used_teams)))
                                 rps = p[p_sal_filt & p_opp_filt] 
                                 n_p = rps.loc[rps['points'].idxmax()]
                                 pi = rps['points'].idxmax()
                                 p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
                                 n_p_id = n_p['fd_id']
                                 n_p_sal = n_p['fd_salary']
                                 salary -= cp_sal
                                 salary += n_p_sal
                                 rem_sal = max_sal - salary
                                 lineup[0] = n_p_id
                                 if not pitchers.get(pi):
                                     pd[pi] = -1
                             except(KeyError, ValueError):
                                h_df = h[h['fd_id'].isin(lineup)]
                                used_teams=h_df['team'].unique()
                                dupe_filt = (~h['fd_id'].isin(lineup) & (~h['team'].isin(used_teams)))
                                replacee = h[h['fd_id'] == lineup[8]]
                                r_salary = replacee['fd_salary'].item()
                                sal_filt = (h['fd_salary'] <= (max_sal - (salary - r_salary)))
                                opp_filt = (h['opp'] != p_info[3])
                                hitters = h[dupe_filt & sal_filt & stack_filt & opp_filt]
                                hitter = hitters.loc[hitters['points'].idxmax()]
                                salary += hitter['fd_salary']
                                salary -+ replacee['fd_salary']
                                rem_sal = max_sal - salary
                                lineup[8] = hitter['fd_id']
                                
                                
                                 
                             
                            
                h_id = hitter['fd_id']
                idx = p_map[ps]
                lineup[idx] = h_id
            if rem_sal > 900:
                cp = p.loc[pi]
                cp_sal = cp['fd_salary']
                max_sal - (salary - cp_sal)
                p_sal_filt = (p['fd_salary'] <= max_sal - (salary - cp_sal))
                p_teams = p.loc[pd.keys(), 'team'].values
                p_team_filt = (p['team'].isin(p_teams))
                h_df = h[h['fd_id'].isin(lineup)]
                used_teams=h_df['team'].unique()
                p_opp_filt = ((p['opp'] != stack) & (~p['opp'].isin(used_teams)))
                rps = p[p_sal_filt & p_team_filt & p_opp_filt]
                if len(rps.index) > 1:
                    n_p = rps.loc[rps['fd_salary'].idxmax()]
                    pi = rps['fd_salary'].idxmax()
                    p_info = p.loc[pi, ['fd_id', 'fd_salary', 'opp', 'team']].values
                    n_p_id = n_p['fd_id']
                    n_p_sal = n_p['fd_salary']
                    salary -= cp_sal
                    salary += n_p_sal
                    rem_sal = max_sal - salary
                    lineup[0] = n_p_id
                
            h_df = h[h['fd_id'].isin(lineup)]
            used_teams=h_df['team'].unique()
            if len(used_teams) < 3:
                dupe_filt = (~h['fd_id'].isin(lineup) & (~h['team'].isin(used_teams)))
                replacee = h[h['fd_id'] == lineup[8]]
                r_salary = replacee['fd_salary'].item()
                sal_filt = (h['fd_salary'].between((r_salary-200), (rem_sal + r_salary)))
                opp_filt = (h['opp'] != p_info[3])
                hitters = h[dupe_filt & sal_filt & stack_filt & opp_filt]
                hitter = hitters.loc[hitters['points'].idxmax()]
                salary += hitter['fd_salary']
                salary -+ replacee['fd_salary']
                rem_sal = max_sal - salary
                lineup[8] = hitter['fd_id']
            used_players = []
            while sorted(lineup) in sorted_lus:
                h_df = h[h['fd_id'].isin(lineup)]
                used_teams = h_df['team'].unique()
                used_filt = (~h['fd_id'].isin(used_players))
                dupe_filt = (~h['fd_id'].isin(lineup) & (~h['team'].isin(used_teams)))
                replacee = h[h['fd_id'] == lineup[8]]
                used_players.append(replacee['fd_id'])
                r_salary = replacee['fd_salary'].item()
                
                sal_filt = (h['fd_salary'] <= (rem_sal + r_salary))
                opp_filt = (h['opp'] != p_info[3])
                hitters = h[dupe_filt & sal_filt & stack_filt & opp_filt & used_filt]
                hitter = hitters.loc[hitters['points'].idxmax()]
                used_players.append(hitter['fd_id'])
                salary += hitter['fd_salary']
                salary -+ replacee['fd_salary']
                rem_sal = max_sal - salary
                lineup[8] = hitter['fd_id']
            sorted_lus.append(sorted(lineup))
            self.insert_lineup(index_track, lineup)
            index_track += 1
            s[stack] -= 1
            pd[pi] -= 1
            lu_filt = (h['fd_id'].isin(lineup))
            h.loc[lu_filt, 't_count'] += 1
            h.loc[lu_filt & stack_filt, 'ns_count'] += 1
            print(lus)
        return sorted_lus

        
s=FDSlate()
# s.get_pitchers()
# s.get_hitters()
s.build_lineups()
s.finalize_entries()



p = s.p_lu_df()
p[['lus', 'name', 'points', 'fd_salary']]
