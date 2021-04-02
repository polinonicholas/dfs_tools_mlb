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
            df['stacks'] = df['stacks'] + np.ceil(((diff / len(df.index))))
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
        
    
    def build_lineups(self, lus = 150, max_order = 7, index_track = 0, max_surplus = 900, max_lu_total = 75,
                      max_lu_stack = 50, max_sal = 35000, stack_sample = 6, util_replace_filt = 0):
        # all hitters in slate
        h = self.h_df()
        #dropping faded, platoon, and low order (max_order) players.
        h_fade_filt = (h['team'].isin(self.h_fades))
        plat_filt = (h['is_platoon'] == True)
        h_order_filt = (h['order'] > max_order)
        hfi = h[h_fade_filt | plat_filt | h_order_filt].index
        h.drop(index=hfi,inplace=True)
        #count each players entries
        h['t_count'] = 0
        #count non_stack
        h['ns_count'] = 0
        h_count_df = h.copy()
        #all pitchers...
        p = self.p_df()
        p_fade_filt = (p['team'].isin(self.p_fades))
        pfi = p[p_fade_filt].index
        p.drop(index=pfi,inplace=True)
        p_count_df = p.copy()
        p_count_df['t_count'] = 0
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
        
        #lineups to build
        while lus > 0:
            #if lineup fails requirements, reset will be set to true.
            reset = False
            #drop players already in max_lu_total/max_lu_stack lineups
            total_filt = (h['t_count'] >= max_lu_total)
            count_filt = (h['ns_count'] >= max_lu_stack)
            c_idx = h[total_filt | count_filt].index
            h.drop(index=c_idx,inplace=True)
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
                continue
            #lookup players on the team for the selected stack
            stack_df = h[h['team'] == stack]
            #filter the selected stack by stack_sample arg. 
            highest = stack_df.loc[stack_df['points'].nlargest(stack_sample).index]
            #array of fanduel ids of the selected hitters
            stack_ids = highest['fd_id'].values
            #initial empty lineup, ordered by fanduel structed and mapped by p_map
            lineup = [None, None, None, None, None, None, None, None, None]
            #insert current pitcher into lineup
            lineup[0] = p_info[0]
            #try to create a 4-man stack that satifies position requirements 5 times, else 3-man stack.
            a = 0
            pl2 = []
            while len(pl2) != 4:
                a +=1
                if a > 5:
                    break
                #empty the list of positions each mapped player will fill
                pl2 = []
                #take an initial random sample of 4
                try:
                    #must sort array to avoid TypeError
                    samp = random.sample(sorted(stack_ids), 4)
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
            if a > 5:
                a = 0
                while len(pl2) != 3:
                    a +=1
                    if a > 5:
                        raise Exception(f"Could not create 3-man stack for {stack}.")
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
            np = [idx for idx, spot in enumerate(lineup) if not spot]
            #create list of positions that need to be filled, util always empty here.
            needed_pos = [x for x, y in p_map.items() if y in np]
            #shuffle the list of positions to encourage variance of best players at each position
            random.shuffle(needed_pos)
            #filter out hitters going against selected pitcher
            opp_filt = (h['opp'] != p_info[3])
            #filter out hitters on the team of the current stack, as they are already in the lineup.
            stack_filt = (h['team'] != stack)
            #filter out players not on a team being stacked on slate and proj. points not in 90th percentile.
            fade_filt = ((h['team'].isin(stacks.keys())) | (h['points'] >= h['points'].quantile(.90)))
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
                    hitters = h[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt]
                    hitter = hitters.loc[hitters['points'].idxmax()]
                except (KeyError, ValueError):
                    try:
                        hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & sal_filt]
                        hitter = hitters.loc[hitters['points'].idxmax()]
                    except (KeyError, ValueError):
                        try:
                            hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt]
                            hitter = hitters.loc[hitters['points'].idxmax()]
                        except (KeyError, ValueError):
                            hitters = h[pos_filt & stack_filt & dupe_filt & sal_filt]
                            hitter = hitters.loc[hitters['points'].idxmax()]
                
                salary += hitter['fd_salary'].item()
                rem_sal = max_sal - salary
                #if the selected hitter's salary put the lineup over the max. salary, try to find replacement.
                if rem_sal < 0:
                     r_sal = hitter['fd_salary'].item()
                     try:
                         salary_df = hitters[hitters['fd_salary'] <= (r_sal + rem_sal)]
                         hitter = salary_df.loc[hitters['points'].idxmax()]
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
                                 break
                                 
                #at this point the selected hitter's salary has not put the salary over max_sal
                h_id = hitter['fd_id']
                idx = p_map[ps]
                lineup[idx] = h_id
            #if remaining salary is greater than specified arg max_surplus
            if rem_sal > max_surplus:
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
            if len(used_teams) < 3 and not reset:
                try:
                    #filter out players on teams already in lineup
                    dupe_filt = ((~h['fd_id'].isin(lineup)) & (~h['team'].isin(used_teams)))
                    utility = h[h['fd_id'] == lineup[8]]
                    r_salary = utility['fd_salary'].item()
                    #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = (h['fd_salary'].between((r_salary-util_replace_filt), (rem_sal + r_salary)))
                    #don't use players on team against current pitcher
                    opp_filt = (h['opp'] != p_info[3])
                    #re-specified for clarity
                    fade_filt = ((h['team'].isin(stacks.keys())) | (h['points'] >= h['points'].quantile(.90)))
                    hitters = h[dupe_filt & sal_filt & opp_filt & fade_filt]
                    hitter = hitters.loc[hitters['points'].idxmax()]
                    salary += hitter['fd_salary'].item()
                    salary -+ utility['fd_salary'].item()
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
                    hitters = h[dupe_filt & sal_filt & opp_filt]
                    hitter = hitters.loc[hitters['points'].idxmax()]
                    salary += hitter['fd_salary'].item()
                    salary -+ utility['fd_salary'].item()
                    rem_sal = max_sal - salary
                    lineup[8] = hitter['fd_id']
                    
            #don't insert lineup unless it's not already inserted        
            used_players = []
            used_teams = h_df['team'].unique()
            while (sorted(lineup) in sorted_lus or len(used_teams) < 3) and not reset:
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
                    #re-specified for clarity
                    fade_filt = ((h['team'].isin(stacks.keys())) | (h['points'] >= h['points'].quantile(.90)))
                    hitters = h[dupe_filt & sal_filt & opp_filt & used_filt & fade_filt]
                    hitter = hitters.loc[hitters['points'].idxmax()]
                    used_players.append(hitter['fd_id'])
                    salary += hitter['fd_salary'].item()
                    salary -+ utility['fd_salary'].item()
                    rem_sal = max_sal - salary
                    lineup[8] = hitter['fd_id']
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
                        hitters = h[dupe_filt & sal_filt & opp_filt & used_filt]
                        hitter = hitters.loc[hitters['points'].idxmax()]
                        used_players.append(hitter['fd_id'])
                        salary += hitter['fd_salary'].item()
                        salary -+ utility['fd_salary'].item()
                        rem_sal = max_sal - salary
                        lineup[8] = hitter['fd_id']
                    except (KeyError, ValueError):
                        reset = True
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

       
s=FDSlate(h_fades=['red sox', 'orioles', 'dodgers'], p_fades=['red sox', 'orioles', 'dodgers', 'rockies'])
# s.get_pitchers()
# s.get_hitters()
s.build_lineups()
# s.finalize_entries()


# p_lu_df = s.p_lu_df()
# p_lu_index = p_lu_df['lus'].nlargest(60).index
# p_lu = p_lu_df.loc[p_lu_index, ['name', 'lus', 'points', 'fd_salary', 'venue_points']]

h_stack_df = s.stacks_df()
h_stacks = h_stack_df['stacks'].nlargest(60)
h_stacks.sum()
h_stack_df.columns.tolist()
h_stack_df[['s_z', 'p_z']]
# pc_df = s.p_counts()
# pc_index = pc_df['t_count'].nlargest(60).index
# pc = pc_df.loc[pc_index, ['name', 't_count']]
# pc_team_filt = (pc_df['team'] == '')
# pc_team = pc_df.loc[pc_team_filt, 'name']

hc_df = s.h_counts()
hc_index = hc_df['t_count'].nlargest(60).index
hc = hc_df.loc[hc_index, ['name', 't_count', 'is_platoon', 'team', 'order']]
hc_team_filt = (hc_df['team'] == '')
hc_team = hc_df.loc[hc_team_filt, 'name']




# h = s.h_df()
# h['points']

# r = teams.Team.all_teams.royals.lineup_df()
