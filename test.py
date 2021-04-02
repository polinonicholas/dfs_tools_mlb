def build_lineups(self):
        # all hitters in slate
        h = self.h_df()
        h_fade_filt = (h['team'].isin(self.h_fades))
        hfi = h[h_fade_filt].index
        h.drop(index=hfi,inplace=True)
        #count each players entries
        h['t_count'] = 0
        #count non_stack
        h['ns_count'] = 0
        h_count_df = h.copy()
        plat_filt = (h['is_platoon'] == True)
        h.loc[plat_filt, 't_count'] = 0
        h.loc[plat_filt, 'ns_count'] = 0
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
        reset = False
        
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
            
            
            # total_filt = (h['t_count'] >= 100)
            # count_filt = (h['ns_count'] >= 75)
            # c_idx = h[total_filt | count_filt].index
            # h.drop(index=c_idx,inplace=True)
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
            # fade_filt = ((h['team'].isin(stacks.keys())) | (h['points'] > h['points'].quantile(.50)))
            
            for ps in needed_pos:
                if reset == True:
                    break
                npl = len([idx for idx, spot in enumerate(lineup) if not spot])
                if rem_sal / npl > h['fd_salary'].median():
                    sal_filt = (h['fd_salary'] > h['fd_salary'].quantile(.01))
                else:
                    sal_filt = (h['fd_salary'] > h['fd_salary'].quantile(.01))
                
                dupe_filt = (~h['fd_id'].isin(lineup))
                if 'of' in ps:
                    pos_filt = (h['fd_position'].apply(lambda x: 'of' in x))
                    
                elif 'util' in ps:
                    pos_filt = (h['fd_r_position'].apply(lambda x: ps in x))  
                else:
                    pos_filt = (h['fd_position'].apply(lambda x: ps in x))
                try:
                    hitters = h[pos_filt & stack_filt & dupe_filt & opp_filt & sal_filt]
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
                                
                                
                                 
                if reset == True:
                    continue             
               
                h_id = hitter['fd_id']
                idx = p_map[ps]
                lineup[idx] = h_id
            if rem_sal > 1500:
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
            # if len(used_teams) < 3:
            #     dupe_filt = (~h['fd_id'].isin(lineup) & (~h['team'].isin(used_teams)))
            #     replacee = h[h['fd_id'] == lineup[8]]
            #     print(lineup[8])
            #     r_salary = replacee['fd_salary'].item()
            #     sal_filt = (h['fd_salary'].between((r_salary-200), (rem_sal + r_salary)))
            #     opp_filt = (h['opp'] != p_info[3])
            #     hitters = h[dupe_filt & sal_filt & stack_filt & opp_filt]
            #     hitter = hitters.loc[hitters['points'].idxmax()]
            #     salary += hitter['fd_salary']
            #     salary -+ replacee['fd_salary']
            #     rem_sal = max_sal - salary
            #     lineup[8] = hitter['fd_id']
            used_players = []
            print(used_teams)
            print(h_df)
            print(len(used_teams))
            while len(used_teams) < 3:
                h_df = h[h['fd_id'].isin(lineup)]
                used_teams = h_df['team'].unique()
                used_filt = (~h['fd_id'].isin(used_players))
                dupe_filt = (~h['fd_id'].isin(lineup))
                replacee = h[h['fd_id'] == lineup[8]]
                used_players.append(replacee['fd_id'])
                r_salary = replacee['fd_salary'].item()
                sal_filt = (h['fd_salary'] <= (rem_sal + r_salary))
                
                hitters = h[dupe_filt & sal_filt & stack_filt & used_filt]
                try:
                    hitter = hitters.loc[hitters['points'].idxmax()]
                except ValueError:
                    reset = True
                    break
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
            lus -=1
            lu_filt = (h['fd_id'].isin(lineup))
            h.loc[lu_filt, 't_count'] += 1
            h.loc[lu_filt & stack_filt, 'ns_count'] += 1
            print(lus)
            h_count_df.loc[(h_count_df['fd_id'].isin(lineup)), 't_count'] += 1
            p_count_df.loc[(p_count_df['fd_id']) == lineup[0], 't_count'] += 1
        h_count_file = pickle_path(name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)   
        p_count_file = pickle_path(name=f"p_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)
        with open(h_count_file, "wb") as f:
                pickle.dump(h_count_df, f)
        with open(p_count_file, "wb") as f:
                pickle.dump(p_count_df, f)
        return sorted_lus
    
    
    
    
    
import random
stack_ids = ['1', '2','3']
random.sample(stack_ids, 3)
x = {}
x = {'diamondbacks': 15.0, 'indians': 20.0, 'rockies': 9.0, 'tigers': 13.0, 'rangers': 13.0, 'pirates': 37.0, 'cubs': 31.0}
stack = random.choice(list(x.keys()))