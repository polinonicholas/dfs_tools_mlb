def p_lu_df(self):
        lineups = self.lineups
        df = self.p_df()
        i = df[(df['points'] == 0) | (df['team'].isin(self.p_fades))].index
        df.drop(index = i, inplace=True)
        p_drops = df.sort_values(by=['fd_salary'], ascending = False)
        if df['fd_salary'].mean() > df['fd_salary'].median():
            filt_sal = df['fd_salary'].mean()
        else:
            filt_sal = df['fd_salary'].median()
        p_drops = p_drops[p_drops['fd_salary'] > filt_sal]
        mp = p_drops['points'].max()
        for x in p_drops.index:
            if p_drops.loc[x, 'points'] == mp:
                break
            else:
                df.drop(index=x, inplace=True)
        df['lus'] = 1000
        increment = .0
        while df['lus'].max() > self.pitcher_threshold:
            df_c = df.copy()
            df_c['p_z'] = ((df_c['points'] - df_c['points'].mean()) / df_c['points'].std()) + increment
            i = df_c[df_c['p_z'] <= 0].index
            df_c.drop(index = i, inplace=True)
            lu_base = lineups / len(df_c.index)
            df_c['lus'] = lu_base * df_c['p_z']
            if df_c['lus'].max() > self.pitcher_threshold:
                increment =+ .01
                continue
            diff = lineups - df_c['lus'].sum()
            df_c['lus'] = round(df_c['lus'])
            while df_c['lus'].sum() < lineups:
                df_c['lus'] = df_c['lus'] + np.ceil(((diff / len(df_c.index)) * df_c['p_z']))
                df_c['lus'] = round(df_c['lus'])
            if df_c['lus'].max() > self.pitcher_threshold:
                increment =+ .01
                continue    
            while df_c['lus'].sum() > lineups:
                for idx in df_c.index:
                    if df_c['lus'].sum() == lineups:
                        break
                    else:
                        df_c.loc[idx, 'lus'] -= 1
            if df_c['lus'].max() > self.pitcher_threshold:
                increment =+ .01
                continue
            df = df_c
        i = df[df['lus'] == 0].index
        df.drop(index = i, inplace=True)
        return df
def stacks_df(self):
        lineups = self.lineups
        df = self.points_df()
        i = df[(df.index.isin(self.h_fades))].index
        df.drop(index = i, inplace=True)
        df['p_z'] = ((df['points'] - df['points'].mean()) / df['points'].std()) * self.points_weight
        df['s_z'] = ((df['salary'] - df['salary'].mean()) / df['salary'].std()) * -(2 - self.points_weight)
        df['stacks'] = 1000
        increment = .01
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
                df_c['stacks'] = df_c['stacks'] + np.ceil(((diff / len(df.index))))
                df_c['stacks'] = round(df_c['stacks'])
            if df_c['stacks'].max() > self.stack_threshold:
                increment += .01
                continue
                
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