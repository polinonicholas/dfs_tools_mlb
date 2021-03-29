import glob
import os
import pandas as pd
import numpy as np
fd_entries = glob.glob("C:/Users/nicho/Downloads/FanDuel-MLB*entries-upload-template*") 
fd_entry = max(fd_entries, key=os.path.getctime)

with open(fd_entry, 'r') as f:
    test = pd.read_csv(f, usecols=['entry_id', 'contest_id', 'contest_name', 'P', 'C/1B', '2B', '3B', 'SS', 'OF' ,'OF.1', 'OF.2', 'UTIL'])
test = test[~test['entry_id'].isna()]
test = test.astype({'entry_id': np.int64})

test.loc[0, ['P', 'C/1B', '2B', '3B', 'SS', 'OF', 'OF.1', 'OF.2', 'UTIL']] = ['xxxxxxxxxxxxx-119426','56054-12863', '56054-71508', '56054-146675', '56054-146739', '56054-112379', '56054-38599', '56054-14019', '56054-118396']
test.rename(columns = {'OF.1': 'OF', 'OF.2': 'OF'}, inplace=True)
x = ['56054-119426','56054-12863', '56054-71508', '56054-146675', '56054-146739', '56054-112379', '56054-38599', '56054-14019', '56054-118396']
test.loc[0]


t = test.to_csv(fd_entry, index=False)






class FDSlate:
    def __init__(self, entries_file, slate_number = 1):
        self.entries = entries_file
        
    def entries_df(self, reset=False):
        df_file = pickle_path(name=f"lineup_entries_{tf.today}_{slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(file)
        if path.exists() and not reset:
            df = pd.read_pickle(path)
        else:
            cols = ['entry_id', 'contest_id', 'contest_name', 'P', 'C/1B', '2B', '3B', 'SS', 'OF' ,'OF.1', 'OF.2', 'UTIL']
            csv_file = self.entries
            with open(csv_file, 'r') as f:
                df = pd.read_csv(f, usecols = cols)
            df = df[~df['entry_id'].isna()]
            df = df.astype({'entry_id': np.int64})
            with open(df_file, "wb") as f:
                pickle.dump(df, f)
        return df
    @cached_property
    def player_info_df(self):
        df_file = pickle_path(name=f"player_info_{tf.today}_{slate_number}", directory=settings.FD_DIR)
        path = settings.FD_DIR.joinpath(df_file)
        if path.exists():
            df = pd.read_pickle(path)
        else:   
            cols = ['Player ID + Player Name', 'Id', 'Position', 'First Name', 'Nickname', 'Last Name', 'FPPG', 'Played', 'Salary', 'Game', 'Team', 'Opponent', 'Injury Indicator', 'Injury Details', 'Tier', 'Probable Pitcher', 'Batting Order', 'Roster Position']
            csv_file = self.entries
            with open(csv_file, 'r') as f:
                df = pd.read_csv(f, skiprows = lambda x: x < 6, usecols=cols)
            with open(df_file, "wb") as f:
                pickle.dump(df, f)
        return df
    def insert_lineup(self, idx, lineup):
        cols = ['P', 'C/1B', '2B', '3B', 'SS', 'OF', 'OF.1', 'OF.2', 'UTIL']
        df = self.entries_df()
        df.loc[idx, cols] = lineup
        file = pickle_path(name=f"lineup_entries_{tf.today}_{slate_number}", directory=settings.FD_DIR)
        with open(file, "wb") as f:
            pickle.dump(df, f)
        return f"Inserted {lineup} at index {idx}."
    def finalize_entries(self):
        df = self.entries_df()
        csv = self.entries
        df.rename(columns = {'OF.1': 'OF', 'OF.2': 'OF'}, inplace=True)
        df.to_csv(csv, index=False)
        return f"Stored lineups at {csv}"
        
        
from dfs_tools_mlb.dataframes import stat_splits
    
x = stat_splits.p_splits.columns.tolist()

[y for y in x if '_' not in y]

stat_splits.p_splits.loc[stat_splits.p_splits['batters_faced_rp'].nlargest(50).index, ['name']]
        
