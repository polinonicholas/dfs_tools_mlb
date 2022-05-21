from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb import settings
import pickle
from pathlib import Path
from dfs_tools_mlb.dataframes.game_data import game_data
data_path = settings.STORAGE_DIR.joinpath(f'venue_data_{tf.today}.pickle')
if not data_path.exists():
    
    
    
    venue_data = game_data.groupby('venue_id', as_index=True)[['fd_points', 'adj_pa',
                                                               'fd_points+',
                                                               'fd_points_lhb', 
                                                               'adj_pa_lhb',
                                                               'fd_points_rhb', 
                                                               'adj_pa_rhb',
                                                               'strikeouts',
                                                               'stolen_base',
                                                               'caught_stealing',
                                                               'ejections',
                                                               'success_lhb',
                                                               'success_rhb',
                                                               'fail_lhb',
                                                               'fail_rhb',
                                                               'void_lhb',
                                                               'void_rhb',
                                                               'field_error',
                                                               'passed_ball',
                                                               'grounded_into_double_play',
                                                               'condition',
                                                               'start_time',
                                                               'temp',
                                                               'wind_direction',
                                                               'wind_speed',
                                                               'single',
                                                               'double',
                                                               'triple',
                                                               'home_run']].sum()
    venue_data['fd_pts_pa'] = venue_data['fd_points'] / venue_data['adj_pa']
    venue_data['fd_pts_pa+'] = venue_data['fd_points+'] / venue_data['adj_pa']
    venue_data['fd_pts_pa_lhb'] = venue_data['fd_points_lhb'] / venue_data['adj_pa_lhb']
    venue_data['fd_pts_pa_rhb'] = venue_data['fd_points_rhb'] / venue_data['adj_pa_rhb']
    venue_data.index = venue_data.index.astype('int')
    
    for file in Path.iterdir(settings.STORAGE_DIR):
        if 'venue_data' in str(file):
            file.unlink()
    with open(data_path, "wb") as file:
        pickle.dump(venue_data, file)
    
else:
    import pandas as pd
    venue_data = pd.read_pickle(data_path)

def current_venue_stats(df, series_or_columns=None, exclude=[]):
    from dfs_tools_mlb.compile.static_mlb import current_parks
    df = df[(df.index.isin(current_parks())) & (~df.index.isin(exclude))]
    if series_or_columns:
        return df[series_or_columns]
    else:
        return df
    
def qualified_venue_stats(df, exclude=[], series_or_columns=None):
    df = df[(df['adj_pa'] > settings.MIN_PA_VENUE) & (~df.index.isin(exclude))]
    if series_or_columns:
        return df[series_or_columns]
    else:
        return df    

def df_z_score(df, column, mult = 1):
    return ((df[column] - df[column].mean()) / df[column].std(ddof = 0)) * mult
