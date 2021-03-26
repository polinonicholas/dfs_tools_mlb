from dfs_tools_mlb import settings
from dfs_tools_mlb.compile.historical_data import historical_data
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf
import pickle
from pathlib import Path
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac
data_path = settings.STORAGE_DIR.joinpath(f'game_data_{tf.today}.pickle')
if not data_path.exists():
    game_data = historical_data(start=settings.stat_range['start'], end = settings.stat_range['end'])
    game_data = game_data[game_data['last_inning'] >= 9].apply(pd.to_numeric, errors = 'ignore')
    game_data.loc[game_data['condition'].isin(mac.weather.roof_closed), 'temp'] = 72
    game_data.loc[game_data['condition'].isin(mac.weather.roof_closed), 'wind_speed'] = 0
    game_data['fd_points'] = (game_data['runs'] * 6.7) + (game_data['hits'] * 3) + (game_data['home_runs'] * 9)
    game_data['date'] = pd.to_datetime(game_data['date'], infer_datetime_format=True)
    for file in Path.iterdir(settings.STORAGE_DIR):
        if 'game_data' in str(file):
            file.unlink()
    with open(data_path, "wb") as file:
        pickle.dump(game_data, file)
    
else:
    game_data = pd.read_pickle(data_path)