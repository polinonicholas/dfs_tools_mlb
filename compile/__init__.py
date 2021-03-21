from dfs_tools_mlb import settings
settings.STORAGE_DIR.mkdir(exist_ok=True, parents=True)

from dfs_tools_mlb.compile.historical_data import historical_data, current_season
import pandas as pd
game_data = historical_data(start=settings.stat_range['start'], end = settings.stat_range['end'])
game_data = game_data[game_data['last_inning'] >= 9].apply(pd.to_numeric, errors = 'ignore')
game_data['fd_points'] = (game_data['runs'] * 6.7) + (game_data['hits'] * 3)

from dfs_tools_mlb.utils.subclass import Map
current_season = Map(current_season())





    




            
            
    