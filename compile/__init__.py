from dfs_tools_mlb import settings
settings.STORAGE_DIR.mkdir(exist_ok=True, parents=True)

from dfs_tools_mlb.compile.historical_data import historical_data
game_data = historical_data(start=settings.stat_range['start'], end = settings.stat_range['end'])


from dfs_tools_mlb.compile.game_data import current_season
from dfs_tools_mlb.utils.subclass import Map
current_season = Map(current_season())





    




            
            
    