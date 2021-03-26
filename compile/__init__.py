from dfs_tools_mlb import settings
settings.STORAGE_DIR.mkdir(exist_ok=True, parents=True)

from dfs_tools_mlb.compile.historical_data import current_season
from dfs_tools_mlb.utils.subclass import Map
current_season = Map(current_season())

from dfs_tools_mlb.utils.storage import clean_directory
clean_directory(settings.LINEUP_DIR)
clean_directory(settings.SP_DIR)
clean_directory(settings.BP_DIR)







