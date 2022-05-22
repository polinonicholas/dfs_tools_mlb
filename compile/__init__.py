from dfs_tools_mlb import settings

settings.STORAGE_DIR.mkdir(exist_ok=True, parents=True)

from dfs_tools_mlb.compile.historical_data import current_season
from dfs_tools_mlb.utils.subclass import Map

current_season = Map(current_season())

from dfs_tools_mlb.utils.storage import clean_directory

clean_directory(settings.LINEUP_DIR, force_delete=True)
clean_directory(settings.SP_DIR, force_delete=True)
clean_directory(settings.BP_DIR, force_delete=True)
clean_directory(settings.NRI_DIR, force_delete=True)
clean_directory(settings.ROSTER_DIR, force_delete=True)
clean_directory(settings.DEPTH_DIR, force_delete=True)
clean_directory(settings.GAME_DIR, force_delete=True)
clean_directory(settings.GAME_DIR, force_delete=True)
clean_directory(settings.SCHED_DIR, force_delete=True)
clean_directory(settings.FD_DIR, force_delete=True)
clean_directory(settings.STORAGE_DIR, force_delete=True)
