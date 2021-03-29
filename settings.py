import dfs_tools_mlb
import inspect
from pathlib import Path
import glob
import os

BASE_DIR = Path(inspect.getfile(dfs_tools_mlb)).resolve().parent
STORAGE_DIR = Path(BASE_DIR, 'compile', 'storage').resolve()
ARCHIVE_DIR = Path(STORAGE_DIR, 'archives').resolve()
LINEUP_DIR = Path(STORAGE_DIR, 'confirmed_lineups').resolve()
SP_DIR = Path(STORAGE_DIR, 'confirmed_sp').resolve()
BP_DIR = Path(STORAGE_DIR, 'bullpens').resolve()
DEPTH_DIR = Path(STORAGE_DIR, 'depth_charts').resolve()
ROSTER_DIR = Path(STORAGE_DIR, 'rosters').resolve()
NRI_DIR = Path(STORAGE_DIR, 'nri').resolve()
GAME_DIR = Path(STORAGE_DIR, 'recent_games').resolve()
FD_DIR = Path(STORAGE_DIR, 'fd_entries').resolve()

DL_FOLDER = "C:/Users/nicho/Downloads"
FD_FILE_MATCH = DL_FOLDER + "/FanDuel-MLB*entries-upload-template*"
FD_FILES = glob.glob(FD_FILE_MATCH) 


    
    
    



use_fangraphs = False

storage_settings = {
	'archive_stats': True,
}

stat_splits ={
	#left/right splits
	'h_lr': True,
	'sp_lr': True,
	'rp_lr': True,
	#home/away splits
	'h_ha': True,
	'sp_ha': True,
	'rp_ha': True,
	#pitching with runners on base
	'sp_ro': True,
	'rp_ro': True,
	#SP second time through order stats
	'sp_so': True,
	#stats over last 30 days
	'h_30': False,
	'sp_30': False,
	'rp_30': False,
	#relif IP yesterday
	'rp_ytd': False
}
# int(current_season()['season_id']) + 1 circular import
stat_range = {
    'start': 2016,
    'end' : 2022,
    'player_start': 2019
    }


