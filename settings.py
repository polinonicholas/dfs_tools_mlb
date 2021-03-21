import os
import dfs_tools_mlb
import inspect
from pathlib import Path
from dfs_tools_mlb.config import get_driver_path, get_driver_options
from dfs_tools_mlb.compile.historical_data import current_season

BASE_DIR = Path(inspect.getfile(dfs_tools_mlb)).resolve().parent
STORAGE_DIR = Path(BASE_DIR, 'compile', 'storage').resolve()
ARCHIVE_DIR = Path(STORAGE_DIR, 'archives').resolve()


driver_settings = {
	'os': 'windows',
	'name':'chrome',
	'profile': os.environ['USERPROFILE'] + '\\AppData\\Local\\Google\\Chrome\\User Data - Copy',
	'profile_target': '--profile-directory=Profile 1',
	'use_profile': True

}

if driver_settings.get('os', None) == 'windows' and driver_settings.get('profile', None) == None:
	driver_settings['profile'] = os.environ['USERPROFILE'] + '\\AppData\\Local\\Google\\Chrome\\User Data'
driver_path = get_driver_path(driver_settings.get('name', 'chrome'))
driver_options = get_driver_options(driver_settings.get('name', 'chrome'), driver_settings.get('profile', None), driver_settings.get('use_profile', False))


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

stat_range = {
    'start': 2019,
    'end' : int(current_season()['season_id']) + 1
    }


