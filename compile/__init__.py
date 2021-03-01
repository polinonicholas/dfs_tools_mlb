from dfs_tools_mlb.config import get_driver_path, get_driver_options
from dfs_tools_mlb import settings

driver_path = get_driver_path(settings.driver_settings.get('name', 'chrome'))
driver_options = get_driver_options(settings.driver_settings.get('name', 'chrome'), settings.driver_settings.get('profile', None), settings.driver_settings.get('use_profile', False))

from pathlib import Path
storage = Path('.', 'storage')
storage.mkdir(exist_ok=True, parents=True)
storage = storage.resolve()

from dfs_tools_mlb.compile.fangraphs import fangraphs_info
fangraphs_urls = fangraphs_info()['fangraphs_urls']
fangraphs_url_params = fangraphs_info()['fangraphs_url_params']

from dfs_tools_mlb.utils.time import time_frames
time_frames = time_frames()

if not settings.storage_settings.get('archive_stats', False):
	for file in Path.iterdir(storage):
		if not str(file).endswith(str(time_frames.today)):
			file.unlink()








