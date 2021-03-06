from dfs_tools_mlb.config import get_driver_path, get_driver_options
from dfs_tools_mlb import settings
import datetime
from dfs_tools_mlb.utils.subclass import Map

time_frames = Map({'today': datetime.date.today(),
	'yesterday': datetime.date.today() - datetime.timedelta(days=1),
	'thirty_days': datetime.date.today() - datetime.timedelta(days=30),
	'one_year': datetime.date.today() - datetime.timedelta(days=365),
	'two_years': datetime.date.today() - datetime.timedelta(days=730)})

driver_path = get_driver_path(settings.driver_settings.get('name', 'chrome'))
driver_options = get_driver_options(settings.driver_settings.get('name', 'chrome'), settings.driver_settings.get('profile', None), settings.driver_settings.get('use_profile', False))

from pathlib import Path
storage = Path('.', 'storage')
storage.mkdir(exist_ok=True, parents=True)
storage = storage.resolve()

if not settings.storage_settings.get('archive_stats', False):
	for file in Path.iterdir(storage):
		if not str(file).endswith(str(time_frames.today)):
			file.unlink()








