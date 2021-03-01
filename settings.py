import os


driver_settings = {
	'os': 'windows',
	'name':'chrome',
	'profile': os.environ['USERPROFILE'] + '\\AppData\\Local\\Google\\Chrome\\User Data - Copy',
	'profile_target': '--profile-directory=Profile 1',
	'use_profile': True

}

if driver_settings.get('os', None) == 'windows' and driver_settings.get('profile', None) == None:
	driver_settings['profile'] = os.environ['USERPROFILE'] + '\\AppData\\Local\\Google\\Chrome\\User Data'


storage_settings = {
	'archive_stats': False,
}
# storage_settings['storage_path'].mkdir(exist_ok=True, parents=True)

