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


