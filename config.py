from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from functools import lru_cache
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

@lru_cache()
def get_driver_path(driver_name):
	if driver_name == 'chrome':
		driver_path=ChromeDriverManager().install()
		return driver_path
@lru_cache()
def get_driver_options(driver_name, driver_profile, use_profile):
	if driver_name == 'chrome':
		options = webdriver.ChromeOptions()
	if driver_profile and use_profile:
		options.add_argument("user-data-dir=" + driver_settings['profile'])
		if driver_settings.get('profile_target', None) != None:
				options.add_argument(driver_settings['profile_target'])
	return options


driver_path = get_driver_path(driver_settings.get('name', 'chrome'))
driver_options = get_driver_options(driver_settings.get('name', 'chrome'), driver_settings.get('profile', None), driver_settings.get('use_profile', False))