from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from dfs_tools_mlb import settings
from functools import lru_cache

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
		options.add_argument("user-data-dir=" + settings.driver_settings['profile'])
		if settings.driver_settings.get('profile_target', None) != None:
				options.add_argument(settings.driver_settings['profile_target'])
	return options

