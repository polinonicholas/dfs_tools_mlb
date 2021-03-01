from dfs_tools_mlb.compile import (driver_path, driver_options, storage)
from dfs_tools_mlb.compile import fangraphs_url_params as fgu
from dfs_tools_mlb.compile import fangraphs_urls, time_frames
from dfs_tools_mlb import settings
from dfs_tools_mlb.utils.selenium import dl_wait_chrome
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from time import sleep
import pandas as pd
import pickle
from functools import cached_property, lru_cache
class Stats:
    def __init__(self, 
        origin=1, 
        start_date = time_frames.one_year, 
        end_date = time_frames.today,
        stat_types = [],
        *args, **kwargs):
        self.origin = origin
        self.start_date = start_date
        self.end_date = end_date
        if origin == 1 or origin == 2:
            self.dfs = []
            self.urls = []
            self.player_type = kwargs.get('player_type')
            self.stat_key = kwargs.get('stat_key')
            self.drop_columns = ['name', 'team', 'tm', 'season']
            if origin == 1:
                self.player_team_split = 'false'
                self.splits = kwargs.get('splits', [])
                if len(self.stat_types) == 0:
                    self.stat_types = [1,2,3]
                if not kwargs.get('qualified_only'):
                    self.qualified_only = 'false'
            elif origin == 2:
                if len(self.stat_types) == 0:
                    self.stat_types = [0,1,2,3,4,5,6,7,8,16,17,18,19,20,21,22,23,24]
                if not kwargs.get('qualified_only'):
                    self.qualified_only = '0'
    @staticmethod
    def get_driver():
        if settings.driver_settings.get('name', 'chrome') == 'chrome':
            driver = webdriver.Chrome(driver_path, options=driver_options)
            return driver
    @staticmethod
    def driver_downloads(driver):
        if settings.driver_settings.get('name', 'chrome') == 'chrome':
            paths = WebDriverWait(driver, 120, 1).until(dl_wait_chrome)
            return paths
    @staticmethod
    @lru_cache()
    def get_current_players():
        dfs = []
        for key, value in fangraphs_urls.items():         
            driver = Stats.get_driver()
            driver.maximize_window()
            driver.get(value)
            wait = WebDriverWait(driver, 120)
            export_button = wait.until(EC.element_to_be_clickable((By.LINK_TEXT,'Export Data')))
            export_button.click()
            sleep(.5)
            paths = Stats.driver_downloads(driver)
            file = paths[0]
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip().str.lower()
            df = df.set_index('playerid')
            df.index = df.index.map(str)
            df = df[['name', 'team']]
            dump = storage.joinpath(f'{key}_{time_frames.today}')
            pickle.dump(df, open(dump, 'wb'))
            driver.close()
            dfs.append(df)
        current_players = dfs[0].combine_first(dfs[1])
        return current_players
    def clean_df(self, file, index):
        df = pd.read_csv(file)
        df.columns = df.columns.str.strip().str.lower()
        df = df.set_index(index)
        df.index = df.index.map(str)
        df.drop(columns=[x for x in self.drop_columns if x in df.columns], inplace=True)
        if self.stat_key:
            df.columns = self.stat_key + '_' + df.columns
            self.dfs.append(df)
        return df
    @cached_property
    def get_urls(self):
        for x in self.stat_types:
            if self.origin == 1:
                url = f'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr={str(self.splits).strip("[]")}&splitArrPitch=&position={self.player_type}&autoPt={self.qualified_only}&splitTeams={self.player_team_split}&statType=player&statgroup={x}&startDate={self.start_date}&endDate={self.end_date}&players=&filter=&sort=1,1'
            elif self.origin == 2:
                url = f'https://www.fangraphs.com/leaders.aspx?pos=all&stats={self.player_type}&lg=all&qual={self.qualified_only}&type={x}&season={self.start_date.year}&month=1000&season1={self.end_date.year}&ind=0&team=0&rost=0&age=0&filter=&players=0&startdate={self.start_date}&enddate={self.end_date}'
            self.urls.append(url)
        return self.urls
    @cached_property
    def location(self):
        if self.player_type == 'B' or self.player_type == 'bat':
            return {'dump': storage.joinpath(f'active_hitters_{time_frames.today}'),
            'primary_df': pd.read_pickle(storage.joinpath(f'active_hitters_{time_frames.today}'))}
        else:
            return {'dump': storage.joinpath(f'active_pitchers_{time_frames.today}'),
            'primary_df': pd.read_pickle(storage.joinpath(f'active_pitchers_{time_frames.today}'))}
    
    @cached_property
    def get_stats(self):
        for key in fangraphs_urls.keys():
            if not storage.joinpath(f'{key}_{time_frames.today}').exists():
                Stats.get_current_players()
        for url in self.get_urls:
            driver = Stats.get_driver()
            wait = WebDriverWait(driver, 120)
            driver.get(url)
            if self.origin == 1:
                for _ in range(10):
                    try:
                        actions = ActionChains(driver)
                        group_menu = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="react-drop-test"]/div[1]/div[1]/div[10]/div')))
                        actions.move_to_element(group_menu).perform()
                        driver.find_element_by_xpath('//*[@id="react-drop-test"]/div[1]/div[1]/div[10]/ul/li[1]').click()
                        update_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="button-update"]')))
                        actions.move_to_element(update_button)
                        update_button.click()
                        sleep(.5)
                    except:
                        sleep(.5)
                        continue
                    else:
                        break
                else:
                    raise Exception("Failed to download.")
            loading = WebDriverWait(driver, 120).until_not(EC.presence_of_element_located((By.CLASS_NAME, 'fgui-loading-screen')))
            export_button = wait.until(EC.element_to_be_clickable((By.LINK_TEXT,'Export Data')))
            export_button.click()
            sleep(.5)
            paths = Stats.driver_downloads(driver)
            file = paths[0]
            self.clean_df(file, 'playerid')
            driver.close()
        dump = self.location['dump']
        primary_df = self.location['primary_df']
        for df in self.dfs:
            primary_df = primary_df.combine_first(df)
        pickle.dump(primary_df, open(dump, 'wb'))
        return primary_df

h_year = Stats(origin=1, player_type='B', stat_key='y', stat_types = [1,2])
x = h_year.get_stats


class StatsSplit(Stats):
    
# print(x)
# h_year_lhp = Scrape(origin=1, player_type='B', stat_key='y_lhp', splits=[])
# h_year_rhp = Scrape(origin=1, player_type='B', stat_key='y_rhp')
# h_year_home = Scrape(origin=1, player_type='B', stat_key='y_h')
# h_year_away = Scrape(origin=1, player_type='B', stat_key='y_a')
# h_year_2 = Scrape(origin=2, player_type='bat', stat_key='y')
# h_year_2.compile_stats()

# h = pd.read_pickle(storage.joinpath(f'active_hitters_{datetime.date.today()}'))
# print(h.columns.tolist())


# print(h['year_hard%'].median())

# sp_year_2 = UpdateStats(origin=2, player_type = 'sta', stat_key='y')
# sp_year.compile_stats()

# print(sp_year.stat_type)

# p = pd.read_pickle(storage.joinpath(f'active_pitchers_{datetime.date.today()}'))

# print(p.loc['10021'])

# def selenium_code(self):
# @cached_property
# def get_driver(self):
#     if settings.driver_settings.get('name', 'chrome') == 'chrome':
#         driver = webdriver.Chrome(driver_path, options=driver_options)
#         return driver

# print(Stats.get_current_players())