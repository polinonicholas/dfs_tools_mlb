from dfs_tools_mlb.compile.fangraphs import fangraphs_url_params as fgu
from dfs_tools_mlb.compile.fangraphs import fangraphs_urls
from dfs_tools_mlb.compile.static_mlb import team_info
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb import settings
from dfs_tools_mlb.config import get_driver_options, get_driver_path, driver_settings
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
from functools import cached_property


class Stats:
    def __init__(
        self,
        player_type,
        origin="fg_splits",
        start_date=tf.one_year,
        end_date=tf.today,
        stat_types=[],
        stat_key="",
        dump=settings.STORAGE_DIR.joinpath(f"stats_{tf.today}"),
        *args,
        **kwargs,
    ):
        self.origin = origin
        self.start_date = start_date
        self.end_date = end_date
        self.stat_types = stat_types
        self.player_type = player_type
        self.stat_key = stat_key
        self.dump = dump
        self.dfs = []
        self.urls = []
        self.drop_columns = ["name", "team", "tm", "season"]
        if origin == "fg_splits":
            self.splits = kwargs.get("splits", [])
            if len(self.stat_types) == 0:
                self.stat_types = [1, 2, 3]
            if not kwargs.get("qualified_only"):
                self.qualified_only = "false"
            if not kwargs.get("player_team_split"):
                self.player_team_split = "false"
        elif origin == "fg_leaders":
            if len(self.stat_types) == 0:
                self.stat_types = [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                ]
            if not kwargs.get("qualified_only"):
                self.qualified_only = "0"
        # self.get_stats

    @staticmethod
    def get_driver():
        if driver_settings.get("name", "chrome") == "chrome":
            driver_path = get_driver_path("chrome")
            driver_options = get_driver_options(
                "chrome",
                driver_settings.get("profile", None),
                driver_settings.get("use_profile", False),
            )
            driver = webdriver.Chrome(driver_path, options=driver_options)
            return driver

    @staticmethod
    def driver_downloads(driver):
        if driver_settings.get("name", "chrome") == "chrome":
            paths = WebDriverWait(driver, 120, 1).until(dl_wait_chrome)
            return paths

    @staticmethod
    def current_stats():
        if not settings.STORAGE_DIR.joinpath(f"stats_{tf.today}").exists():
            dfs = []
            for key, value in fangraphs_urls.items():
                driver = Stats.get_driver()
                driver.maximize_window()
                driver.get(value)
                wait = WebDriverWait(driver, 120)
                export_button = wait.until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Export Data"))
                )
                export_button.click()
                sleep(0.5)
                paths = Stats.driver_downloads(driver)
                file = paths[0]
                df = pd.read_csv(file)
                df.columns = df.columns.str.strip().str.lower()
                df = df.set_index("playerid")
                df.index = df.index.map(str)
                if key == "active_pitchers":
                    df["p"] = True
                    df = df[["p", "name", "team"]]
                else:
                    df["h"] = True
                    df = df[["h", "name", "team"]]
                driver.close()
                dfs.append(df)
            current_players = dfs[0].combine_first(dfs[1])
            current_players = current_players.fillna({"p": False, "h": False})
            current_players = current_players[["name", "team", "h", "p"]]
            dump = settings.STORAGE_DIR.joinpath(f"stats_{tf.today}")
            with open(dump, "wb") as file:
                pickle.dump(current_players, file)
                file.close()
        else:
            current_players = pd.read_pickle(
                settings.STORAGE_DIR.joinpath(f"stats_{tf.today}")
            )
        current_players.reset_index(inplace=True, drop=True)
        return current_players

    @staticmethod
    def modify_team_name(
        df,
        team_info,  # dfs_tools_mlb.complile.mlb
        column="team",
        dump=True,
        path=settings.STORAGE_DIR.joinpath(f"stats_{tf.today}"),
    ):
        name = df[column]
        name = name.str.casefold()
        for team in team_info:
            a = team_info[team]["abbreviations"]
            df.loc[(name.isin(a) | name.str.contains(team)), ["team"]] = team
        if dump:
            with open(path, "wb") as file:
                pickle.dump(df, file)
                file.close()
        return df

    def clean_df(self, file, index):
        df = pd.read_csv(file)
        df.columns = df.columns.str.strip().str.lower()
        df = df.set_index(index)
        df = df.replace({"%": "", "$": "", ",": ""}, regex=True)
        df.index = df.index.map(str)
        df.drop(columns=[x for x in self.drop_columns if x in df.columns], inplace=True)
        if self.stat_key != "":
            df.columns = df.columns + "_" + self.stat_key
            self.dfs.append(df)
        return df

    @cached_property
    def get_urls(self):
        for x in self.stat_types:
            if self.origin == "fg_splits":
                url = f'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr={str(self.splits).strip("[]")}&splitArrPitch=&position={self.player_type}&autoPt={self.qualified_only}&splitTeams={self.player_team_split}&statType=player&statgroup={x}&startDate={self.start_date}&endDate={self.end_date}&players=&filter=&sort=1,1'
            elif self.origin == "fg_leaders":
                url = f"https://www.fangraphs.com/leaders.aspx?pos=all&stats={self.player_type}&lg=all&qual={self.qualified_only}&type={x}&season={self.start_date.year}&month=1000&season1={self.end_date.year}&ind=0&team=0&rost=0&age=0&filter=&players=0&startdate={self.start_date}&enddate={self.end_date}"
            self.urls.append(url)
        return self.urls

    # @cached_property
    # def location(self):
    #     if self.player_type == 'B' or self.player_type == 'bat':
    #         return {'dump': settings.STORAGE_DIR.joinpath(f'active_hitters_{tf.today}'),
    #         'primary_df': pd.read_pickle(settings.STORAGE_DIR.joinpath(f'active_hitters_{tf.today}'))}
    #     else:
    #         return {'dump': settings.STORAGE_DIR.joinpath(f'active_pitchers_{tf.today}'),
    #         'primary_df': pd.read_pickle(_p.joinpath(f'active_pitchers_{tf.today}'))}

    @cached_property
    def get_stats(self):
        try:
            Stats.current_stats()[self.stat_key]
        except KeyError:
            for url in self.get_urls:
                driver = Stats.get_driver()
                wait = WebDriverWait(driver, 120)
                driver.get(url)
                if self.origin == "fg_splits":
                    for _ in range(10):
                        try:
                            actions = ActionChains(driver)
                            group_menu = wait.until(
                                EC.element_to_be_clickable(
                                    (
                                        By.XPATH,
                                        '//*[@id="react-drop-test"]/div[1]/div[1]/div[10]/div',
                                    )
                                )
                            )
                            actions.move_to_element(group_menu).perform()
                            driver.find_element_by_xpath(
                                '//*[@id="react-drop-test"]/div[1]/div[1]/div[10]/ul/li[1]'
                            ).click()
                            update_button = wait.until(
                                EC.element_to_be_clickable(
                                    (By.XPATH, '//*[@id="button-update"]')
                                )
                            )
                            actions.move_to_element(update_button)
                            update_button.click()
                            sleep(0.5)
                        except:
                            sleep(0.5)
                            continue
                        else:
                            break
                    else:
                        raise Exception("Failed to download.")
                    loading = WebDriverWait(driver, 120).until_not(
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, "fgui-loading-screen")
                        )
                    )
                export_button = wait.until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Export Data"))
                )
                export_button.click()
                sleep(0.5)
                paths = Stats.driver_downloads(driver)
                file = paths[0]
                self.clean_df(file, "playerid")
                driver.close()
            for df in self.dfs:
                if "REPLACE" in self.stat_key:
                    df.columns = df.columns.str.replace("REPLACE", "")
                primary_df = Stats.current_stats()
                primary_df = primary_df.combine_first(df)
            primary_df[self.stat_key] = True
            with open(self.dump, "wb") as file:
                pickle.dump(primary_df, file)
                file.close()
        columns = [
            col
            for col in Stats.current_stats().columns
            if self.stat_key in col or col in self.drop_columns
        ]
        return Stats.current_stats()[columns]


__all__ = []
h_year = Stats(player_type="B", stat_key="hy")
h_30 = Stats(
    origin="fg_leaders", player_type="bat", stat_key="h30", start_date=tf.thirty_days
)
h_year_lhp = Stats(stat_types=[1], player_type="B", stat_key="hyl", splits=[fgu.h_lhp])
h_year_rhp = Stats(stat_types=[1], player_type="B", stat_key="hyr", splits=[fgu.h_rhp])
h_year_home = Stats(player_type="B", stat_key="hyh", splits=[fgu.h_home])
h_year_away = Stats(player_type="B", stat_key="hya", splits=[fgu.h_away])
sp_year = Stats(player_type="P", stat_key="spy", splits=[fgu.p_sp])
sp_30 = Stats(
    origin="fg_leaders", player_type="sta", stat_key="sp30", start_date=tf.thirty_days
)
sp_year_lhh = Stats(player_type="P", stat_key="spyl", splits=[fgu.p_sp, fgu.p_lhh])
sp_year_rhh = Stats(player_type="P", stat_key="spyr", splits=[fgu.p_sp, fgu.p_rhh])
sp_year_home = Stats(player_type="P", stat_key="spyh", splits=[fgu.p_sp, fgu.p_home])
sp_year_away = Stats(player_type="P", stat_key="spya", splits=[fgu.p_sp, fgu.p_away])
sp_year_runners_on = Stats(
    player_type="P", stat_key="spyro", splits=[fgu.p_sp, fgu.runners_on]
)
sp_year_second_order = Stats(
    player_type="P", stat_key="spyso", splits=[fgu.p_sp, fgu.p_2]
)
rp_year = Stats(player_type="P", stat_key="rpy", splits=[fgu.p_rp])
rp_30 = Stats(
    origin="fg_leaders", player_type="rel", stat_key="rp30", start_date=tf.thirty_days
)
rp_year_runners_on = Stats(
    player_type="P", stat_key="rpyro", splits=[fgu.p_rp, fgu.runners_on]
)
rp_year_lhh = Stats(player_type="P", stat_key="rpyl", splits=[fgu.p_rp, fgu.p_lhh])
rp_year_rhh = Stats(player_type="P", stat_key="rpyr", splits=[fgu.p_rp, fgu.p_rhh])
rp_ytd = Stats(
    origin="fg_leaders",
    player_type="rel",
    stat_key="rpytd",
    start_date=tf.yesterday,
    stat_types=[0],
)
rp_year_leaders = Stats(origin="fg_leaders", player_type="rel", stat_key="rpyREPLACE")
sp_year_leaders = Stats(origin="fg_leaders", player_type="sta", stat_key="spyREPLACE")
Stats.modify_team_name(Stats.current_stats(), team_info)
