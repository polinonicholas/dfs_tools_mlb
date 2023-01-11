from functools import cached_property, lru_cache
import re
import pandas as pd
import json
from json import JSONDecodeError
import numpy as np
import pickle
import datetime
from math import floor, ceil
from pathlib import Path

import statsapi

from dfs_tools_mlb.compile import current_season as cs, cs_yr
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac
from dfs_tools_mlb.compile.static_mlb import api_player_info_dict, api_pitcher_info_dict
from dfs_tools_mlb.utils.statsapi import full_schedule
from dfs_tools_mlb.utils.storage import json_path
from dfs_tools_mlb.compile.static_mlb import team_info
from dfs_tools_mlb.utils.strings import plural
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.utils.pd import numeric_df
from dfs_tools_mlb.dataframes.game_data import game_data
from dfs_tools_mlb.dataframes.venue_data import qualified_venue_stats, venue_data
from dfs_tools_mlb.utils.storage import pickle_path, pickle_dump
from dfs_tools_mlb.utils.strings import ids_string
from dfs_tools_mlb import settings
from dfs_tools_mlb.dataframes.stat_splits import h_splits, p_splits
from dfs_tools_mlb.dataframes.stat_splits import adjustment_dfs as a_dfs


class IterTeam(type):
    def __iter__(cls):
        return iter(cls._all_teams)

class Team(metaclass=IterTeam):
    _all_teams = []

    def __init__(
        self,
        mlb_id,
        name=""
    ):
        self._all_teams.append(self)
        self.id = mlb_id
        self.name = name
        self.file = Path(f"{settings.TEAM_DIR.joinpath(self.name)}.json")
        try:
            with open(self.file) as f:
                self.team_vars = json.load(f)
        except (FileNotFoundError, JSONDecodeError, PermissionError):

            self.team_vars = {"custom_lineup": None,
             "custom_sp": None,
             "custom_pps": None,
             "ppd": False,
             "custom_temp": None,
             "custom_wind_speed": None,
             "custom_wind_direction": None}
        
        self.custom_lineup = self.team_vars["custom_lineup"]
        self.custom_sp = self.team_vars["custom_sp"]
        self.custom_pps = self.team_vars["custom_pps"]
        self.custom_temp = self.team_vars["custom_temp"]
        self.custom_wind_speed = self.team_vars["custom_wind_speed"]
        self.custom_wind_direction = self.team_vars["custom_wind_direction"]
        
        if self.name in settings.ppd:
            self.ppd = True
        else:
            self.ppd = self.team_vars["ppd"]

    @staticmethod
    def roster_dict(instance, roster_type, is_depth=False):
        call = Team.get_roster_api(instance.id, roster_type)
        if not is_depth:
            info = []
            for p in call:
                player = api_player_info_dict(p)
                player["team"] = instance.name
                position = str(player["position"])
                if position in mac.players.h:
                    player["h"] = True
                elif position in mac.players.sp:
                    player["sp"] = True
                    player["p"] = True
                elif position in mac.players.bullpen:
                    player["bp"] = True
                    player["p"] = True
                elif position in mac.players.twp:
                    player["p"] = True
                    player["h"] = True
                info.append(player)
        else:
            info = {"starters": [], "bullpen": [], "hitters": []}
            for p in call:
                player = api_player_info_dict(p)
                player["team"] = instance.name
                position = str(player["position"])
                if position in mac.players.h:
                    info["hitters"].append(player)
                elif position in mac.players.sp:
                    info["starters"].append(player)
                elif position in mac.players.bullpen:
                    info["bullpen"].append(player)
        return info

    @staticmethod
    def join_player_stats(df1, col="mlb_id", hitters=True):
        """
        """
        if hitters:
            return df1.join(h_splits.set_index(col), on=col, rsuffix='_drop')
        else:
            return df1.join(p_splits.set_index(col), on=col, rsuffix='_drop')
    
    @staticmethod
    def get_player_api(player_id):
        return statsapi.get("people", {"personIds": player_id})["people"][0]

    @staticmethod
    def get_game_api(game_id):
        return statsapi.get("game", {"gamePk": game_id})

    @staticmethod
    def get_roster_api(teamid, roster_type):
        hydrate = settings.api_hydrate["p_info"]
        call = statsapi.get(
            "team_roster",
            {"teamId": teamid, "rosterType": roster_type, "hydrate": hydrate},
                          )["roster"]
        return call

    @staticmethod
    def dump_json_data(file, json_data):
        try:
            with open(file, "w+") as f:
                json.dump(json_data, f)
            return "Dumped daily info."
        except PermissionError:
            return "Could not dump data due to permission error."

    @staticmethod
    def lineups():
        try:
            with open(settings.team_lineups_file) as file:
                team_lineups = json.load(file)
                file.close()
            return team_lineups
        except (FileNotFoundError, JSONDecodeError, PermissionError):
            team_lineups = {}
            for team in team_info.keys():
                team_lineups[team] = {'L': [], 'R': []}
            Team.dump_json_data(settings.team_lineups_file, team_lineups)
            return team_lineups

    @staticmethod
    def daily_info():
        file = json_path(name=f"daily_info_{tf.today}", directory=settings.STORAGE_DIR)
        daily_info = settings.daily_info_dict
        try:
            try:
                with open(file) as f:
                    daily_info = json.load(f)
            except (FileNotFoundError, JSONDecodeError):
                with open(file, "w+") as f:
                    json.dump(daily_info, f)
        except PermissionError:
            pass
        return daily_info

    @staticmethod
    def default_sp():
        df = a_dfs["P"]["SP"]["RAW"]["raw"]
        empty = df[df["mlb_id"] == 0]
        defaults = df.median()
        empty = empty.append(defaults, ignore_index=True)
        empty["name"] = "Unknown"
        empty["pitch_hand"] = "R"
        return empty

    @staticmethod
    def get_split_filter(df, hand, pitchers=False, return_percentage=False):
       
        if not pitchers:
            split = "bat_side"
        else:
            split = "pitch_hand"
        filt = df[split] == hand.upper()
        if return_percentage:
            split_length = len(df[filt].index)
            return split_length / settings.LU_LENGTH
        return filt

    @staticmethod
    def non_rested_teams(travel_only=True):
        no_rest = {"away": set(), "home": set()}
        for t in Team:
            if not t.no_rest:
                continue
            if travel_only:
                if t.no_rest_travel and t.is_home:
                    no_rest["home"].add(t.name)
                elif t.no_rest_travel and not t.is_home:
                    no_rest["away"].add(t.name)
            else:
                if not t.no_rest:
                    continue
                if t.no_rest and t.is_home:
                    no_rest["home"].add(t.name)
                elif t.no_rest and not t.is_home:
                    no_rest["away"].add(t.name)
        return no_rest

    @staticmethod
    def adjust_non_qualified_p(df, bullpen = False, base="batters_faced"):
        l_filt = Team.get_split_filter(df, "L", pitchers=True)
        r_filt = Team.get_split_filter(df, "R", pitchers=True)
        if not bullpen:
            min_bf = settings.MIN_BF_SP
            min_bf_sp = settings.MIN_BF_SP_SPLIT
            stats = settings.STATS_TO_ADJUST_SP
            p_col = "_sp"
            a_dfs_branch = a_dfs["P"]["SP"]
        else:
            min_bf = settings.MIN_BF_BP
            min_bf_sp = setting.MIN_BF_BP_SPLIT
            stats = settings.STATS_TO_ADJUST_RP
            p_col = "_rp"
            a_dfs_branch = a_dfs["P"]["BP"]
        for stat in stats:
            medians_vs = {
                ("r", "r"): a_dfs_branch["R"]["vs_r"][stat + "_vr"].median(),
                ("l", "l"): a_dfs_branch["L"]["vs_l"][stat + "_vl"].median(),
                ("l", "r"): a_dfs_branch["L"]["vs_r"][stat + "_vr"].median(),
                ("r", "l"): a_dfs_branch["R"]["vs_l"][stat + "_vl"].median(),
                ("_", "_"): a_dfs_branch["RAW"]["raw"][stat + "_rp"].median()
                        }
            vl_filt = ((df[base + "_vl"] < min_bf_sp) | (df[stat + "_vl"].isna()))
            vr_filt = ((df[base + "_vr"] < min_bf_sp) | (df[stat + "_vr"].isna()))
            raw_filt = ((df[base + p_col] < min_bf) | (df[stat + p_col].isna()))
            df.loc[vr_filt & r_filt, stat + "_vr"] = medians_vs[("r", "r")]
            df.loc[vl_filt & l_filt, stat + "_vl"] = medians_vs[("l", "l")]
            df.loc[vr_filt & l_filt, stat + "_vr"] = medians_vs[("l", "r")]
            df.loc[vl_filt & r_filt, stat + "_vl"] = medians_vs[("r", "l")]
            df.loc[raw_filt, stat + p_col] = medians_vs[("_", "_")]

        return df

    def adjust_non_qualified_h(df, base="pa"):

        l_filt = Team.get_split_filter(df, "L")
        r_filt = Team.get_split_filter(df, "R")
        min_pa = settings.MIN_PA_HITTER
        min_pa_sp = settings.MIN_PA_HITTER_SPLIT
        for stat in settings.STATS_TO_ADJUST_H:
            medians_vs = {
                ("r", "r"): a_dfs["H"]["R"]["vs_r"][stat + "_vr"].median(),
                ("l", "l"): a_dfs["H"]["L"]["vs_l"][stat + "_vl"].median(),
                ("l", "r"): a_dfs["H"]["L"]["vs_r"][stat + "_vr"].median(),
                ("r", "l"): a_dfs["H"]["R"]["vs_l"][stat + "_vl"].median(),
                ("_", "_"): a_dfs["H"]["RAW"]["hitters"][stat].median()
                        }
            vl_filt = ((df[base + "pa_vl"] < min_pa_sp) | (df[stat + "_vl"].isna()))
            vr_filt = ((df[base + "_vr"] < min_pa_sp) | (df[stat + "_vr"].isna()))
            raw_filt = ((df[base] < min_pa) | (df[stat].isna()))
            df.loc[vr_filt & r_filt, base + "_vr",] = medians_vs[("r", "r")]
            df.loc[vl_filt & l_filt, base + "_vl",] = medians_vs[("l", "l")]
            df.loc[vr_filt & l_filt, base + "_vr",] = medians_vs[("l", "r")]
            df.loc[vl_filt & r_filt, base + "_vl",] = medians_vs[("r", "l")]
            df.loc[raw_filt, stat] = medians_vs[("_", "_")]
            
        return df

    @staticmethod
    def update_daily_info(current_daily_info, to_update, update_with):
        ##TODO check for permission when instance created
        current_daily_info[to_update].append(update_with)
        Team.dump_json_data(settings.daily_info_file, current_daily_info)

    @staticmethod
    def get_game_officials(game):
        return game["liveData"]["boxscore"]["officials"]

    @staticmethod
    def get_game_pk(game):
        return game["games"][0]["gamePk"]
    
    @staticmethod
    def get_game_plays(game):
        return game["liveData"]["plays"]["allPlays"]

    @staticmethod
    def get_official_id(official):
        return official["official"]["id"]

    @staticmethod
    def get_official(game, ump_type, return_id = True):
        officials = Team.get_game_officials(game)
        try:
            ump = next(o for o in officials if o["officialType"] == ump_type)
            if return_id:
                return Team.get_official_id(ump)
            else:
                return ump
        except StopIteration:
            return "Unable to project ump."
        
    @cached_property
    def depth(self):
        file = pickle_path(
            name=f"{self.name}_depth_{tf.today}", directory=settings.DEPTH_DIR
        )
        path = settings.DEPTH_DIR.joinpath(file)
        if path.exists():
            depth = pd.read_pickle(path)
        else:
            depth = Team.roster_dict(self, roster_type = 'depthChart', is_depth=True)
            pickle_dump(depth, file)
        return depth

    @cached_property
    def full_roster(self):
        file = pickle_path(
            name=f"{self.name}_roster_{tf.today}", directory=settings.ROSTER_DIR
        )
        path = settings.ROSTER_DIR.joinpath(file)
        if path.exists():
            roster = pd.read_pickle(path)
        else:
            roster = Team.roster_dict(self, roster_type="40Man")
            roster.extend(self.nri)
            pickle_dump(roster, file)
        return roster

    @cached_property
    def nri(self):
        file = pickle_path(
            name=f"{self.name}_nri_{tf.today}", directory=settings.NRI_DIR
        )
        path = settings.NRI_DIR.joinpath(file)
        if path.exists():
            nri = pd.read_pickle(path)
        else:
            try:
                nri = Team.roster_dict(self, roster_type="nonRosterInvitees")
                pickle_dump(nri, file)
            except KeyError:
                return []
        return nri

    # list of integers representing ids of every affiliated player.
    @cached_property
    def all_player_ids(self):
        return [player['mlb_id'] for player in self.full_roster]

    @lru_cache
    def batters(self, active_players_only=False):
        roster = pd.DataFrame(self.full_roster)
        batters = roster[roster['h'] == True]
        if active_players_only:
            batters = batters[batters['status'] == 'A']
        return Team.join_player_stats(batters)


    @cached_property
    def hitter(self):
        hitters = numeric_df(self.depth["hitters"])
        hitters["h"] = True
        return Team.join_player_stats(hitters)

    @cached_property
    def starter(self):
        starters = numeric_df(self.depth["starters"])
        starters = starters[starters["status"] == "A"]
        starters = Team.join_player_stats(starters, hitters=False)
        starters["p"] = True
        return starters

    @cached_property
    def bullpen(self):
        bullpen = numeric_df(self.depth["bullpen"])
        bullpen = Team.join_player_stats(bullpen, hitters=False)
        bullpen["p"] = True
        return bullpen

    @cached_property
    def catcher(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["c"]
]

    @cached_property
    def first_base(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["1b"]
]

    @cached_property
    def second_base(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["2b"]
]

    @cached_property
    def third_base(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["3b"]
]

    @cached_property
    def shorstop(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["ss"]
]

    @cached_property
    def left_field(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["lf"]
]

    @cached_property
    def center_field(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["cf"]
]

    @cached_property
    def right_field(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["rf"]
]

    @cached_property
    def dh(self):
        return self.hitter[self.hitter["position"].astype("int") == mac.players["dh"]
]

    @cached_property
    def infield(self):
        return self.hitter[
            self.hitter["position_type"].str.casefold().str.contains("infield")
        ]

    @cached_property
    def outfield(self):
        return self.hitter[
            self.hitter["position_type"].str.casefold().str.contains("outfield")
        ]

    @cached_property
    def pitcher(self):
        pitchers = self.starter.combine_first(self.bullpen)
        return pitchers

    @cached_property
    def coaches(self):
        ## TODO functin to get coaches, coach dict in mlb_static.py
        data = statsapi.get("team_roster", {"teamId": self.id, "rosterType": "coach"})["roster"]
        coaches = []
        for c in data:
            coach = {
                "mlb_id": c.get("person", {}).get("id", ""),
                "name": c.get("person", {}).get("fullName", ""),
                "mlb_api": c.get("person", {}).get("link", ""),
                "job": c.get("job", ""),
            }
            coaches.append(coach)
        return pd.DataFrame(coaches).set_index("mlb_id")

    @cached_property
    def manager(self):
        return self.coaches[self.coaches["job"].str.casefold().str.contains("manage")]

    @cached_property
    def bench_coach(self):
        return self.coaches[self.coaches["job"].str.casefold().str.contains("bench")]

    @cached_property
    def hitting_coach(self):
        return self.coaches[self.coaches["job"].str.casefold().str.contains("hitting")]

    @cached_property
    def pitching_coach(self):
        return self.coaches[self.coaches["job"].str.casefold().str.contains("pitch")]

    @cached_property
    def first_base_coach(self):
        return self.coaches[self.coaches["job"].str.casefold().str.contains("first")]

    @cached_property
    def third_base_coach(self):
        return self.coaches[self.coaches["job"].str.casefold().str.contains("third")]

    @cached_property
    def bullpen_staff(self):
        return self.coaches[self.coaches["job"].str.casefold().str.contains("bullpen")]

    @cached_property
    def all_games(self):
        
        file = pickle_path(
            name=f"{self.name}_schedule_{tf.today}", directory=settings.SCHED_DIR
        )
        path = settings.SCHED_DIR.joinpath(file)
        if path.exists():
            schedule = pd.read_pickle(path)
        else:
            schedule = full_schedule(
                                     team=self.id, 
                                     start_date=cs.spring_start, 
                                     end_date=cs.playoff_end
                                     )
            pickle_dump(schedule, file)
        return schedule

    @cached_property
    def future_games(self):
        games = self.all_games
        pre = mac.game_status.pre
        return [g for g in games if g["games"][0]["status"]["codedGameState"] in pre]

    @cached_property
    def past_games(self): 
        games = self.all_games
        past = mac.game_status.post
        return [g for g in games if g["games"][0]["status"]["codedGameState"] in past]

    @cached_property
    def next_game_pk(self):
        try:
            return self.future_games[0]["games"][0]["gamePk"]
        except IndexError:
            print("No games on schedule.")
            return False

    @cached_property
    def last_game_pk(self):
        try:
            return self.past_games[-1]["games"][0]["gamePk"]
        except IndexError:
            print(f"The {self.name.capitalize()} have yet to play in {cs.season_id}.")
            return False

    @cached_property
    def next_game(self):
        if self.next_game_pk:
            file = pickle_path(
                name=f"{self.name}_next_game_{tf.today}", directory=settings.GAME_DIR
            )
            path = settings.GAME_DIR.joinpath(file)
            if path.exists():
                game = pd.read_pickle(path)
            else:
                print(f"Getting boxscore for {self.name.capitalize()}' next game.")
                game = Team.get_game_api(self.next_game_pk)
            return game
        return self.next_game_pk

    @cached_property
    def last_game(self):
        if self.last_game_pk:
            file = pickle_path(
                name=f"{self.name}_last_game_{tf.today}", directory=settings.GAME_DIR
            )
            path = settings.GAME_DIR.joinpath(file)
            if path.exists():
                game = pd.read_pickle(path)
            else:
                # if not game['gameData']['game']['doubleHeader'] == 'Y' ???
                print(f"Getting boxscore {self.name.capitalize()}' last game.")
                game = Team.get_game_api(self.last_game_pk)
                pickle_dump(game, file)
            return game
        return self.last_game_pk

    @cached_property
    def is_home(self):
        if self.next_game_pk:
            return int(self.next_game["gameData"]["teams"]["home"]["id"]) == self.id
        return self.next_game_pk

    @cached_property
    def was_home(self):
        if self.last_game_pk:
            return int(self.last_game["gameData"]["teams"]["home"]["id"]) == self.id
        return self.last_game_pk

    @cached_property
    def ha(self):
        if self.next_game_pk:
            if self.is_home:
                return "home"
            else:
                return "away"
        return self.next_game_pk

    @cached_property
    def opp_ha(self):
        if self.next_game_pk:
            if self.ha == "home":
                return "away"
            elif self.ha == "away":
                return "home"
        return self.next_game_pk

    @cached_property
    def was_ha(self):
        if self.last_game_pk:
            if self.was_home:
                return "home"
            else:
                return "away"
        return self.last_game_pk

    @cached_property
    def opp_was_ha(self):
        if self.last_game_pk:
            if self.was_ha == "home":
                return "away"
            elif self.was_ha == "away":
                return "home"
        return self.last_game_pk

    def next_game_probable_pitchers(self):
        return self.next_game["gameData"]["probablePitchers"]

    def probable_pitcher_id_string(self, home_or_away):
        return "ID" + str(self.next_game_probable_pitchers()[self.opp_ha]["id"])
    
    def probable_pitcher_info(self, home_or_away):
        sp_id = self.probable_pitcher_id_string(home_or_away)
        return self.next_game["gameData"]["players"][sp_id]

    

    @cached_property
    def opp_sp(self):
        if self.opp_instance.custom_sp:
            return Team.get_player_api(self.opp_instance_custom_sp)
        if self.next_game_pk:
            daily_info = Team.daily_info()
            opp_sp_confirmed = Team.check_confirmed_sp(daily_info, self.opp_name)
            if not opp_sp_confirmed and not self.ppd:
                self.del_next_game()
            try:
                #KeyError would be raised on opp_sp_info, if no listed pitcher
                sp_info = self.probable_pitcher_info(self.opp_ha)
                if not opp_sp_confirmed:
                    Team.update_daily_info(daily_info, "confirmed_sp", self.opp_name)
                self.opp_instance.cache_next_game()
                return sp_info
            except KeyError:
                starters = self.opp_instance.rested_sp()
                try:
                    column = settings.PROJECTED_SP_COLUMN
                    projected_sp = starters.loc[starters[column].idxmax()]
                except (KeyError, ValueError):
                    bullpen = self.opp_instance.rested_bullpen
                    try:
                        col = "batters_faced_sp"
                        projected_sp = bullpen.loc[bullpen[col].idxmax()]
                    except (KeyError, ValueError):
                        #pitches per appearance
                        col = "ppa"
                        projected_sp = bullpen.loc[bullpen["ppa"].idxmax()]
                i_d = int(projected_sp["mlb_id"].item())
                return Team.get_player_api(i_d)
        return self.next_game_pk

    def projected_sp(self):
        if self.custom_sp:
            return Team.get_player_api(self.custom_sp)
        if self.next_game_pk:
            daily_info = Team.daily_info()
            sp_confirmed = Team.check_confirmed_sp(daily_info, self.name)
            if not sp_confirmed and not self.ppd:
                self.del_next_game()
            try:
                #KeyError would be here
                sp_info = self.probable_pitcher_info(self.ha)
                if sp_confirmed:
                    Team.update_daily_info(daily_info, "confirmed_sp", self.name)
                self.cache_next_game()
                return sp_info
            except KeyError:
                starters = self.rested_sp()
                try:
                    col = settings.PROJECTED_SP_COLUMN
                    projected_sp = starters.loc[starters[col].idxmax()]
                except (KeyError, ValueError):
                    bullpen = self.rested_bullpen
                    try:
                        col = "batters_faced_sp"
                        projected_sp = bullpen.loc[bullpen[col].idxmax()]
                    except (KeyError, ValueError):
                        col = "ppa"
                        projected_sp = bullpen.loc[bullpen[col].idxmax()]
                i_d = int(projected_sp["mlb_id"].item())
                return Team.get_player_api(i_d)
        return self.next_game_pk

    @cached_property
    def last_opp_sp(self):
        if self.last_game_pk:
            sp_id = "ID" + str(
                self.last_game["gameData"]["probablePitchers"][self.opp_was_ha]["id"]
            )
            sp_info = self.last_game["gameData"]["players"][sp_id]
            return sp_info
        return self.last_game_pk

    @cached_property
    def opp_sp_hand(self):
        try:
            return self.opp_sp["pitchHand"]["code"]
        except TypeError:
            print(f"{self.opp_sp} returning default 'R'")
            return "R"

    @cached_property
    def o_split(self):
        if self.opp_sp_hand == "R":
            return "vr"
        else:
            return "vl"

    @cached_property
    def last_opp_sp_hand(self):
        if self.last_game_pk:
            return self.last_opp_sp["pitchHand"]["code"]
        return self.last_game_pk

    @cached_property
    def opp_name(self):
        if self.next_game_pk:
            name = self.next_game["gameData"]["teams"][self.opp_ha]["teamName"].lower()
            if "backs" in name:
                name = "diamondbacks"
            return name
        return self.next_game_pk

    @cached_property
    def lineup(self):
        if self.custom_lineup:
            return self.custom_lineup
        if self.next_game_pk:
            daily_info = Team.daily_info()
            if self.name in daily_info["confirmed_lu"]:
                lineup = Team.lineups()[self.name][self.opp_sp_hand]
                if len(lineup) == 9:
                    return lineup
            if self.name not in daily_info["confirmed_lu"] and not self.ppd:
                self.del_next_game()
            lineup = self.next_game["liveData"]["boxscore"]["teams"][self.ha][
                "battingOrder"
            ]
            if len(lineup) == 9:
                self.update_lineup(lineup, self.opp_sp_hand)
                if self.name not in daily_info["confirmed_lu"]:
                    daily_info["confirmed_lu"].append(self.name)
                    Team.dump_json_data(settings.daily_info_file, daily_info)
                self.cache_next_game()
                return lineup
            else:
                return self.projected_lineup
        return self.next_game_pk

    

    @cached_property
    def projected_lineup(self):
        print(f"Getting projected lineup for {self.name}")
        team_lineups = Team.lineups()
        try:
            if len(team_lineups[self.name][self.opp_sp_hand]) == 9:
                return team_lineups[self.name][self.opp_sp_hand]
            else:
                lineup = []
                plays = Team.get_game_plays(self.last_game)
                if self.was_home:
                    top_inning = False
                else:
                    top_inning = True
                for play in plays:
                    if play["about"]["isTopInning"] == top_inning:
                        lineup.append(play["matchup"]["batter"]["id"])
                        if len(lineup) == 9:
                            self.update_lineup(lineup, self.last_opp_sp_hand)
                            return lineup
        except (KeyError, TypeError):
            return self.last_game_pk

    def update_lineup(self, lineup, hand):
        team_lineups = Team.lineups()
        team_lineups[self.name][hand] = lineup
        with open(json_path(name="team_lineups"), "w+") as file:
            json.dump(team_lineups, file)
            file.close()
        return lineup

    def get_replacement(self, position, position_type, excluded):
        roster = Team.join_player_stats(pd.DataFrame(self.full_roster))
        h_key = "fd_wps_pa_" + self.o_split
        if str(position) in mac.players.p:
            if not self.is_dh:
                projected_sp = roster[roster["mlb_id"] == self.projected_sp()["id"]]
                if len(projected_sp.index) != 0:
                    return projected_sp.loc[projected_sp["age"].idxmax()]
                else:
                    bullpen = self.rested_bullpen[
                        ~self.rested_bullpen["mlb_id"].isin(excluded)
                    ]
                    try:
                        return bullpen.loc[bullpen["batters_faced_sp"].idxmax()]
                    except KeyError:
                        return bullpen.loc[bullpen["ppa"].idxmax()]
            else:
                dh = self.dh[~self.dh["mlb_id"].isin(excluded)]
                try:
                    return dh.loc[dh[h_key].idxmax()]
                except (KeyError, ValueError):
                    hitters = self.batters(active_players_only=True)
                    hitters = hitters[
                        (~hitters["mlb_id"].isin(excluded))
                        & (hitters["pa_" + self.o_split] > 100)
                    ]
                    try:
                        return hitters.loc[hitters[h_key].idxmax()]
                    except (KeyError, ValueError):
                        hitters = self.batters(active_players_only=True)
                        return hitters.loc[hitters[h_key].idxmax()]
        else:
            hitters = self.batters(active_players_only=True)
            hitters = hitters[~hitters["mlb_id"].isin(excluded)]
            hitters = hitters[hitters["position"].astype(str) == position]
            try:
                return hitters.loc[hitters[h_key].idxmax()]
            except (KeyError, ValueError):
                hitters = self.batters(active_players_only=True)
                hitters = hitters[~hitters["mlb_id"].isin(excluded)]
                hitters = hitters[hitters["position_type"].astype(str) == position_type]
                try:
                    return hitters.loc[hitters[h_key].idxmax()]
                except (KeyError, ValueError):
                    hitters = self.batters(active_players_only=True)
                    hitters = hitters[~hitters["mlb_id"].isin(excluded)]
                    return hitters.loc[hitters[h_key].idxmax()]

    @staticmethod
    def check_confirmed_lineup(current_daily_info, teamname):
        return teamname in current_daily_info["confirmed_lu"]

    @staticmethod
    def check_confirmed_sp(current_daily_info, teamname):
        return teamname in current_daily_info["confirmed_sp"]

    def should_reset_lineup(self):
        if settings.reset_all_lineups:
            return True
        return (self.name in settings.reset_specific_lineups and not self.ppd)
    
    def calculate_team_lu_points(self, lineup_df):
            self.raw_points = lineup_df["exp_ps_raw"].sum()
            self.venue_points = lineup_df["venue_points"].sum()
            self.temp_points = (self.raw_points * self.temp_boost) - self.raw_points
            self.ump_points = (self.raw_points * self.ump_boost) - self.raw_points
            self.points = (
                self.venue_points + self.temp_points + self.ump_points + self.raw_points
            )
            self.sp_mu = lineup_df["sp_mu"].sum()
            self.lu_talent_sp = lineup_df["sp_split"].sum() - lineup_df["sp_split"].std(ddof=0)
            return lineup_df
    
    @staticmethod
    def add_missing_players_h_df(h_df, ids):
        for i_d in [i_d for i_d in ids if i_d not in h_df["mlb_id"].values]:
            player = Team.get_player_api(i_d)
            position = player["primaryPosition"]["code"]
            position_type = player["primaryPosition"]["type"]
            new_row = self.get_replacement(position, position_type, ids)
            idx = ids.index(i_d)
            ids[idx] = int(new_row["mlb_id"])
            h_df = h_df.append(new_row, ignore_index=True)
        return h_df
    @staticmethod
    def set_order_h_df(h_df, ids):
        sorter = dict(zip(ids, range(len(ids))))
        h_df["order"] = h_df["mlb_id"].map(sorter)
        h_df.sort_values(by="order", inplace=True, kind="mergesort")
        h_df["order"] = h_df["order"] + 1
        h_df.reset_index(inplace=True, drop=True)
        return h_df

    @staticmethod
    def set_switch_hand_h_df(h_df, hand):
        if hand == "R":
            h_df.loc[h_df["bat_side"] == "S", "bat_side"] = "L"
        else:
            h_df.loc[h_df["bat_side"] == "S", "bat_side"] = "R"
        return h_df

    ## TODO reference the series' origin
    @staticmethod
    def set_pit_per_pa_h_df(h_df_series, h_df, p_df, lefties, righties):
        splits = {"_vl": lefties, "_vr": righties}
        h_weight = (h_df_series * settings.H_PIT_PER_AB_WGT)
        for col, filt in splits.items():
            p_weight = (p_df["ppb" + col].max() * settings.P_PIT_PER_AB_WGT)
            h_df.loc[filt, 'pitches_pa_sp'] = (h_weight + p_weight)
        return h_df

    @cached_property
    def opp_sp_df(self):
        try:
            p_info = self.opp_sp
            pitcher = api_pitcher_info_dict(p_info)
            pitcher["team"] = self.opp_name
            p_df = Team.join_player_stats(pd.DataFrame([pitcher]), hitters=False)
        except TypeError:
            p_df = Team.default_sp()
        return p_df

    @staticmethod
    def pitches_per_order(h_df, p_df):
        l_weight = Team.get_split_filter(h_df, "L", return_percentage=True)
        r_weight = Team.get_split_filter(h_df, "R", return_percentage=True)
        pitches_per_lhb = (l_weight * p_df["ppb_vl"].max())
        pitches_per_rhb = (r_weight * p_df["ppb_vr"].max())
        return ((pitches_per_lhb + pitches_per_rhb)) * settings.LU_LENGTH

    @staticmethod
    def adjust_na_pitches_start_p_df(p_df):
        adjustment_df = a_dfs["P"]["SP"]["RAW"]["raw"]["pitches_start"]
        p_df["pitches_start"].fillna(adjustment_df.median(), inplace=True)
        return p_df

    def lineup_df(self):
        file = pickle_path(
            name=f"{self.name}_lu_{tf.today}", directory=settings.LINEUP_DIR
        )
        daily_info = Team.daily_info()
        confirmed = Team.check_confirmed_lineup(daily_info, self.name)
        reset_flag = self.should_reset_lineup())
        path = settings.LINEUP_DIR.joinpath(file)
        if (not confirmed or reset_flag):
            self.del_props()
        if not path.exists() or (not confirmed or reset_flag):
            lineup_ids = self.lineup
            lineup = pd.DataFrame(lineup_ids, columns=["mlb_id"])
            roster = pd.DataFrame(self.full_roster)
            h_df = Team.join_player_stats(pd.merge(lineup, roster, on="mlb_id"))
            if not self.custom_lineup and len(h_df.index) < 9:
                h_df = Team.add_missing_players_h_df(h_df, lineup_ids)
            h_df = h_df.loc[~h_df.duplicated(subset=["mlb_id"])]
            assert len(h_df.index) == 9, f"{self.name}' has {len(hf.index)} players."
            h_df = Team.set_order_h_df(h_df, lineup_ids)
            h_df = Team.set_switch_hand_h_df(h_df, self.opp_sp_hand)
            h_df = Team.adjust_non_qualified_h(h_df)
            p_df = self.opp_sp_df
            lefties = Team.get_split_filter(h_df, "L")
            righties = Team.get_split_filter(h_df, "R")
            p_df = Team.adjust_non_qualified_p(p_df)
            # p_ppb = Team.pitches_per_order(h_df, p_df)

            if not self.opp_instance.custom_pps:
                p_df = Team.adjust_na_pitches_start_p_df(p_df)
            else:
                p_df["pitches_start"] = self.opp_instance.custom_pps
            
            
                    
            

            key = "pitches_pa_" + self.o_split
            series = h_df[key]
            h_df = Team.set_pit_per_pa_h_df(series, h_df, p_df, lefties, righties)
            
            pitches_ab_array = h_df[key].to_numpy()
            pitches = p_df['pitches_start'].item()
            idx = 0
            total_batters_faced = 0
            while pitches > 0:
                pitches -= pitches_ab_array[idx]
                total_batters_faced += 1
                idx += 1
                if idx == 9:
                    idx = 0
            p_df["exp_x_lu"] = total_batters_faced / settings.LU_LENGTH
            
            # p_df["exp_x_lu"] = p_df["pitches_start"] / ((h_df[key].sum() * settings.H_PIT_PER_AB_WGT) + (p_ppb * settings.P_PIT_PER_AB_WGT))
            h_df["sp_exp_x_lu"] = p_df["exp_x_lu"].max()
            
            print(
                f"{p_df['name'].max()} expected to go through {self.name} LU {p_df['exp_x_lu'].max()} times. P:{p_ppb} LU: {h_df[key].sum()}"
            )
            p_df["exp_bf"] = total_batters_faced
            # p_df["exp_bf"] = round((p_df["exp_x_lu"] * settings.LU_LENGTH))
            sp_rollover = floor((p_df["exp_x_lu"] % 1) * settings.LU_LENGTH)
            h_df.loc[h_df["order"] <= sp_rollover, "exp_pa_sp"] = ceil(p_df["exp_x_lu"])
            h_df.loc[h_df["order"] > sp_rollover, "exp_pa_sp"] = floor(p_df["exp_x_lu"])
            h_df['exp_pitches'] = h_df['exp_pa_sp'] * h_df[key]
            # average_p_ab_top_lu = h_df.loc[(h_df['exp_pa_sp'] == h_df['exp_pa_sp'].max()), key].mean()
            key = "fd_wps_pa_" + self.o_split
            h_df.loc[lefties, "exp_ps_sp_pa"] = (
                p_df["fd_wpa_b_vl"].max() * settings.P_PTS_WEIGHT
            ) + (h_df[key] * settings.H_PTS_WEIGHT)
            h_df.loc[righties, "exp_ps_sp_pa"] = (
                p_df["fd_wpa_b_vr"].max() * settings.P_PTS_WEIGHT
            ) + (h_df[key] * settings.H_PTS_WEIGHT)
            h_df.loc[lefties, "exp_ps_sp"] = (
                (p_df["fd_wpa_b_vl"].max() * settings.P_PTS_WEIGHT)
                + (h_df[key] * settings.H_PTS_WEIGHT)
            ) * h_df["exp_pa_sp"]
            h_df.loc[righties, "exp_ps_sp"] = (
                (p_df["fd_wpa_b_vr"].max() * settings.P_PTS_WEIGHT)
                + (h_df[key] * settings.H_PTS_WEIGHT)
            ) * h_df["exp_pa_sp"]
            h_df.loc[lefties, "sp_mu"] = p_df["fd_wpa_b_vl"].max()
            h_df.loc[righties, "sp_mu"] = p_df["fd_wpa_b_vr"].max()
            h_df["sp_split"] = h_df[key]
            h_df["exp_ps_sp_raw"] = h_df[key] * h_df["exp_pa_sp"]
            # points conceded
            key = "fd_wpa_pa_" + self.o_split
            k_key = "k_pa_" + self.o_split
           
            h_df.loc[lefties, "exp_pc_sp"] = ((
                p_df["fd_wps_b_vl"].max() * settings.P_PTS_WEIGHT
            ) + (h_df[key] * settings.H_PTS_WEIGHT) * h_df["exp_pa_sp"])
            h_df.loc[righties, "exp_pc_sp"] = ((
                p_df["fd_wps_b_vr"].max() * settings.P_PTS_WEIGHT
            ) + (h_df[key] * settings.H_PTS_WEIGHT) * h_df["exp_pa_sp"])
            
            h_df["exp_pc_sp_raw"] = (h_df[key])
            h_df["raw_exp_pc_sp"] = (h_df[key] * h_df["exp_pa_sp"])
            h_df.loc[lefties, "exp_k"] = ((
                p_df["k_b_vl"].max() * settings.P_K_WEIGHT
            ) + (h_df[k_key] * settings.H_K_WEIGHT) * h_df["exp_pa_sp"])
            h_df.loc[righties, "exp_k"] = ((
                p_df["k_b_vr"].max() * settings.P_K_WEIGHT
            ) + (h_df[k_key] * settings.H_K_WEIGHT) * h_df["exp_pa_sp"])
            # h_df.loc[lefties, 'exp_k'] = (((p_df['k_b_vl'].max() * 1) + (h_df[k_key] * 1)) / 2) * h_df['exp_pa_sp']
            # h_df.loc[righties, 'exp_k'] = (((p_df['k_b_vr'].max() * 1) + (h_df[k_key] * 1)) / 2) * h_df['exp_pa_sp']
            h_df["exp_k_sp_raw"] = (h_df[k_key] * h_df["exp_pa_sp"])
            exp_pa_r_sp = h_df.loc[righties, "exp_pa_sp"].sum()
            exp_pa_l_sp = h_df.loc[lefties, "exp_pa_sp"].sum()
            key = "rb-_pa_" + self.o_split
            h_df['exp_hits_sp'] = (h_df[key] * h_df["exp_pa_sp"])
            exp_hits_lu = floor(h_df['exp_hits_sp'].sum())
            p_df["exp_ra"] = (floor(
                (exp_pa_r_sp * p_df["ra-_b_vr"].max()) * settings.P_HITS_ALLOWED_WEIGHT
                + (exp_pa_l_sp * p_df["ra-_b_vl"].max()))
             + (exp_hits_lu * settings.H_HITS_ALLOWED_WEIGHT))
            p_df["exp_inn"] = (p_df["exp_bf"].max() - p_df["exp_ra"].max()) / 3
            h_df["sp_exp_inn"] = p_df["exp_inn"].max()
            if self.is_home:
                exp_bp_inn = settings.BP_INNINGS_HOME - p_df["exp_inn"].max()
            else:
                exp_bp_inn = settings.BP_INNINGS_ROAD - p_df["exp_inn"].max()
            

            
            bp = Team.adjust_non_qualified_p(self.proj_opp_bp, bullpen = True)
            try:
                exp_bf_bp = round(
                    (exp_bp_inn * 3) + ((exp_bp_inn * 3) * bp["ra-_b_rp"].mean())
                )
            except ValueError:
                print(f"USING DEFAULT BP RA- for {self.name}")
                exp_bf_bp = round(
                    (exp_bp_inn * 3)
                    + (
                        (exp_bp_inn * 3)
                        * a_dfs["P"]["BP"]["RAW"]["raw"]["ra-_b_rp"].median()
                    )
                )

            first_bp_pa = h_df.loc[
                (h_df["exp_pa_sp"] == floor(p_df["exp_x_lu"])), "order"
            ].idxmin()
            order = h_df.loc[first_bp_pa, "order"].item()
            h_df["exp_pa_bp"] = 0
            while exp_bf_bp > 0:
                if order == 10:
                    order = 1
                h_df.loc[h_df["order"] == order, "exp_pa_bp"] += 1
                order += 1
                exp_bf_bp -= 1
            h_df.loc[lefties, "exp_ps_bp"] = h_df["exp_pa_bp"] * (
                (bp["fd_wpa_b_vl"].mean() * settings.P_PTS_WEIGHT) + (h_df["fd_wps_pa"] * settings.H_PTS_WEIGHT)
            )
            h_df.loc[righties, "exp_ps_bp"] = h_df["exp_pa_bp"] * (
                (bp["fd_wpa_b_vr"].mean() * settings.P_PTS_WEIGHT) + (h_df["fd_wps_pa"] * settings.H_PTS_WEIGHT)
            )

            # h_df['exp_ps_bp'] = h_df['exp_pa_bp'] * (((bp['fd_wpa_b_rp'].mean() * 1) + (h_df['fd_wps_pa'] * 1)) / 2)
            h_df["exp_ps_raw"] = h_df["exp_ps_sp"] + h_df["exp_ps_bp"]
            h_df.loc[lefties, "venue_points"] = (
                h_df["exp_ps_raw"] * self.next_venue_boost_lhb
            ) - h_df["exp_ps_raw"]
            h_df.loc[righties, "venue_points"] = (
                h_df["exp_ps_raw"] * self.next_venue_boost_rhb
            ) - h_df["exp_ps_raw"]
            h_df = calculate_team_lu_points(self, h_df)
            # self.raw_points = h_df["exp_ps_raw"].sum()
            # self.temp_points = (self.raw_points * self.temp_boost) - self.raw_points
            # self.ump_points = (self.raw_points * self.ump_boost) - self.raw_points
            # h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp']
            
            # self.venue_points = h_df["venue_points"].sum()
            # self.points = (
            #     self.venue_points + self.temp_points + self.ump_points + self.raw_points
            # )
            # self.venue_points = (self.raw_points * self.next_venue_boost) - self.raw_points

            h_df['temp_points'] = (h_df['exp_ps_raw'] * self.temp_boost) - h_df['exp_ps_raw']
            h_df['ump_points'] = (h_df['exp_ps_raw'] * self.ump_boost) - h_df['exp_ps_raw']
            h_df['points'] = h_df['venue_points'] + h_df['temp_points'] + h_df['ump_points'] + h_df['exp_ps_raw']
            pickle_dump(h_df, file)

        else:
            h_df = pd.read_pickle(path)
            lefties = Team.get_split_filter(h_df, "L")
            righties = Team.get_split_filter(h_df, "R")
            h_df.loc[lefties, "venue_points"] = (
                h_df["exp_ps_raw"] * self.next_venue_boost_lhb
            ) - h_df["exp_ps_raw"]
            h_df.loc[righties, "venue_points"] = (
                h_df["exp_ps_raw"] * self.next_venue_boost_rhb
            ) - h_df["exp_ps_raw"]
            h_df["temp_points"] = (h_df["exp_ps_raw"] * self.temp_boost) - h_df[
                "exp_ps_raw"
            ]
            h_df["ump_points"] = (h_df["exp_ps_raw"] * self.ump_boost) - h_df[
                "exp_ps_raw"
            ]
            h_df["points"] = (
                h_df["venue_points"]
                + h_df["temp_points"]
                + h_df["ump_points"]
                + h_df["exp_ps_raw"]
            )
            # h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp'] + h_df['exp_ps_bp']
            # self.raw_points = h_df["exp_ps_raw"].sum()
            # self.venue_points = h_df["venue_points"].sum()
            # self.temp_points = (self.raw_points * self.temp_boost) - self.raw_points
            # self.ump_points = (self.raw_points * self.ump_boost) - self.raw_points
            # self.points = (
            #     self.venue_points + self.temp_points + self.ump_points + self.raw_points
            # )
            # self.sp_mu = h_df["sp_mu"].sum()
            # self.lu_talent_sp = h_df["sp_split"].sum() - h_df["sp_split"].std(ddof=0)
            # h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp']
            h_df = calculate_team_lu_points(self, h_df)
        return h_df
    
        
        

    def sp_df(self):
        daily_info = Team.daily_info()
        projected_sp = self.projected_sp()
        slug = projected_sp["nameSlug"]
        file = pickle_path(name=f"{slug}_{tf.today}", directory=settings.SP_DIR)
        path = settings.SP_DIR.joinpath(file)
        if path.exists() and self.name not in settings.reset_specific_pitchers and not settings.reset_all_pitchers:
            p_df = pd.read_pickle(path)
            p_df["venue_points"] = (
                p_df["exp_ps_raw"] * (-self.next_venue_boost % 2)
            ) - p_df["exp_ps_raw"]
            p_df["temp_points"] = (p_df["exp_ps_raw"] * (-self.temp_boost % 2)) - p_df[
                "exp_ps_raw"
            ]
            p_df["ump_points"] = (p_df["exp_ps_raw"] * (-self.ump_boost % 2)) - p_df[
                "exp_ps_raw"
            ]
            p_df["points"] = (
                p_df["exp_ps_raw"]
                + p_df["venue_points"]
                + p_df["temp_points"]
                + p_df["ump_points"]
            )
            p_df["env_points"] = self.env_avg
            return p_df
        try:
            p_info = projected_sp
            player = api_pitcher_info_dict(p_info)
            player["team"] = self.name
            p_df = Team.join_player_stats(pd.DataFrame([player]), hitters=False)
            
        except TypeError:
            print("Getting default SP stats.")
            p_df = Team.default_sp()
        h_df = self.opp_instance.lineup_df()
        p_df["opponent"] = self.opp_instance.name
        p_df["exp_inn"] = h_df["sp_exp_inn"].max()
        p_df["k_pred"] = (h_df["exp_k"].sum() - h_df["exp_k"].std(ddof=0))
        p_df["k_pred_raw"] = h_df["exp_k_sp_raw"].sum() - h_df["exp_k_sp_raw"].std(
            ddof=0
        )
        p_df["exp_ps_raw"] = (
            h_df["exp_pc_sp"].sum() - h_df["exp_pc_sp"].std(ddof=0)
        )
        p_df["mu"] = h_df["exp_pc_sp"].sum() - h_df["exp_pc_sp"].std(ddof=0)
        p_df["raw_mu"] = h_df["raw_exp_pc_sp"].sum() - h_df["raw_exp_pc_sp"].std(ddof=0)
        
        
        

        p_df['venue_points'] = (p_df['exp_ps_raw'] * (-self.next_venue_boost % 2)) - p_df['exp_ps_raw']
        p_df['temp_points'] = (p_df['exp_ps_raw'] * (-self.temp_boost % 2)) - p_df['exp_ps_raw']
        p_df['ump_points'] = (p_df['exp_ps_raw'] * (-self.ump_boost % 2)) - p_df['exp_ps_raw']
        p_df['points'] = p_df['exp_ps_raw'] + p_df['venue_points'] + p_df['temp_points'] + p_df['ump_points']
        p_df['ump_avg'] = self.ump_avg
        p_df['venue_temp'] = self.venue_temp
        p_df['venue_avg'] = self.venue_avg
        p_df['env_points'] = self.env_avg
        
        if self.name in daily_info['confirmed_sp']:
            pickle_dump(p_df, file)

        return p_df

    def live_game(self):
        try:
            game = next(
                x
                for x in self.all_games
                if x["games"][0]["status"]["codedGameState"] in mac.game_status.live
            )["games"][0]
            game_pk = game["gamePk"]
            print(game_pk)
            game_box = Team.get_game_api(game_pk)
        except (StopIteration, TypeError):
            return f"The {self.name.capitalize()} are not currently playing."
        all_plays = game_box["liveData"]["plays"]["allPlays"]
        scoring_plays = game_box["liveData"]["plays"]["scoringPlays"]  # integer_list
        current_play = game_box["liveData"]["plays"]["currentPlay"]
        home_name = game_box["gameData"]["teams"]["home"]["teamName"]
        away_name = game_box["gameData"]["teams"]["away"]["teamName"]
        game_line = game_box["liveData"]["linescore"]
        home_score = game_line["teams"]["home"]["runs"]
        away_score = game_line["teams"]["away"]["runs"]
        inning_pre = game_line["inningHalf"]
        current_inning = game_line["currentInning"]
        home_hits = game_line["teams"]["home"]["hits"]
        away_hits = game_line["teams"]["away"]["hits"]
        total_hits = home_hits + away_hits
        if not scoring_plays:
            pass
        else:
            print("All Scoring Plays:")
            for play in all_plays:
                play_index = play.get("atBatIndex", 1000)
                if play_index > scoring_plays[-1]:
                    break
                elif play_index in scoring_plays:
                    score_event = play.get("result", {}).get(
                        "description", "No description."
                    )
                    print("-" + re.sub(" +", " ", score_event))
        current_batter = current_play["matchup"]["batter"]["fullName"]
        current_pitcher = current_play["matchup"]["pitcher"]["fullName"]
        runners = current_play["runners"]
        while not runners:
            try:
                flag = -2
                runners = all_plays[flag]["runners"]
                flag -= 1
            except IndexError:
                break
        count = current_play["count"]
        outs = count["outs"]
        out_string = plural(outs, "out")
        strikes = count["strikes"]
        balls = count["balls"]
        print(f"At bat: {current_batter}")
        print(f"Pitching: {current_pitcher}")
        print(f"Count: {balls}-{strikes}, {outs} {out_string}")
        if runners:
            runner_d = {}
            for runner in runners:
                runner_name = runner["details"]["runner"]["fullName"]
                runner_base = runner["movement"]["end"]
                runner_d[runner_name] = runner_base
            for k, v in runner_d.items():
                if v:
                    if str(v) == "score":
                        print(f"{k} {v}d.")
                    else:
                        print((f"{k} is on {v}."))
        print(f"{away_name}: {away_score}")
        print(f"{home_name}: {home_score}")
        print(f"{inning_pre} {current_inning}")
        print(f"Total hits: {total_hits}")
        try:
            print("Last update:")
            last_event = next(
                x for x in reversed(all_plays) if x["result"].get("description")
            )["result"]["description"]
            print(re.sub(" +", " ", last_event))

        except StopIteration:
            pass
        return None

    @cached_property
    def is_new_series(self):
        if self.next_game_pk:
            return self.future_games[0]["games"][0]["seriesGameNumber"] == 1
        return self.next_game_pk

    @cached_property
    def projected_ump_id(self):
        if not self.is_new_series:
            return Team.get_official(self.last_game, "First Base")
        elif self.next_game["liveData"]["boxscore"]["officials"]:
            return Team.get_official(self.next_game, "Home Plate")
        else:
            return "Unable to project ump."

    @cached_property
    def projected_ump_data(self):
        return game_data[game_data["umpire"] == self.projected_ump_id]

    @cached_property
    def next_venue(self):
        if self.next_game_pk:
            v_id = self.next_game["gameData"]["venue"]["id"]
            return v_id
        return self.next_game_pk

    @cached_property
    def last_venue(self):
        if self.last_game_pk:
            return self.last_game["gameData"]["venue"]["id"]
        return self.last_game_pk

    @cached_property
    def home_venue(self):
        return team_info[self.name]["venue"]["id"]

    @cached_property
    def next_venue_data(self):
        if self.next_game_pk:
            # return venue_data.loc[self.next_venue]
            return game_data[game_data["venue_id"] == self.next_venue]
        return self.next_game_pk

    @cached_property
    def last_venue_data(self):
        if self.last_game_pk:
            return game_data[game_data["venue_id"] == self.next_venue]
        return self.next_game_pk

    @cached_property
    def home_venue_data(self):
        return game_data[game_data["venue_id"] == self.home_venue]

    @cached_property
    def weather(self):
        if self.next_game_pk:
            return self.next_game["gameData"]["weather"]
        return self.next_game_pk

    @cached_property
    def wind_speed(self):
        if self.custom_wind_speed:
            return self.custom_wind_speed
        if self.roof_closed:
            return 0
        elif not self.next_has_roof:
            avg_wind = self.next_venue_data["wind_speed"].mean()
        else:
            avg_wind = 0
        if self.weather:
            wind_description = self.weather.get("wind")
            if wind_description:
                try:
                    return int(wind_description.split(" ")[0])
                except (TypeError, ValueError):
                    if not np.isnan(avg_wind):
                        return avg_wind
                    return 0
            else:
                if not np.isnan(avg_wind):
                    return avg_wind
                return 0
        else:
            if not np.isnan(avg_wind):
                return avg_wind
            return 0

    @cached_property
    def wind_direction(self):
        if self.custom_wind_direction:
            return self.custom_wind_direction
        wind_direction = ""
        if self.weather:
            wind_description = self.weather.get("wind")
            if wind_description:
                return wind_description.split(", ")[-1]
        return wind_direction

    @cached_property
    def venue_condition(self):
        condition = ""
        if self.weather:
            condition = self.weather.get("condition")
            if condition in mac.weather.rain:

                daily_info = Team.daily_info()
                if self.name not in daily_info["rain"]:
                    daily_info["rain"].append(self.name)
                    Team.dump_json_data(settings.daily_info_file, daily_info)
        return condition

    @cached_property
    def venue_temp(self):
        if self.custom_temp:
            return self.custom_temp
        if self.weather:
            temp = self.weather.get("temp")
            if temp and not self.roof_closed:
                try:
                    return int(temp)
                except ValueError:
                    return self.next_venue_data["temp"].median()
        if self.roof_closed:
            return 72
        return self.next_venue_data["temp"].median()

    @cached_property
    def roof_closed(self):
        return (
            self.next_has_roof and not self.venue_condition
        ) or self.venue_condition in mac.weather.roof_closed

    @cached_property
    def home_has_roof(self):
        return any(
            i in mac.weather.roof_closed
            for i in self.home_venue_data["condition"].unique()
        )

    @cached_property
    def next_has_roof(self):
        return any(
            i in mac.weather.roof_closed
            for i in self.next_venue_data["condition"].unique()
        )

    @cached_property
    def last_has_roof(self):
        return any(
            i in mac.weather.roof_closed
            for i in self.last_venue_data["condition"].unique()
        )

    @cached_property
    def no_rest(self):
        if self.last_game_pk:
            date = self.last_game["gameData"]["datetime"]["originalDate"]
            if datetime.date.fromisoformat(date) >= tf.yesterday:
                return True
            return False
        return self.last_game_pk

    @cached_property
    def used_rp(self):
        if self.no_rest:
            return self.last_game["liveData"]["boxscore"]["teams"][self.was_ha][
                "pitchers"
            ][1:]
        return []

    @cached_property
    def used_rp_names(self):
        if self.used_rp:
            pitcher_df = self.bullpen[self.bullpen["mlb_id"].isin(self.used_rp)]
            return pitcher_df["name"]
        return None

    @cached_property
    def no_rest_travel(self):
        if self.no_rest and self.last_venue != self.next_venue:
            return True
        return False

    @cached_property
    def is_dh(self):
        return True
        # return self.next_game['gameData']['teams']['home']['league']['id'] == 103

    @cached_property
    def was_dh(self):
        return True
        # return self.last_game['gameData']['teams']['home']['league']['id'] == 103

    @cached_property
    def qualified_venues(self):
        return qualified_venue_stats(exclude=[self.next_venue])
    
    @cached_property
    def wind_out(self):
        return self.wind_direction in mac.weather.wind_out
    
    @cached_property
    def wind_in(self):
        return self.wind_direction in mac.weather.wind_in

    @cached_property
    def boost_data(self):
        if self.next_game_pk:
            data = self.next_venue_data
        else:
            data = self.home_venue_data
        return data

    @cached_property
    def wind_data(self):
        data = self.boost_data
        wind_in = data[data["wind_direction"].isin(mac.weather.wind_in)]
        wind_out = data[data["wind_direction"].isin(mac.weather.wind_out)]
        return wind_in, wind_out

    @cached_property
    def wind_in_data(self):
        return self.wind_data[0]
    
    @cached_property
    def wind_out_data(self):
        return self.wind_out_data[1]

    @cached_property
    def wind_in_or_out(self):
        if (settings.wind_factor and not self.roof_closed):
            return (self.wind_in or self.wind_out)
        return False

    @cached_property
    def sufficient_wind_data(self):
        sufficient = settings.MIN_GAMES_VENUE_WIND
        in_sufficient = len(self.wind_in_data.index) >= sufficient
        out_sufficient = len(self.wind_out_data.index) >= sufficient
        return all(in_sufficient, out_sufficient)

    @cached_property
    def logical_wind_data(self):
        wind_in_mean = self.wind_in_data["fd_points"].mean()
        wind_out_mean = self.wind_out_data["fd_points"].mean()
        return (wind_in_mean < wind_out_mean)

    @cached_property
    def sufficient_wind_speed(self):
        return (self.wind_speed >= (game_data["wind_speed"].median() - 1))

    @cached_property
    def wind_factor(self):
        is_factor = all(
                        self.wind_in_or_out,
                        self.sufficient_wind_data,
                        self.logical_wind_data,
                        self.sufficient_wind_speed
                        )
        return is_factor

    @cached_property
    def next_venue_boost_data(self):
        data = self.boost_data
        
        if self.wind_factor:
            if self.wind_out:
                data = self.wind_out_data
            elif self.wind_in:
                data = self.wind_in_data
        return data

    @cached_property
    def next_venue_boost(self):
        data = self.next_venue_boost_data
        q_venues = self.qualified_venues
        pts_pa_next_venue = data["fd_points"].sum() / data["adj_pa"].sum()
        pts_pa_q_venues = q_venues["fd_pts_pa"].mean()
        lhb_pts_pa_next_venue = data["fd_points_lhb"].sum() / data["adj_pa_lhb"].sum()
        lhb_pts_pa_q_venues = q_venues["fd_pts_pa_lhb"].mean()
        rhb_pts_pa_next_venue = data["fd_points_rhb"].sum() / data["adj_pa_rhb"].sum()
        rhb_pts_pa_q_venues = q_venues["fd_pts_pa_rhb"].mean()
        self.next_venue_boost_lhb = lhb_pts_pa_next_venue / lhb_pts_pa_q_venues
        self.next_venue_boost_rhb = rhb_pts_pa_next_venue / rhb_pts_pa_q_venues
        return pts_pa_next_venue / pts_pa_q_venues

    # @cached_property
    # def next_venue_boost_lhb(self):
    #     q_venues = self.qualified_venues
    #     data = self.next_venue_boost_data
    #     lhb_pts_pa_next_venue = data["fd_points_lhb"].sum() / data["adj_pa_lhb"].sum()
    #     lhb_pts_pa_q_venues = q_venues["fd_pts_pa_lhb"].mean()
    #     return lhb_pts_pa_next_venue / lhb_pts_pa_q_venues

    # @cached_property
    # def next_venue_boost_rhb(self):
    #     q_venues = self.qualified_venues
    #     data = self.next_venue_boost_data
    #     rhb_pts_pa_next_venue = data["fd_points_rhb"].sum() / data["adj_pa_rhb"].sum()
    #     rhb_pts_pa_q_venues = q_venues["fd_pts_pa_rhb"].mean()
    #     return rhb_pts_pa_next_venue / rhb_pts_pa_q_venues

    @cached_property
    def venue_avg(self):
        if self.wind_factor:
            if self.wind_out:
                return self.wind_out_data["fd_points"].mean()
            if self.wind_in:
                return self.wind_in_data["fd_points"].mean()
        return self.next_venue_data["fd_points"].mean()
    
    @cached_property
    def temp_avg(self):
        if self.venue_temp:
            count = 0
            mult = 0.01
            temp = self.venue_temp
            data = game_data[game_data["condition"].isin(mac.weather.roof_open)]
            if len(data.index) < 10000:
                stop = len(data.index) * 0.1
            else:
                stop = 1000
            while count < stop:
                df = data[data['temp'].between(temp - mult, temp + mult, inclusive='neither')]
                count = len(df.index)
                mult += 1
            return df['fd_points'].mean()
        return game_data['fd_points'].mean()

    @cached_property
    def temp_boost(self):
        if self.venue_temp:
            return self.temp_avg / game_data["fd_points"].mean()
        return 1

    @cached_property
    def ump_boost(self):
        if len(self.projected_ump_data.index) >= settings.MIN_UMP_SAMP:
            return (
                self.projected_ump_data["fd_points"].mean()
                / game_data["fd_points"].mean()
            )
        return 1

    @cached_property
    def ump_avg(self):
        if len(self.projected_ump_data.index) >= settings.MIN_UMP_SAMP:
            return self.projected_ump_data["fd_points"].mean()
        return game_data["fd_points"].mean()

    @cached_property
    def env_avg(self):
        temp = self.temp_avg
        ump = self.ump_avg
        venue = self.venue_avg
        if self.roof_closed:
            sample = game_data[game_data["condition"].isin(mac.weather.roof_closed)]
            temp = sample["fd_points"].mean()
        return ((temp * settings.ENV_TEMP_WEIGHT) + 
                (venue * settings.ENV_VENUE_WEIGHT) + 
                (ump * settings.ENV_UMP_WEIGHT))

    def sp_avg(self, return_full_dict=False):
        away = game_data[(game_data["away_sp"] == self.opp_sp["id"])]
        home = game_data[(game_data["home_sp"] == self.opp_sp["id"])]
        if len(away.index) > 0:
            away_score = away["home_score"].mean()
            away_hits = away["home_hits"].mean()
            if np.isnan(away_score):
                away_score = game_data["home_score"].median()
            if np.isnan(away_hits):
                away_hits = game_data["home_hits"].median()
        else:
            away_score = game_data["home_score"].median()
            away_hits = game_data["home_hits"].median()
        if len(home.index) > 0:
            home_score = home["away_score"].mean()
            home_hits = home["away_hits"].mean()
            if np.isnan(home_score):
                home_score = game_data["away_score"].median()
            if np.isnan(home_hits):
                home_hits = game_data["away_hits"].median()
        else:
            home_score = game_data["away_score"].median()
            home_hits = game_data["away_hits"].median()

        total_score = (away_score + home_score) / 2
        total_hits = (away_hits + home_hits) / 2
        home_fd_points = (home_score * 6.7) + (home_hits * 3)
        away_fd_points = (away_score * 6.7) + (away_hits * 3)
        total_fd_points = (total_score * 6.7) + (total_hits * 3)

        sp_information = {
            "avg_home_score": home_score,
            "avg_away_score": away_score,
            "avg_home_hits": home_hits,
            "avg_away_hits": away_hits,
            "sample_home": len(home.index),
            "sample_away": len(away.index),
            "avg_hits": total_hits,
            "avg_score": total_score,
            "home_fd": home_fd_points,
            "away_fd": away_fd_points,
            "total_fd": total_fd_points,
            "pitcher": self.opp_sp["fullName"],
        }
        if return_full_dict:
            return sp_information
        if self.is_home:
            return sp_information["away_fd"]
        else:
            return sp_information["home_fd"]

    @cached_property
    def opp_bullpen(self):
        file = pickle_path(
            name=f"{self.opp_name}_bp_{tf.today}", directory=settings.BP_DIR
        )
        path = settings.BP_DIR.joinpath(file)
        if path.exists():
            bp = pd.read_pickle(path)
            return bp
        bp = self.opp_instance.bullpen
        bp = bp[(~bp["mlb_id"].isin(self.opp_instance.used_rp)) & (bp["status"] == "A")]
        if len(bp.index) < settings.RESTED_BP_SAMP:
            bp = self.opp_instance.bullpen

            bp = bp[(bp['status'] == 'A')]
            bp['fd_wpa_b_rp'] = bp['fd_wpa_b_rp'] * settings.TIRED_BP_INCREASE
            bp['fd_wpa_b_vr'] = bp['fd_wpa_b_vr'] * settings.TIRED_BP_INCREASE
            bp['fd_wpa_b_vl'] = bp['fd_wpa_b_vl'] * settings.TIRED_BP_INCREASE
        pickle_dump(bp, file)
        return bp

    @cached_property
    def rested_bullpen(self):
        file = pickle_path(name=f"{self.name}_bp_{tf.today}", directory=settings.BP_DIR)
        path = settings.BP_DIR.joinpath(file)
        if path.exists():
            bullpen = pd.read_pickle(path)
        else:
            bullpen = self.bullpen
        return bullpen

    @cached_property
    def proj_opp_bp(self):
        return self.opp_bullpen.loc[
            self.opp_bullpen["batters_faced_rp"].nlargest(settings.RESTED_BP_SAMP).index
        ]

    @cached_property
    def opp_instance(self):
        for team in Team:
            if team.name == self.opp_name:
                return team
              
    def get_past_games(self, number_of_games):
        return self.past_games[-number_of_games:]

    def rested_sp(self, return_used_ids=False):
        if self.last_game_pk:
            try:
                games = self.get_past_games(4)
            except IndexError:
                games = self.past_games
            game_ids = []
            for game in games:
                game_ids.append(Team.get_game_pk(game))
            game_ids = ids_string(game_ids)
            call = statsapi.get(
                "schedule",
                {"gamePks": game_ids, "sportId": 1, "hydrate": "probablePitcher"},
            )
            recent_sp = set()
            for g in call["dates"]:
                home_sp = (
                    g["games"][0]["teams"]["home"].get("probablePitcher", {}).get("id")
                )
                away_sp = (
                    g["games"][0]["teams"]["away"].get("probablePitcher", {}).get("id")
                )
                recent_sp.add(home_sp)
                recent_sp.add(away_sp)

            starters = self.pitcher[
                (self.pitcher["batters_faced_sp"] > 0) & (self.pitcher["status"] == "A")
            ]
            if return_used_ids:
                return recent_sp

            return starters[~starters["mlb_id"].isin(recent_sp)]
        return set()

    def del_props(self):
        try:
            del self.next_game
        except AttributeError:
            pass
        try:
            del self.lineup
        except AttributeError:
            pass

        return None

    def del_next_game(self):
        try:
            del self.next_game
        except AttributeError:
            pass
        return None

    def cache_next_game(self):
        file = pickle_path(
            name=f"{self.name}_next_game_{tf.today}", directory=settings.GAME_DIR
        )
        path = settings.GAME_DIR.joinpath(file)
        ## TODO delete the file at some point
        if not path.exists() and self.weather:
            daily_info = Team.daily_info()
            name = self.name
            if name in daily_info["confirmed_lu"] and name in daily_info["confirmed_sp"]:
                game = self.next_game
                pickle_dump(game, file)
                print(f"Cached {self.name}' next game.")
        return None

    def clear_team_cache(self, directories=settings.VITAL_DIR_LIST):
        for d in directories:
            for file in Path.iterdir(d):
                if not file.is_dir() and self.name in str(file):
                    file.unlink()
        return f"Cleared cache for {self.name}."

    def clear_all_team_cache(self):
        self.clear_team_cache(directories=settings.VITAL_DIR_LIST)
        return f"Cleared vital directories for {self.name}."

if __name__ == "__main__":
    angels = Team(mlb_id = 108, name = 'angels')
    astros = Team(mlb_id = 117, name = 'astros')
    athletics = Team(mlb_id = 133, name = 'athletics')
    blue_jays = Team(mlb_id = 141, name = 'blue jays')
    braves = Team(mlb_id = 144, name = 'braves')
    brewers = Team(mlb_id = 158, name = 'brewers')
    cardinals = Team(mlb_id = 138, name = 'cardinals')
    cubs = Team(mlb_id = 112, name = 'cubs')
    diamondbacks = Team(mlb_id = 109, name = 'diamondbacks')
    dodgers = Team(mlb_id = 119, name = 'dodgers')
    giants = Team(mlb_id = 137, name = 'giants')
    guardians = Team(mlb_id = 114, name = 'guardians')
    mariners = Team(mlb_id = 136, name = 'mariners')
    marlins = Team(mlb_id = 146, name = 'marlins')
    mets = Team(mlb_id = 121, name = 'mets')
    nationals = Team(mlb_id = 120, name = 'nationals') 
    orioles = Team(mlb_id = 110, name = 'orioles')
    phillies = Team(mlb_id = 143, name = 'phillies')
    pirates = Team(mlb_id = 134, name = 'pirates')
    padres = Team(mlb_id = 135, name = 'padres')
    rays = Team(mlb_id = 139, name = 'rays')
    rangers = Team(mlb_id = 140, name = 'rangers')
    red_sox = Team(mlb_id = 111, name = 'red sox')
    reds = Team(mlb_id = 113, name = 'reds')
    rockies = Team(mlb_id = 115, name = 'rockies')
    royals = Team(mlb_id = 118, name = 'royals')
    tigers = Team(mlb_id = 116, name = 'tigers')
    twins = Team(mlb_id = 142, name = 'twins')
    white_sox = Team(mlb_id = 145, name = 'white sox')
    yankees = Team(mlb_id = 147, name = 'yankees')
