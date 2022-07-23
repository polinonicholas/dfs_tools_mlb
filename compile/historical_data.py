from dfs_tools_mlb.utils.time import time_frames as tf
import datetime
from json import JSONDecodeError
import json
import statsapi
from dfs_tools_mlb.utils.storage import json_path, pickle_path
from functools import lru_cache
import pickle
from dfs_tools_mlb.utils.time import mlb_months
import gc
from dfs_tools_mlb.utils.statsapi import get_big
import os
from dfs_tools_mlb import settings
import re
import pandas as pd
from collections import Counter


def format_season(season):
    season_info = {
        "spring_start": season.get("preSeasonStartDate"),
        "reg_start": season["regularSeasonStartDate"],
        "playoff_start": season.get("postSeasonStartDate"),
        "second_half_start": season.get("firstDate2ndHalf"),
        "reg_end": season["regularSeasonEndDate"],
        "playoff_end": season.get("postSeasonEndDate"),
        "season_id": season["seasonId"],
    }
    return season_info


# args used for starting at the last compiled date of current_season
def season_start_end(season, *args, start_from_last=False):
    months = mlb_months(int(season["season_id"]))
    # error occurs when loading data for 2004-04-04 (missing 3 games total).
    if int(season["season_id"]) == 2004:
        start = "2004-04-05"
    else:
        start = season["reg_start"]
    april_end = months[4][1]
    may_start = months[5][0]
    may_end = months[5][1]
    june_start = months[6][0]
    june_end = months[6][1]
    july_start = months[7][0]
    july_end = months[7][1]
    august_start = months[8][0]
    august_end = months[8][1]
    september_start = months[9][0]
    end = season["reg_end"]
    # covid season began 07-23-2020
    if int(season["season_id"]) == 2020:
        periods = {start: july_end, august_start: august_end, september_start: end}
    # strike ended season on 1994-08-11
    elif int(season["season_id"]) == 1994:
        periods = {
            start: april_end,
            may_start: may_end,
            june_start: june_end,
            july_start: july_end,
            august_start: end,
        }
    else:
        periods = {
            start: april_end,
            may_start: may_end,
            june_start: june_end,
            july_start: july_end,
            august_start: august_end,
            september_start: end,
        }

    if (
        int(season["season_id"]) == int(current_season()["season_id"])
        and start_from_last
        and args
    ):
        adj_m = {
            x[0]: x[1]
            for x in months.values()
            if datetime.date.fromisoformat(x[1]).month
            >= datetime.date.fromisoformat(args[0]).month
        }
        months_adj = {
            args[0] if k == min(adj_m.keys()) else k: v for k, v in adj_m.items()
        }
        return months_adj

    return periods


@lru_cache
def current_season():
    path = json_path(name="current_season")
    try:
        with open(path) as file:
            season_info = json.load(file)
            file.close()
        if (
            str(season_info["season_id"]) != str(tf.today.year)
            and not settings.OFFSEASON_TESTING
        ):
            raise KeyError("Must update info for current season.")
        return season_info
    except (FileNotFoundError, JSONDecodeError, KeyError):
        season = statsapi.get("season", {"seasonId": tf.today.year, "sportId": 1})[
            "seasons"
        ][0]
        season_info = format_season(season)
        with open(path, "w+") as file:
            json.dump(season_info, file)
            file.close()
        return season_info


def past_seasons(
    seasons=range(2010, int(current_season()["season_id"])),
    path=json_path(name="past_seasons"),
):
    try:
        with open(path) as file:
            season_list = json.load(file)
            file.close()
        if int(season_list[-1]["season_id"]) < int(seasons[-1]):
            for year in range(
                int(season_list[-1]["season_id"]) + 1, int(seasons[-1]) + 1
            ):
                season = statsapi.get("season", {"seasonId": year, "sportId": 1})[
                    "seasons"
                ][0]
                season_info = format_season(season)
                season_list.append(season_info)
            with open(path, "w+") as file:
                json.dump(season_list, file)
                file.close()
        if int(season_list[0]["season_id"]) > int(seasons[0]):
            flag = 0
            for year in range(seasons[0], int(season_list[0]["season_id"])):
                season = statsapi.get("season", {"seasonId": year, "sportId": 1})[
                    "seasons"
                ][0]
                season_info = format_season(season)
                season_list.insert(flag, season_info)
                flag += 1
            with open(path, "w+") as file:
                json.dump(season_list, file)
                file.close()
        return season_list
    except (FileNotFoundError, JSONDecodeError, KeyError):
        try:
            with open(path) as file:
                season_list = json.load(file)
                file.close()
        except (FileNotFoundError, JSONDecodeError):
            season_list = []
        for year in seasons:
            season = statsapi.get("season", {"seasonId": year, "sportId": 1})[
                "seasons"
            ][0]
            season_info = format_season(season)
            season_list.append(season_info)
        with open(path, "w+") as file:
            json.dump(season_list, file)
            file.close()
        print(f"Saved season info. for {seasons[0]}-{seasons[-1]} to {path}.")
        return season_list


# get historical data for a given season, pickled as list of dictionaries.
# use a for loop for multiple years.
def get_historical_data(year, extensive=True, create_new=False):
    gc.collect()
    games = []
    path = pickle_path("historical_data" + "_" + str(year))
    fields = settings.api_fields["get_historical_data"]
    hydrate = settings.api_hydrate["get_historical_data"]
    if not create_new:
        try:
            with open(path, "rb") as f:
                games = pickle.load(f)
            if not int(year) == int(current_season()["season_id"]):
                return games
        except (FileNotFoundError, PermissionError):
            pass
        except ValueError:
            print("Problem converting year and/or season_id to int.")
    try:
        season = next(x for x in past_seasons() if str(x["season_id"]) == str(year))
    except StopIteration:
        seasons = past_seasons(seasons=[year])
        season = next(x for x in seasons if str(x["season_id"]) == str(year))
    is_current_season = (
        (int(year) == int(current_season()["season_id"])) and not create_new and games
    )
    if is_current_season:
        total_games = None
        total_cached = None
        last_date = games[-1]["date"]
        while not total_games or total_games != total_cached:
            date_call = get_big(
                "schedule",
                {
                    "hydrate": hydrate,
                    "sportId": 1,
                    "startDate": last_date,
                    "endDate": last_date,
                    "fields": fields,
                },
            )["dates"]
            # completed_games?
            total_games = len(date_call[0]["games"])
            total_cached = len([g for g in games if g["date"] == last_date])
            if total_games == total_cached:
                break
            last_date = str(
                datetime.date.fromisoformat(last_date) - datetime.timedelta(days=1)
            )
        periods = season_start_end(season, last_date, start_from_last=True)
    else:
        periods = season_start_end(season)
    for start, end in periods.items():
        if datetime.date.fromisoformat(start) > tf.today:
            break
        if games:
            cached_games = {g["game"] for g in games}
        else:
            cached_games = set()
        games_info = get_big(
            "schedule",
            {
                "hydrate": hydrate,
                "sportId": 1,
                "startDate": start,
                "endDate": end,
                "fields": fields,
            },
        )["dates"]
        for game in games_info:
            for game_detail in game["games"]:
                if game_detail["status"]["codedGameState"] == "F":

                    if game_detail["gamePk"] in cached_games:
                        continue
                    try:
                        cached_games.add(game_detail["gamePk"])
                        formatted_game_dict = {}
                        formatted_game_dict["date"] = game["date"]
                        formatted_game_dict["game"] = game_detail["gamePk"]
                        home_team = game_detail["linescore"]["teams"]["home"]
                        away_team = game_detail["linescore"]["teams"]["away"]
                        formatted_game_dict["runs"] = (
                            home_team["runs"] + away_team["runs"]
                        )
                        formatted_game_dict["hits"] = (
                            home_team["hits"] + away_team["hits"]
                        )
                        formatted_game_dict["home_score"] = home_team["runs"]
                        formatted_game_dict["away_score"] = away_team["runs"]
                        formatted_game_dict["home_hits"] = home_team["hits"]
                        formatted_game_dict["away_hits"] = away_team["hits"]
                        formatted_game_dict["venue_id"] = game_detail["venue"]["id"]
                        formatted_game_dict["home_runs"] = 0
                        for scores in game_detail["scoringPlays"]:
                            if scores["result"]["event"] == "Home Run":
                                formatted_game_dict["home_runs"] += 1
                        try:
                            formatted_game_dict["last_inning"] = game_detail[
                                "linescore"
                            ]["currentInning"]
                        except KeyError:
                            pass
                        try:
                            formatted_game_dict["home_sp"] = game_detail["teams"][
                                "home"
                            ]["probablePitcher"]["id"]
                        except KeyError:
                            pass
                        try:
                            formatted_game_dict["away_sp"] = game_detail["teams"][
                                "away"
                            ]["probablePitcher"]["id"]
                        except KeyError:
                            pass
                        try:
                            wind_description = game_detail["weather"]["wind"]
                            formatted_game_dict["wind_speed"] = wind_description.split(
                                " "
                            )[0]
                            formatted_game_dict[
                                "wind_direction"
                            ] = wind_description.split(", ")[-1]
                        except KeyError:
                            pass
                        try:
                            formatted_game_dict["temp"] = game_detail["weather"]["temp"]
                        except KeyError:
                            pass
                        try:
                            formatted_game_dict["series_game"] = game_detail[
                                "seriesGameNumber"
                            ]
                        except KeyError:
                            pass
                        umpires = game_detail["officials"]
                        try:
                            ump_id = next(
                                ump["official"]["id"]
                                for ump in umpires
                                if ump["officialType"] == "Home Plate"
                            )
                            formatted_game_dict["umpire"] = ump_id
                        except (StopIteration, KeyError):
                            pass
                        try:
                            formatted_game_dict["condition"] = game_detail["weather"][
                                "condition"
                            ]
                        except KeyError:
                            pass
                        try:
                            formatted_game_dict["day_night"] = game_detail["dayNight"]
                        except KeyError:
                            pass
                        if extensive:
                            try:
                                game_call = statsapi.get(
                                    "game_playByPlay",
                                    {"gamePk": formatted_game_dict["game"]},
                                )
                                plays = game_call["allPlays"]
                                conclusive_events = [
                                    game["result"]["eventType"] for game in plays
                                ]
                                result_counts = Counter(conclusive_events)
                                formatted_game_dict.update(result_counts)
                                total_rbi = sum(
                                    [game["result"].get("rbi", 0) for game in plays]
                                )
                                formatted_game_dict["rbi"] = total_rbi
                                try:
                                    first_play = game_call["allPlays"][0]["about"][
                                        "startTime"
                                    ]
                                    formatted_game_dict[
                                        "start_time"
                                    ] = datetime.datetime.strptime(
                                        first_play, "%Y-%m-%dT%H:%M:%S.%fZ"
                                    )
                                except (KeyError, IndexError):
                                    pass
                                batter_splits = [
                                    game["matchup"]["splits"]["pitcher"]
                                    for game in plays
                                ]
                                split_flag = 0
                                for conclusive_event in conclusive_events:
                                    split_str = f"{batter_splits[split_flag]}_{conclusive_event}"
                                    if not formatted_game_dict.get(split_str):
                                        formatted_game_dict[split_str] = 1
                                    else:
                                        formatted_game_dict[split_str] += 1
                                    split_flag += 1
                            except KeyError:
                                pass
                        games.append(formatted_game_dict)
                    except KeyError:
                        continue
    try:
        with open(path, "wb") as file:
            pickle.dump(games, file)
            file.close()
    except (FileNotFoundError, PermissionError):
        pass
    return games


# pass range(2000,2005) to pickle those years in ARCHIVE_DIR/historical_data_2000-2004.pickle
# if not already done, compile stats for those years calling historical_data(year)
def historical_data(start, end=None):
    if not end:
        end = start + 1
    if start > end:
        raise ValueError("Start cannot be after end.")
    season_range = range(start, end)
    dir_path = settings.ARCHIVE_DIR
    if len(season_range) == 1:
        file_path = dir_path.joinpath(
            pickle_path(name="historical_data" + "_" + str(season_range[0]))
        )
        try:
            if int(season_range[0]) != int(current_season()["season_id"]):
                with open(file_path, "rb") as f:
                    season = pd.read_pickle(f)
                    f.close()
                df = pd.DataFrame(season)
                return df
            else:
                get_historical_data(season_range[0])
                with open(file_path, "rb") as f:
                    season = pd.read_pickle(f)
                df = pd.DataFrame(season)
                return df
        except FileNotFoundError:
            get_historical_data(season_range[0])
            with open(file_path, "rb") as f:
                season = pd.read_pickle(f)
                f.close()
            df = pd.DataFrame(season)
            return df
    else:
        for year in season_range:
            file_path = dir_path.joinpath(
                pickle_path(name="historical_data" + "_" + str(year))
            )
            if (
                not file_path.exists()
                or year == int(current_season()["season_id"])
                and not settings.OFFSEASON_TESTING
            ):
                get_historical_data(year)
        season_data = []
        files = [
            file
            for file in sorted(os.listdir(dir_path))
            if re.search("[historical_data][0-9]{4}[.][pickle]", file)
            and int(file.split("_")[-1].split(".")[0]) in season_range
        ]
        for file in files:
            file_path = dir_path.joinpath(file)
            with open(file_path, "rb") as f:
                season = pd.read_pickle(f)
                f.close()
            season_data.extend(season)
        prefix = files[0].split("_")[-1].split(".")[0]
        suffix = files[-1].split("_")[-1].split(".")[0]
        data_path = pickle_path(name="historical_data" + "_" + prefix + "-" + suffix)
        with open(data_path, "wb") as file:
            pickle.dump(season_data, file)
            file.close()
        print(f"Data saved at {data_path}.")
        df = pd.DataFrame(season_data)
        return df
