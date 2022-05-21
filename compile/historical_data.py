from dfs_tools_mlb.utils.time import time_frames as tf
import datetime
from json import JSONDecodeError
import json
import statsapi
from dfs_tools_mlb.utils.storage import json_path,pickle_path
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
from pathlib import Path


def format_season(season):
    season_info = {
            'spring_start': season.get('preSeasonStartDate'),
            'reg_start': season['regularSeasonStartDate'],
            'playoff_start': season.get('postSeasonStartDate'),
            'second_half_start': season.get('firstDate2ndHalf'),
            'reg_end': season['regularSeasonEndDate'],
            'playoff_end': season.get('postSeasonEndDate'),
            'season_id': season['seasonId']
            }
    return season_info
#args used for starting at the last compiled date of current_season
def season_start_end(season, *args, start_from_last = False):
    months = mlb_months(int(season['season_id']))
    #error occurs when loading data for 2004-04-04 (missing 3 games total).
    if int(season['season_id']) == 2004:
        start = '2004-04-05'
    else:
        start = season['reg_start']
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
    end = season['reg_end']
    #covid season began 07-23-2020
    if int(season['season_id']) == 2020:
        periods = {
        start:july_end,
        august_start: august_end,
        september_start: end
        }
    #strike ended season on 1994-08-11
    elif int(season['season_id']) == 1994:
        periods = {
            start:april_end,
            may_start: may_end,
            june_start: june_end,
            july_start: july_end,
            august_start: end,
            }
    else:
        periods = {
            start:april_end,
            may_start: may_end,
            june_start: june_end,
            july_start: july_end,
            august_start: august_end,
            september_start: end
            }
        
    if int(season['season_id']) == int(current_season()['season_id']) and start_from_last and args:
           adj_m = {x[0]:x[1] for x in months.values() if datetime.date.fromisoformat(x[1]).month >= datetime.date.fromisoformat(args[0]).month}
           months_adj = {args[0] if k == min(adj_m.keys()) else k:v for k,v in adj_m.items()}
           return months_adj
       
    return periods

@lru_cache
def current_season():
    path = json_path(name='current_season')
    try:
        with open(path) as file:
            season_info = json.load(file)
            file.close()
        if str(season_info['season_id']) != str(tf.today.year) and not settings.OFFSEASON_TESTING:
            raise KeyError('Must update info for current season.')
        return season_info
    except (FileNotFoundError, JSONDecodeError, KeyError):
        season = statsapi.get('season',{'seasonId':tf.today.year,'sportId':1})['seasons'][0]
        season_info = format_season(season)
        with open(path, "w+") as file:
            json.dump(season_info, file)
            file.close()
        return season_info
    
def past_seasons(seasons=range(2010,int(current_season()['season_id'])),path=json_path(name='past_seasons')):
    try:
        with open(path) as file:
            season_list = json.load(file)
            file.close()
        if int(season_list[-1]['season_id']) < int(seasons[-1]):
            for year in range(int(season_list[-1]['season_id']) + 1, int(seasons[-1]) + 1):
                season = statsapi.get('season',{'seasonId':year,'sportId':1})['seasons'][0]
                season_info = format_season(season)
                season_list.append(season_info)
            with open(path, "w+") as file:
                json.dump(season_list, file)
                file.close()
        if int(season_list[0]['season_id']) > int(seasons[0]):
            flag = 0
            for year in range(seasons[0], int(season_list[0]['season_id'])):
                season = statsapi.get('season',{'seasonId':year,'sportId':1})['seasons'][0]
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
            season = statsapi.get('season',{'seasonId':year,'sportId':1})['seasons'][0]
            season_info = format_season(season)
            season_list.append(season_info)
        with open(path, "w+") as file:
             json.dump(season_list, file)
             file.close()
        print(f"Saved season info. for {seasons[0]}-{seasons[-1]} to {path}.")
        return season_list
    
#get historical data for a given season, pickled as list of dictionaries.
#use a for loop for multiple years.
def get_historical_data(year, extensive=True, from_scratch=False):
                    gc.collect()
                    path=pickle_path('historical_data' + '_' + str(year))
                    if Path(path).resolve().exists() and not from_scratch:
                        with open(path, 'rb') as f:
                           games = pickle.load(f) 
                        if int(year) != int(current_season()['season_id']):
                            return games
                    else:
                        games = []
                    fields = settings.api_fields['get_historical_data']
                    hydrate = settings.api_hydrate['get_historical_data']
                    try:
                        season = next(x for x in past_seasons() if str(x['season_id']) == str(year))
                    except StopIteration:
                        seasons = past_seasons(seasons=[year])
                        season = next(x for x in seasons if str(x['season_id']) == str(year))
                    if int(year) == int(current_season()['season_id']) and not from_scratch and games[-1].get('date'):
                        total_games = None
                        total_cached = None
                        last_date = games[-1]['date']
                        while not total_games or total_games != total_cached:
                            date_call = get_big('schedule', {'hydrate': hydrate,'sportId': 1, 'startDate': last_date, 'endDate': last_date, 'fields': fields})['dates']
                            # completed_games?
                            total_games = len(date_call[0]['games'])
                            total_cached = len([g for g in games if str(g['date']) == str(last_date)])
                            
                            if total_games == total_cached:
                                break
                            last_date = str(datetime.date.fromisoformat(last_date) - datetime.timedelta(days=1))
                        periods = season_start_end(season, last_date, start_from_last = True)

                    else:
                        periods = season_start_end(season)
                    for k, v in periods.items():
                        if datetime.date.fromisoformat(k) > tf.today:
                            break
                        if games:
                            cached_games = {g['game'] for g in games}
                        else:
                            cached_games = set()
                        games_info = get_big('schedule', {'hydrate': hydrate,'sportId': 1, 'startDate': k, 'endDate': v, 'fields': fields})['dates']
                        for x in games_info:
                            for y in x['games']:
                                if y['status']['codedGameState'] == "F":
                                    
                                    if y['gamePk'] in cached_games:
                                        continue
                                    
                                    try:
                                        cached_games.add(y['gamePk'])
                                        d = {}
                                        d['date'] = x['date']
                                        d['game'] = y['gamePk']
                                        home_team = y['linescore']['teams']['home']
                                        away_team = y['linescore']['teams']['away']
                                        d['runs'] = home_team['runs'] + away_team['runs']
                                        d['hits'] = home_team['hits'] + away_team['hits']
                                        d['home_score'] = home_team['runs']
                                        d['away_score'] = away_team['runs']
                                        d['home_hits'] = home_team['hits']
                                        d['away_hits'] = away_team['hits']
                                        d['venue_id'] = y['venue']['id']
                                        d['home_runs'] = 0
                                        for z in y['scoringPlays']:
                                            if z['result']['event'] == 'Home Run':
                                                d['home_runs'] += 1
                                        try:
                                            d['last_inning'] = y['linescore']['currentInning']
                                        except KeyError:
                                            pass
                                        try:
                                            d['home_sp'] = y['teams']['home']['probablePitcher']['id']
                                        except KeyError:
                                            pass
                                        try:
                                            d['away_sp'] = y['teams']['away']['probablePitcher']['id']
                                        except KeyError:
                                            pass
                                        try:
                                            wind_description = y['weather']['wind']
                                            d['wind_speed'] = wind_description.split(' ')[0]
                                            d['wind_direction'] = wind_description.split(', ')[-1]
                                        except KeyError:
                                            pass
                                        try:
                                            d['temp'] = y['weather']['temp']
                                        except KeyError:
                                            pass
                                        try:
                                            d['series_game'] = y['seriesGameNumber']
                                        except KeyError:
                                            pass
                                        umpires = y['officials']
                                        try:
                                            ump_id = next(x['official']['id'] for x in umpires if x['officialType'] == 'Home Plate')
                                            d['umpire'] = ump_id
                                        except (StopIteration, KeyError):
                                            pass
                                        try:
                                            d['condition'] = y['weather']['condition']
                                        except KeyError:
                                            pass
                                        try:
                                            d['day_night'] = y['dayNight']
                                        except KeyError:
                                            pass
                                       
                                        if extensive:
                                            try:
                                                game_call = statsapi.get('game_playByPlay', {'gamePk': d['game']})
                                                plays = game_call['allPlays']
                                                conclusive_events = [x['result']['eventType'] for x in plays]
                                                result_counts = Counter(conclusive_events)
                                                d.update(result_counts)
                                                total_rbi = sum([x['result'].get('rbi', 0) for x in plays])
                                                d['rbi'] = total_rbi
                                                try:
                                                    first_play = game_call['allPlays'][0]['about']['startTime']
                                                    d['start_time'] = datetime.datetime.strptime(first_play, '%Y-%m-%dT%H:%M:%S.%fZ')
                                                except (KeyError, IndexError):
                                                    pass
                                                batter_splits = [x['matchup']['splits']['pitcher'] for x in plays]
                                                split_flag = 0
                                                for z in conclusive_events:
                                                    split_str = f"{batter_splits[split_flag]}_{z}"
                                                    if not d.get(split_str):
                                                        d[split_str] = 1
                                                    else: 
                                                        d[split_str] += 1
                                                    split_flag += 1
                                            except KeyError:
                                                pass
                                        games.append(d)
                                    except KeyError:
                                        continue
                    with open(path, "wb") as file:
                        pickle.dump(games, file)
                        file.close()
                    # return f"{path}"   
                    return games

#pass range(2000,2005) to pickle those years in ARCHIVE_DIR/historical_data_2000-2004.pickle
#if not already done, compile stats for those years calling historical_data(year)
def historical_data(start, end=None):
    if not end:
        end = start + 1
    if start > end:
        raise ValueError('Start cannot be after end.')
    season_range = range(start,end)
    dir_path = settings.ARCHIVE_DIR
    if len(season_range) == 1:
        file_path = dir_path.joinpath(pickle_path(name = 'historical_data' + '_' + str(season_range[0])))
        try:
            if int(season_range[0]) != int(current_season()['season_id']):
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
            file_path = dir_path.joinpath(pickle_path(name = 'historical_data' + '_' + str(year)))
            if not file_path.exists() or year == int(current_season()['season_id']) and not settings.OFFSEASON_TESTING:
                get_historical_data(year)
        season_data = []
        files = [file for file in sorted(os.listdir(dir_path)) if re.search('[historical_data][0-9]{4}[.][pickle]', file) and int(file.split('_')[-1].split('.')[0]) in season_range]
        for file in files:
            file_path = dir_path.joinpath(file)
            with open(file_path, "rb") as f:
                season = pd.read_pickle(f)
                f.close()
            season_data.extend(season)
        prefix = files[0].split('_')[-1].split('.')[0]
        suffix = files[-1].split('_')[-1].split('.')[0]
        data_path = pickle_path(name = 'historical_data' + '_' + prefix + '-' + suffix)
        with open(data_path,"wb") as file:
            pickle.dump(season_data, file)
            file.close()
        print(f"Data saved at {data_path}.")
        df = pd.DataFrame(season_data)
        return df