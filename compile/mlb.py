# from dfs_tools_mlb.utils.subclass import Map
# from dfs_tools_mlb.utils.time import time_frames as tf
# import datetime
# from json import JSONDecodeError
# import json
# import statsapi
# from dfs_tools_mlb.utils.storage import json_path,pickle_path
# from functools import lru_cache
# import pickle
# from dfs_tools_mlb.utils.time import mlb_months
# import gc  
# from dfs_tools_mlb.utils.statsapi import get_big
# import os
# from dfs_tools_mlb import settings
# import re
# import pandas as pd
# from dfs_tools_mlb.utils.strings import ids_string

# team_info = Map({
#     'athletics': {
#         'full_name': 'oakland athletics',
#         'mlb_id': 133,
#         'abbreviations': ['oak'],
#         'location': 'oakland',
#         'venue': {'id': 10, 'name': 'Oakland Coliseum', 'link': '/api/v1/venues/10'}
#         },
#     'pirates': {
#         'full_name': 'pittsburgh pirates',
#         'mlb_id': 134,
#         'abbreviations': ['pit'],
#         'location': 'pittsburgh',
#         'venue': {'id': 31, 'name': 'PNC Park', 'link': '/api/v1/venues/31'}
#         },
#     'padres': {
#         'full_name': 'san diego padres',
#         'mlb_id': 135,
#         'abbreviations': ['sd', 'sdp', 'sdn'],
#         'location': 'san diego',
#         'venue': {'id': 2680, 'name': 'Petco Park', 'link': '/api/v1/venues/2680'}
#         },
#     'mariners': {
#         'full_name': 'seattle mariners',
#         'mlb_id': 136,
#         'abbreviations': ['sea'],
#         'location': 'seattle',
#         'venue': {'id': 680, 'name': 'T-Mobile Park', 'link': '/api/v1/venues/680'}
#         },
#     'giants': {
#         'full_name': 'san francisco giants',
#         'mlb_id': 137,
#         'abbreviations': ['sfg', 'sf', 'sfn'],
#         'location': 'san francisco',
#         'venue': {'id': 2395, 'name': 'Oracle Park', 'link': '/api/v1/venues/2395'}
#         },
#     'cardinals': {
#         'full_name': 'st. louis cardinals',
#         'mlb_id': 138,
#         'abbreviations': ['stl', 'sln'],
#         'location': 'st. louis',
#         'venue': {'id': 2889, 'name': 'Busch Stadium', 'link': '/api/v1/venues/2889'}
#         },
#     'rays': {
#         'full_name': 'tampa bay rays',
#         'mlb_id': 139,
#         'abbreviations': ['tb', 'tbr', 'tam', 'tba'],
#         'location': 'st. petersburg',
#         'venue': {'id': 12, 'name': 'Tropicana Field', 'link': '/api/v1/venues/12'}
#         },
#     'rangers': {
#         'full_name': 'texas rangers',
#         'mlb_id': 140,
#         'abbreviations': ['tx', 'tex'],
#         'location': 'arlington',
#         'venue': {'id': 5325, 'name': 'Globe Life Field', 'link': '/api/v1/venues/5325'}
#         },
#     'blue jays': {
#         'full_name': 'toronto blue jays',
#         'mlb_id': 141,
#         'abbreviations': ['tor', 'blue-jays'],
#         'location': 'toronto',
#         'venue': {'id': 14, 'name': 'Rogers Centre', 'link': '/api/v1/venues/14'}
#         },
#     'twins': {
#         'full_name': 'minnesota twins',
#         'mlb_id': 142,
#         'abbreviations': ['min'],
#         'location': 'minneapolis',
#         'venue': {'id': 3312, 'name': 'Target Field', 'link': '/api/v1/venues/3312'}
#         },
#     'phillies': {
#         'full_name': 'philadelphia phillies',
#         'mlb_id': 143,
#         'abbreviations': ['phi'],
#         'location': 'philadelphia',
#         'venue': {'id': 2681, 'name': 'Citizens Bank Park', 'link': '/api/v1/venues/2681'}
#         },
#     'braves': {
#         'full_name': 'atlanta braves',
#         'mlb_id': 144,
#         'abbreviations': ['atl'],
#         'location': 'atlanta',
#         'venue': {'id': 4705, 'name': 'Truist Park', 'link': '/api/v1/venues/4705'}
#         },
#     'white sox': {
#         'full_name': 'white sox',
#         'mlb_id': 145,
#         'abbreviations': ['cha', 'cws', 'chw', 'white-sox'],
#         'location': 'chicago',
#         'venue': {'id': 4, 'name': 'Guaranteed Rate Field', 'link': '/api/v1/venues/4'}
#         },
#     'marlins': {
#         'full_name': 'miami marlins',
#         'mlb_id': 146,
#         'abbreviations': ['mia'],
#         'location': 'miami',
#         'venue': {'id': 4169, 'name': 'Marlins Park', 'link': '/api/v1/venues/4169'}
#         },
#     'yankees': {
#         'full_name': 'new york yankees',
#         'mlb_id': 147,
#         'abbreviations': ['nya', 'nyy'],
#         'location': 'bronx',
#         'venue': {'id': 3313, 'name': 'Yankee Stadium', 'link': '/api/v1/venues/3313'}
#         },
#     'brewers': {
#         'full_name': 'milwaukee brewers',
#         'mlb_id': 158,
#         'abbreviations': ['mil'],
#         'location': 'milwaukee',
#         'venue': {'id': 32, 'name': 'American Family Field', 'link': '/api/v1/venues/32'}
#         },
#     'angels': {
#         'full_name': 'los angeles angels',
#         'mlb_id': 108,
#         'abbreviations': ['ana', 'laa'],
#         'location': 'anaheim',
#         'venue': {'id': 1, 'name': 'Angel Stadium', 'link': '/api/v1/venues/1'}
#         },
#     'diamondbacks': {
#         'full_name': 'arizona diamondbacks',
#         'mlb_id': 109,
#         'abbreviations': ['ari', 'az'],
#         'location': 'phoenix',
#         'venue': {'id': 1, 'name': 'Angel Stadium', 'link': '/api/v1/venues/1'}
#         },
#     'orioles': {
#         'full_name': 'baltimore orioles',
#         'mlb_id': 110,
#         'abbreviations': ['bal'],
#         'location': 'baltimore',
#         'venue': {'id': 2, 'name': 'Oriole Park at Camden Yards', 'link': '/api/v1/venues/2'}
#         },
#     'red sox': {
#         'full_name': 'boston red sox',
#         'mlb_id': 111,
#         'abbreviations': ['bos', 'red-sox'],
#         'location': 'boston',
#         'venue': {'id': 3, 'name': 'Fenway Park', 'link': '/api/v1/venues/3'}
#         },
#     'cubs': {
#         'full_name': 'chicago cubs',
#         'mlb_id': 112,
#         'abbreviations': ['chc', 'chn'],
#         'location': 'chicago',
#         'venue': {'id': 17, 'name': 'Wrigley Field', 'link': '/api/v1/venues/17'}
#         },
#     'reds': {
#         'full_name': 'cincinnati reds',
#         'mlb_id': 113,
#         'abbreviations': ['cin'],
#         'location': 'cincinnati',
#         'venue': {'id': 2602, 'name': 'Great American Ball Park', 'link': '/api/v1/venues/2602'}
#         },
#     'indians': {
#         'full_name': 'cleveland indians',
#         'mlb_id': 114,
#         'abbreviations': ['cle'],
#         'location': 'cleveland',
#         'venue': {'id': 5, 'name': 'Progressive Field', 'link': '/api/v1/venues/5'}
#         },
#     'rockies': {
#         'full_name': 'colorado rockies',
#         'mlb_id': 115,
#         'abbreviations': ['col'],
#         'location': 'denver',
#         'venue': {'id': 19, 'name': 'Coors Field', 'link': '/api/v1/venues/19'}
#         },
#     'tigers': {
#         'full_name': 'detroit tigers',
#         'mlb_id': 116,
#         'abbreviations': ['det'],
#         'location': 'detroit',
#         'venue': {'id': 2394, 'name': 'Comerica Park', 'link': '/api/v1/venues/2394'}
#         },
#     'astros': {
#         'full_name': 'houston astros',
#         'mlb_id': 117,
#         'abbreviations': ['hou'],
#         'location': 'houston',
#         'venue': {'id': 2392, 'name': 'Minute Maid Park', 'link': '/api/v1/venues/2392'}
#         },
#     'royals': {
#         'full_name': 'kansas city royals',
#         'mlb_id': 118,
#         'abbreviations': ['kca, kc', 'kcr'],
#         'location': 'kansas city',
#         'venue': {'id': 7, 'name': 'Kauffman Stadium', 'link': '/api/v1/venues/7'}
#         },
#     'dodgers': {
#         'full_name': 'los angeles dodgers',
#         'mlb_id': 119,
#         'abbreviations': ['lan', 'los', 'lad', 'la'],
#         'location': 'los angeles',
#         'venue': {'id': 22, 'name': 'Dodger Stadium', 'link': '/api/v1/venues/22'}
#         },
#     'nationals': {
#         'full_name': 'washington nationals',
#         'mlb_id': 120,
#         'abbreviations': ['was', 'wsh', 'wsn'],
#         'location': 'washington',
#         'venue': {'id': 3309, 'name': 'Nationals Park', 'link': '/api/v1/venues/3309'}
#         },
#     'mets': {
#         'full_name': 'new york mets',
#         'mlb_id': 121,
#         'abbreviations': ['nyn', 'nym'],
#         'location': 'flushing',
#         'venue': {'id': 3289, 'name': 'Citi Field', 'link': '/api/v1/venues/3289'}
#         },
#     })


# def format_season(season):
#     season_info = {
#             'spring_start': season.get('preSeasonStartDate'),
#             'reg_start': season['regularSeasonStartDate'],
#             'playoff_start': season.get('postSeasonStartDate'),
#             'second_half_start': season.get('firstDate2ndHalf'),
#             'reg_end': season['regularSeasonEndDate'],
#             'playoff_end': season.get('postSeasonEndDate'),
#             'season_id': season['seasonId']
#             }
#     return season_info
# @lru_cache
# def current_season():
#     path = json_path(name='current_season')
#     try:
#         with open(path) as file:
#             season_info = json.load(file)
#             file.close()
#         if str(season_info['season_id']) != str(tf.today.year):
#             raise KeyError('Must update info for current season.')
#         return season_info
#     except (FileNotFoundError, JSONDecodeError, KeyError):
#         season = statsapi.get('season',{'seasonId':tf.today.year,'sportId':1})['seasons'][0]
#         season_info = format_season(season)
#         with open(path, "w+") as file:
#             json.dump(season_info, file)
#             file.close()
#         return season_info

# def team_lineups():
#     path = json_path(name='team_lineups')
#     try:
#         with open(path) as file:
#             team_lineups = json.load(file)
#             file.close()
#         return team_lineups
#     except (FileNotFoundError, JSONDecodeError):
#         team_lineups = {}
#         for team in team_info.keys():
#             team_lineups[team] = {'L': [], 'R': []}
#         with open(path, "w+") as file:
#             json.dump(team_lineups, file)
#             file.close()
#         return team_lineups
    
# mlb_api_codes = Map({
#     'players': Map({
#         'h': ['8', '4', '2', '6', 'O', '5', '3', '7', '9', '10', '11', '12', '13', 'BR', 'I', 'U', 'V', 'W', 'A', 'J', 'Z', 'Y'],
#         'p': ['1','S', 'C', 'K', 'L', 'M', 'G', 'F', 'A', 'J', 'Z', 'Y'],
#         'sp': ['S', 'M', 'N'],
#         'bullpen': ['1', 'C', 'E', 'K', 'L', 'G', 'F'],
#         'rhp': ['K', 'M', 'F'],
#         'lhp': ['L', 'N', 'G'],
#         'twp': ['Y', 'Z', 'A', 'J'],
#         'unknown':['X'],
#     }),
#     'game_status': Map({
#         #codedGameState
#         'pre': ['S', 'P',],
#         'post': ['F', 'O', 'R', 'Q'],
#         'live': ['M', 'N', 'I'],
#         'postponed': ['D', 'C'],
#         'suspended': ['U', 'T'],
#         'other':['W', 'X'],
#         #statusCode
#         'delay_pre': ['PO', 'PA', 'PD', 'PL', 'PY', 'PP', 'PB', 'PC', 'PF', 'PV', 'PG', 'PS', 'PR', 'PI'],
#         'delay_mid': ['IZ', 'IO', 'IA', 'ID', 'IL', 'IY', 'IP', 'IB', 'IC', 'IF', 'IV', 'IG', 'IS', 'IR', 'II'],
#         }),
#     #weather[wind/condition]
#     'weather': Map({
#         'wind_in': ['In From RF', 'In From LF', 'In From CF'],
#         'wind_out': ['Out To CF', 'Out To RF', 'Out To LF'],
#         'wind_cross': ['R To L', 'L To R'],
#         'varied_wind': ['Varies'],
#         'no_wind': ['None'],
#         'rain': ['Rain', 'Drizzle'],
#         'roof_closed':['Roof Closed', 'Dome'],
#         'roof_open': ['Rain', 'Overcast', 'Snow', 'Cloudy', 'Clear', 'Sunny', 'Partly Cloudy', 'Drizzle'],
#         'clear':['Sunny', 'Clear'],
#         'inclement': ['Rain', 'Overcast', 'Snow', 'Cloudy', 'Partly Cloudy', 'Drizzle']
#         }),
#     'stats': Map({
#         #statGroups
#         'groups': Map({
#             'hitting':'hitting',
#             'pitching':'pitching',
#             'fielding':'fielding',
#             'catching':'catching',
#             'running': 'running',
#             'game': 'game',
#             'team':'team',
#             'streak':'streak'
#             }),
#         'categories':None
#         })
#     })


# def generate_team_instances():
#     instances = set()
#     for k, v in team_info.items():
#         value = k.replace(' ', '_') + ' = ' + f"Team(mlb_id = {v['mlb_id']}, name = '{k}')"
#         print(value)
#         instances.add(value)
#     return instances

# def past_seasons(seasons=range(2010,int(current_season()['season_id'])),path=json_path(name='past_seasons')):
#     try:
#         with open(path) as file:
#             season_list = json.load(file)
#             file.close()
#         if int(season_list[-1]['season_id']) < int(seasons[-1]):
#             print(season_list[-1]['season_id'])
#             print(int(seasons[-1]))
#             for year in range(int(season_list[-1]['season_id']) + 1, int(seasons[-1]) + 1):
#                 season = statsapi.get('season',{'seasonId':year,'sportId':1})['seasons'][0]
#                 season_info = format_season(season)
#                 season_list.append(season_info)
#             with open(path, "w+") as file:
#                 json.dump(season_list, file)
#                 file.close()
#         if int(season_list[0]['season_id']) > int(seasons[0]):
#             flag = 0
#             for year in range(seasons[0], int(season_list[0]['season_id'])):
#                 season = statsapi.get('season',{'seasonId':year,'sportId':1})['seasons'][0]
#                 season_info = format_season(season)
#                 season_list.insert(flag, season_info)
#                 flag += 1
#             with open(path, "w+") as file:
#                 json.dump(season_list, file)
#                 file.close()
#         return season_list
#     except (FileNotFoundError, JSONDecodeError, KeyError):
#         try:
#             with open(path) as file:
#                 season_list = json.load(file)
#                 file.close()
#         except (FileNotFoundError, JSONDecodeError):
#             season_list = []
#         for year in seasons:
#             season = statsapi.get('season',{'seasonId':year,'sportId':1})['seasons'][0]
#             season_info = format_season(season)
#             season_list.append(season_info)
#         with open(path, "w+") as file:
#              json.dump(season_list, file)
#              file.close()
#         print(f"Saved season info. for {seasons[0]}-{seasons[-1]} to {path}.")
#         return season_list
        

# #get historical data for a given season, pickled as list of dictionaries.
# #use a for loop for multiple years.
# def get_historical_data(year):
#                     gc.collect()
#                     games = []
#                     path=pickle_path('historical_data' + '_' + str(year))
#                     fields = 'dates,date,games,status,codedGameState,weather,condition,temp,wind,linescore,teams,home,away,runs,hits,currentInning,venue,id,dayNight,seriesGameNumber,officials,gamePk,probablePitcher'
#                     hydrate = 'weather,linescore,officials,probablePitcher'
#                     try:
#                         season = next(x for x in past_seasons() if str(x['season_id']) == str(year))
#                     except StopIteration:
#                         seasons = past_seasons(seasons=[year])
#                         season = next(x for x in seasons if str(x['season_id']) == str(year))
#                     months = mlb_months(int(season['season_id']))
#                     #error occurs when loading data for 2004-04-04 (missing 3 games total).
#                     if int(season['season_id']) == 2004:
#                         start = '2004-04-05'
#                     else:
#                         start = season['reg_start']
#                     april_end = months[4][1]
#                     may_start = months[5][0]
#                     may_end = months[5][1]
#                     june_start = months[6][0]
#                     june_end = months[6][1]
#                     july_start = months[7][0]
#                     july_end = months[7][1]
#                     august_start = months[8][0]
#                     august_end = months[8][1]
#                     september_start = months[9][0]
#                     end = season['reg_end']
#                     #covid season began 07-23-2020
#                     if int(season['season_id']) == 2020:
#                         periods = {
#                         start:july_end,
#                         august_start: august_end,
#                         september_start: end
#                         }
#                     #strike ended season on 1994-08-11
#                     elif int(season['season_id']) == 1994:
#                         periods = {
#                             start:april_end,
#                             may_start: may_end,
#                             june_start: june_end,
#                             july_start: july_end,
#                             august_start: end,
#                             }
#                     else:
#                         periods = {
#                             start:april_end,
#                             may_start: may_end,
#                             june_start: june_end,
#                             july_start: july_end,
#                             august_start: august_end,
#                             september_start: end
#                             }
                        
#                     for k, v in periods.items():
#                         if datetime.date.fromisoformat(k) > tf.tomorrow:
#                             break
#                         games_info = get_big('schedule', {'hydrate': hydrate,'sportId': 1, 'startDate': k, 'endDate': v, 'fields': fields})['dates']
#                         for x in games_info:
#                             for y in x['games']:
#                                 if y['status']['codedGameState'] == "F":
#                                     try:
#                                         d = {}
#                                         d['date'] = x['date']
#                                         d['game'] = y['gamePk']
#                                         home_team = y['linescore']['teams']['home']
#                                         away_team = y['linescore']['teams']['away']
#                                         d['runs'] = home_team['runs'] + away_team['runs']
#                                         d['hits'] = home_team['hits'] + away_team['hits']
#                                         d['home_score'] = home_team['runs']
#                                         d['away_score'] = away_team['runs']
#                                         d['venue_id'] = y['venue']['id']
#                                         try:
#                                             d['last_inning'] = y['linescore']['currentInning']
#                                         except KeyError:
#                                             pass
#                                         try:
#                                             d['home_sp'] = y['teams']['home']['probablePitcher']['id']
#                                         except KeyError:
#                                             pass
#                                         try:
#                                             d['away_sp'] = y['teams']['away']['probablePitcher']['id']
#                                         except KeyError:
#                                             pass
#                                         try:
#                                             wind_description = y['weather']['wind']
#                                             d['wind_speed'] = wind_description.split(' ')[0]
#                                             d['wind_direction'] = wind_description.split(', ')[-1]
#                                         except KeyError:
#                                             pass
#                                         try:
#                                             d['temp'] = y['weather']['temp']
#                                         except KeyError:
#                                             pass
#                                         try:
#                                             d['series_game'] = y['seriesGameNumber']
#                                         except KeyError:
#                                             pass
#                                         umpires = y['officials']
#                                         try:
#                                             if umpires[0]['officialType'] == 'Home Plate':
#                                                 d['umpire'] = umpires[0]['official']['id']
#                                         except (KeyError, IndexError):
#                                             pass
#                                         try:
#                                             d['condition'] = y['weather']['condition']
#                                         except KeyError:
#                                             pass
#                                         try:
#                                             d['day_night'] = y['dayNight']
#                                         except KeyError:
#                                             pass
                                        
#                                         games.append(d)
#                                     except KeyError:
#                                         continue
#                     with open(path, "wb") as file:
#                         pickle.dump(games, file)
#                         file.close()
#                     return f"{path}"       

# #pass range(2000,2005) to pickle those years in ARCHIVE_DIR/historical_data_2000-2004.pickle
# #if not already done, compile stats for those years calling historical_data(year)
# def historical_data(start, end=None):
#     if not end:
#         end = start + 1
#     if start > end:
#         raise ValueError('Start cannot be after end. Get da')
#     season_range = range(start,end)
#     dir_path = settings.ARCHIVE_DIR
#     if len(season_range) == 1:
#         file_path = dir_path.joinpath(pickle_path(name = 'historical_data' + '_' + str(season_range[0])))
#         try:
#             if season_range[0] != int(current_season()['season_id']):
#                 print()
#                 with open(file_path, "rb") as f:
#                         season = pd.read_pickle(f)
#                         f.close()
#                 df = pd.DataFrame(season)
#                 return df
#             else:
#                 get_historical_data(season_range[0])
#                 with open(file_path, "rb") as f:
#                     season = pd.read_pickle(f)
#                 df = pd.DataFrame(season)
#                 return df
#         except FileNotFoundError:
#             get_historical_data(season_range[0])
#             with open(file_path, "rb") as f:
#                     season = pd.read_pickle(f)
#                     f.close()
#             df = pd.DataFrame(season)
#             return df
#     else:
#         for year in season_range:
#             file_path = dir_path.joinpath(pickle_path(name = 'historical_data' + '_' + str(year)))
#             if not file_path.exists() or year == int(current_season()['season_id']):
#                 get_historical_data(year)
#         season_data = []
#         files = [file for file in sorted(os.listdir(dir_path)) if re.search('[historical_data][0-9]{4}[.][pickle]', file) and int(file.split('_')[-1].split('.')[0]) in season_range]
#         for file in files:
#             file_path = dir_path.joinpath(file)
#             with open(file_path, "rb") as f:
#                 season = pd.read_pickle(f)
#                 f.close()
#             season_data.extend(season)
#         prefix = files[0].split('_')[-1].split('.')[0]
#         suffix = files[-1].split('_')[-1].split('.')[0]
#         data_path = pickle_path(name = 'historical_data' + '_' + prefix + '-' + suffix)
#         with open(data_path,"wb") as file:
#             pickle.dump(season_data, file)
#             file.close()
#         print(f"Data saved at {data_path}.")
#         df = pd.DataFrame(season_data)
#         return df



# """
# seasons: list of years (e.g. [2020])
# player_group: 'hitting' or 'pitching'
# player_ids: list, string (e.g. '12345,67890'), or integer - 404 error if single id does not exist.
# """
# def get_statcast_longterm(seasons=[], player_group='', player_ids=[]):
#     all_players = []
#     if type(player_ids) == list:
#         player_ids = ids_string(player_ids)
#     if player_group == 'hitting':
#         fields = 'people,id,stats,splits,stat,metric,name,averageValue,minValue,maxValue,unit,numOccurrences,season'
#     elif player_group == 'pitching':
#         fields='people,id,stats,splits,stat,metric,name,averageValue,minValue,maxValue,unit,numOccurrences,details,event,type,code,EP,PO,AB,AS,CH,CU,FA,FT,FF,FC,FS,FO,GY,IN,KC,KN,NP,SC,SI,SL,UN,ST,SV,CS,season'
#     for season in seasons:
#         season_players = []
#         if player_group == 'hitting':
#             hydrate = f"stats(group=[hitting],type=[metricAverages],metrics=[distance,launchSpeed,launchAngle,maxHeight,travelTime,travelDistance,hrDistance,launchSpinRate],season={season})"
#             call = statsapi.get('people', {'personIds': player_ids,'hydrate': hydrate, 'fields':fields}, force=True)
#             for x in call['people']:
#                 player = {}
#                 player['mlb_id'] = x['id']
#                 player['season'] = season
#                 for y in x['stats'][0]['splits']:
#                     if not y['stat']['metric'].get('averageValue'):
#                         continue
#                     avg = f"{y['stat']['metric']['name']}_avg"
#                     count = f"{y['stat']['metric']['name']}_count"
#                     player[avg] = y['stat']['metric']['averageValue']
#                     player[count] = y['numOccurrences']
#                 season_players.append(player)
#             all_players.extend(season_players)
#         elif player_group == 'pitching':
#             hydrate = f"stats(group=[pitching],type=[metricAverages],metrics=[releaseSpinRate,releaseExtension,releaseSpeed,effectiveSpeed,launchSpeed,launchAngle],season={season})"
#             call = statsapi.get('people', {'personIds': player_ids,'hydrate': hydrate, 'fields':fields}, force=True)
#             for x in call['people']:
#                 player = {}
#                 player['mlb_id'] = x['id']
#                 player['pitches'] = 0
#                 player['season'] = season
#                 for y in x['stats'][0]['splits']:
#                     if not y['stat']['metric'].get('averageValue'):
#                         continue
#                     if y['stat'].get('event'):
#                         avg = f"{y['stat']['metric']['name']}_avg_{y['stat']['event']['details']['type']['code']}"
#                         count = f"count_{y['stat']['event']['details']['type']['code']}"
#                     else:
#                         avg = f"{y['stat']['metric']['name']}_avg"
#                         count = f"{y['stat']['metric']['name']}_count"
#                     player[avg] = y['stat']['metric']['averageValue']
#                     if y['numOccurrences'] > player.get(count,0):
#                         if y['stat'].get('event'):
#                             player['pitches'] -= player.get(count,0)
#                             player['pitches'] += y['numOccurrences']
#                         player[count] = y['numOccurrences']
                
#                 season_players.append(player)
#             all_players.extend(season_players)
#     return all_players

# def get_statcast_h(player_id, season):
#     plays = []
#     play_ids = set()
#     metrics = '[distance,launchSpeed,launchAngle]'
#     hydrate = f"stats(group=[hitting],type=[metricLog],metrics={metrics},season={season},limit=1000)"
#     fields = "people,id,stats,splits,metric,name,value,event,details,type,player,venue,date,stat,playId"
#     call = statsapi.get('people', {'hydrate':hydrate,'personIds':player_id,'season':season,'fields':fields})
#     for x in call['people'][0]['stats'][0]['splits']:
#         play_id = x['stat']['event']['playId']
#         if play_id in play_ids:
#             play = next(p for p in plays if p['play_id'] == play_id)
#             plays = [p for p in plays if p['play_id'] != play_id]
#             description = f"{x['stat']['metric']['name']}"
#             play[description] = x['stat']['metric']['value']
#             plays.append(play)
#         else:
#             play = {}
#             description = f"{x['stat']['metric']['name']}"
#             play[description] = x['stat']['metric']['value']
#             play['result'] = x['stat'].get('event', {}).get('details', {}).get('event','')
#             play['date'] = x['date']
#             play['venue'] = x['venue']['id']
#             play_id = x['stat']['event']['playId']
#             play['play_id'] = play_id
#             play_ids.add(play_id)
#             plays.append(play)
#     df = pd.DataFrame(plays)
#     df['date'] = df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
#     return df

# def get_statcast_p(player_id, season):
#     plays = []
#     play_ids = set()
#     metrics = '[releaseSpinRate,effectiveSpeed,launchAngle]'
#     hydrate =f"stats(group=[pitching],type=[metricLog],metrics={metrics},season={season},limit=1000)"
#     fields =  "people,id,stats,splits,metric,name,value,event,details,type,player,venue,date,stat,playId,EP,PO,AB,AS,CH,CU,FA,FT,FF,FC,FS,FO,GY,IN,KC,KN,NP,SC,SI,SL,UN,ST,SV,CS,season,code"
#     call = statsapi.get('people', {'hydrate':hydrate,'personIds':player_id,'season':season,'fields':fields, 'limit':10000})
#     for x in call['people'][0]['stats'][0]['splits']:
#         play_id = x['stat']['event']['playId']
#         if play_id in play_ids:
#             play = next(p for p in plays if p['play_id'] == play_id)
#             plays = [p for p in plays if p['play_id'] != play_id]
#             description = f"{x['stat']['metric']['name']}"
#             play[description] = x['stat']['metric']['value']
#             plays.append(play)
#         else:
#             play = {}
#             description = f"{x['stat']['metric']['name']}"
#             play[description] = x['stat']['metric']['value']
#             play['pitch'] = x['stat'].get('event', {}).get('details', {}).get('type',{}).get('code','')
#             play['date'] = x['date']
#             play['venue'] = x['venue']['id']
#             play_id = x['stat']['event']['playId']
#             play['result'] = x['stat'].get('event', {}).get('details', {}).get('event','')
#             play['play_id'] = play_id
#             play_ids.add(play_id)
#             plays.append(play)
        
#     df = pd.DataFrame(plays)
#     df['date'] = df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
#     return df

# def get_splits_h(seasons,sport=1,pool='ALL',get_all=True):
#     splits=['vr','vl']
#     cs = current_season()['season_id']
#     fields='stats,splits,totalSplits,season,stat,team,id,player,stat,groundOuts,airOuts,runs,doubles,triples,homeRuns,strikeOuts,baseOnBalls,intentionalWalks,hits,hitByPitch,avg,atBats,obp,slg,ops,caughtStealing,stolenBases,stolenBasePercentage,groundIntoDoublePlay,numberOfPitches,plateAppearances,totalBases,rbi,sacBunts,sacFlies,babip,groundOutsToAirouts,atBatsPerHomeRun,fullName'
#     if len(seasons) > 1:
#         final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_' + str(seasons[0]) + '-' + str(seasons[-1])))
#         if str(seasons[-1]) == cs and len(seasons) > 2:
#             static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_'  + str(seasons[0]) + '-' + str(seasons[-2])))
#         elif str(seasons[-1]) == cs:
#             static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_'  + str(seasons[0])))
#         else: 
#             static_path = final_path
#     else:
#         final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_'  + str(seasons[0])))
#         static_path = final_path
#     if static_path.exists():
#         store_static = False
#         complete_seasons = re.findall('[0-9]{4}', str(static_path))
#         with open(static_path, 'rb') as file:
#             complete_data = pickle.load(file)
#             players = complete_data
#             player_ids = set()
#             for player in players:
#                 player_ids.add(player['mlb_id'])
#     else:
#         player_ids = set()
#         players = []
#         complete_seasons = []
#         store_static = True
#         static_players = []
#     def compile_stats(data, split, player):
#             player['mlb_id'] = data['player']['id']
#             player['name'] = data['player']['fullName']
#             stats = data['stat']
#             player['gb' + '_' + split] = stats['groundOuts']
#             player['fb' + '_' + split] = stats['airOuts']
#             player['runs' + '_' + split] = stats['runs']
#             player['2b' + '_' + split] = stats['doubles']
#             player['3b' + '_' + split] = stats['triples']
#             player['hr' + '_' + split] = stats['homeRuns']
#             player['k' + '_' + split] = stats['strikeOuts']
#             player['bb' + '_' + split] = stats['baseOnBalls']
#             player['ibb' + '_' + split] = stats['intentionalWalks']
#             player['hits' + '_' + split] = stats['hits']
#             player['hbp' + '_' + split] = stats['hitByPitch']
#             player['ab' + '_' + split] = stats['atBats']
#             player['cs' + '_' + split] = stats['caughtStealing']
#             player['sb' + '_' + split] = stats['stolenBases']
#             player['gidp' + '_' + split] = stats['groundIntoDoublePlay']
#             player['total_pitches' + '_' + split] = stats['numberOfPitches']
#             player['pa' + '_' + split] = stats['plateAppearances']
#             player['total_bases' + '_' + split] = stats['totalBases']
#             player['rbi' + '_' + split] = stats['rbi']
#             player['sac_bunts' + '_' + split] = stats['sacBunts']
#             player['sac_flies' + '_' + split] = stats['sacFlies']
#             return player
#     def combine_stats(original, new, split):
#         try:
#             stats = new['stat']
#             original['gb' + '_' + split] += stats['groundOuts']
#             original['fb' + '_' + split] += stats['airOuts']
#             original['runs' + '_' + split] += stats['runs']
#             original['2b' + '_' + split] += stats['doubles']
#             original['3b' + '_' + split] += stats['triples']
#             original['hr' + '_' + split] += stats['homeRuns']
#             original['k' + '_' + split] += stats['strikeOuts']
#             original['bb' + '_' + split] += stats['baseOnBalls']
#             original['ibb' + '_' + split] += stats['intentionalWalks']
#             original['hits' + '_' + split] += stats['hits']
#             original['hbp' + '_' + split] += stats['hitByPitch']
#             original['ab' + '_' + split] += stats['atBats']
#             original['cs' + '_' + split] += stats['caughtStealing']
#             original['sb' + '_' + split] += stats['stolenBases']
#             original['gidp' + '_' + split] += stats['groundIntoDoublePlay']
#             original['total_pitches' + '_' + split] += stats['numberOfPitches']
#             original['pa' + '_' + split] += stats['plateAppearances']
#             original['total_bases' + '_' + split] += stats['totalBases']
#             original['rbi' + '_' + split] += stats['rbi']
#             original['sac_bunts' + '_' + split] += stats['sacBunts']
#             original['sac_flies' + '_' + split] += stats['sacFlies']
#             return original
#         except KeyError:
#             return compile_stats(new, split, original)
            
#     seasons = [x for x in seasons if str(x) not in complete_seasons]
#     for season in seasons:
#         if season == cs and store_static and players:
#             static_players = players
#         for split in splits:
#             call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 10000, 'sitCodes': split, 'season': season, 'group':'hitting', 'sportIds':sport, 'fields':fields}, force=True)
#             data = call['stats'][0]['splits']
#             total_splits = call['stats'][0]['totalSplits']
#             total_returned = len(data)
#             for p in data:
#                 player_id = p['player']['id']
#                 if player_id not in player_ids:
#                     player=compile_stats(p, split, {})
#                     player['season'] = season
#                     players.append(player)
#                     player_ids.add(player_id)
#                 else:
#                     current_player = next(x for x in players if x['mlb_id'] == player_id)
#                     player = combine_stats(current_player, p, split)
#                     players = [x for x in players if x['mlb_id'] != player_id]
#                     players.append(player)
#             if get_all and total_splits != total_returned:
#                 flag = 0
#                 while total_splits != total_returned:
#                     offset = total_returned
#                     call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 10000, 'sitCodes': split, 'season': season, 'group':'hitting', 'sportIds':sport, 'fields':fields,'offset':offset}, force=True)
#                     data = call['stats'][0]['splits']
#                     total_offset = len(data)
#                     flag += 1
#                     if flag > 1:
#                         print('Could not get all data.')
#                         break
#                     for p in data:
#                         player_id = p['player']['id']
#                         if player_id not in player_ids:
#                             player=compile_stats(p)
#                             player['season'] = season
#                             players.append(player)
#                             player_ids.add(player_id)
#                         else:
#                             current_player = next(x for x in players if x['mlb_id'] == player_id)
#                             player = combine_stats(current_player,p)
#                             players = [x for x in players if x['mlb_id'] != player_id]
#                             players.append(player)
#                     total_returned += total_offset
#     with open(final_path, 'wb') as file:
#         pickle.dump(players, file)
#     if store_static:
#         with open(static_path, 'wb') as file:
#             if static_players:
#                 pickle.dump(static_players, file)
#             else:
#                 pickle.dump(players, file)
        
#     return players

# def get_splits_p(seasons,sport=1,pool='ALL',get_all=True):
#     splits=['vr','vl','sp','rp']
#     cs = current_season()['season_id']
#     fields='stats,totalSplits,splits,stat,id,player,groundOuts,airOuts,runs,doubles,triples,homeRuns,strikeOuts,baseOnBalls,intentionalWalks,hits,hitByPitch,avg,atBats,obp,slg,ops,caughtStealing,stolenBases,stolenBasePercentage,groundIntoDoublePlay,numberOfPitches,era,inningsPitched,earnedRuns,whip,battersFaced,outs,balls,strikes,strikePercentage,hitBatsmen,balks,wildPitches,pickoffs,totalBases,groundOutsToAirouts,rbi,pitchesPerInning,strikeoutWalkRatio,strikeoutsPer9Inn,walksPer9Inn,hitsPer9Inn,runsScoredPer9,homeRunsPer9,inheritedRunners,inheritedRunnersScored,inheritedRunnersStrandedPercentage,sacBunts,sacFlies,gamesStarted,gamesPitched,wins,losses,saves,saveOpportunities,holds,completeGames,shutouts,fullName'
#     if len(seasons) > 1:
#         final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_' + str(seasons[0]) + '-' + str(seasons[-1])))
#         if str(seasons[-1]) == cs and len(seasons) > 2:
#             static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_'  + str(seasons[0]) + '-' + str(seasons[-2])))
#         elif str(seasons[-1]) == cs:
#             static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_'  + str(seasons[0])))
#         else: 
#             static_path = final_path
#     else:
#         final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_'  + str(seasons[0])))
#         static_path = final_path
#     if static_path.exists():
#         store_static = False
#         complete_seasons = re.findall('[0-9]{4}', str(static_path))
#         with open(static_path, 'rb') as file:
#             complete_data = pickle.load(file)
#             players = complete_data
#             player_ids = set()
#             for player in players:
#                 player_ids.add(player['mlb_id'])
#     else:
#         player_ids = set()
#         players = []
#         complete_seasons = []
#         store_static = True
#         static_players = []
#     def non_splits(player_data, season):
#         player = {}
#         if player_data['player'].get('stats'):
#                 key ='_' + str(season)[2:]
#                 player_non_splits = player_data['player']['stats'][0]['splits'][0]['stat']
#                 player['games' + key] = player_non_splits.get('gamesPitched',0)
#                 player['games_sp' + key] = player_non_splits.get('gamesStarted',0)
#                 player['wins' + key] = player_non_splits.get('wins',0)
#                 player['losses' + key] = player_non_splits.get('losses',0)
#                 player['saves' + key] = player_non_splits.get('saves',0)
#                 player['save_chances' + key] = player_non_splits.get('saveOpportunities',0)
#                 player['holds' + key] = player_non_splits.get('',0)
#                 player['complete_games' + key] = player_non_splits.get('completeGames',0)
#                 player['shutouts' + key] = player_non_splits.get('shutouts',0)
#         return player
#     def compile_stats(d, split, player, season):
#             stats = d['stat']
#             player['mlb_id'] = d['player']['id']
#             player['name'] = d['player']['fullName']
#             player['gb' + '_' + split] = stats['groundOuts']
#             player['fb' + '_' + split] = stats['airOuts']
#             player['runs' + '_' + split] = stats['runs']
#             player['2b' + '_' + split] = stats['doubles']
#             player['3b' + '_' + split] = stats['triples']
#             player['hr' + '_' + split] = stats['homeRuns']
#             player['k' + '_' + split] = stats['strikeOuts']
#             player['bb' + '_' + split] = stats['baseOnBalls']
#             player['ibb' + '_' + split] = stats['intentionalWalks']
#             player['hits' + '_' + split] = stats['hits']
#             player['ab' + '_' + split] = stats['atBats']
#             player['cs' + '_' + split] = stats['caughtStealing']
#             player['sb' + '_' + split] = stats['stolenBases']
#             player['gidp' + '_' + split] = stats['groundIntoDoublePlay']
#             player['total_pitches' + '_' + split] = stats['numberOfPitches']
#             player['total_bases' + '_' + split] = stats['totalBases']
#             player['rbi' + '_' + split] = stats['rbi']
#             player['sac_bunts' + '_' + split] = stats['sacBunts']
#             player['sac_flies' + '_' + split] = stats['sacFlies']
#             player['ir' + '_' + split] = stats.get('inheritedRunners', 0)
#             player['irs' + '_' + split] = stats.get('inheritedRunnersScored',0)
#             player['pickoffs' + '_' + split] = stats['pickoffs']
#             player['wild_pitches' + '_' + split] = stats['wildPitches']
#             player['balks' + '_' + split] = stats['balks']
#             player['strikes' + '_' + split] = stats['strikes']
#             player['balls' + '_' + split] = stats['balls']
#             player['outs' + '_' + split] = stats['outs']
#             player['hb' + '_' + split] = stats['hitBatsmen']
#             player['batters_faced' + '_' + split] = stats['battersFaced']
#             player['er' + '_' + split] = stats['earnedRuns']
#             if split == 'vr':
#                 ns = non_splits(d, season)
#                 player.update(ns)
#             return player
#     def combine_stats(original, new, split, season):
#         try:
#             stats = new['stat']
#             original['gb' + '_' + split] += stats['groundOuts']
#             original['fb' + '_' + split] += stats['airOuts']
#             original['runs' + '_' + split] += stats['runs']
#             original['2b' + '_' + split] += stats['doubles']
#             original['3b' + '_' + split] += stats['triples']
#             original['hr' + '_' + split] += stats['homeRuns']
#             original['k' + '_' + split] += stats['strikeOuts']
#             original['bb' + '_' + split] += stats['baseOnBalls']
#             original['ibb' + '_' + split] += stats['intentionalWalks']
#             original['hits' + '_' + split] += stats['hits']
#             original['ab' + '_' + split] += stats['atBats']
#             original['cs' + '_' + split] += stats['caughtStealing']
#             original['sb' + '_' + split] += stats['stolenBases']
#             original['gidp' + '_' + split] += stats['groundIntoDoublePlay']
#             original['total_pitches' + '_' + split] += stats['numberOfPitches']
#             original['total_bases' + '_' + split] += stats['totalBases']
#             original['rbi' + '_' + split] += stats['rbi']
#             original['sac_bunts' + '_' + split] += stats['sacBunts']
#             original['sac_flies' + '_' + split] += stats['sacFlies']
#             original['ir' + '_' + split] += stats['inheritedRunners']
#             original['irs' + '_' + split] += stats['inheritedRunnersScored']
#             original['pickoffs' + '_' + split] += stats['pickoffs']
#             original['wild_pitches' + '_' + split] += stats['wildPitches']
#             original['balks' + '_' + split] += stats['balks']
#             original['strikes' + '_' + split] += stats['strikes']
#             original['balls' + '_' + split] += stats['balls']
#             original['outs' + '_' + split] += stats['outs']
#             original['hb' + '_' + split] += stats['hitBatsmen']
#             original['batters_faced' + '_' + split] += stats['battersFaced']
#             original['er' + '_' + split] += stats['earnedRuns']
#             if split == 'vr':
#                 ns = non_splits(new, season)
#                 original.update(ns)
#             return original
#         except KeyError:
#             return compile_stats(new, split, original, season)
#     seasons = [x for x in seasons if str(x) not in complete_seasons]
#     for season in seasons:
#         if season == cs and store_static and players:
#             static_players = players
#         for split in splits:
#             if split == 'vr':
#                 hydrate = f"person(stats(group=[pitching],type=[season],season={season}))"
#                 call = statsapi.get('stats', {'hydrate': hydrate, 'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields}, force=True)
#             else:
#                 call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields}, force=True)
#             data = call['stats'][0]['splits']
#             total_splits = call['stats'][0]['totalSplits']
#             total_returned = len(data)
#             for p in data:
#                 player_id = p['player']['id']
#                 if player_id not in player_ids:
#                     player=compile_stats(p, split, {}, season)
#                     players.append(player)
#                     player_ids.add(player_id)
#                 else:
#                     current_player = next(x for x in players if x['mlb_id'] == player_id)
#                     player = combine_stats(current_player, p, split, season)
#                     players = [x for x in players if x['mlb_id'] != player_id]
#                     players.append(player)
#             if get_all and total_splits != total_returned:
#                 flag = 0
#                 while total_splits != total_returned:
#                     offset = total_returned
#                     if split == 'vr':
#                         call = statsapi.get('stats', {'hydrate': hydrate, 'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields,'offset':offset}, force=True)
#                     else:
#                         call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields,'offset':offset}, force=True)
#                     data = call['stats'][0]['splits']
#                     total_offset = len(data)
#                     flag += 1
#                     if flag > 1:
#                         print('Could not get all data.')
#                         break
#                     for p in data:
#                             player_id = p['player']['id']
#                             if player_id not in player_ids:
#                                 player=compile_stats(p, split, {}, season)
#                                 players.append(player)
#                                 player_ids.add(player_id)
#                             else:
#                                 current_player = next(x for x in players if x['mlb_id'] == player_id)
#                                 player = combine_stats(current_player, p, split, season)
#                                 players = [x for x in players if x['mlb_id'] != player_id]
#                                 players.append(player)
#                     total_returned += total_offset
#     with open(final_path, 'wb') as file:
#         pickle.dump(players, file)
#     if store_static:
#         with open(static_path, 'wb') as file:
#             if static_players:
#                 pickle.dump(static_players, file)
#             else:
#                 pickle.dump(players, file)
#     return players
