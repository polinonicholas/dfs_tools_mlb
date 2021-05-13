from dfs_tools_mlb.utils.strings import ids_string
from dfs_tools_mlb.compile.historical_data import current_season
from dfs_tools_mlb.utils.storage import pickle_path
from dfs_tools_mlb import settings
import statsapi
import pandas as pd
import pickle
import re

"""
seasons: list of years (e.g. [2020])
player_group: 'hitting' or 'pitching'
player_ids: list, string (e.g. '12345,67890'), or integer - 404 error if single id does not exist.
"""
def get_statcast_longterm(seasons=[], player_group='', player_ids=[]):
    all_players = []
    if type(player_ids) == list:
        player_ids = ids_string(player_ids)
    if player_group == 'hitting':
        fields = 'people,id,stats,splits,stat,metric,name,averageValue,minValue,maxValue,unit,numOccurrences,season'
    elif player_group == 'pitching':
        fields='people,id,stats,splits,stat,metric,name,averageValue,minValue,maxValue,unit,numOccurrences,details,event,type,code,EP,PO,AB,AS,CH,CU,FA,FT,FF,FC,FS,FO,GY,IN,KC,KN,NP,SC,SI,SL,UN,ST,SV,CS,season'
    for season in seasons:
        season_players = []
        if player_group == 'hitting':
            hydrate = f"stats(group=[hitting],type=[metricAverages],metrics=[distance,launchSpeed,launchAngle,maxHeight,travelTime,travelDistance,hrDistance,launchSpinRate],season={season})"
            call = statsapi.get('people', {'personIds': player_ids,'hydrate': hydrate, 'fields':fields}, force=True)
            for x in call['people']:
                player = {}
                player['mlb_id'] = x['id']
                player['season'] = season
                for y in x['stats'][0]['splits']:
                    if not y['stat']['metric'].get('averageValue'):
                        continue
                    avg = f"{y['stat']['metric']['name']}_avg"
                    count = f"{y['stat']['metric']['name']}_count"
                    player[avg] = y['stat']['metric']['averageValue']
                    player[count] = y['numOccurrences']
                season_players.append(player)
            all_players.extend(season_players)
        elif player_group == 'pitching':
            hydrate = f"stats(group=[pitching],type=[metricAverages],metrics=[releaseSpinRate,releaseExtension,releaseSpeed,effectiveSpeed,launchSpeed,launchAngle],season={season})"
            call = statsapi.get('people', {'personIds': player_ids,'hydrate': hydrate, 'fields':fields}, force=True)
            for x in call['people']:
                player = {}
                player['mlb_id'] = x['id']
                player['pitches'] = 0
                player['season'] = season
                for y in x['stats'][0]['splits']:
                    if not y['stat']['metric'].get('averageValue'):
                        continue
                    if y['stat'].get('event'):
                        avg = f"{y['stat']['metric']['name']}_avg_{y['stat']['event']['details']['type']['code']}"
                        count = f"count_{y['stat']['event']['details']['type']['code']}"
                    else:
                        avg = f"{y['stat']['metric']['name']}_avg"
                        count = f"{y['stat']['metric']['name']}_count"
                    player[avg] = y['stat']['metric']['averageValue']
                    if y['numOccurrences'] > player.get(count,0):
                        if y['stat'].get('event'):
                            player['pitches'] -= player.get(count,0)
                            player['pitches'] += y['numOccurrences']
                        player[count] = y['numOccurrences']
                
                season_players.append(player)
            all_players.extend(season_players)
    return all_players

def get_statcast_h(player_id, season):
    plays = []
    play_ids = set()
    metrics = '[distance,launchSpeed,launchAngle]'
    hydrate = f"stats(group=[hitting],type=[metricLog],metrics={metrics},season={season},limit=1000)"
    fields = "people,id,stats,splits,metric,name,value,event,details,type,player,venue,date,stat,playId"
    call = statsapi.get('people', {'hydrate':hydrate,'personIds':player_id,'season':season,'fields':fields})
    for x in call['people'][0]['stats'][0]['splits']:
        play_id = x['stat']['event']['playId']
        if play_id in play_ids:
            play = next(p for p in plays if p['play_id'] == play_id)
            plays = [p for p in plays if p['play_id'] != play_id]
            description = f"{x['stat']['metric']['name']}"
            play[description] = x['stat']['metric']['value']
            plays.append(play)
        else:
            play = {}
            description = f"{x['stat']['metric']['name']}"
            play[description] = x['stat']['metric']['value']
            play['result'] = x['stat'].get('event', {}).get('details', {}).get('event','')
            play['date'] = x['date']
            play['venue'] = x['venue']['id']
            play_id = x['stat']['event']['playId']
            play['play_id'] = play_id
            play_ids.add(play_id)
            plays.append(play)
    df = pd.DataFrame(plays)
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    return df

def get_statcast_p(player_id, season):
    plays = []
    play_ids = set()
    metrics = '[releaseSpinRate,effectiveSpeed,launchAngle]'
    hydrate =f"stats(group=[pitching],type=[metricLog],metrics={metrics},season={season},limit=1000)"
    fields =  "people,id,stats,splits,metric,name,value,event,details,type,player,venue,date,stat,playId,EP,PO,AB,AS,CH,CU,FA,FT,FF,FC,FS,FO,GY,IN,KC,KN,NP,SC,SI,SL,UN,ST,SV,CS,season,code"
    call = statsapi.get('people', {'hydrate':hydrate,'personIds':player_id,'season':season,'fields':fields, 'limit':10000})
    for x in call['people'][0]['stats'][0]['splits']:
        play_id = x['stat']['event']['playId']
        if play_id in play_ids:
            play = next(p for p in plays if p['play_id'] == play_id)
            plays = [p for p in plays if p['play_id'] != play_id]
            description = f"{x['stat']['metric']['name']}"
            play[description] = x['stat']['metric']['value']
            plays.append(play)
        else:
            play = {}
            description = f"{x['stat']['metric']['name']}"
            play[description] = x['stat']['metric']['value']
            play['pitch'] = x['stat'].get('event', {}).get('details', {}).get('type',{}).get('code','')
            play['date'] = x['date']
            play['venue'] = x['venue']['id']
            play_id = x['stat']['event']['playId']
            play['result'] = x['stat'].get('event', {}).get('details', {}).get('event','')
            play['play_id'] = play_id
            play_ids.add(play_id)
            plays.append(play)
        
    df = pd.DataFrame(plays)
    df['date'] = df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    return df

def get_splits_h(seasons,sport=1,pool='ALL',get_all=True):
    splits=['vr','vl']
    cs = current_season()['season_id']
    fields='stats,splits,totalSplits,season,stat,team,id,player,stat,groundOuts,airOuts,runs,doubles,triples,homeRuns,strikeOuts,baseOnBalls,intentionalWalks,hits,hitByPitch,avg,atBats,obp,slg,ops,caughtStealing,stolenBases,stolenBasePercentage,groundIntoDoublePlay,numberOfPitches,plateAppearances,totalBases,rbi,sacBunts,sacFlies,babip,groundOutsToAirouts,atBatsPerHomeRun,fullName,code,batSide,primaryPosition'
    hydrate = 'person'
    if len(seasons) > 1:
        final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_' + str(seasons[0]) + '-' + str(seasons[-1])))
        if str(seasons[-1]) == cs and len(seasons) > 2:
            static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_'  + str(seasons[0]) + '-' + str(seasons[-2])))
        elif str(seasons[-1]) == cs:
            static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_'  + str(seasons[0])))
        else: 
            static_path = final_path
    else:
        final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='h_data_'  + str(seasons[0])))
        static_path = final_path
    if static_path.exists():
        store_static = False
        complete_seasons = re.findall('[0-9]{4}', str(static_path))
        with open(static_path, 'rb') as file:
            complete_data = pickle.load(file)
            players = complete_data
            player_ids = set()
            for player in players:
                player_ids.add(player['mlb_id'])
    else:
        player_ids = set()
        players = []
        complete_seasons = []
        store_static = True
        static_players = []
    def compile_stats(data, split, player):
            player['mlb_id'] = data['player']['id']
            player['name'] = data['player']['fullName']
            player['bat_side'] = data['player']['batSide']['code']
            player['position_code'] = data['player']['primaryPosition']['code']
            stats = data['stat']
            player['gb' + '_' + split] = stats['groundOuts']
            player['fb' + '_' + split] = stats['airOuts']
            player['runs' + '_' + split] = stats['runs']
            player['2b' + '_' + split] = stats['doubles']
            player['3b' + '_' + split] = stats['triples']
            player['hr' + '_' + split] = stats['homeRuns']
            player['k' + '_' + split] = stats['strikeOuts']
            player['bb' + '_' + split] = stats['baseOnBalls']
            player['ibb' + '_' + split] = stats['intentionalWalks']
            player['hits' + '_' + split] = stats['hits']
            player['hbp' + '_' + split] = stats['hitByPitch']
            player['ab' + '_' + split] = stats['atBats']
            player['cs' + '_' + split] = stats['caughtStealing']
            player['sb' + '_' + split] = stats['stolenBases']
            player['gidp' + '_' + split] = stats['groundIntoDoublePlay']
            player['total_pitches' + '_' + split] = stats['numberOfPitches']
            player['pa' + '_' + split] = stats['plateAppearances']
            player['total_bases' + '_' + split] = stats['totalBases']
            player['rbi' + '_' + split] = stats['rbi']
            player['sac_bunts' + '_' + split] = stats['sacBunts']
            player['sac_flies' + '_' + split] = stats['sacFlies']
            return player
    def combine_stats(original, new, split):
        try:
            stats = new['stat']
            original['gb' + '_' + split] += stats['groundOuts']
            original['fb' + '_' + split] += stats['airOuts']
            original['runs' + '_' + split] += stats['runs']
            original['2b' + '_' + split] += stats['doubles']
            original['3b' + '_' + split] += stats['triples']
            original['hr' + '_' + split] += stats['homeRuns']
            original['k' + '_' + split] += stats['strikeOuts']
            original['bb' + '_' + split] += stats['baseOnBalls']
            original['ibb' + '_' + split] += stats['intentionalWalks']
            original['hits' + '_' + split] += stats['hits']
            original['hbp' + '_' + split] += stats['hitByPitch']
            original['ab' + '_' + split] += stats['atBats']
            original['cs' + '_' + split] += stats['caughtStealing']
            original['sb' + '_' + split] += stats['stolenBases']
            original['gidp' + '_' + split] += stats['groundIntoDoublePlay']
            original['total_pitches' + '_' + split] += stats['numberOfPitches']
            original['pa' + '_' + split] += stats['plateAppearances']
            original['total_bases' + '_' + split] += stats['totalBases']
            original['rbi' + '_' + split] += stats['rbi']
            original['sac_bunts' + '_' + split] += stats['sacBunts']
            original['sac_flies' + '_' + split] += stats['sacFlies']
            return original
        except KeyError:
            return compile_stats(new, split, original)
            
    seasons = [x for x in seasons if str(x) not in complete_seasons]
    for season in seasons:
        if season == cs and store_static and players:
            static_players = players
        for split in splits:
            call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 10000, 'sitCodes': split, 'season': season, 'group':'hitting', 'sportIds':sport, 'fields':fields, 'hydrate':hydrate}, force=True)
            data = call['stats'][0]['splits']
            total_splits = call['stats'][0]['totalSplits']
            total_returned = len(data)
            for p in data:
                player_id = p['player']['id']
                if player_id not in player_ids:
                    player=compile_stats(p, split, {})
                    player['season'] = season
                    players.append(player)
                    player_ids.add(player_id)
                else:
                    current_player = next(x for x in players if x['mlb_id'] == player_id)
                    player = combine_stats(current_player, p, split)
                    players = [x for x in players if x['mlb_id'] != player_id]
                    players.append(player)
            if get_all and total_splits != total_returned:
                flag = 0
                while total_splits != total_returned:
                    offset = total_returned
                    call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 10000, 'sitCodes': split, 'season': season, 'group':'hitting', 'sportIds':sport, 'fields':fields,'offset':offset, 'hydrate': hydrate}, force=True)
                    data = call['stats'][0]['splits']
                    total_offset = len(data)
                    flag += 1
                    if flag > 1:
                        print('Could not get all data.')
                        break
                    for p in data:
                        player_id = p['player']['id']
                        if player_id not in player_ids:
                            player=compile_stats(p)
                            player['season'] = season
                            players.append(player)
                            player_ids.add(player_id)
                        else:
                            current_player = next(x for x in players if x['mlb_id'] == player_id)
                            player = combine_stats(current_player,p)
                            players = [x for x in players if x['mlb_id'] != player_id]
                            players.append(player)
                    total_returned += total_offset
    with open(final_path, 'wb') as file:
        pickle.dump(players, file)
    if store_static:
        with open(static_path, 'wb') as file:
            if static_players:
                pickle.dump(static_players, file)
            else:
                pickle.dump(players, file)
        
    return players

def get_splits_p(seasons,sport=1,pool='ALL',get_all=True):
    splits=['vr','vl','sp','rp']
    cs = current_season()['season_id']
    fields='stats,totalSplits,splits,stat,id,player,groundOuts,airOuts,runs,doubles,triples,homeRuns,strikeOuts,baseOnBalls,intentionalWalks,hits,hitByPitch,avg,atBats,obp,slg,ops,caughtStealing,stolenBases,stolenBasePercentage,groundIntoDoublePlay,numberOfPitches,era,inningsPitched,earnedRuns,whip,battersFaced,outs,balls,strikes,strikePercentage,hitBatsmen,balks,wildPitches,pickoffs,totalBases,groundOutsToAirouts,rbi,pitchesPerInning,strikeoutWalkRatio,strikeoutsPer9Inn,walksPer9Inn,hitsPer9Inn,runsScoredPer9,homeRunsPer9,inheritedRunners,inheritedRunnersScored,inheritedRunnersStrandedPercentage,sacBunts,sacFlies,gamesStarted,gamesPitched,wins,losses,saves,saveOpportunities,holds,completeGames,shutouts,fullName,pitchHand,code'
    if len(seasons) > 1:
        final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_' + str(seasons[0]) + '-' + str(seasons[-1])))
        if str(seasons[-1]) == cs and len(seasons) > 2:
            static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_'  + str(seasons[0]) + '-' + str(seasons[-2])))
        elif str(seasons[-1]) == cs:
            static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_'  + str(seasons[0])))
        else: 
            static_path = final_path
    else:
        final_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_'  + str(seasons[0])))
        static_path = final_path
    if static_path.exists():
        store_static = False
        complete_seasons = re.findall('[0-9]{4}', str(static_path))
        with open(static_path, 'rb') as file:
            complete_data = pickle.load(file)
            players = complete_data
            player_ids = set()
            for player in players:
                player_ids.add(player['mlb_id'])
    else:
        player_ids = set()
        players = []
        complete_seasons = []
        store_static = True
        static_players = []
    def non_splits(player_data, season):
        player = {}
        if player_data['player'].get('stats'):
                key ='_' + str(season)[2:]
                player_non_splits = player_data['player']['stats'][0]['splits'][0]['stat']
                player['games' + key] = player_non_splits.get('gamesPitched',0)
                player['games_sp' + key] = player_non_splits.get('gamesStarted',0)
                player['wins' + key] = player_non_splits.get('wins',0)
                player['losses' + key] = player_non_splits.get('losses',0)
                player['saves' + key] = player_non_splits.get('saves',0)
                player['save_chances' + key] = player_non_splits.get('saveOpportunities',0)
                player['holds' + key] = player_non_splits.get('',0)
                player['complete_games' + key] = player_non_splits.get('completeGames',0)
                player['shutouts' + key] = player_non_splits.get('shutouts',0)
        return player
    def compile_stats(d, split, player, season):
            stats = d['stat']
            player['mlb_id'] = d['player']['id']
            player['name'] = d['player']['fullName']
            player['pitch_hand']  = d['player']['pitchHand']['code']
            player['gb' + '_' + split] = stats['groundOuts']
            player['fb' + '_' + split] = stats['airOuts']
            player['runs' + '_' + split] = stats['runs']
            player['2b' + '_' + split] = stats['doubles']
            player['3b' + '_' + split] = stats['triples']
            player['hr' + '_' + split] = stats['homeRuns']
            player['k' + '_' + split] = stats['strikeOuts']
            player['bb' + '_' + split] = stats['baseOnBalls']
            player['ibb' + '_' + split] = stats['intentionalWalks']
            player['hits' + '_' + split] = stats['hits']
            player['ab' + '_' + split] = stats['atBats']
            player['cs' + '_' + split] = stats['caughtStealing']
            player['sb' + '_' + split] = stats['stolenBases']
            player['gidp' + '_' + split] = stats['groundIntoDoublePlay']
            player['total_pitches' + '_' + split] = stats['numberOfPitches']
            player['total_bases' + '_' + split] = stats['totalBases']
            player['rbi' + '_' + split] = stats['rbi']
            player['sac_bunts' + '_' + split] = stats['sacBunts']
            player['sac_flies' + '_' + split] = stats['sacFlies']
            player['ir' + '_' + split] = stats.get('inheritedRunners', 0)
            player['irs' + '_' + split] = stats.get('inheritedRunnersScored',0)
            player['pickoffs' + '_' + split] = stats['pickoffs']
            player['wild_pitches' + '_' + split] = stats['wildPitches']
            player['balks' + '_' + split] = stats['balks']
            player['strikes' + '_' + split] = stats['strikes']
            player['balls' + '_' + split] = stats['balls']
            player['outs' + '_' + split] = stats['outs']
            player['hb' + '_' + split] = stats['hitBatsmen']
            player['batters_faced' + '_' + split] = stats['battersFaced']
            player['er' + '_' + split] = stats['earnedRuns']
            if split == 'vr':
                ns = non_splits(d, season)
                player.update(ns)
            return player
    def combine_stats(original, new, split, season):
        try:
            stats = new['stat']
            original['gb' + '_' + split] += stats['groundOuts']
            original['fb' + '_' + split] += stats['airOuts']
            original['runs' + '_' + split] += stats['runs']
            original['2b' + '_' + split] += stats['doubles']
            original['3b' + '_' + split] += stats['triples']
            original['hr' + '_' + split] += stats['homeRuns']
            original['k' + '_' + split] += stats['strikeOuts']
            original['bb' + '_' + split] += stats['baseOnBalls']
            original['ibb' + '_' + split] += stats['intentionalWalks']
            original['hits' + '_' + split] += stats['hits']
            original['ab' + '_' + split] += stats['atBats']
            original['cs' + '_' + split] += stats['caughtStealing']
            original['sb' + '_' + split] += stats['stolenBases']
            original['gidp' + '_' + split] += stats['groundIntoDoublePlay']
            original['total_pitches' + '_' + split] += stats['numberOfPitches']
            original['total_bases' + '_' + split] += stats['totalBases']
            original['rbi' + '_' + split] += stats['rbi']
            original['sac_bunts' + '_' + split] += stats['sacBunts']
            original['sac_flies' + '_' + split] += stats['sacFlies']
            original['ir' + '_' + split] += stats['inheritedRunners']
            original['irs' + '_' + split] += stats['inheritedRunnersScored']
            original['pickoffs' + '_' + split] += stats['pickoffs']
            original['wild_pitches' + '_' + split] += stats['wildPitches']
            original['balks' + '_' + split] += stats['balks']
            original['strikes' + '_' + split] += stats['strikes']
            original['balls' + '_' + split] += stats['balls']
            original['outs' + '_' + split] += stats['outs']
            original['hb' + '_' + split] += stats['hitBatsmen']
            original['batters_faced' + '_' + split] += stats['battersFaced']
            original['er' + '_' + split] += stats['earnedRuns']
            if split == 'vr':
                ns = non_splits(new, season)
                original.update(ns)
            return original
        except KeyError:
            return compile_stats(new, split, original, season)
    seasons = [x for x in seasons if str(x) not in complete_seasons]
    for season in seasons:
        if season == cs and store_static and players:
            static_players = players
        for split in splits:
            if split == 'vr':
                hydrate = f"person(stats(group=[pitching],type=[season],season={season}))"
                call = statsapi.get('stats', {'hydrate': hydrate, 'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields}, force=True)
            else:
                hydrate = 'person'
                call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields, 'hydrate': hydrate}, force=True)
            data = call['stats'][0]['splits']
            total_splits = call['stats'][0]['totalSplits']
            total_returned = len(data)
            for p in data:
                player_id = p['player']['id']
                if player_id not in player_ids:
                    player=compile_stats(p, split, {}, season)
                    players.append(player)
                    player_ids.add(player_id)
                else:
                    current_player = next(x for x in players if x['mlb_id'] == player_id)
                    player = combine_stats(current_player, p, split, season)
                    players = [x for x in players if x['mlb_id'] != player_id]
                    players.append(player)
            if get_all and total_splits != total_returned:
                flag = 0
                while total_splits != total_returned:
                    offset = total_returned
                    if split == 'vr':
                        call = statsapi.get('stats', {'hydrate': hydrate, 'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields,'offset':offset}, force=True)
                    else:
                        call = statsapi.get('stats', {'stats':'statSplits','playerPool':pool, 'limit': 1000, 'sitCodes': split, 'season': season, 'group':'pitching', 'sportIds':sport, 'fields':fields,'offset':offset}, force=True)
                    data = call['stats'][0]['splits']
                    total_offset = len(data)
                    flag += 1
                    if flag > 1:
                        print('Could not get all data.')
                        break
                    for p in data:
                            player_id = p['player']['id']
                            if player_id not in player_ids:
                                player=compile_stats(p, split, {}, season)
                                players.append(player)
                                player_ids.add(player_id)
                            else:
                                current_player = next(x for x in players if x['mlb_id'] == player_id)
                                player = combine_stats(current_player, p, split, season)
                                players = [x for x in players if x['mlb_id'] != player_id]
                                players.append(player)
                    total_returned += total_offset
    with open(final_path, 'wb') as file:
        pickle.dump(players, file)
    if store_static:
        with open(static_path, 'wb') as file:
            if static_players:
                pickle.dump(static_players, file)
            else:
                pickle.dump(players, file)
    return players


# optionally pass a filter: filt2={'start': '2016-04-01','end': '2016-06-01'}
def get_p_diff(player_id, season1, season2, filt1='', filt2=''):
    df1 = get_statcast_p(player_id, season1)
    df2 = get_statcast_p(player_id, season2)
    if filt1:
        end1 = filt1.get('end', pd.to_datetime('today'))
        start1 = filt1['start']
        df1 = df1[(df1['date'] >= start1) & (df1['date'] < end1)].reset_index()
        print('xxxxx')
    if filt1:
        end2 = filt2.get('end', pd.to_datetime('today'))
        start2 = filt2['start']
        df2 = df2[(df2['date'] >= start2) & (df2['date'] < end2)].reset_index()
        print('yyyyyy')
    speed_diff = df1['effectiveSpeed'].max() - df2['effectiveSpeed'].max()
    spin_diff = df1['releaseSpinRate'].max() - df2['releaseSpinRate'].max()
    launch_diff = df1['launchAngle'].max() - df2['launchAngle'].max()
    return {'speed': speed_diff, 'spin': spin_diff, 'launch': launch_diff}