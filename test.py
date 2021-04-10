from dfs_tools_mlb import settings
from dfs_tools_mlb.utils.storage import pickle_path
cs = current_season()['season_id']
seasons = range(settings.stat_range['player_start'], settings.stat_range['end'])
len(seasons)
str(seasons[-1])== cs
static_path = settings.ARCHIVE_DIR.joinpath(pickle_path(name='p_data_'  + str(seasons[0]) + '-' + str(seasons[-2])))
import pandas as pd
test = pd.read_pickle(static_path)
test.columns.tolist()
p_splits = pd.DataFrame(get_splits_p(range(settings.stat_range['player_start'], settings.stat_range['end']),sport=1,pool='ALL',get_all=True))

pd.DataFrame(get_splits_h(range(settings.stat_range['player_start'], settings.stat_range['end']),sport=1,pool='ALL',get_all=True))
from dfs_tools_mlb.compile.historical_data import current_season
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

test = {"stat":{"gamesStarted":1,"groundOuts":6,"airOuts":6,"runs":0,"doubles":1,"triples":0,"homeRuns":0,"strikeOuts":5,"baseOnBalls":3,"intentionalWalks":1,"hits":3,"hitByPitch":1,"avg":".150","atBats":20,"obp":".292","slg":".200","ops":".492","caughtStealing":0,"stolenBases":1,"stolenBasePercentage":"1.000","groundIntoDoublePlay":1,"numberOfPitches":81,"era":"0.00","inningsPitched":"6.0","wins":1,"losses":0,"saves":0,"saveOpportunities":0,"holds":0,"earnedRuns":0,"whip":"1.00","battersFaced":24,"outs":18,"gamesPitched":1,"completeGames":0,"shutouts":0,"balls":28,"strikes":53,"strikePercentage":".650","hitBatsmen":1,"balks":0,"wildPitches":1,"pickoffs":0,"totalBases":4,"groundOutsToAirouts":"1.00","rbi":0,"pitchesPerInning":"13.50","strikeoutWalkRatio":"1.67","strikeoutsPer9Inn":"7.50","walksPer9Inn":"4.50","hitsPer9Inn":"4.50","runsScoredPer9":"0.00","homeRunsPer9":"0.00","inheritedRunners":0,"inheritedRunnersScored":0,"inheritedRunnersStrandedPercentage":"-.--","sacBunts":0,"sacFlies":0},"player":{"id":642203,"fullName":"Taylor Widener","pitchHand":{"code":"R"}}}
test.keys()
test['stat'].keys()
