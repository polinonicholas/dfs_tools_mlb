import statsapi
from functools import cached_property
import re
import pandas as pd
from dfs_tools_mlb.utils.pd import sm_merge_arb, sm_merge
from dfs_tools_mlb.compile import current_season as cs
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac
from dfs_tools_mlb.utils.statsapi import full_schedule
from dfs_tools_mlb.utils.storage import json_path
import json
from dfs_tools_mlb.compile.static_mlb import team_info
from json import JSONDecodeError
from dfs_tools_mlb.utils.strings import plural
from dfs_tools_mlb.compile.stats_fangraphs import Stats
from dfs_tools_mlb.compile import game_data

class Team:
    def __init__(self, mlb_id, name='',):
        self.id = mlb_id
        self.name = name
    @cached_property
    def depth_get(self):
        print(f"Getting {self.name.capitalize()}' depth chart.")
        return statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'depthChart'})['roster']
    @cached_property
    def depth_lu(self, player_ids=set()):
        for player in self.depth_get:
            player_ids.add(str(player['person']['id']))
        ids = ",".join(player_ids)
        print(f"Getting information for players on {self.name.capitalize()}' depth chart.")
        return statsapi.get('people', {'personIds': ids})['people']
    @cached_property
    def depth(self):
        depth = {
            'starters': [],
            'bullpen': [],
            'hitters': []
            }
        for p in self.depth_get:
            p_id = p['person']['id']
            p_info = next(x for x in self.depth_lu if x['id'] == p_id)
            player = {'mlb_id': p_info['id'],
                      'name': p_info.get('fullName', ''),
                      'mlb_api': p_info.get('link', ''),
                      'number': p_info.get('primaryNumber', ''),
                      'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                      'weight': p_info.get('weight', ''),
                      'nickname': p_info.get('nickName', ''),
                      'debut': p_info.get('mlbDebutDate', ''),
                      'bat_side': p_info.get('batSide', {}).get('code', ''),
                      'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                      'age': p_info.get('currentAge', ''),
                      'note': p.get('note', ''),
                      'position_type': p['position']['type'],
                      'position': p['position']['code'],
                      'status': p.get('status', {}).get('code', ''),
                      'team': self.name
                      }
            position = str(player['position'])
            if position in mac.players.h:
                depth['hitters'].append(player)
            elif position in mac.players.sp:
                depth['starters'].append(player)
            elif position in mac.players.bullpen:
                depth['bullpen'].append(player)
        return depth
    @cached_property
    def full_roster_get(self):
        print(f"Getting {self.name.capitalize()}' full roster.")
        return statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'fullRoster'})['roster'] 
    @cached_property
    def full_roster_lu(self, player_ids=set()):
        for player in self.full_roster_get:
            player_ids.add(str(player['person']['id']))
        ids = ",".join(player_ids)
        print(f"Getting information for players on {self.name.capitalize()}' full-roster.")
        return statsapi.get('people', {'personIds': ids})['people']
    @cached_property
    def full_roster(self):
        roster = []
        for p in self.full_roster_get:
            p_id = p['person']['id']
            p_info = next(x for x in self.full_roster_lu if x['id'] == p_id)
            player = {'mlb_id': p_info['id'],
                      'name': p_info.get('fullName', ''),
                      'mlb_api': p_info.get('link', ''),
                      'number': p_info.get('primaryNumber', ''),
                      'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                      'weight': p_info.get('weight', ''),
                      'nickname': p_info.get('nickName', ''),
                      'debut': p_info.get('mlbDebutDate', ''),
                      'bat_side': p_info.get('batSide', {}).get('code', ''),
                      'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                      'age': p_info.get('currentAge', ''),
                      'position_type': p['position']['type'],
                      'position': p['position']['code'],
                      'status': p.get('status', {}).get('code', ''),
                      'team': self.name
                      }
            roster.append(player)
        roster.extend(self.nri)
        return roster
    @cached_property
    def nri_get(self):
        print(f"Getting {self.name.capitalize()}' non-roster invitees.")
        return statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'nonRosterInvitees'})['roster']
    @cached_property
    def nri_lu(self, player_ids=set()):
        if self.nri_get:
            for player in self.nri_get:
                player_ids.add(str(player['person']['id']))
            ids = ",".join(player_ids)
            print(f"Getting information for {self.name.capitalize()}' non-roster-invitees.")
            return statsapi.get('people', {'personIds': ids})['people']
        return []
    @cached_property
    def nri(self):
        nri = []
        if self.nri_get:
            for p in self.nri_get:
                p_id = p['person']['id']
                p_info = next(x for x in self.nri_lu if x['id'] == p_id)
                player = {'mlb_id': p_info['id'],
                          'name': p_info.get('fullName', ''),
                          'mlb_api': p_info.get('link', ''),
                          'number': p_info.get('primaryNumber', ''),
                          'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                          'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                          'weight': p_info.get('weight', ''),
                          'nickname': p_info.get('nickName', ''),
                          'debut': p_info.get('mlbDebutDate', ''),
                          'bat_side': p_info.get('batSide', {}).get('code', ''),
                          'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                          'age': p_info.get('currentAge', ''),
                          'position_type': p['position']['type'],
                          'position': p['position']['code'],
                          'status': p.get('status', {}).get('code', ''),
                          'team': self.name
                          }
                nri.append(player)
        return nri
    #list of integers representing ids of every affiliated player.
    @cached_property
    def all_player_ids(self):
        player_ids = []
        nri_ids = []
        for player in self.nri_get:
            nri_ids.append(player['person']['id'])
        for player in self.full_roster_get:
            player_ids.append(player['person']['id'])
        player_ids.extend(nri_ids)
        return player_ids
    @cached_property
    def hitter(self):
        hitters = pd.DataFrame(self.depth['hitters']).apply(pd.to_numeric, errors = 'ignore')
        hitters['h'] = True
        players = Stats.current_stats()
        hitters = sm_merge_arb(hitters, players, columns=['name', 'team', 'h'], ratios=[.8, 1, 1], suffixes=('_mlb', '_fg'))
        hitters.columns = hitters.columns.str.replace("_mlb", '')
        return hitters
    @cached_property
    def starter(self):
        starters = pd.DataFrame(self.depth['starters']).apply(pd.to_numeric, errors = 'ignore')
        starters['p'] = True
        players = Stats.current_stats()
        starters = sm_merge_arb(starters, players, columns=['name', 'team', 'h'], ratios=[.8, 1, 1], suffixes=('_mlb', '_fg'))
        starters.columns = starters.columns.str.replace("_mlb", '')
        return starters
    @cached_property
    def bullpen(self):
        bullpen = pd.DataFrame(self.depth['bullpen']).apply(pd.to_numeric, errors = 'ignore')
        bullpen['p'] = True
        players = Stats.current_stats()
        bullpen = sm_merge_arb(bullpen, players, columns=['name', 'team', 'h'], ratios=[.8, 1, 1], suffixes=('_mlb', '_fg'))
        bullpen.columns = bullpen.columns.str.replace("_mlb", '')
        bullpen['team']
        return bullpen
    @cached_property
    def catcher(self):
        return self.hitter[self.hitter['position'].astype('int') == 2]
    @cached_property
    def first_base(self):
        return self.hitter[self.hitter['position'].astype('int') == 3]
    @cached_property
    def second_base(self):
        return self.hitter[self.hitter['position'].astype('int') == 4]
    @cached_property
    def third_base(self):
        return self.hitter[self.hitter['position'].astype('int') == 5]
    @cached_property
    def shorstop(self):
        return self.hitter[self.hitter['position'].astype('int') == 6]
    @cached_property
    def left_field(self):
        return self.hitter[self.hitter['position'].astype('int') == 7]
    @cached_property
    def center_field(self):
        return self.hitter[self.hitter['position'].astype('int') == 8]
    @cached_property
    def right_field(self):
        return self.hitter[self.hitter['position'].astype('int') == 9]
    @cached_property
    def dh(self):
        return self.hitter[self.hitter['position'].astype('int') == 10]
    @cached_property
    def infield(self):
        return self.hitter[self.hitter['position_type'].str.casefold().str.contains('infield')]
    @cached_property
    def outfield(self):
        return self.hitter[self.hitter['position_type'].str.casefold().str.contains('outfield')]
    @cached_property
    def pitcher(self):
        pitchers = self.starter.combine_first(self.bullpen)
        return pitchers
    @cached_property
    def coaches(self):
        print(f"Getting {self.name.capitalize()}' coaches.")
        data = statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'coach'})['roster']
        coaches = []
        for c in data:
            coach = {'mlb_id': c.get('person', {}).get('id', ''),
                     'name': c.get('person', {}).get('fullName', ''),
                     'mlb_api': c.get('person', {}).get('link', ''),
                     'job': c.get('job', '')
                }
            coaches.append(coach)
        return pd.DataFrame(coaches).set_index('mlb_id')
    @cached_property
    def manager(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('manage')]
    @cached_property
    def bench_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('bench')]
    @cached_property
    def hitting_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('hitting')]
    @cached_property
    def pitching_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('pitch')]
    @cached_property
    def first_base_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('first')]
    @cached_property
    def third_base_coach(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('third')]
    @cached_property
    def bullpen_staff(self):
        return self.coaches[self.coaches['job'].str.casefold().str.contains('bullpen')]
    @cached_property
    def all_games(self):
        return full_schedule(team=self.id, start_date=cs.spring_start, end_date=cs.playoff_end)
    @cached_property
    def future_games(self):
        return [x for x in self.all_games if x['games'][0]['status']['codedGameState'] in mac.game_status.pre]
    @cached_property
    def past_games(self):
        return [x for x in self.all_games if x['games'][0]['status']['codedGameState'] in mac.game_status.post]
    @cached_property
    def next_game_pk(self):
        try:
            return self.future_games[0]['games'][0]['gamePk']
        except IndexError:
            print("No games on schedule.")
            return False
    @cached_property
    def last_game_pk(self):
        try:
            return self.past_games[-1]['games'][0]['gamePk']
        except IndexError:
            print(f'The {self.name.capitalize()} have yet to play in {cs.season_id}.')
            return False
    @cached_property
    def next_game(self):
        if self.next_game_pk:
            print(f"Getting boxscore for {self.name.capitalize()}' next game.")
            return statsapi.get('game', {'gamePk': self.next_game_pk})
        return self.next_game_pk
    @cached_property
    def last_game(self):
        if self.last_game_pk:
            print(f"Getting boxscore {self.name.capitalize()}' last game.")
            return statsapi.get('game', {'gamePk': self.last_game_pk})
        return self.last_game_pk
    @cached_property
    def is_home(self):
        if self.next_game_pk:
            return int(self.next_game['gameData']['teams']['home']['id']) == self.id
        return self.next_game_pk
    @cached_property
    def was_home(self):
        if self.last_game_pk:
            return int(self.last_game['gameData']['teams']['home']['id']) == self.id
        return self.last_game_pk
    
    @cached_property
    def ha(self):
        if self.next_game_pk:
            if self.is_home:
                return 'home'
            else:
                return 'away'
        return self.next_game_pk
    
    @cached_property
    def opp_ha(self):
        if self.next_game_pk:
            if self.ha == 'home':
                return 'away'
            elif self.ha == 'away':
                return 'home'
        return self.next_game_pk
    
    @cached_property
    def was_ha(self):
        if self.last_game_pk:
            if self.was_home:
                return 'home'
            else:
                return 'away'
        return self.last_game_pk
    @cached_property
    def opp_was_ha(self):
        if self.last_game_pk:
            if self.was_ha == 'home':
                return 'away'
            elif self.was_ha == 'away':
                return 'home'
        return self.last_game_pk
    @cached_property
    def opp_sp(self):
        if self.next_game_pk:
            try:
                sp_id = 'ID' + str(self.next_game['gameData']['probablePitchers'][self.opp_ha]['id'])
                sp_info = self.next_game['gameData']['players'][sp_id]
                return sp_info
            except KeyError:
                return f"The {self.opp_name} have not confirmed their starting pitcher."
        return self.next_game_pk
    @cached_property
    def last_opp_sp(self):
        if self.last_game_pk:
            sp_id = 'ID' + str(self.last_game['gameData']['probablePitchers'][self.opp_was_ha]['id'])
            sp_info = self.last_game['gameData']['players'][sp_id]
            return sp_info
        return self.last_game_pk
    @cached_property
    def opp_sp_hand(self):
        try:
            return self.opp_sp['pitchHand']['code']
        except TypeError:
            print(f"{self.opp_sp} returning default 'R'")
            return 'R'
    @cached_property
    def last_opp_sp_hand(self):
        if self.last_game_pk:
            return self.last_opp_sp['pitchHand']['code']
        return self.last_game_pk
    @cached_property
    def opp_name(self):
        if self.next_game_pk:
            return self.next_game['gameData']['teams'][self.opp_ha]['teamName']
        return self.next_game_pk
    @staticmethod
    def lineups():
        path = json_path(name='team_lineups')
        try:
            with open(path) as file:
                team_lineups = json.load(file)
                file.close()
            return team_lineups
        except (FileNotFoundError, JSONDecodeError):
            team_lineups = {}
            for team in team_info.keys():
                team_lineups[team] = {'L': [], 'R': []}
            with open(path, "w+") as file:
                json.dump(team_lineups, file)
                file.close()
            return team_lineups
    @cached_property
    def lineup(self):
        if self.next_game_pk:
            lineup = self.next_game['liveData']['boxscore']['teams'][self.ha]['battingOrder']
            if len(lineup) == 9:
                self.update_lineup(lineup, self.opp_sp_hand)
                return lineup
            else:
                return self.projected_lineup
        return self.next_game_pk
    @cached_property
    def projected_lineup(self):
        team_lineups = Team.lineups()
        try:
            if len(team_lineups[self.name][self.opp_sp_hand]) == 9:
                return team_lineups[self.name][self.opp_sp_hand]
            else:
                lineup = []
                for play in self.last_game['liveData']['plays']['allPlays']:
                    batter_id = play['matchup']['batter']['id']
                    if batter_id in self.all_player_ids:
                        lineup.append(play['matchup']['batter']['id'])
                    if len(lineup) == 9:
                        break
                self.update_lineup(lineup, self.last_opp_sp_hand)
                return lineup
        except (KeyError, TypeError):
            return self.last_game_pk + "projected lineup will be available after first spring training game."
    def update_lineup(self, lineup, hand):
        team_lineups = Team.lineups()
        team_lineups[self.name][hand] = lineup
        with open(json_path(name="team_lineups"), "w+") as file:
            json.dump(team_lineups, file)
            file.close()
        return lineup
    def lineup_df(self):
        try:
            lineup_ids = self.lineup
            lineup = pd.DataFrame(lineup_ids, columns= ["mlb_id"])
            roster = pd.DataFrame(self.full_roster)
            merged = pd.merge(lineup, roster, on="mlb_id")
            stats = Stats.current_stats()
            lineup_stats = sm_merge(merged, stats, columns=['name', 'team'], ratios=[1, 1], prefix='m_', reset_index=False, post_drop=True, suffixes=('_mlb', '_fg'))
            def merge_closest(stats_df):
                ratio = .99
                while len(stats_df.index) < 9:
                    stats_df = sm_merge(merged, stats, columns=['name', 'team'], ratios=[ratio, 1], prefix='m_', reset_index=True, post_drop=True, suffixes=('_mlb', '_fg'))
                    ratio -= .01
                    if ratio < .8:
                        break
                stats_df = stats_df[stats_df['mlb_id'].isin(lineup_ids)]
                if len(stats_df.index) != 9:
                    if len(stats_df.index) < 9:
                        for i_d in lineup_ids:
                            if i_d not in stats_df['mlb_id'].values:
                                new_row = roster[roster['mlb_id'] == i_d]
                                stats_df = stats_df.append(new_row, ignore_index = True)
                    if len(stats_df.index) > 9:
                        stats_df = stats_df.loc[~stats_df.duplicated(subset=['mlb_id'])]
                sorter = dict(zip(lineup_ids, range(len(lineup_ids))))
                stats_df['order'] = stats_df['mlb_id'].map(sorter)
                stats_df.sort_values(by='order',inplace=True, kind='mergesort ')
                stats_df['order'] = stats_df['order'] + 1
                stats_df.reset_index(inplace=True, drop=True)
                        
                return stats_df
            return merge_closest(lineup_stats)
        except ValueError:
            return self.lineup
        
    def live_game(self):
        try:
            game = next(x for x in self.all_games if x['games'][0]['status']['codedGameState'] in mac.game_status.live)['games'][0]
            game_pk = game['gamePk']
            print(game_pk)
            game_box = statsapi.get('game', {'gamePk': game_pk})
        except(StopIteration, TypeError):
            return f"The {self.name.capitalize()} are not currently playing."
        all_plays = game_box['liveData']['plays']['allPlays']
        scoring_plays = game_box['liveData']['plays']['scoringPlays'] #integer_list
        current_play = game_box['liveData']['plays']['currentPlay']
        home_name = game_box['gameData']['teams']['home']['teamName']
        away_name = game_box['gameData']['teams']['away']['teamName']
        game_line = game_box['liveData']['linescore']
        home_score = game_line['teams']['home']['runs']
        away_score = game_line['teams']['away']['runs']
        inning_pre = game_line['inningHalf']
        current_inning = game_line['currentInning']
        home_hits = game_line['teams']['home']['hits']
        away_hits = game_line['teams']['away']['hits']
        total_hits = home_hits + away_hits
        if not scoring_plays:
            pass
        else:
            print('All Scoring Plays:')
            for play in all_plays:
                play_index = play.get('atBatIndex', 1000)
                if play_index > scoring_plays[-1]:
                    break
                elif play_index in scoring_plays:
                    score_event = play.get('result', {}).get('description', "No description.")
                    print("-" + re.sub(' +', ' ', score_event))
        current_batter = current_play['matchup']['batter']['fullName']
        current_pitcher = current_play['matchup']['pitcher']['fullName']
        runners = current_play['runners']
        while not runners:
            try:
                flag = -2
                runners = all_plays[flag]['runners']
                flag -= 1
            except IndexError:
                break
        count = current_play['count']
        outs = count['outs']
        out_string = plural(outs,'out')
        strikes = count['strikes']
        balls = count['balls']
        print(f"At bat: {current_batter}")
        print(f"Pitching: {current_pitcher}")
        print(f"Count: {balls}-{strikes}, {outs} {out_string}")
        if runners:
            runner_d = {}
            for runner in runners:
                runner_name = runner['details']['runner']['fullName']
                runner_base = runner['movement']['end']
                runner_d[runner_name] = runner_base
            for k, v in runner_d.items():
                if v:
                    if str(v) == 'score':
                        print(f"{k} {v}d.")
                    else:
                        print((f"{k} is on {v}."))
        print(f"{away_name}: {away_score}")
        print(f"{home_name}: {home_score}")
        print(f"{inning_pre} {current_inning}")
        print(f"Total hits: {total_hits}")
        try:
            print('Last update:')
            last_event = next(x for x in reversed(all_plays) if x['result'].get('description'))['result']['description']
            print(re.sub(' +', ' ', last_event))
            
        except StopIteration:
            pass
        return None
    
    @cached_property
    def is_new_series(self):
        if self.next_game_pk:
            return self.future_games[0]['games'][0]['seriesGameNumber'] == 1
        return self.next_game_pk     
    @cached_property
    def projected_ump(self):
        def get_official(game,ump_type):
            try:
                officials = game['liveData']['boxscore']['officials']
                projected_official = next(x for x in officials if x['officialType'] == ump_type)
                return projected_official['official']['id']
            except StopIteration:
                return 'Unable to project umpire.'
        if self.is_new_series:
            return (get_official(self.last_game, 'First Base'))
        elif self.next_game['liveData']['boxscore']['officials']:
            return (get_official(self.next_game,'Home Plate'))
        else:
            return 'Unable to project umpire.'
    @cached_property
    def next_venue(self):
        if self.next_game_pk:
            return self.next_game['gameData']['venue']['id']
        return self.next_game_pk
    
    @cached_property
    def last_venue(self):
        if self.last_game_pk:
            return self.next_game['gameData']['venue']['id']
        return self.last_game_pk
    @cached_property
    def home_venue(self):
        return team_info[self.name]['venue']['id']
            
    @cached_property
    def next_venue_data(self):
        if self.next_game_pk:
            return game_data[game_data['venue_id'] == self.next_venue]
        return self.next_game_pk
   
    @cached_property
    def last_venue_data(self):
        if self.last_game_pk:
            return game_data[game_data['venue_id'] == self.next_venue]
        return self.next_game_pk
    
    @cached_property
    def home_venue_data(self):
        return game_data[game_data['venue_id'] == self.home_venue]
    @cached_property
    def weather(self):
        if self.next_game_pk:
            return self.next_game['gameData']['weather']
        return self.next_game_pk
    @cached_property
    def wind_speed(self):
        avg_wind = self.next_venue_data['wind_speed'].mean()
        if self.weather:
            wind_description = self.weather.get('wind')
            if wind_description:
                try:
                    return int(wind_description.split(' ')[0])
                except (TypeError, ValueError):
                    if not np.isnan(avg_wind):
                        return avg_wind
                    else:
                        return game_data['wind_speed'].mean()
            else:
                if not np.isnan(avg_wind):
                    return avg_wind
                else:
                    return game_data['wind_speed'].mean()
        else:
            if not np.isnan(avg_wind):
                return avg_wind
            else:
                return game_data['wind_speed'].mean()
    @cached_property
    def wind_direction(self):
        wind_direction = 'None'
        if self.weather:
            wind_description = self.weather.get('wind')
            if wind_description:
                return wind_description.split(', ')[-1]
        return wind_direction
    @cached_property
    def venue_condition(self):
        if self.weather:
            condition = self.weather.get('condition')
            if condition:
                return condition
            else:
                try:
                    return self.next_venue_data['condition'].mode().iloc[0]
                except IndexError:
                    if self.is_home == self.was_home and self.last_game_pk:
                        try:
                            return self.last_game['gameData']['weather']['condition']
                        except KeyError:
                            return game_data['condition'].mode().iloc[0]
                    else:
                        return game_data['condition'].mode().iloc[0]
        else:
            try:
                return self.next_venue_data['condition'].mode().iloc[0]
            except IndexError:
                if self.is_home == self.was_home and self.last_game_pk:
                    try:
                        return self.last_game['gameData']['weather']['condition']
                    except KeyError:
                        return game_data['condition'].mode().iloc[0]
                else:
                    return game_data['condition'].mode().iloc[0]
athletics = Team(mlb_id = 133, name = 'athletics')
pirates = Team(mlb_id = 134, name = 'pirates')
padres = Team(mlb_id = 135, name = 'padres')
mariners = Team(mlb_id = 136, name = 'mariners')
giants = Team(mlb_id = 137, name = 'giants')
cardinals = Team(mlb_id = 138, name = 'cardinals')
rays = Team(mlb_id = 139, name = 'rays')
rangers = Team(mlb_id = 140, name = 'rangers')
blue_jays = Team(mlb_id = 141, name = 'blue jays')
twins = Team(mlb_id = 142, name = 'twins')
phillies = Team(mlb_id = 143, name = 'phillies')
braves = Team(mlb_id = 144, name = 'braves')
white_sox = Team(mlb_id = 145, name = 'white sox')
marlins = Team(mlb_id = 146, name = 'marlins')
yankees = Team(mlb_id = 147, name = 'yankees')
brewers = Team(mlb_id = 158, name = 'brewers')
angels = Team(mlb_id = 108, name = 'angels')
diamondbacks = Team(mlb_id = 109, name = 'diamondbacks')
orioles = Team(mlb_id = 110, name = 'orioles')
red_sox = Team(mlb_id = 111, name = 'red sox')
cubs = Team(mlb_id = 112, name = 'cubs')
reds = Team(mlb_id = 113, name = 'reds')
indians = Team(mlb_id = 114, name = 'indians')
rockies = Team(mlb_id = 115, name = 'rockies')
tigers = Team(mlb_id = 116, name = 'tigers')
astros = Team(mlb_id = 117, name = 'astros')
royals = Team(mlb_id = 118, name = 'royals')
dodgers = Team(mlb_id = 119, name = 'dodgers')
nationals = Team(mlb_id = 120, name = 'nationals')
mets = Team(mlb_id = 121, name = 'mets')


import numpy as np
test = game_data.loc[(game_data['umpire'] == cubs.projected_ump), ['fd_points']].mean()['fd_points']
np.isnan(test)
                    
mets.venue_condition
mets.weather
# test = game_data[game_data['wind_speed'] == '1']
# test['runs'].mode().iloc[0]