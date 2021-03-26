import statsapi
from functools import cached_property, lru_cache
import re
import pandas as pd
from dfs_tools_mlb.utils.pd import sm_merge
from dfs_tools_mlb.compile import current_season as cs
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac
from dfs_tools_mlb.utils.statsapi import full_schedule
from dfs_tools_mlb.utils.storage import json_path
import json
from dfs_tools_mlb.compile.static_mlb import team_info
from json import JSONDecodeError
from dfs_tools_mlb.utils.strings import plural
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.dataframes.game_data import game_data
from dfs_tools_mlb.compile.fanduel import fd_weights as fw
from dfs_tools_mlb.utils.storage import pickle_path
import numpy as np
import pickle
import datetime
from dfs_tools_mlb import settings
from math import floor, ceil
from dfs_tools_mlb.dataframes.stat_splits import (h_splits, p_splits, h_q, h_q_vr,
                                                  h_q_vl, p_q_rp, p_q_sp, p_q_vl,
                                                  p_q_vr, p_q)


class IterTeam(type):
    def __iter__(cls):
        return iter(cls._all_teams)
    
class Team(metaclass=IterTeam):
    _all_teams = []
    def __init__(self, mlb_id, name=''):
        self._all_teams.append(self)
        self.id = mlb_id
        self.name = name
        self.confirmed_lu = False
        
    @cached_property
    def depth_get(self):
        print(f"Getting {self.name.capitalize()}' depth chart.")
        return statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'depthChart'})['roster']
    
    @cached_property
    def depth_lu(self):
        player_ids=set()
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
        return statsapi.get('team_roster', {'teamId': self.id, 'rosterType': '40Man'})['roster'] 
    
    @cached_property
    def full_roster_lu(self):
        player_ids=set()
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
            position = str(player['position'])
            if position in mac.players.h:
                player['h'] = True
            elif position in mac.players.sp:
                player['sp'] = True
            elif position in mac.players.bullpen:
                player['bp'] = True
            elif position in mac.players.twp:
                player['p'] = True
                player['h'] = True
            roster.append(player)
        roster.extend(self.nri)
        return roster
    @lru_cache
    def batters(self, active=False):
        roster = pd.DataFrame(self.full_roster)
        batters = roster[roster['h'] == True]
        if active:
            batters = batters[batters['status'] == 'A']
        batters = batters.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        return batters
    
    @cached_property
    def nri_get(self):
        print(f"Getting {self.name.capitalize()}' non-roster invitees.")
        return statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'nonRosterInvitees'})['roster']
    
    @cached_property
    def nri_lu(self):
        player_ids=set()
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
                position = str(player['position'])
                if position in mac.players.h:
                    player['h'] = True
                elif position in mac.players.sp:
                    player['sp'] = True
                    player['p'] = True
                elif position in mac.players.bullpen:
                    player['bp'] = True
                    player['p'] = True
                elif position in mac.players.twp:
                    player['p'] = True
                    player['h'] = True
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
        hitters = hitters.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        return hitters
    
    @cached_property
    def starter(self):
        starters = pd.DataFrame(self.depth['starters']).apply(pd.to_numeric, errors = 'ignore')
        starters = starters.join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        starters['p'] = True
        return starters
    
    @cached_property
    def bullpen(self):
        bullpen = pd.DataFrame(self.depth['bullpen']).apply(pd.to_numeric, errors = 'ignore')
        bullpen = bullpen.join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        bullpen['p'] = True
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
    def proj_sp(self):
        if self.next_game_pk:
            try:
                sp_id = 'ID' + str(self.next_game['gameData']['probablePitchers'][self.ha]['id'])
                sp_info = self.next_game['gameData']['players'][sp_id]
                return sp_info
            except KeyError:
                return f"The {self.name} have not confirmed their starting pitcher."
        return self.next_game_pk
    
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
    def projected_sp(self):
        if self.next_game_pk:
            try:
                sp_id = 'ID' + str(self.next_game['gameData']['probablePitchers'][self.ha]['id'])
                sp_info = self.next_game['gameData']['players'][sp_id]
                self.confirmed_sp = True
                return sp_info
            except KeyError:
                return f"The {self.name} have not confirmed their starting pitcher."
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
    def o_split(self):
        if self.opp_sp_hand == 'R':
            return 'vr'
        else:
            return 'vl'
    
    @cached_property
    def last_opp_sp_hand(self):
        if self.last_game_pk:
            return self.last_opp_sp['pitchHand']['code']
        return self.last_game_pk
    
    @cached_property
    def opp_name(self):
        if self.next_game_pk:
            return self.next_game['gameData']['teams'][self.opp_ha]['teamName'].lower()
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
    @staticmethod
    def drop(df,filt):
        df = df.loc[~df.duplicated(subset=['mlb_id'])]
        df = df[df['mlb_id'].isin(filt)]
        return df
    @staticmethod
    def default_sp():
        empty = p_q_sp[p_q_sp['mlb_id'] == 0]
        defaults = p_q_sp.median()
        empty = empty.append(defaults, ignore_index = True)
        empty['name'] = 'Unknown'
        empty['pitch_hand'] = 'R'
        return empty
    
    @cached_property
    def lineup(self):
        if self.next_game_pk:
            if self.confirmed_lineup:
                lineup =  Team.lineups[self.name][self.opp_sp_hand]
                if len(lineup) == 9:
                    return lineup
            lineup = self.next_game['liveData']['boxscore']['teams'][self.ha]['battingOrder']
            if len(lineup) == 9:
                self.update_lineup(lineup, self.opp_sp_hand)
                self.confirmed_lineup = True
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
                plays = self.last_game['liveData']['plays']['allPlays']
                if self.was_home:
                    h = False
                else:
                    h = True
                for play in plays:
                    if play['about']['isTopInning'] == h:
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
    
    def get_replacement(self, position, position_type, excluded):
        roster = pd.DataFrame(self.full_roster)
        roster = roster.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        h_key = 'fd_wps_pa_' + self.o_split
        if str(position) in mac.players.p:
            if not self.is_dh:
                try:
                    projected_sp = roster[roster['mlb_id'] == self.projected_sp['id']]
                except TypeError:
                    starters = self.starter[~self.starter['mlb_id'].isin(excluded)]
                    return starters.loc[starters['fd_ps_s'].idxmax()]
                if len(projected_sp.index) != 0:
                    return projected_sp.loc[projected_sp['age'].idxmax()]
                else:
                    starters = self.starter[~self.starter['mlb_id'].isin(excluded)]
                    return starters.loc[starters['fd_ps_s'].idxmax()]
            else: 
                hitters = self.hitter[~self.hitter['mlb_id'].isin(excluded)]
                return hitters.loc[hitters[h_key].idxmax()]
        else:
            hitters = self.hitter[~self.hitter['mlb_id'].isin(excluded)]
            hitters = hitters[hitters['position'].astype(str) == position]
            projected_replacement = hitters.loc[hitters[h_key].idxmax()]
            if len(projected_replacement) != 0:
                return projected_replacement
            else:
                hitters = self.hitter[~self.hitter['mlb_id'].isin(excluded)]
                hitters = hitters[hitters['position_type'].astype(str) == position_type]
                projected_replacement = hitters.loc[hitters[h_key].idxmax()]
                if len(projected_replacement) != 0:
                    return projected_replacement
                else:
                    hitters = self.hitter[~self.hitter['mlb_id'].isin(excluded)]
                    return hitters.loc[hitters[h_key].idxmax()]
    
    def lineup_df(self):
        data_path = pickle_path(name=f"{self.name}_lu_{tf.today}", directory=settings.LINEUP_DIR)
        if not data_path.exists():
            lineup_ids = self.lineup
            lineup = pd.DataFrame(lineup_ids, columns= ["mlb_id"])
            roster = pd.DataFrame(self.full_roster)
            merged = pd.merge(lineup, roster, on="mlb_id")
            h_df = merged.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            roster = roster.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            if settings.use_fangraphs:
                def drop(df):
                    df = df.loc[~df.duplicated(subset=['mlb_id'])]
                    df = df[df['mlb_id'].isin(lineup_ids)]
                    return df
                from dfs_tools_mlb.compile.stats_fangraphs import Stats
                fg_stats = Stats.current_stats()
                h_df = sm_merge(h_df, fg_stats, columns=['name', 'team'], ratios=[.63, 1], prefix='m_', reset_index=False, post_drop=True, suffixes=('', '_fg'))
                h_df = drop(h_df)
            h_df = h_df[~h_df['position'].astype(str).isin(mac.players.p)]
            if not self.is_dh and len(h_df.index) == 9:
                h_key = 'fd_wps_pa_' + self.o_split
                i_d = h_df.loc[h_df[h_key].idxmin(), 'mlb_id'].item()
                h_df.drop(h_df[h_key].idxmin(), inplace=True)
                new_row = self.get_replacement('S', 'pitcher', lineup_ids)
                h_df = h_df.append(new_row, ignore_index = True)
                lineup_ids.remove(i_d)
                lineup_ids.append(int(new_row['mlb_id']))
            if len(h_df.index) < 9:
                for i_d in lineup_ids:
                    if i_d not in h_df['mlb_id'].values:
                        new_row = roster[roster['mlb_id'] == i_d]
                        new_row = new_row[~new_row['position'].astype(str).isin(mac.players.p)]
                        if len(new_row.index) == 0:
                            player = statsapi.get('people', {'personIds': i_d})
                            position = player['people'][0]['primaryPosition']['code']
                            position_type = player['people'][0]['primaryPosition']['type']
                            new_row = self.get_replacement(position, position_type, lineup_ids)
                            index = lineup_ids.index(i_d)
                            lineup_ids[index] = int(new_row['mlb_id'])
                            self.update_lineup(lineup_ids, self.opp_sp_hand)
                        h_df = h_df.append(new_row, ignore_index = True)
            sorter = dict(zip(lineup_ids, range(len(lineup_ids))))
            h_df['order'] = h_df['mlb_id'].map(sorter)
            h_df.sort_values(by='order',inplace=True, kind='mergesort ')
            h_df['order'] = h_df['order'] + 1
            h_df.reset_index(inplace=True, drop=True)
            h_df.loc[((h_df['pa'] < 100) | (h_df['pitches_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa'] = h_q['pitches_pa'].median() - h_q['pitches_pa'].std()
            h_df.loc[((h_df['pa_vr'] < 100) | (h_df['pitches_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa_vr'] = h_q_vr['pitches_pa_vr'].median() - h_q_vr['pitches_pa_vr'].std()
            h_df.loc[((h_df['pa_vl'] < 100) | (h_df['pitches_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa_vl'] = h_q_vl['pitches_pa_vl'].median() - h_q_vl['pitches_pa_vl'].std()
            h_df.loc[(h_df['pa'] < 100) | (h_df['pitches_pa'].isna()), 'pitches_pa'] = h_q['pitches_pa'].median()
            h_df.loc[(h_df['pa_vr'] < 100) | (h_df['pitches_pa_vr'].isna()), 'pitches_pa_vr'] = h_q_vr['pitches_pa_vr'].median()
            h_df.loc[(h_df['pa_vl'] < 100) | (h_df['pitches_pa_vl'].isna()), 'pitches_pa_vl'] = h_q_vl['pitches_pa_vl'].median()
            h_df.loc[((h_df['pa'] < 100) | (h_df['fd_wps_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa'] = h_q['fd_wps_pa'].median() - h_q['fd_wps_pa'].std()
            h_df.loc[((h_df['pa_vr'] < 100) | (h_df['fd_wps_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa_vr'] = h_q_vr['fd_wps_pa_vr'].median() - h_q_vr['fd_wps_pa_vr'].std()
            h_df.loc[((h_df['pa_vl'] < 100) | (h_df['fd_wps_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa_vl'] = h_q_vl['fd_wps_pa_vl'].median() - h_q_vl['fd_wps_pa_vl'].std()
            h_df.loc[(h_df['pa'] < 100) | (h_df['fd_wps_pa'].isna()), 'fd_wps_pa'] = h_q['fd_wps_pa'].median()
            h_df.loc[(h_df['pa_vr'] < 100) | (h_df['fd_wps_pa_vr'].isna()), 'fd_wps_pa_vr'] = h_q_vr['fd_wps_pa_vr'].median()
            h_df.loc[(h_df['pa_vl'] < 100) | (h_df['fd_wps_pa_vl'].isna()), 'fd_wps_pa_vl'] = h_q_vl['fd_wps_pa_vl'].median()
            h_df.loc[((h_df['pa'] < 100) | (h_df['fd_wpa_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa'] = h_q['fd_wpa_pa'].median() + h_q['fd_wpa_pa'].std()
            h_df.loc[((h_df['pa_vr'] < 100) | (h_df['fd_wpa_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa_vr'] = h_q_vr['fd_wpa_pa_vr'].median() + h_q_vr['fd_wpa_pa_vr'].std()
            h_df.loc[((h_df['pa_vl'] < 100) | (h_df['fd_wpa_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa_vl'] = h_q_vl['fd_wpa_pa_vl'].median() + h_q_vl['fd_wpa_pa_vl'].std()
            h_df.loc[(h_df['pa'] < 100) | (h_df['fd_wpa_pa'].isna()), 'fd_wpa_pa'] = h_q['fd_wpa_pa'].median()
            h_df.loc[(h_df['pa_vr'] < 100) | (h_df['fd_wpa_pa_vr'].isna()), 'fd_wpa_pa_vr'] = h_q_vr['fd_wpa_pa_vr'].median()
            h_df.loc[(h_df['pa_vl'] < 100) | (h_df['fd_wpa_pa_vl'].isna()), 'fd_wpa_pa_vl'] = h_q_vl['fd_wpa_pa_vl'].median()
            if self.opp_sp_hand == 'R':
                h_df.loc[h_df['bat_side'] == 'S', 'bat_side'] = 'L'
            else:
                h_df.loc[h_df['bat_side'] == 'S', 'bat_side'] = 'R'
            
            try:
                p_info = self.opp_sp
                player = {'mlb_id': p_info['id'],
                          'name': p_info.get('fullName', ''),
                          'mlb_api': p_info.get('link', ''),
                          'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                          'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                          'weight': p_info.get('weight', ''),
                          'nickname': p_info.get('nickName', ''),
                          'debut': p_info.get('mlbDebutDate', ''),
                          'bat_side': p_info.get('batSide', {}).get('code', ''),
                          'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                          'age': p_info.get('currentAge', ''),
                          'team': self.opp_name
                              }
                p_df = pd.DataFrame([player]).join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            except TypeError:
                p_df = Team.default_sp()
            p_df['pitches_start'].fillna(p_q_sp['pitches_start'].median(), inplace = True)
            key = 'pitches_pa_' + self.o_split
            p_df['exp_x_lu'] = p_df['pitches_start'] / h_df[key].sum()
            p_df['exp_bf'] = round((p_df['exp_x_lu'] * 9))
            sp_rollover = floor((p_df['exp_x_lu'] % 1) * 9)
            h_df.loc[h_df['order'] <= sp_rollover, 'exp_pa_sp'] = ceil(p_df['exp_x_lu'])
            h_df.loc[h_df['order'] > sp_rollover, 'exp_pa_sp'] = floor(p_df['exp_x_lu'])
            p_df.loc[(p_df['batters_faced_vr'] < 100) | (p_df['fd_wpa_b_vr'].isna()), 'fd_wpa_b_vr'] = p_q_vr['fd_wpa_b_vr'].median()
            p_df.loc[(p_df['batters_faced_vl'] < 100) | (p_df['fd_wpa_b_vl'].isna()), 'fd_wpa_b_vl'] = p_q_vl['fd_wpa_b_vl'].median()
            lefties = (h_df['bat_side'] == 'L')
            righties = (h_df['bat_side'] == 'R')
            key = 'fd_wps_pa_' + self.o_split
            h_df.loc[lefties, 'exp_ps_sp'] = ((p_df['fd_wpa_b_vl'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df.loc[righties, 'exp_ps_sp'] = ((p_df['fd_wpa_b_vr'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df.loc[lefties, 'sp_mu'] = p_df['fd_wpa_b_vl'].max()
            h_df.loc[righties, 'sp_mu'] = p_df['fd_wpa_b_vr'].max()
            #points conceded
            key = 'fd_wpa_pa_' + self.o_split
            h_df.loc[lefties, 'exp_pc_sp'] = ((p_df['fd_wps_b_vl'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df.loc[righties, 'exp_pc_sp'] = ((p_df['fd_wpa_b_vr'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df['exp_pc_sp_raw'] = h_df[key] * h_df['exp_pa_sp']
            p_df.loc[(p_df['batters_faced_vr'] < 100) | (p_df['ra-_b_vr'].isna()), 'ra-_b_vr'] = p_q_vr['ra-_b_vr'].median()
            p_df.loc[(p_df['batters_faced_vl'] < 100) | (p_df['ra-_b_vl'].isna()), 'ra-_b_vl'] = p_q_vl['ra-_b_vl'].median()
            exp_pa_r_sp = h_df.loc[righties, 'exp_pa_sp'].sum()
            exp_pa_l_sp = h_df.loc[lefties, 'exp_pa_sp'].sum()
            p_df['exp_ra'] = floor((exp_pa_r_sp * p_df['ra-_b_vr'].max()) + (exp_pa_l_sp * p_df['ra-_b_vl'].max()))
            p_df['exp_inn'] = (p_df['exp_bf'].max() - p_df['exp_ra'].max()) / 3
            if self.is_home:
                exp_bp_inn = 8.5 - p_df['exp_inn'].max()
            else:
                exp_bp_inn = 9 - p_df['exp_inn'].max()
            bp = self.proj_opp_bp
            bp.loc[(bp['batters_faced_vr'] < 100) | (bp['fd_wpa_b_vr'].isna()), 'fd_wpa_b_vr'] = p_q_vr['fd_wpa_b_vr'].median()
            bp.loc[(bp['batters_faced_vl'] < 100) | (bp['fd_wpa_b_vl'].isna()), 'fd_wpa_b_vl'] = p_q_vl['fd_wpa_b_vl'].median()
            bp.loc[(bp['batters_faced_vr'] < 100) | (bp['ra-_b_vr'].isna()), 'ra-_b_vr'] = p_q_vr['ra-_b_vr'].median()
            bp.loc[(bp['batters_faced_vl'] < 100) | (bp['ra-_b_vl'].isna()), 'ra-_b_vl'] = p_q_vl['ra-_b_vl'].median()
            bp.loc[(bp['batters_faced_rp'] < 100) | (bp['fd_wpa_b_rp'].isna()), 'fd_wpa_b_rp'] = p_q_rp['fd_wpa_b_rp'].median()
            bp.loc[(bp['batters_faced_rp'] < 100) | (bp['ra-_b_rp'].isna()), 'ra-_b_rp'] = p_q_rp['ra-_b_rp'].median()
            exp_bf_bp = floor((exp_bp_inn * 3) + ((exp_bp_inn * 3) * bp['ra-_b_rp'].mean()))
            first_bp_pa = h_df.loc[(h_df['exp_pa_sp'] == floor(p_df['exp_x_lu'])), 'order'].idxmin()
            order = h_df.loc[first_bp_pa, 'order'].item()
            print(order)
            h_df['exp_pa_bp'] = 0
            print(exp_bf_bp)
            while exp_bf_bp > 0:
                if order == 10:
                    order = 1
                h_df.loc[h_df['order'] == order, 'exp_pa_bp'] += 1
                order += 1
                exp_bf_bp -= 1
            h_df['exp_ps_bp'] = h_df['exp_pa_bp'] * ((bp['fd_wpa_b_rp'].mean() + h_df['fd_wps_pa']) / 2)
            h_df['exp_ps_raw'] = h_df['exp_ps_sp'] + h_df['exp_ps_bp']
            self.exp_ps_raw = h_df['exp_ps_raw'].sum()
            h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp']
            h_df['venue_points'] = h_df['exp_ps_raw'] * self.next_venue_boost
            h_df['temp_points'] = h_df['exp_ps_raw'] * self.temp_boost
            h_df['ump_points'] = h_df['exp_ps_raw'] * self.ump_boost
            h_df['points'] = (h_df['venue_points'] * fw['venue_h']) + \
                (h_df['temp_points'] * fw['temp_h']) + (h_df['ump_points'] * fw['ump_h'])
            h_df['points'] = (h_df['venue_points'] + h_df['temp_points'] + h_df['ump_points']) / 3
            self.venue_points = self.exp_ps_raw * self.next_venue_boost
            self.temp_points = self.exp_ps_raw * self.temp_boost
            self.ump_points = self.exp_ps_raw * self.ump_boost
            self.points= (self.venue_points + self.temp_points + self.ump_points) / 3
            if self.confirmed_lu:
                with open(data_path, "wb") as file:
                    pickle.dump(h_df, file)
        else:
            h_df = pd.read_pickle(data_path)
        return h_df
    def sp_df(self):
        if self.confirmed_sp:
            slug = self.projected_sp['nameSlug']
            data_path = pickle_path(name=f"{slug}_{tf.today}", directory=settings.LINEUP_DIR)
            if data_path.exists():
                p_df = pd.read_pickle(data_path)
                return p_df
        try:
            p_info = self.projected_sp
            player = {'mlb_id': p_info['id'],
                      'name': p_info.get('fullName', ''),
                      'mlb_api': p_info.get('link', ''),
                      'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                      'weight': p_info.get('weight', ''),
                      'nickname': p_info.get('nickName', ''),
                      'debut': p_info.get('mlbDebutDate', ''),
                      'bat_side': p_info.get('batSide', {}).get('code', ''),
                      'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                      'age': p_info.get('currentAge', ''),
                      'team': self.opp_name
                          }
            p_df = pd.DataFrame([player]).join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        except TypeError:
            p_df = Team.default_sp()
        h_df = self.opp_instance.lineup_df()
        p_df['exp_ps_raw'] = h_df['exp_pc_sp'].sum()
        p_df['venue_points'] = p_df['exp_ps_raw'] * self.next_venue_boost
        p_df['temp_points'] = p_df['exp_ps_raw'] * self.temp_boost
        p_df['ump_points'] = p_df['exp_ps_raw'] * self.ump_boost
        p_df['points'] = \
            (p_df['venue_points'] * fw['venue_p']) + \
                (p_df['temp_points'] * fw['temp_p']) + (p_df['ump_points'] * fw['ump_p'])
        p_df['lu_mu'] = h_df['exp_pc_sp_raw'].sum()
        if self.confirmed_sp:
            with open(data_path, "wb") as file:
                pickle.dump(p_df, file)
        return p_df
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
    def projected_ump_data(self):
        return game_data[game_data['umpire'] == self.projected_ump]
    
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
        if self.roof_closed:
            return 0
        elif not self.next_has_roof:
            avg_wind = self.next_venue_data['wind_speed'].mean()
        else:
            avg_wind = 0
        if self.weather:
            wind_description = self.weather.get('wind')
            if wind_description:
                try:
                    return int(wind_description.split(' ')[0])
                except (TypeError, ValueError):
                    if not np.isnan(avg_wind):
                        return avg_wind
                    return ''
            else:
                if not np.isnan(avg_wind):
                    return avg_wind
                return ''
        else:
            if not np.isnan(avg_wind):
                return avg_wind
            return ''
                
    @cached_property
    def wind_direction(self):
        wind_direction = ''
        if self.weather:
            wind_description = self.weather.get('wind')
            if wind_description:
                return wind_description.split(', ')[-1]
        return wind_direction
    
    @cached_property
    def venue_condition(self):
        condition = ''
        if self.weather:
            condition = self.weather.get('condition')
        return condition
    @cached_property
    
    def venue_temp(self):
        if self.weather:
            temp = self.weather.get('temp')
            if temp and not self.roof_closed:
                try:
                    return int(temp)
                except ValueError:
                    return ''
        if self.roof_closed:
            return 72
        return ''
    @cached_property
    def roof_closed(self):
        return ((self.next_has_roof and not self.venue_condition) or self.venue_condition in mac.weather.roof_closed)
    
    @cached_property
    def home_has_roof(self):
        return any(i in mac.weather.roof_closed for i in self.home_venue_data['condition'].unique())
    
    @cached_property
    def next_has_roof(self):
        return any(i in mac.weather.roof_closed for i in self.next_venue_data['condition'].unique())
    
    @cached_property
    def last_has_roof(self):
        return any(i in mac.weather.roof_closed for i in self.last_venue_data['condition'].unique())
    
    @cached_property
    def no_rest(self):
        if self.last_game_pk:
            date = self.last_game['gameData']['datetime']['originalDate']
            if datetime.date.fromisoformat(date) >= tf.yesterday:
                return True
            return False
        return self.last_game_pk
    
    @cached_property
    def used_rp(self):
        if self.no_rest:
            return self.last_game['liveData']['boxscore']['teams'][self.was_ha]['pitchers'][1:]
        return []
    
    @cached_property
    def no_rest_travel(self):
        if self.no_rest and self.last_venue != self.next_venue:
            return True
        return False
    
    @cached_property
    def is_dh(self):
        return self.next_game['gameData']['teams']['home']['league']['id'] == 103
    
    @cached_property
    def was_dh(self):
        return self.last_game['gameData']['teams']['home']['league']['id'] == 103
    
    @cached_property
    def next_venue_boost(self):
        if (self.wind_direction in mac.weather.wind_out or self.wind_direction in mac.weather.wind_in) and not self.roof_closed:
            wind_in = self.next_venue_data[self.next_venue_data['wind_direction'].isin(mac.weather.wind_in)]
            wind_out = self.next_venue_data[self.next_venue_data['wind_direction'].isin(mac.weather.wind_out)]
            if len(wind_in.index) >= 50 and len(wind_out.index) >= 50:
                if ((wind_out['fd_points'].mean() - wind_in['fd_points'].mean()) / game_data['fd_points'].mean()) > 0:
                    if self.wind_speed >= game_data['wind_speed'].median():
                        if self.wind_direction in mac.weather.wind_out:
                            return wind_out['fd_points'].mean() / game_data['fd_points'].mean()
                        if self.wind_direction in mac.weather.wind_in:
                            return wind_in['fd_points'].mean() / game_data['fd_points'].mean()
        if len(self.next_venue_data.index) >= 100:
            return 1
        return self.next_venue_data['fd_points'].mean() / game_data['fd_points'].mean()
    @cached_property
    def temp_boost(self):
        if self.venue_temp:
            count = 0
            mult = .01
            temp  = self.venue_temp
            data = game_data[game_data['condition'].isin(mac.weather.roof_open)]
            if len(data.index) < 10000:
                stop = len(data.index) * .1
            else:
                stop = 1000
            while count < stop:
                df = data[data['temp'].between(temp - mult, temp + mult, inclusive=False)]
                count = len(df.index)
                mult += 1
            return df['fd_points'].mean() / game_data['fd_points'].mean()
        return 1
    @cached_property
    def ump_boost(self):
        if len(self.projected_ump_data.index) >= 100:
            return self.projected_ump_data['fd_points'].mean() / game_data['fd_points'].mean()
        return 1
    @cached_property
    def opp_bullpen(self):
        data_path = pickle_path(name=f"{self.opp_name}_bp_{tf.today}", directory=settings.BP_DIR)
        if data_path.exists():
            bp = pd.read_pickle(data_path)
            return bp
        bp = self.opp_instance.bullpen
        bp = bp[(~bp['mlb_id'].isin(self.opp_instance.used_rp)) & (bp['status'] == 'A')]
        with open(data_path, "wb") as file:
            pickle.dump(bp, file)
        return bp
    @cached_property
    def proj_opp_bp(self):
        return self.opp_bullpen.loc[self.opp_bullpen['batters_faced_rp'].nlargest(4).index]
    @cached_property
    def opp_instance(self):
        for team in Team:
            if team.name == self.opp_name:
                return team
        
       
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


# cardinals.points
# test = cardinals.lineup_df()
# cardinals.opp_name
# test2=marlins.lineup_df()
# # test.columns.tolist()
# marlins.points
# test[['name', 'points' , 'exp_ps_sp', 'exp_ps_bp', 'exp_ps_raw', 'venue_points', 'temp_points', 'ump_points', 'is_platoon']]
# # print(test['points'].sum())

# # red_sox.temp_boost
# # # # dodgers.temp_boost
# # # # dodgers.weather
# # # red_sox.opp_sp_hand

# # astros.opp_sp
# # cardinals.projected_sp


