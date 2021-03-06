import statsapi
from functools import cached_property
import re
import datetime
import requests
import pandas as pd
from dfs_tools_mlb.utils.pd import sm_merge
from dfs_tools_mlb.compile import time_frames

class Team:
    def __init__(self, mlb_id, abbreviations=[], name='', season=time_frames.today.year):
        self.id = mlb_id
        self.abbr = abbreviations
        self.name = name
        self.season = season
    @cached_property
    def venue(self):
        return statsapi.get('team', {'teamId':self.id})['teams'][0]['venue']['name']
    @cached_property
    def depth_chart(self):
        depth_chart = {
            'starters': [],
            'bullpen': [],
            'hitter': []
            }
        team = statsapi.get('team_roster', {'teamId': self.id, 'rosterType': 'depthChart'})['roster']
        for p in team:
            info = statsapi.get('people', {'personIds': p['person']['id']})['people'][0]
            player = {'mlb_id': info['id'],
                      'name': info.get('fullName', ''),
                      'mlb_api': info.get('link', ''),
                      'number': info.get('primaryNumber', ''),
                      'born': info.get('birthCity', '') + ', ' + info.get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', info.get('height', ''))),
                      'weight': info.get('weight', ''),
                      'nickname': info.get('nickName', ''),
                      'debut': info.get('mlbDebutDate', ''),
                      'bat_side': info.get('batSide', {}).get('code', ''),
                      'pitch_hand': info.get('pitchHand', {}).get('code', ''),
                      'age': info.get('currentAge', ''),
                      'note': p['note'],
                      'position_type': p['position']['type'],
                      'position': p['position']['code']
                      }
            try:
                position = int(player['position'])
            except ValueError:
                position = player['position']
            if position in range(2,14):
                depth_chart['hitter'].append(player)
            elif position == 'S':
                depth_chart['starters'].append(player)
            else:
                depth_chart['bullpen'].append(player)
        return depth_chart
    
    @cached_property
    def hitter(self):
        hitter = pd.DataFrame(self.depth_chart['hitter']).set_index('mlb_id').apply(pd.to_numeric, errors = 'ignore')
        h['h'] = True
        return hitter
    @cached_property
    def starter(self):
        starters = pd.DataFrame(self.depth_chart['starters']).set_index('mlb_id').apply(pd.to_numeric, errors = 'ignore')
        starters['p'] = True
        return starters
    @cached_property
    def bullpen(self):
        bullpen = pd.DataFrame(self.depth_chart['bullpen']).set_index('mlb_id').apply(pd.to_numeric, errors = 'ignore')
        bullpen['p'] True
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
    def coach(self):
        pitchers = self.starter.combine_first(self.bullpen)
        return pitchers
    @cached_property
    def coaches(self):
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
    def next_game(self):
        return statsapi.schedule(team=self.id)[0]