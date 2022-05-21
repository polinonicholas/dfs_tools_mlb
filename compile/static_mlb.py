from dfs_tools_mlb.utils.subclass import Map
import re
from functools import lru_cache

# from dfs_tools_mlb.utils.statsapi import event_types

mlb_api_codes = Map({
    'players': Map({
        'h': ['8', '4', '2', '6', 'O', '5', '3', '7', '9', '10', '11', '12', '13', 'BR', 'I', 'U', 'V', 'W'],
        'p': ['1','S', 'C', 'K', 'L', 'M', 'G', 'F'],
        'sp': ['S', 'M', 'N'],
        'bullpen': ['1', 'C', 'E', 'K', 'L', 'G', 'F'],
        'rhp': ['K', 'M', 'F'],
        'lhp': ['L', 'N', 'G'],
        'twp': ['Y', 'Z', 'A', 'J'],
        'unknown':['X'],
    }),
    'game_status': Map({
        #codedGameState
        'pre': ['S', 'P',],
        'post': ['F', 'O', 'R', 'Q'],
        'live': ['M', 'N', 'I'],
        'postponed': ['D', 'C'],
        'suspended': ['U', 'T'],
        'other':['W', 'X'],
        #statusCode
        'delay_pre': ['PO', 'PA', 'PD', 'PL', 'PY', 'PP', 'PB', 'PC', 'PF', 'PV', 'PG', 'PS', 'PR', 'PI'],
        'delay_mid': ['IZ', 'IO', 'IA', 'ID', 'IL', 'IY', 'IP', 'IB', 'IC', 'IF', 'IV', 'IG', 'IS', 'IR', 'II'],
        }),
    #weather[wind/condition]
    'weather': Map({
        'wind_in': ['In From RF', 'In From LF', 'In From CF'],
        'wind_out': ['Out To CF', 'Out To RF', 'Out To LF'],
        'wind_cross': ['R To L', 'L To R'],
        'varied_wind': ['Varies'],
        'no_wind': ['None'],
        'rain': ['Rain', 'Drizzle'],
        'roof_closed':['Roof Closed', 'Dome'],
        'roof_open': ['Rain', 'Overcast', 'Snow', 'Cloudy', 'Clear', 'Sunny', 'Partly Cloudy', 'Drizzle'],
        'clear':['Sunny', 'Clear'],
        'inclement': ['Rain', 'Overcast', 'Snow', 'Cloudy', 'Partly Cloudy', 'Drizzle']
        }),
    'stats': Map({
        #statGroups
        'groups': Map({
            'hitting':'hitting',
            'pitching':'pitching',
            'fielding':'fielding',
            'catching':'catching',
            'running': 'running',
            'game': 'game',
            'team':'team',
            'streak':'streak'
            }),
        'categories':None
        }),
    'pitch_types': Map({
        'fb': ['FA', 'FT', 'FF', 'FC', 'FS', 'FO'],
        'bb': ['CU', 'KC', 'KN', 'SC', 'SI', 'SL', 'SV', 'CS'],
        'cu': ['CH', 'EP']
        }),
    'event_types': Map({
        'all': ['pickoff_1b',
         'pickoff_2b',
         'pickoff_3b',
         'pickoff_error_1b',
         'pickoff_error_2b',
         'pickoff_error_3b',
         'no_pitch',
         'single',
         'double',
         'triple',
         'home_run',
         'double_play',
         'field_error',
         'error',
         'field_out',
         'fielders_choice',
         'fielders_choice_out',
         'force_out',
         'grounded_into_double_play',
         'grounded_into_triple_play',
         'strikeout',
         'strike_out',
         'strikeout_double_play',
         'strikeout_triple_play',
         'triple_play',
         'sac_fly',
         'catcher_interf',
         'batter_interference',
         'fielder_interference',
         'runner_interference',
         'fan_interference',
         'batter_turn',
         'ejection',
         'cs_double_play',
         'defensive_indiff',
         'sac_fly_double_play',
         'sac_bunt',
         'sac_bunt_double_play',
         'walk',
         'intent_walk',
         'hit_by_pitch',
         'injury',
         'os_ruling_pending_prior',
         'os_ruling_pending_primary',
         'at_bat_start',
         'passed_ball',
         'other_advance',
         'runner_double_play',
         'runner_placed',
         'pitching_substitution',
         'offensive_substitution',
         'defensive_switch',
         'umpire_substitution',
         'pitcher_switch',
         'game_advisory',
         'stolen_base',
         'stolen_base_2b',
         'stolen_base_3b',
         'stolen_base_home',
         'caught_stealing',
         'caught_stealing_2b',
         'caught_stealing_3b',
         'caught_stealing_home',
         'defensive_substitution',
         'pickoff_caught_stealing_2b',
         'pickoff_caught_stealing_3b',
         'pickoff_caught_stealing_home',
         'balk',
         'wild_pitch',
         'other_out'],
        'plate_appearance':['single',
         'double',
         'triple',
         'home_run',
         'double_play',
         'field_error',
         'field_out',
         'fielders_choice',
         'fielders_choice_out',
         'force_out',
         'grounded_into_double_play',
         'strikeout',
         'strike_out',
         'strikeout_double_play',
         'strikeout_triple_play',
         'triple_play',
         'sac_fly',
         'catcher_interf',
         'batter_interference',
         'fan_interference',
         'sac_fly_double_play',
         'sac_bunt',
         'sac_bunt_double_play',
         'walk',
         'intent_walk',
         'hit_by_pitch',
         'os_ruling_pending_primary'],
        'hit': ['single', 'double', 'triple', 'home_run'],
        'base_running': ['pickoff_1b',
         'pickoff_2b',
         'pickoff_3b',
         'pickoff_error_1b',
         'pickoff_error_2b',
         'pickoff_error_3b',
         'error',
         'cs_double_play',
         'defensive_indiff',
         'passed_ball',
         'other_advance',
         'runner_double_play',
         'stolen_base_2b',
         'stolen_base_3b',
         'stolen_base_home',
         'caught_stealing_2b',
         'caught_stealing_3b',
         'caught_stealing_home',
         'pickoff_caught_stealing_2b',
         'pickoff_caught_stealing_3b',
         'pickoff_caught_stealing_home',
         'balk',
         'wild_pitch',
         'other_out'],
        'void': ['sac_fly',
                 'catcher_interf',
                 'intent_walk',
                 'sac_bunt',
                 'fan_interference',
                 'batter_interference',
                 'hit_by_pitch',
                 'field_error'  
            ],
        'success': [
         'walk',
         'home_run',
         'single',
         'double',
         'triple',
        ],
        
        'fail': [
         'double_play',
         'field_out',
         'fielders_choice',
         'fielders_choice_out',
         'force_out',
         'grounded_into_double_play',
         'strikeout',
         'strike_out',
         'strikeout_double_play',
         'strikeout_triple_play',
         'triple_play',
         'sac_fly_double_play',
         'sac_bunt_double_play',
         ],
        'strikeout': ['strikeout',
        'strike_out',
        'strikeout_double_play',
        'strikeout_triple_play'],
        'error': Map({'field': ['field_error'],
                       'all': ['field_error',
                               'pickoff_error_1b',
                               'pickoff_error_2b',
                               'pickoff_error_3b']}),
        'stolen_base': Map({'success': [ 'stolen_base_2b',
                                       'stolen_base_3b',
                                       'stolen_base_home'],
                           'fail': ['caught_stealing_2b',
                           'caught_stealing_3b',
                           'caught_stealing_home',
                           'pickoff_caught_stealing_2b',
                           'pickoff_caught_stealing_3b',
                           'pickoff_caught_stealing_home']}),
       
        
        })
    })


team_info = Map({
    'athletics': {
        'full_name': 'oakland athletics',
        'mlb_id': 133,
        'abbreviations': ['oak'],
        'location': 'oakland',
        'venue': {'id': 10, 'name': 'Oakland Coliseum', 'link': '/api/v1/venues/10'}
        },
    'pirates': {
        'full_name': 'pittsburgh pirates',
        'mlb_id': 134,
        'abbreviations': ['pit'],
        'location': 'pittsburgh',
        'venue': {'id': 31, 'name': 'PNC Park', 'link': '/api/v1/venues/31'}
        },
    'padres': {
        'full_name': 'san diego padres',
        'mlb_id': 135,
        'abbreviations': ['sd', 'sdp', 'sdn'],
        'location': 'san diego',
        'venue': {'id': 2680, 'name': 'Petco Park', 'link': '/api/v1/venues/2680'}
        },
    'mariners': {
        'full_name': 'seattle mariners',
        'mlb_id': 136,
        'abbreviations': ['sea'],
        'location': 'seattle',
        'venue': {'id': 680, 'name': 'T-Mobile Park', 'link': '/api/v1/venues/680'}
        },
    'giants': {
        'full_name': 'san francisco giants',
        'mlb_id': 137,
        'abbreviations': ['sfg', 'sf', 'sfn'],
        'location': 'san francisco',
        'venue': {'id': 2395, 'name': 'Oracle Park', 'link': '/api/v1/venues/2395'}
        },
    'cardinals': {
        'full_name': 'st. louis cardinals',
        'mlb_id': 138,
        'abbreviations': ['stl', 'sln'],
        'location': 'st. louis',
        'venue': {'id': 2889, 'name': 'Busch Stadium', 'link': '/api/v1/venues/2889'}
        },
    'rays': {
        'full_name': 'tampa bay rays',
        'mlb_id': 139,
        'abbreviations': ['tb', 'tbr', 'tam', 'tba'],
        'location': 'st. petersburg',
        'venue': {'id': 12, 'name': 'Tropicana Field', 'link': '/api/v1/venues/12'}
        },
    'rangers': {
        'full_name': 'texas rangers',
        'mlb_id': 140,
        'abbreviations': ['tx', 'tex'],
        'location': 'arlington',
        'venue': {'id': 5325, 'name': 'Globe Life Field', 'link': '/api/v1/venues/5325'}
        },
    'blue jays': {
        'full_name': 'toronto blue jays',
        'mlb_id': 141,
        'abbreviations': ['tor', 'blue-jays'],
        'location': 'toronto',
        'venue': {'id': 14, 'name': 'Rogers Centre', 'link': '/api/v1/venues/14'}
        },
    'twins': {
        'full_name': 'minnesota twins',
        'mlb_id': 142,
        'abbreviations': ['min'],
        'location': 'minneapolis',
        'venue': {'id': 3312, 'name': 'Target Field', 'link': '/api/v1/venues/3312'}
        },
    'phillies': {
        'full_name': 'philadelphia phillies',
        'mlb_id': 143,
        'abbreviations': ['phi'],
        'location': 'philadelphia',
        'venue': {'id': 2681, 'name': 'Citizens Bank Park', 'link': '/api/v1/venues/2681'}
        },
    'braves': {
        'full_name': 'atlanta braves',
        'mlb_id': 144,
        'abbreviations': ['atl'],
        'location': 'atlanta',
        'venue': {'id': 4705, 'name': 'Truist Park', 'link': '/api/v1/venues/4705'}
        },
    'white sox': {
        'full_name': 'white sox',
        'mlb_id': 145,
        'abbreviations': ['cha', 'cws', 'chw', 'white-sox'],
        'location': 'chicago',
        'venue': {'id': 4, 'name': 'Guaranteed Rate Field', 'link': '/api/v1/venues/4'}
        },
    'marlins': {
        'full_name': 'miami marlins',
        'mlb_id': 146,
        'abbreviations': ['mia'],
        'location': 'miami',
        'venue': {'id': 4169, 'name': 'Marlins Park', 'link': '/api/v1/venues/4169'}
        },
    'yankees': {
        'full_name': 'new york yankees',
        'mlb_id': 147,
        'abbreviations': ['nya', 'nyy'],
        'location': 'bronx',
        'venue': {'id': 3313, 'name': 'Yankee Stadium', 'link': '/api/v1/venues/3313'}
        },
    'brewers': {
        'full_name': 'milwaukee brewers',
        'mlb_id': 158,
        'abbreviations': ['mil'],
        'location': 'milwaukee',
        'venue': {'id': 32, 'name': 'American Family Field', 'link': '/api/v1/venues/32'}
        },
    'angels': {
        'full_name': 'los angeles angels',
        'mlb_id': 108,
        'abbreviations': ['ana', 'laa'],
        'location': 'anaheim',
        'venue': {'id': 1, 'name': 'Angel Stadium', 'link': '/api/v1/venues/1'}
        },
    'diamondbacks': {
        'full_name': 'arizona diamondbacks',
        'mlb_id': 109,
        'abbreviations': ['ari', 'az'],
        'location': 'phoenix',
        'venue': {'id': 15, 'name': 'Chase Field', 'link': '/api/v1/venues/15'}
        },
    'orioles': {
        'full_name': 'baltimore orioles',
        'mlb_id': 110,
        'abbreviations': ['bal'],
        'location': 'baltimore',
        'venue': {'id': 2, 'name': 'Oriole Park at Camden Yards', 'link': '/api/v1/venues/2'}
        },
    'red sox': {
        'full_name': 'boston red sox',
        'mlb_id': 111,
        'abbreviations': ['bos', 'red-sox'],
        'location': 'boston',
        'venue': {'id': 3, 'name': 'Fenway Park', 'link': '/api/v1/venues/3'}
        },
    'cubs': {
        'full_name': 'chicago cubs',
        'mlb_id': 112,
        'abbreviations': ['chc', 'chn'],
        'location': 'chicago',
        'venue': {'id': 17, 'name': 'Wrigley Field', 'link': '/api/v1/venues/17'}
        },
    'reds': {
        'full_name': 'cincinnati reds',
        'mlb_id': 113,
        'abbreviations': ['cin'],
        'location': 'cincinnati',
        'venue': {'id': 2602, 'name': 'Great American Ball Park', 'link': '/api/v1/venues/2602'}
        },
    'guardians': {
        'full_name': 'cleveland guardians',
        'mlb_id': 114,
        'abbreviations': ['cle'],
        'location': 'cleveland',
        'venue': {'id': 5, 'name': 'Progressive Field', 'link': '/api/v1/venues/5'}
        },
    'rockies': {
        'full_name': 'colorado rockies',
        'mlb_id': 115,
        'abbreviations': ['col'],
        'location': 'denver',
        'venue': {'id': 19, 'name': 'Coors Field', 'link': '/api/v1/venues/19'}
        },
    'tigers': {
        'full_name': 'detroit tigers',
        'mlb_id': 116,
        'abbreviations': ['det'],
        'location': 'detroit',
        'venue': {'id': 2394, 'name': 'Comerica Park', 'link': '/api/v1/venues/2394'}
        },
    'astros': {
        'full_name': 'houston astros',
        'mlb_id': 117,
        'abbreviations': ['hou'],
        'location': 'houston',
        'venue': {'id': 2392, 'name': 'Minute Maid Park', 'link': '/api/v1/venues/2392'}
        },
    'royals': {
        'full_name': 'kansas city royals',
        'mlb_id': 118,
        'abbreviations': ['kca', 'kc', 'kcr', 'kan'],
        'location': 'kansas city',
        'venue': {'id': 7, 'name': 'Kauffman Stadium', 'link': '/api/v1/venues/7'}
        },
    'dodgers': {
        'full_name': 'los angeles dodgers',
        'mlb_id': 119,
        'abbreviations': ['lan', 'los', 'lad', 'la'],
        'location': 'los angeles',
        'venue': {'id': 22, 'name': 'Dodger Stadium', 'link': '/api/v1/venues/22'}
        },
    'nationals': {
        'full_name': 'washington nationals',
        'mlb_id': 120,
        'abbreviations': ['was', 'wsh', 'wsn'],
        'location': 'washington',
        'venue': {'id': 3309, 'name': 'Nationals Park', 'link': '/api/v1/venues/3309'}
        },
    'mets': {
        'full_name': 'new york mets',
        'mlb_id': 121,
        'abbreviations': ['nyn', 'nym'],
        'location': 'flushing',
        'venue': {'id': 3289, 'name': 'Citi Field', 'link': '/api/v1/venues/3289'}
        },
    })

@lru_cache
def current_parks():
    parks = set()
    for k in team_info:
        if team_info[k].get('venue'):
            parks.add(team_info[k]['venue']['id'])
    return parks


def api_player_info_dict(d):
    p_info = d['person']
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
              'note': p_info.get('note', ''),
              'position_type': d['position']['type'],
              'position': d['position']['code'],
              'status': d.get('status', {}).get('code', ''),
              
              }
    return player

def api_pitcher_info_dict(d):
    player = {'mlb_id': d['id'],
              'name': d.get('fullName', ''),
              'mlb_api': d.get('link', ''),
              'born': d.get('birthCity', '') + ', ' + d.get('birthStateProvince', '') ,
              'height': '.'.join(re.findall('[0-9]', d.get('height', ''))),
              'weight': d.get('weight', ''),
              'nickname': d.get('nickName', ''),
              'debut': d.get('mlbDebutDate', ''),
              'bat_side': d.get('batSide', {}).get('code', ''),
              'pitch_hand': d.get('pitchHand', {}).get('code', ''),
              'age': d.get('currentAge', ''),
              
                  }
    return player



    


