from dfs_tools_mlb.utils.subclass import Map

mlb_api_codes = Map({
    'players': Map({
        'h': ['8', '4', '2', '6', 'O', '5', '3', '7', '9', '10', '11', '12', '13', 'BR', 'I', 'U', 'V', 'W', 'A', 'J', 'Z', 'Y'],
        'p': ['1','S', 'C', 'K', 'L', 'M', 'G', 'F', 'A', 'J', 'Z', 'Y'],
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
        'venue': {'id': 1, 'name': 'Angel Stadium', 'link': '/api/v1/venues/1'}
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
    'indians': {
        'full_name': 'cleveland indians',
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
        'abbreviations': ['kca, kc', 'kcr'],
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
    'info':{
        'updated': 2021
        } 
    })
from functools import lru_cache
@lru_cache
def current_parks():
    parks = set()
    for k in team_info:
        if team_info[k].get('venue'):
            parks.add(team_info[k]['venue']['id'])
    return parks
        
parks = current_parks()