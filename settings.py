import dfs_tools_mlb
import inspect
from pathlib import Path
import glob

# teams with postponed game, lowercase team name.
ppd= []

BASE_DIR = Path(inspect.getfile(dfs_tools_mlb)).resolve().parent
STORAGE_DIR = Path(BASE_DIR, 'compile', 'storage').resolve()
ARCHIVE_DIR = Path(STORAGE_DIR, 'archives').resolve()
LINEUP_DIR = Path(STORAGE_DIR, 'confirmed_lineups').resolve()
SP_DIR = Path(STORAGE_DIR, 'confirmed_sp').resolve()
BP_DIR = Path(STORAGE_DIR, 'bullpens').resolve()
DEPTH_DIR = Path(STORAGE_DIR, 'depth_charts').resolve()
ROSTER_DIR = Path(STORAGE_DIR, 'rosters').resolve()
NRI_DIR = Path(STORAGE_DIR, 'nri').resolve()
GAME_DIR = Path(STORAGE_DIR, 'recent_games').resolve()
FD_DIR = Path(STORAGE_DIR, 'fd_entries').resolve()
SCHED_DIR = Path(STORAGE_DIR, 'schedules').resolve()
TEAM_DIR = Path(BASE_DIR, 'compile', 'team_settings').resolve()
VITAL_DIR_LIST = [BP_DIR, SCHED_DIR, LINEUP_DIR, GAME_DIR]

DL_FOLDER = "C:/Users/nicho/Downloads"
FD_FILE_MATCH = DL_FOLDER + "/FanDuel-MLB*entries-upload-template*"
FD_FILES = glob.glob(FD_FILE_MATCH) 

use_fangraphs = False
OFFSEASON_TESTING = False
wind_factor = True
SEASON_WEIGHT_START = 2015
INDOOR_TEMP = 72
INDOOR_WIND = 0
MIN_PA_VENUE = 5000
MIN_PA_UMP = 500
#Hitters given median stats if in lineup with fewer PA.
MIN_PA_HITTER = 50
MIN_PA_HITTER_SPLIT = 25
MIN_BF_PITCHER = 50
MIN_BF_PITCHER_SPLIT = 25
MIN_BF_BP = 50
MIN_BF_BP_SPLIT = 25
STATS_TO_ADJUST_H = ('pitches_pa', 'fd_wps_pa', 'fd_wpa_pa', 'k_pa', 'fd_hr_weight' )
STATS_TO_ADJUST_P = ('k_b', 'fd_wpa_b', 'fd_wps_b', 'ra-_b', 'ppb')
STATS_TO_ADJUST_RP = ('fd_wpa_b', 'ra-_b')
MIN_GAMES_VENUE_WIND = 50
STACK_KEY_1 = 'points'
STACK_KEY_2 = 'sp_split'
PITCHER_REPLACE_KEY_1 = 'points'
PITCHER_REPLACE_KEY_2 = 'k_pred'
FD_NON_STACK_KEY_1 = 'fd_salary'
FD_NON_STACK_KEY_2 = 'fd_hr_weight'

RESTED_BP_SAMP = 4
TIRED_BP_INCREASE = 1.075 
MIN_UMP_SAMP = 100




P_PTS_WEIGHT = .5
H_PTS_WEIGHT = .5
if P_PTS_WEIGHT + H_PTS_WEIGHT != 1:
    P_PTS_WEIGHT = .5
    H_PTS_WEIGHT = .5
    
P_K_WEIGHT = .5
H_K_WEIGHT = .5
if P_K_WEIGHT + H_K_WEIGHT != 1:
    P_K_WEIGHT = .5
    H_K_WEIGHT = .5
    

    
LU_LENGTH = 9

BP_INNINGS_ROAD = 9
BP_INNINGS_HOME = 9


storage_settings = {
	'archive_stats': True,
}

stat_splits ={
	#left/right splits
	'h_lr': True,
	'sp_lr': True,
	'rp_lr': True,
	#home/away splits
	'h_ha': True,
	'sp_ha': True,
	'rp_ha': True,
	#pitching with runners on base
	'sp_ro': True,
	'rp_ro': True,
	#SP second time through order stats
	'sp_so': True,
	#stats over last 30 days
	'h_30': False,
	'sp_30': False,
	'rp_30': False,
	#relif IP yesterday
	'rp_ytd': False
}
# int(current_season()['season_id']) + 1 circular import
stat_range = {
    'start': 2017,
    'end' : 2023,
    #currently, must manually change _year columns in dataframes.stat_splits for pitchers
    'player_start': 2021
    }

fd_scoring = {
'hitters': {
    'base': 3,
    'rbi' : 3.5,
    'run' : 3.2,
    'sb' : 6,
    'hr': 18.7,
    
    },
'pitchers': {
    'inning': 3,
    'out': 1,
    'k' : 3,
    'er': -3,
    'qs': 4,
    'win': 6
    }
}

api_hydrate = {
    'p_info': 'person',
    'get_historical_data': 'weather,linescore,officials,probablePitcher,scoringplays'
    }
api_fields = {
    'get_historical_data': 'dates,date,games,status,codedGameState,weather,condition,temp,wind,linescore,teams,home,away,runs,hits,currentInning,venue,id,dayNight,seriesGameNumber,officials,gamePk,probablePitcher,scoringPlays,result,event',
    }


