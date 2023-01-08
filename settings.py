import dfs_tools_mlb
import inspect
from pathlib import Path
import glob


from dfs_tools_mlb.utils.time import time_frames as tf

# teams with postponed game, lowercase team name.
ppd = []
current_fd_slate_number = 1
reset_all_lineups = True
reset_specific_lineups = ()
reset_all_team_vars = True
reset_team_vars = ()
reset_all_pitchers = True
reset_specific_pitchers = []




BASE_DIR = Path(inspect.getfile(dfs_tools_mlb)).resolve().parent
STORAGE_DIR = Path(BASE_DIR, "compile", "storage").resolve()
ARCHIVE_DIR = Path(STORAGE_DIR, "archives").resolve()
LINEUP_DIR = Path(STORAGE_DIR, "confirmed_lineups").resolve()
SP_DIR = Path(STORAGE_DIR, "confirmed_sp").resolve()
BP_DIR = Path(STORAGE_DIR, "bullpens").resolve()
DEPTH_DIR = Path(STORAGE_DIR, "depth_charts").resolve()
ROSTER_DIR = Path(STORAGE_DIR, "rosters").resolve()
NRI_DIR = Path(STORAGE_DIR, "nri").resolve()
GAME_DIR = Path(STORAGE_DIR, "recent_games").resolve()
FD_DIR = Path(STORAGE_DIR, "fd_entries").resolve()
SCHED_DIR = Path(STORAGE_DIR, "schedules").resolve()
TEAM_DIR = Path(BASE_DIR, "compile", "team_settings").resolve()
VITAL_DIR_LIST = [BP_DIR, SCHED_DIR, LINEUP_DIR, GAME_DIR]

DL_FOLDER = "C:/Users/nicho/Downloads"
FD_FILE_MATCH = DL_FOLDER + "/FanDuel-MLB*entries-upload-template*"
FD_FILES = glob.glob(FD_FILE_MATCH)

daily_info_file = str(STORAGE_DIR.joinpath(f"daily_info_{tf.today}.json"))
team_lineups_file = str(ARCHIVE_DIR.joinpath("team_lineups.json"))




OFFSEASON_TESTING = False
wind_factor = True
#Year to first include in average of weighted run stats.
SEASON_WEIGHT_START = 2016
INDOOR_TEMP = 72
INDOOR_WIND = 0
MIN_PA_VENUE = 5000
MIN_PA_UMP = 500
# Hitters given median stats if in lineup with fewer PA.
MIN_PA_HITTER = 100
MIN_PA_HITTER_SPLIT = 50
MIN_BF_SP = 100
MIN_BF_SP_SPLIT = 50
MIN_BF_BP = 100
MIN_BF_BP_SPLIT = 50
STATS_TO_ADJUST_H = ("pitches_pa", "fd_wps_pa", "fd_wpa_pa", "k_pa", "fd_hr_weight", "rb-_pa")
STATS_TO_ADJUST_SP = ("k_b", "fd_wpa_b", "fd_wps_b", "ra-_b", "ppb")
STATS_TO_ADJUST_RP = ("fd_wpa_b", "ra-_b")
PROJECTED_SP_COLUMN = "fd_ps_s"
MIN_GAMES_VENUE_WIND = 100
#Fanduel lineup builder consants
STACK_KEY_1 = "sp_split"
STACK_KEY_2 = "sp_split"
PITCHER_REPLACE_KEY_1 = "points"
PITCHER_REPLACE_KEY_2 = "points"
FD_NON_STACK_KEY_1 = "sp_split"
FD_NON_STACK_KEY_2 = "sp_split"

FD_PLAYER_RANK_COLS = ["name", "points", "exp_ps_sp_pa", "sp_split", "fd_salary",
                       "fd_id", "mlb_id", "team", "fd_injury_d", "is_platoon",
                       "venue_points",  "total_pitches", "bat_side", "order"]

RESTED_BP_SAMP = 3
TIRED_BP_INCREASE = 1.10
MIN_UMP_SAMP = 100


P_PTS_WEIGHT = 0.5
H_PTS_WEIGHT = 0.5

P_K_WEIGHT = 0.5
H_K_WEIGHT = 0.5

P_HITS_ALLOWED_WEIGHT = .5
H_HITS_ALLOWED_WEIGHT = .5
ENV_TEMP_WEIGHT = .3 
ENV_VENUE_WEIGHT = .5 
ENV_UMP_WEIGHT = .2

P_PIT_PER_BAT_WEIGHT = .5
H_PIT_PER_BAT_WEIGHT = .5


LU_LENGTH = 9

BP_INNINGS_ROAD = 9
BP_INNINGS_HOME = 9


storage_settings = {"archive_stats": True}

stat_splits = {
    # left/right splits
    "h_lr": True,
    "sp_lr": True,
    "rp_lr": True,
    # home/away splits
    "h_ha": True,
    "sp_ha": True,
    "rp_ha": True,
    # pitching with runners on base
    "sp_ro": True,
    "rp_ro": True,
    # SP second time through order stats
    "sp_so": True,
    # stats over last 30 days
    "h_30": False,
    "sp_30": False,
    "rp_30": False,
    # relif IP yesterday
    "rp_ytd": False,
}
# int(current_season()['season_id']) + 1 circular import
stat_range = {
    "start": 2017,
    "end": 2023,
    # currently, must manually change _year columns in dataframes.stat_splits for pitchers
    "player_start": 2021,
    "pitcher_start": 2022,
    "hitter_start": 2021
}

fd_scoring = {
    "hitters": {"base": 3, "rbi": 3.5, "run": 3.2, "sb": 6, "hr": 18.7},
    "pitchers": {"inning": 3, "out": 1, "k": 3, "er": -3, "qs": 4, "win": 6},
}

api_hydrate = {
    "p_info": "person",
    "get_historical_data": "weather,linescore,officials,probablePitcher,scoringplays",
}
api_fields = {
    "get_historical_data": "dates,date,games,status,codedGameState,weather,condition,temp,wind,linescore,teams,home,away,runs,hits,currentInning,venue,id,dayNight,seriesGameNumber,officials,gamePk,probablePitcher,scoringPlays,result,event"
}

daily_info_dict = {
                "confirmed_lu": [],
                "confirmed_sp": [],
                "rain": [],
                "auto_projected_sp": []
                }
