from dfs_tools_mlb.compile import current_season as cs
import pandas as pd
import pickle
from dfs_tools_mlb.utils.storage import pickle_path
from dfs_tools_mlb.compile.fanduel import FDSlate
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb import settings
from dfs_tools_mlb.compile.stats_mlb import get_statcast_p, get_p_diff
from dfs_tools_mlb.fd_active_team_info import opposing_sp, active_teams

for team in active_teams:
    print(team.name)
pitcher_statcast_file = pickle_path(
    name=f"pitcher_statcast_{tf.today}_{settings.current_fd_slate_number}", directory=settings.FD_DIR
)
pitcher_statcast_path = settings.FD_DIR.joinpath(pitcher_statcast_file)
if pitcher_statcast_path.exists():
    pitcher_statcast = pd.read_pickle(pitcher_statcast_path)

else:
    pitcher_statcast = pd.DataFrame(
        columns=[
            "name",
            "spin",
            "pc",
            "speed",
            "launch",
            "speed_diff",
            "spin_diff",
            "launch_diff",
            "last_speed",
            "last_spin",
            "last_launch",
        ]
    )
    
    
    for name, i_d in opposing_sp.items():
        try:
            p = get_statcast_p(int(i_d['id']), int(cs['season_id'])
)
            p_diff = get_p_diff(int(i_d['id']), (int(cs['season_id'])
- 1), int(cs['season_id'])
, filt1="", filt2="")
            last_game = p[p["date"] == p["date"].max()]

            row = [
                name,
                p["releaseSpinRate"].mean(),
                len(p["pitch"].unique()),
                p["effectiveSpeed"].max(),
                p["launchAngle"].mean(),
                p_diff.get("speed"),
                p_diff.get("spin"),
                p_diff.get("launch"),
                last_game["effectiveSpeed"].max(),
                last_game["releaseSpinRate"].mean(),
                last_game["launchAngle"].mean(),
            ]
        except KeyError:
            continue
        to_append = row
        df_length = len(pitcher_statcast)
        pitcher_statcast.loc[df_length] = to_append
    pitcher_statcast = pitcher_statcast.set_index("name")
    with open(pitcher_statcast_file, "wb") as f:
        pickle.dump(pitcher_statcast, f)
pitcher_statcast["speed_last_speed"] = (
    pitcher_statcast["last_speed"] - pitcher_statcast["speed"]
)

