from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb import settings
import pickle
from pathlib import Path

data_path = settings.STORAGE_DIR.joinpath(f"ump_data_{tf.today}.pickle")
if not data_path.exists():
    from dfs_tools_mlb.dataframes.game_data import game_data

    ump_data = game_data.groupby("umpire", as_index=True)[
        [
            "fd_points",
            "adj_pa",
            "fd_points+",
            "fd_points_lhb",
            "adj_pa_lhb",
            "fd_points_rhb",
            "adj_pa_rhb",
            "strikeouts",
            "stolen_base",
            "caught_stealing",
            "ejections",
            "success_lhb",
            "success_rhb",
            "fail_lhb",
            "fail_rhb",
            "void_lhb",
            "void_rhb",
            "field_error",
            "passed_ball",
            "grounded_into_double_play",
            "condition",
            "start_time",
            "temp",
            "wind_direction",
            "wind_speed",
            "single",
            "double",
            "triple",
            "home_run",
        ]
    ].sum()
    ump_data["fd_pts_pa"] = ump_data["fd_points"] / ump_data["adj_pa"]
    ump_data["fd_pts_pa_lhb"] = ump_data["fd_points_lhb"] / ump_data["adj_pa_lhb"]
    ump_data["fd_pts_pa_rhb"] = ump_data["fd_points_rhb"] / ump_data["adj_pa_rhb"]
    ump_data.index = ump_data.index.astype("int")

    for file in Path.iterdir(settings.STORAGE_DIR):
        if "ump_data" in str(file):
            file.unlink()
    with open(data_path, "wb") as file:
        pickle.dump(ump_data, file)

else:
    import pandas as pd

    ump_data = pd.read_pickle(data_path)


def qualified_ump_stats(df, exclude=[], series_or_columns=None):
    df = df[(df["adj_pa"] > settings.MIN_PA_UMP) & (~df.index.isin(exclude))]
    if series_or_columns:
        return df[series_or_columns]
    else:
        return df
