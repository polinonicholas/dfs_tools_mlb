from dfs_tools_mlb import settings
from dfs_tools_mlb.compile.historical_data import historical_data
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf
import pickle
from pathlib import Path
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac

data_path = settings.STORAGE_DIR.joinpath(f"game_data_{tf.today}.pickle")
if not data_path.exists():

    def split_fd_pts(df):
        df.fillna(0, inplace=True)
        b = settings.fd_scoring["hitters"]["base"]
        rbi = settings.fd_scoring["hitters"]["rbi"]
        r = settings.fd_scoring["hitters"]["run"]
        df["fd_points"] = (
            (df["single"] * b)
            + (df["walk"] * b)
            + (df["double"] * (b * 2))
            + (df["triple"] * (b * 3))
            + (df["home_run"] * (b * 4))
        )
        df["fd_points+"] = (
            (df["single"] * b)
            + (df["walk"] * b)
            + (df["double"] * (b * 2))
            + (df["triple"] * (b * 3))
            + (df["home_run"] * (b * 4))
            + (df["runs"] * r)
            + (df["rbi"] * rbi)
        )
        df["fd_points_lhb"] = (
            (df["single_lhb"] * b)
            + (df["walk_lhb"] * b)
            + (df["double_lhb"] * (b * 2))
            + (df["triple_lhb"] * (b * 3))
            + (df["home_run_lhb"] * ((b * 4) + r))
        )
        df["fd_points_rhb"] = (
            (df["single_rhb"] * b)
            + (df["walk_rhb"] * b)
            + (df["double_rhb"] * (b * 2))
            + (df["triple_rhb"] * (b * 3))
            + (df["home_run_rhb"] * ((b * 4) + r))
        )

        return df

    game_data = historical_data(
        start=settings.stat_range["start"], end=settings.stat_range["end"]
    )
    game_data = game_data[game_data["last_inning"] >= 9].apply(
        pd.to_numeric, errors="ignore"
    )
    game_data.loc[
        game_data["condition"].isin(mac.weather.roof_closed), "temp"
    ] = settings.INDOOR_TEMP
    game_data.loc[
        game_data["condition"].isin(mac.weather.roof_closed), "wind_speed"
    ] = settings.INDOOR_WIND
    game_data["date"] = pd.to_datetime(game_data["date"], infer_datetime_format=True)

    def split_columns(split="RHB", event_type="success"):
        all_split = (
            "vs_" + split.upper() + "_" + x for x in mac.event_types[event_type]
        )
        return [x for x in all_split if x in game_data.columns]

    lhb_fail = split_columns(split="LHB", event_type="fail")
    lhb_success = split_columns(split="LHB", event_type="success")
    lhb_void = split_columns(split="LHB", event_type="void")
    rhb_fail = split_columns(split="RHB", event_type="fail")
    rhb_success = split_columns(split="RHB", event_type="success")
    rhb_void = split_columns(split="RHB", event_type="void")
    strikeouts = [x for x in game_data.columns if x in mac.event_types.strikeout]
    stolen_base = [
        x for x in game_data.columns if x in mac.event_types.stolen_base.success
    ]
    caught_stealing = [
        x for x in game_data.columns if x in mac.event_types.stolen_base.fail
    ]
    fail = [x for x in game_data.columns if x in mac.event_types.fail]
    success = [x for x in game_data.columns if x in mac.event_types.success]
    void = [x for x in game_data.columns if x in mac.event_types.void]
    field_errors = [x for x in game_data.columns if x in mac.event_types.error.field]
    total_errors = [x for x in game_data.columns if x in mac.event_types.error.all]
    ejections = [x for x in game_data.columns if "ejection" in x]
    game_data["strikeouts"] = game_data[strikeouts].sum(axis=1)
    game_data["stolen_base"] = game_data[stolen_base].sum(axis=1)
    game_data["caught_stealing"] = game_data[caught_stealing].sum(axis=1)
    game_data["ejections"] = game_data[ejections].sum(axis=1)
    game_data["success_lhb"] = game_data[lhb_success].sum(axis=1)
    game_data["success_rhb"] = game_data[rhb_success].sum(axis=1)
    game_data["fail_lhb"] = game_data[lhb_fail].sum(
        axis=1, skipna=True, numeric_only=True
    )
    game_data["fail_rhb"] = game_data[rhb_fail].sum(axis=1)
    game_data["void_lhb"] = game_data[lhb_void].sum(axis=1)
    game_data["void_rhb"] = game_data[rhb_void].sum(axis=1)
    game_data["adj_pa_lhb"] = game_data["success_lhb"] + game_data["fail_lhb"]
    game_data["adj_pa_rhb"] = game_data["success_rhb"] + game_data["fail_rhb"]
    game_data["adj_pa"] = game_data["adj_pa_lhb"] + game_data["adj_pa_rhb"]
    drop_index = game_data[(game_data["adj_pa"] == 0)].index
    # write function
    game_data = game_data.rename(lambda x: x + "_rhb" if "vs_RHB_" in x else x, axis=1)
    game_data = game_data.rename(lambda x: x + "_lhb" if "vs_LHB_" in x else x, axis=1)
    game_data.columns = game_data.columns.str.replace("vs_RHB_", "")
    game_data.columns = game_data.columns.str.replace("vs_LHB_", "")
    game_data = split_fd_pts(game_data)
    for file in Path.iterdir(settings.STORAGE_DIR):
        if "game_data" in str(file):
            file.unlink()
    with open(data_path, "wb") as file:
        pickle.dump(game_data, file)

else:
    game_data = pd.read_pickle(data_path)
