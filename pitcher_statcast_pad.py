from dfs_tools_mlb.compile.stats_mlb import get_statcast_p, get_statcast_h
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def print_p_stats(player_id, year):
    p = get_statcast_p(player_id, year)
    recent_p = p[(p["date"] >= "2021-06-20")].reset_index()
    previous_p = p[
        ((p["date"] <= "2021-06-20") & (p["date"] >= "2021-04-14"))
    ].reset_index()
    # recent_p.columns.tolist()

    print(recent_p["result"].value_counts())
    print(recent_p["pitch"].unique())

    print(previous_p["effectiveSpeed"].median())
    print(recent_p["effectiveSpeed"].median())

    print(previous_p["releaseSpinRate"].median())
    print(recent_p["releaseSpinRate"].median())

    print(previous_p["launchAngle"].median())
    print(recent_p["launchAngle"].median())
    try:
        print("PREVIOUS")
        strikes = previous_p[
            (previous_p["result"].str.contains("strike"))
            | (previous_p["result"].str.contains("_tip"))
        ]
        print("Strikes:")
        print(len(strikes.index) / len(previous_p.index))
        extra_base = previous_p[
            (previous_p["result"].str.contains("home_run"))
            | (previous_p["result"].str.contains("double"))
            | (previous_p["result"].str.contains("triple"))
        ]
        print("Extra Base:")
        print(len(extra_base.index) / len(previous_p.index))
        walk = previous_p[(previous_p["result"].str.contains("walk"))]
        strikeout = previous_p[(previous_p["result"].str.contains("strikeout"))]
        print("K-BB:")
        final_ab_pitch = previous_p[
            (previous_p["result"].str.contains("strikeout"))
            | (previous_p["result"].str.contains("single"))
            | (previous_p["result"].str.contains("double"))
            | (previous_p["result"].str.contains("walk"))
            | (previous_p["result"].str.contains("home_run"))
            | (previous_p["result"].str.contains("fielders_choice"))
            | (previous_p["result"].str.contains("grounded_into_double_play"))
            | (previous_p["result"].str.contains("triple"))
            | (previous_p["result"].str.contains("sac_fly"))
        ]
        walk_per = len(walk.index) / len(final_ab_pitch.index)
        k_per = len(strikeout.index) / len(final_ab_pitch.index)
        print(k_per - walk_per)
    except:
        pass
    # print(previous_p.columns.tolist())
    print("Total:")
    print(len(previous_p.index))
    print("")
    try:
        print("RECENT")
        strikes = recent_p[
            (recent_p["result"].str.contains("strike"))
            | (recent_p["result"].str.contains("_tip"))
        ]
        print("Strikes:")
        print(len(strikes.index) / len(recent_p.index))
        extra_base = recent_p[
            (recent_p["result"].str.contains("home_run"))
            | (recent_p["result"].str.contains("double"))
            | (recent_p["result"].str.contains("triple"))
        ]
        print("Extra Base:")
        print(len(extra_base.index) / len(recent_p.index))
        walk = recent_p[(recent_p["result"].str.contains("walk"))]
        strikeout = recent_p[(recent_p["result"].str.contains("strikeout"))]
        print("K-BB:")
        final_ab_pitch = recent_p[
            (recent_p["result"].str.contains("strikeout"))
            | (recent_p["result"].str.contains("single"))
            | (recent_p["result"].str.contains("double"))
            | (recent_p["result"].str.contains("walk"))
            | (recent_p["result"].str.contains("home_run"))
            | (recent_p["result"].str.contains("fielders_choice"))
            | (recent_p["result"].str.contains("grounded_into_double_play"))
            | (recent_p["result"].str.contains("triple"))
            | (recent_p["result"].str.contains("sac_fly"))
        ]
        walk_per = len(walk.index) / len(final_ab_pitch.index)
        k_per = len(strikeout.index) / len(final_ab_pitch.index)
        print(k_per - walk_per)
    except:
        pass
    # print(recent_p.columns.tolist())
    print("Total:")
    print(len(recent_p.index))
    return None


def print_h_stats(player_id, year):
    # lookup individual hitter statcast
    hitter_mlb_id = player_id
    hitter_statcast_time = str(tf.fifteen_days)
    hitter_plays = get_statcast_h(hitter_mlb_id, year)
    filtered_hitter_plays = hitter_plays[
        (hitter_plays["date"] >= hitter_statcast_time)
    ].reset_index()
    print(filtered_hitter_plays.describe())

    hitter_statcast_time = str(tf.seven_days)
    hitter_plays = get_statcast_h(hitter_mlb_id, year)
    filtered_hitter_plays = hitter_plays[
        (hitter_plays["date"] >= hitter_statcast_time)
    ].reset_index()
    print(filtered_hitter_plays.describe())

    hitter_statcast_time = str(tf.yesterday)
    hitter_plays = get_statcast_h(hitter_mlb_id, year)
    filtered_hitter_plays = hitter_plays[
        (hitter_plays["date"] >= hitter_statcast_time)
    ].reset_index()
    print(filtered_hitter_plays.describe())
    return None


print_p_stats(343434, 2022)
print_h_stats(12123, 2022)
