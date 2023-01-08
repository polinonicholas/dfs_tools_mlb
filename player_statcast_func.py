from dfs_tools_mlb.compile.stats_mlb import get_statcast_p, get_statcast_h
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.compile import current_season as cs

def print_p_stats(player_id, year=int(cs['season_id']), start = cs['reg_start']):
    p = get_statcast_p(player_id, year)
    recent_p = p[(p["date"] >= str(tf.fifteen_days))].reset_index()
    previous_p = p[
        ((p["date"] < str(tf.fifteen_days)) & (p["date"] >= start))
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


def print_h_stats(player_id, year=int(cs['season_id']), graph = True):
    # lookup individual hitter statcast
    hitter_mlb_id = player_id
    hitter_statcast_time = str(tf.thirty_days)

    hitter_plays = get_statcast_h(hitter_mlb_id, year)
    filtered_hitter_plays = hitter_plays[
        (hitter_plays["date"] >= hitter_statcast_time)
    ].reset_index()
    month_distance = filtered_hitter_plays['distance'].median()
    month_launch = filtered_hitter_plays['launchSpeed'].median()
    month_count = filtered_hitter_plays['launchSpeed'].count()
    # print(filtered_hitter_plays.describe())
    hitter_statcast_time = str(tf.fifteen_days)
    filtered_hitter_plays = hitter_plays[
        (hitter_plays["date"] >= hitter_statcast_time)
    ].reset_index()
    fifteen_distance = filtered_hitter_plays['distance'].median()
    fifteen_launch = filtered_hitter_plays['launchSpeed'].median()
    fifteen_count = filtered_hitter_plays['launchSpeed'].count()
    # print(filtered_hitter_plays.describe())
    hitter_statcast_time = str(tf.seven_days)
    filtered_hitter_plays = hitter_plays[
        (hitter_plays["date"] >= hitter_statcast_time)
    ].reset_index()
    week_distance = filtered_hitter_plays['distance'].median()
    week_launch = filtered_hitter_plays['launchSpeed'].median()
    week_count = filtered_hitter_plays['launchSpeed'].count()
    # print(filtered_hitter_plays.describe())
    hitter_statcast_time = str(tf.yesterday)
    filtered_hitter_plays = hitter_plays[
        (hitter_plays["date"] >= hitter_statcast_time)
    ].reset_index()
    ytd_distance = filtered_hitter_plays['distance'].median()
    ytd_launch = filtered_hitter_plays['launchSpeed'].median()
    ytd_count = filtered_hitter_plays['launchSpeed'].count()
    # print(filtered_hitter_plays.describe())
    data = [[month_distance, month_launch, month_count],
            [fifteen_distance, fifteen_launch, fifteen_count],
            [week_distance, week_launch, week_count],
            [ytd_distance, ytd_launch, ytd_count]]
    print(data)
    
    
    
    df = pd.DataFrame(data, columns = ['distance', 'launch', 'count'], 
                      index = [30,
                               15,
                               7,
                               1])
    if graph:
        import matplotlib.pyplot as plt
        df.reset_index().plot(x = 'distance', y = 'index', ylabel = 'Day', xlim = (0,270)).invert_yaxis()
        df.reset_index().plot(x = 'launch', y = 'index', ylabel = 'Day', xlim = (0,100)).invert_yaxis()
        df.reset_index().plot(x = 'count', y = 'index', ylabel = 'Day', xlim = (0,65))
    return df




