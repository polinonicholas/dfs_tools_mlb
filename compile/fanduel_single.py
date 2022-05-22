import pickle
import pandas as pd
import numpy as np
from functools import cached_property
from dfs_tools_mlb import settings
from dfs_tools_mlb import config
from dfs_tools_mlb.utils.storage import pickle_path
from dfs_tools_mlb.utils.time import time_frames as tf
from dfs_tools_mlb.compile import teams
from dfs_tools_mlb.utils.pd import modify_team_name
from dfs_tools_mlb.utils.pd import sm_merge_single
import random


class FDSlateSingle:
    def __init__(
        self,
        entries_file=config.get_fd_file(),
        slate_number=1,
        lineups=150,
        stack_points_weight=2,
        stack_threshold=120,
        heavy_weight_stack=False,
        max_batting_order=7,
        h_fades=[],
    ):

        self.entry_csv = entries_file
        if not self.entry_csv:
            raise TypeError(
                "There are no fanduel entries files in specified DL_FOLDER, obtain one at fanduel.com/upcoming"
            )

        self.slate_number = slate_number
        self.lineups = lineups

        self.points_weight = stack_points_weight
        self.stack_threshold = stack_threshold

        self.heavy_weight_stack = heavy_weight_stack

        self.max_batting_order = max_batting_order
        self.h_fades = h_fades

    def entries_df(self, reset=False):
        df_file = pickle_path(
            name=f"lineup_entries_{tf.today}_{self.slate_number}",
            directory=settings.FD_DIR,
        )
        path = settings.FD_DIR.joinpath(df_file)
        if path.exists() and not reset:
            df = pd.read_pickle(path)
        else:
            cols = [
                "entry_id",
                "contest_id",
                "contest_name",
                "MVP - 2X Points",
                "STAR - 1.5X Points",
                "UTIL",
                "UTIL.1",
                "UTIL.2",
            ]
            csv_file = self.entry_csv
            with open(csv_file, "r") as f:
                df = pd.read_csv(f, usecols=cols)
            df = df[~df["entry_id"].isna()]
            df = df.astype({"entry_id": np.int64})
            with open(df_file, "wb") as f:
                pickle.dump(df, f)
        return df

    @cached_property
    def player_info_df(self):
        df_file = pickle_path(
            name=f"player_info_{tf.today}_{self.slate_number}",
            directory=settings.FD_DIR,
        )
        path = settings.FD_DIR.joinpath(df_file)
        if path.exists():
            df = pd.read_pickle(path)
        else:
            cols = [
                "Player ID + Player Name",
                "Id",
                "Position",
                "First Name",
                "Nickname",
                "Last Name",
                "FPPG",
                "Salary",
                "Game",
                "Team",
                "Opponent",
                "Injury Indicator",
                "Injury Details",
                "Roster Position",
            ]
            csv_file = self.entry_csv
            with open(csv_file, "r") as f:
                df = pd.read_csv(f, skiprows=lambda x: x < 6, usecols=cols)
            df.rename(
                columns={
                    "Id": "fd_id",
                    "Player ID + Player Name": "fd_id_name",
                    "Nickname": "name",
                    "Position": "fd_position",
                    "Roster Position": "fd_r_position",
                    "Injury Indicator": "fd_injury_i",
                    "Injury Details": "fd_injury_d",
                    "Opponent": "opp",
                    "First Name": "f_name",
                    "Last Name": "l_name",
                    "Salary": "fd_salary",
                },
                inplace=True,
            )

            df.columns = df.columns.str.lower()
            df = modify_team_name(df, columns=["team", "opp"])

            with open(df_file, "wb") as f:
                pickle.dump(df, f)
        return df

    @cached_property
    def active_teams(self):
        return self.player_info_df["team"].unique()

    @cached_property
    def default_stack_dict(self):
        team_dict = {}
        for team in self.active_teams:
            team_dict[team] = 0
        return team_dict

    def insert_lineup(self, idx, lineup):
        cols = ["MVP - 2X Points", "STAR - 1.5X Points", "UTIL", "UTIL.1", "UTIL.2"]
        df = self.entries_df()
        df.loc[idx, cols] = lineup
        file = pickle_path(
            name=f"lineup_entries_{tf.today}_{self.slate_number}",
            directory=settings.FD_DIR,
        )
        with open(file, "wb") as f:
            pickle.dump(df, f)
        return f"Inserted {lineup} at index {idx}."

    def finalize_entries(self):
        df = self.entries_df()
        csv = self.entry_csv
        df.rename(columns={"UTIL.1": "UTIL", "UTIL.2": "UTIL"}, inplace=True)
        df.to_csv(csv, index=False)
        return f"Stored lineups at {csv}"

    @cached_property
    def team_instances(self):
        instances = set()
        for team in teams.Team:
            if team.name in self.active_teams:
                instances.add(team)
        return instances

    def get_hitters(self):
        df_file = pickle_path(
            name=f"all_h_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(df_file)
        for team in self.team_instances:
            hitters = team.lineup_df()
            merge = self.player_info_df[(self.player_info_df["team"] == team.name)]
            hitters = sm_merge_single(hitters, merge, ratio=0.75, suffixes=("", "_fd"))
            hitters.drop_duplicates(subset="mlb_id", inplace=True)
            order_filter = hitters["order"] <= self.max_batting_order
            hitters_order = hitters[order_filter]
            team.salary = hitters_order["fd_salary"].sum() / len(hitters_order.index)
            cols = [
                "raw_points",
                "venue_points",
                "temp_points",
                "points",
                "salary",
                "sp_mu",
            ]
            points_file = pickle_path(
                name=f"team_points_{tf.today}_{self.slate_number}",
                directory=settings.FD_DIR,
            )
            points_path = settings.FD_DIR.joinpath(points_file)
            if points_path.exists():
                p_df = pd.read_pickle(points_path)
            else:
                p_df = pd.DataFrame(columns=cols)
            p_df.loc[team.name, cols] = [
                team.raw_points,
                team.venue_points,
                team.temp_points,
                team.points,
                team.salary,
                team.sp_mu,
            ]
            with open(points_file, "wb") as f:
                pickle.dump(p_df, f)
            if path.exists():
                df = pd.read_pickle(path)
                df.drop(index=df[df["team"] == team.name].index, inplace=True)
                df = pd.concat([df, hitters], ignore_index=True)
            else:
                df = hitters
            with open(df_file, "wb") as f:
                pickle.dump(df, f)

        return df

    def h_df(self):
        df_file = pickle_path(
            name=f"all_h_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(df_file)
        if not path.exists():
            df = self.get_hitters()
        else:
            df = pd.read_pickle(path)
        return df

    def points_df(self):
        file = pickle_path(
            name=f"team_points_{tf.today}_{self.slate_number}",
            directory=settings.FD_DIR,
        )
        path = settings.FD_DIR.joinpath(file)
        if not path.exists():
            self.get_hitters()
        df = pd.read_pickle(path).apply(pd.to_numeric)
        return df

    def stacks_df(self):
        lineups = self.lineups
        df = self.points_df()

        df["p_z"] = (
            (df["points"] - df["points"].mean()) / df["points"].std()
        ) * self.points_weight
        df["s_z"] = ((df["salary"] - df["salary"].mean()) / df["salary"].std()) * -(
            2 - self.points_weight
        )
        df["stacks"] = 1000
        increment = 0
        while df["stacks"].max() > self.stack_threshold:
            df_c = df.copy()
            df_c["z"] = ((df_c["p_z"] + df_c["s_z"]) / 2) + increment
            df_c = df_c[df_c["z"] > 0]
            lu_base = lineups / len(df_c.index)
            df_c["stacks"] = lu_base * df_c["z"]
            if df_c["stacks"].max() > self.stack_threshold:
                increment += 0.01
                continue
            diff = lineups - df_c["stacks"].sum()
            df_c["stacks"] = round(df_c["stacks"])
            while df_c["stacks"].sum() < lineups:
                if self.heavy_weight_stack:
                    df_c["stacks"] = df_c["stacks"] + np.ceil(
                        ((diff / len(df.index)) * df_c["z"])
                    )
                else:
                    df_c["stacks"] = df_c["stacks"] + np.ceil((diff / len(df.index)))
                df_c["stacks"] = round(df_c["stacks"])
            while df_c["stacks"].sum() > lineups:
                for idx in df_c.index:
                    if df_c["stacks"].sum() == lineups:
                        break
                    else:
                        df_c.loc[idx, "stacks"] -= 1
            if df_c["stacks"].max() > self.stack_threshold:
                increment += 0.01
                continue
            df = df_c

        i = df[df["stacks"] == 0].index
        df.drop(index=i, inplace=True)
        return df

    def h_counts(self):
        file = pickle_path(
            name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(file)
        if path.exists():
            df = pd.read_pickle(path)
            return df
        else:
            return "No lineups stored yet."

    def build_lineups(
        self,
        lus=150,
        index_track=0,
        max_lu_total=90,
        max_sal=35000,
        stack_sample=4,
        util_replace_filt=1000,
        variance=0,
        non_stack_max_order=5,
        custom_counts={},
        custom_stacks=None,
        below_avg_count=30,
        stack_expand_limit=30,
        exempt=[],
        full_stack_cutoff=50,
    ):
        max_order = self.max_batting_order
        # all hitters in slate
        h = self.h_df()
        # dropping faded, platoon, and low order (max_order) players.
        h_fade_filt = h["fd_id"].isin(self.h_fades)
        h_order_filt = h["order"] > max_order
        h_exempt_filt = ~h["fd_id"].isin(exempt)
        hfi = h[(h_fade_filt | h_order_filt) & h_exempt_filt].index
        h.drop(index=hfi, inplace=True)
        # count each players entries
        h["t_count"] = 0
        # count non_stack
        h["ns_count"] = 0
        h_count_df = h.copy()
        # risk_limit should always be >= below_avg_count
        h.loc[
            (h["exp_ps_sp_pa"] < h["exp_ps_sp_pa"].median()), "t_count"
        ] = below_avg_count
        exempt_filt = h["fd_id"].isin(exempt)
        h.loc[exempt_filt, "t_count"] = 0
        for k, v in custom_counts.items():
            h.loc[h["fd_id"] == k, "t_count"] = v
        # team: stacks to build
        if not custom_stacks:
            s = self.stacks_df()["stacks"].to_dict()
        else:
            s = custom_stacks
        # lineups to build
        sorted_lus = []
        while lus > 0:
            salary = 0
            # if lineup fails requirements, reset will be set to true.
            reset = False
            if lus > full_stack_cutoff:
                stack_size = 4
            else:
                stack_size = 3

            stacks = {k: v for k, v in s.items() if v > 0}
            stack = random.choice(list(stacks.keys()))
            remaining_stacks = stacks[stack]
            # lookup players on the team for the selected stack
            stack_df = h[h["team"] == stack]
            # stack_key = 'exp_ps_sp_pa'
            # non_stack_key = 'exp_ps_sp_pa'
            if remaining_stacks % 4 == 0:
                stack_key = "total_pitches"
            elif remaining_stacks % 3 == 0:
                stack_key = "fd_hr_weight"
            elif remaining_stacks % 2 == 0:
                stack_key = "points"
            else:
                stack_key = "exp_ps_sp_pa"
            if lus % 2 == 0:
                non_stack_key = "exp_ps_sp_pa"
            else:
                non_stack_key = "points"
            # filter the selected stack by stack_sample arg.
            if remaining_stacks > stack_expand_limit:
                highest = stack_df.loc[
                    stack_df[stack_key].nlargest(stack_sample + 1).index
                ]
            else:
                highest = stack_df.loc[stack_df[stack_key].nlargest(stack_sample).index]
            # array of fanduel ids of the selected hitters
            stack_ids = highest["fd_id"].values
            # initial empty lineup, ordered by fanduel structed and mapped by p_map
            lineup = [None, None, None, None, None]
            # insert current pitcher into lineup

            # try to create a 4-man stack that satifies position requirements 5 times, else 3-man stack.
            try:
                samp = random.sample(sorted(stack_ids), stack_size)
            except ValueError:
                samp = random.sample(sorted(stack_ids), stack_size - 1)
            stack_salary = h.loc[h["fd_id"].isin(samp), "fd_salary"].sum()
            salary += stack_salary
            rem_sal = max_sal - salary
            random.shuffle(stack_ids)

            for x, y in enumerate(samp):
                lineup[x] = y

            # filter out hitters on the team of the current stack, as they are already in the lineup.
            stack_filt = h["team"] != stack
            # filter out players hitting below specified lineup spot
            order_filt = (h["order"] <= non_stack_max_order) | exempt_filt
            # filter out players not on a team being stacked on slate and proj. points not in 90th percentile.
            max_stack = max(stacks.values())
            # variance default is 0
            count_filt = h["t_count"] < ((max_lu_total - max_stack) - variance)
            plat_filt = h["is_platoon"] != True
            for y, z in enumerate(lineup):
                if not z:
                    # filter out players already in lineup, lineup will change with each iteration
                    dupe_filt = ~h["fd_id"].isin(lineup)
                    # filter out players not eligible for the current position being filled.
                    # get the ammount of roster spots that need filling.
                    npl = len([idx for idx, spot in enumerate(lineup) if not spot])
                    # the avgerage salary remaining for each empty lineup spot
                    avg_sal = rem_sal / npl
                    # filter out players with a salary greater than the average avg_sal above
                    sal_filt = h["fd_salary"] <= avg_sal

                    try:
                        hitters = h[
                            stack_filt
                            & dupe_filt
                            & count_filt
                            & sal_filt
                            & order_filt
                            & plat_filt
                        ]
                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    except (KeyError, ValueError):
                        try:
                            hitters = h[
                                stack_filt
                                & dupe_filt
                                & count_filt
                                & sal_filt
                                & order_filt
                            ]
                            hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                        except (KeyError, ValueError):
                            try:
                                hitters = h[
                                    stack_filt
                                    & dupe_filt
                                    & count_filt
                                    & sal_filt
                                    & plat_filt
                                ]
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                            except (KeyError, ValueError):
                                try:
                                    hitters = h[
                                        stack_filt
                                        & dupe_filt
                                        & count_filt
                                        & order_filt
                                        & plat_filt
                                    ]
                                    hitter = hitters.loc[
                                        hitters[non_stack_key].idxmax()
                                    ]
                                except:
                                    hitters = h[
                                        stack_filt
                                        & dupe_filt
                                        & sal_filt
                                        & order_filt
                                        & plat_filt
                                    ]
                                    hitter = hitters.loc[
                                        hitters[non_stack_key].idxmax()
                                    ]
                    salary += hitter["fd_salary"].item()
                    rem_sal = max_sal - salary
                    # if the selected hitter's salary put the lineup over the max. salary, try to find replacement.
                    if rem_sal < 0:
                        r_sal = hitter["fd_salary"].item()
                        try:
                            salary_df = hitters[
                                (hitters["fd_salary"] <= (r_sal + rem_sal))
                            ]
                            hitter = salary_df.loc[hitters[non_stack_key].idxmax()]
                            salary += hitter["fd_salary"]
                            salary -= r_sal
                            rem_sal = max_sal - salary
                        except (ValueError, KeyError):
                            try:
                                hitters = h[stack_filt & dupe_filt]
                                salary_df = hitters[
                                    (hitters["fd_salary"] <= (r_sal + rem_sal))
                                ]
                                hitter = salary_df.loc[hitters[non_stack_key].idxmax()]
                                salary += hitter["fd_salary"]
                                salary -= r_sal
                                rem_sal = max_sal - salary

                            except (KeyError, ValueError):
                                reset = True
                                print("resetting")
                                break

                    h_id = hitter["fd_id"]
                    lineup[y] = h_id
                    used_players = []
                    print(lineup)

            while not reset and lineup[0:2] + sorted(lineup[2:5]) in sorted_lus:
                mvp = lineup[0]
                all_star = lineup[1]
                potential_lu = lineup[0:2] + sorted(lineup[2:5])
                potential_lu[0] = all_star
                potential_lu[1] = mvp
                if potential_lu not in sorted_lus:
                    lineup = potential_lu
                    break
                try:
                    # redeclare use_teams each pass
                    h_df = h[h["fd_id"].isin(lineup)]
                    # append players already attempted to used_players and filter them out each loop
                    used_filt = ~h["fd_id"].isin(used_players)
                    dupe_filt = ~h["fd_id"].isin(lineup)
                    utility = h[h["fd_id"] == lineup[4]]
                    used_players.append(utility["fd_id"])
                    r_salary = utility["fd_salary"].item()
                    # only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                    sal_filt = h["fd_salary"].between(
                        (r_salary - util_replace_filt), (rem_sal + r_salary)
                    )
                    # don't use players on team against current pitcher
                    hitters = h[
                        dupe_filt
                        & sal_filt
                        & used_filt
                        & count_filt
                        & stack_filt
                        & order_filt
                        & plat_filt
                    ]
                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    used_players.append(hitter["fd_id"])
                    salary += hitter["fd_salary"].item()
                    salary -= utility["fd_salary"].item()
                    rem_sal = max_sal - salary
                    lineup[4] = hitter["fd_id"]
                    h_df = h[h["fd_id"].isin(lineup)]
                # same as above, but no fade_filt and increasing minimum thresehold by 100.
                except (KeyError, ValueError):
                    try:
                        # redeclare use_teams each pass
                        h_df = h[h["fd_id"].isin(lineup)]
                        # append players already attempted to used_players and filter them out each loop
                        used_filt = ~h["fd_id"].isin(used_players)
                        dupe_filt = ~h["fd_id"].isin(lineup)
                        utility = h[h["fd_id"] == lineup[3]]
                        used_players.append(utility["fd_id"])
                        r_salary = utility["fd_salary"].item()
                        # only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                        sal_filt = h["fd_salary"].between(
                            (r_salary - util_replace_filt), (rem_sal + r_salary)
                        )
                        # don't use players on team against current pitcher
                        hitters = h[
                            dupe_filt
                            & sal_filt
                            & used_filt
                            & count_filt
                            & stack_filt
                            & order_filt
                            & plat_filt
                        ]
                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                        used_players.append(hitter["fd_id"])
                        salary += hitter["fd_salary"].item()
                        salary -= utility["fd_salary"].item()
                        rem_sal = max_sal - salary
                        lineup[3] = hitter["fd_id"]
                        h_df = h[h["fd_id"].isin(lineup)]
                    except (KeyError, ValueError):
                        reset = True
                        print("resetting")
                        break
            if reset == True:
                continue
            #!!the lineup meets all parameters at this point.!!
            # decrease lus arg, loop ends at 0.
            lus -= 1
            # append the new lineup to the sorted lus list for next loop.
            sorted_lus.append(lineup[0:2] + sorted(lineup[2:5]))
            # insert the lineup in the lineup df
            self.insert_lineup(index_track, lineup)
            # increase index for next lineup insertion.
            index_track += 1
            # decrease the current stack's value, so it won't be attempted once it reaches 0.
            s[stack] -= 1
            # keep track of total insertions and stack insertions, players exceeding max_lu_total will be dropped next loop.
            lu_filt = h["fd_id"].isin(lineup)
            h.loc[lu_filt, "t_count"] += 1
            h.loc[lu_filt & stack_filt, "ns_count"] += 1
            # print(lus)
            # print(index_track)
            print(salary)
            print(lus)
            # keep track of players counts, regardless if they're eventually dropped.
            h_count_df.loc[(h_count_df["fd_id"].isin(lineup)), "t_count"] += 1

        # dump the counts into pickled df for analysis
        h_count_file = pickle_path(
            name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )

        with open(h_count_file, "wb") as f:
            pickle.dump(h_count_df, f)

        return sorted_lus
