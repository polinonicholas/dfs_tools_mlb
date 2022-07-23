import pickle
import pandas
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

from itertools import combinations 
from dfs_tools_mlb.utils.sort import (sort_k_by_v,
                                      sort_list_col_by_list_order,
                                      sort_list_by_list)

class FDSlate:
    def __init__(self, 
                 entries_file=config.get_fd_file(), 
                 slate_number = 1, 
                 lineups = 150,
                 p_fades = [],
                 h_fades=[],
                 no_stack=[],
                 stack_threshold = 150, 
                 pitcher_threshold = 150,
                 heavy_weight_stack=True, 
                 heavy_weight_p=True,
                 stack_salary_factor = False,
                 salary_batting_order=5,
                 value_batting_order=5,
                 default_stack_count = 15,
                 name_merge_ratio = .8,
                 min_proj_p_stat=0,
                 drop_p_stat='points',
                 h_drop={'min_proj_h_stat': 0,
                         'drop_h_stat': 'points'},
                 stack_wgts = {'points': 7.5,
                  'raw points': 0,
                  'salary': 2.5,
                  'opp_avg_vs': 0,
                  'matchup': 0,
                  'lu_talent': 0,
                  'venue': 0,
                  'environment': 0},
                 tot_stack_wgt = 10):
        
        

        self.entry_csv = entries_file
        if not self.entry_csv:
            raise TypeError(
                "There are no fanduel entries files in specified DL_FOLDER, obtain one at fanduel.com/upcoming"
            )
        p_fades.extend(settings.ppd)
        h_fades.extend(settings.ppd)

        no_stack.extend(h_fades)    
      

        self.slate_number = slate_number
        self.lineups = lineups
        self.p_fades = p_fades
        self.h_fades = h_fades
        # daily_info = teams.Team.daily_info()
        # self.p_fades.extend(daily_info['rain'])

        self.stack_threshold = stack_threshold
        self.pitcher_threshold = pitcher_threshold
        self.no_stack = no_stack
        self.heavy_weight_stack = heavy_weight_stack
        self.heavy_weight_p = heavy_weight_p
        self.salary_batting_order = salary_batting_order
        self.stack_salary_factor = stack_salary_factor
        self.default_stack_count = default_stack_count
        self.name_merge_ratio = name_merge_ratio

        self.value_batting_order = value_batting_order
        self.min_proj_p_stat = min_proj_p_stat 
        self.drop_p_stat = drop_p_stat 
        self.h_drop = h_drop
        self.stack_wgts = stack_wgts
        self.tot_stack_wgt = tot_stack_wgt
        

    def entries_df(self, reset=False):
        df_file = pickle_path(
            name=f"lineup_entries_{tf.today}_{self.slate_number}",
            directory=settings.FD_DIR,
        )
        path = settings.FD_DIR.joinpath(df_file)
        if path.exists() and not reset:
            df = pandas.read_pickle(path)
        else:
            cols = [
                "entry_id",
                "contest_id",
                "contest_name",
                "P",
                "C/1B",
                "2B",
                "3B",
                "SS",
                "OF",
                "OF.1",
                "OF.2",
                "UTIL",
            ]
            csv_file = self.entry_csv
            with open(csv_file, "r") as f:
                df = pandas.read_csv(f, usecols=cols)
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
            df = pandas.read_pickle(path)
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
                df = pandas.read_csv(f, skiprows=lambda x: x < 6, usecols=cols)
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
            df.loc[df["fd_position"].isna(), "fd_position"] = "xxxx"
            df.loc[df["fd_r_position"].isna(), "fd_r_position"] = "xxxx"
            df["fd_position"] = (
                df["fd_position"].str.replace("C", "1B").str.lower().str.split("/")
            )
            df["fd_r_position"] = df["fd_r_position"].str.lower().str.split("/")

            p_idx = df[df["fd_position"].apply(lambda x: "p" in x)].index
            df.loc[p_idx, "is_p"] = True
            h_idx = df[df["fd_r_position"].apply(lambda x: "util" in x)].index
            df.loc[h_idx, "is_h"] = True
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
            team_dict[team] = self.default_stack_count
        return team_dict

    def insert_lineup(self, idx, lineup):
        cols = ["P", "C/1B", "2B", "3B", "SS", "OF", "OF.1", "OF.2", "UTIL"]
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
        df.rename(columns={"OF.1": "OF", "OF.2": "OF"}, inplace=True)
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
            merge = self.player_info_df[
                (self.player_info_df["team"] == team.name)
                & (self.player_info_df["is_h"] == True)
            ]
            hitters = sm_merge_single(
                hitters, merge, ratio=self.name_merge_ratio, suffixes=("", "_fd")
            )
            hitters.drop_duplicates(subset="mlb_id", inplace=True)
            order_filter = hitters["order"] <= self.salary_batting_order
            hitters["points_salary"] = hitters["points"] - (hitters["fd_salary"] / 1000)
            hitters_order = hitters[order_filter]
            team.salary = hitters_order["fd_salary"].sum() / len(hitters_order.index)
            cols = [
                "raw_points",
                "venue_points",
                "temp_points",
                "points",
                "salary",
                "sp_mu",
                "raw_talent",
                "ump_avg",
                "venue_temp",
                "venue_avg",
                "env_avg",
                "sp_avg",
            ]
            points_file = pickle_path(
                name=f"team_points_{tf.today}_{self.slate_number}",
                directory=settings.FD_DIR,
            )
            points_path = settings.FD_DIR.joinpath(points_file)
            if points_path.exists():
                p_df = pandas.read_pickle(points_path)
            else:
                p_df = pandas.DataFrame(columns=cols)
            p_df.loc[team.name, cols] = [
                team.raw_points,
                team.venue_points,
                team.temp_points,
                team.points,
                team.salary,
                team.sp_mu,
                team.lu_talent_sp,
                team.ump_avg,
                team.venue_temp,
                team.venue_avg,
                team.env_avg,
                team.sp_avg(),
            ]
            with open(points_file, "wb") as f:
                pickle.dump(p_df, f)
            if path.exists():
                df = pandas.read_pickle(path)
                df.drop(index=df[df["team"] == team.name].index, inplace=True)
                df = pandas.concat([df, hitters], ignore_index=True)
            else:
                df = hitters
            with open(df_file, "wb") as f:

                 pickle.dump(df, f)
        
        position_order_of_importance = sort_k_by_v(self.positional_counts())
        df = sort_list_col_by_list_order(df, 'fd_position', list_to_sort_by = position_order_of_importance)
        # drop_stat = ((df[self.h_drop['drop_h_stat']] <= self.h_drop['min_proj_h_stat']) | (df[self.h_drop['drop_h_stat']] == 0))


        return df

    def get_pitchers(self):
        df_file = pickle_path(
            name=f"all_p_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(df_file)
        for team in self.team_instances:
            # incase of PPD, sometimes a team's game will register vs. another team in next game.
            try:
                pitcher = team.sp_df()
                pitcher["fav"] = team.points - team.opp_instance.points
                merge = self.player_info_df[
                    (self.player_info_df["team"] == team.name)
                    & (self.player_info_df["is_p"] == True)
                ]
                try:
                    pitcher = sm_merge_single(
                        pitcher,
                        merge,
                        ratio=self.name_merge_ratio,
                        suffixes=("", "_fd"),
                    )
                except KeyError:
                    continue
                pitcher.drop_duplicates(subset="mlb_id", inplace=True)
                if path.exists():
                    df = pandas.read_pickle(path)
                    df.drop(index=df[df["team"] == team.name].index, inplace=True)
                    df = pandas.concat([df, pitcher], ignore_index=True)
                else:
                    df = pitcher
                with open(df_file, "wb") as f:
                    pickle.dump(df, f)
            except AttributeError:
                continue


        return df

    def h_df(self, refresh=False):
        df_file = pickle_path(
            name=f"all_h_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(df_file)
        if not path.exists() or refresh:
            df = self.get_hitters()
        else:
            df = pandas.read_pickle(path)
        return df

    def p_df(self, refresh=False):
        df_file = pickle_path(
            name=f"all_p_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(df_file)
        if not path.exists() or refresh:
            df = self.get_pitchers()
        else:
            df = pandas.read_pickle(path)
        df = df[df["is_p"] == True]
        for team in self.team_instances:
            if team.ppd and team.name in self.active_teams:
                filt = df[
                    (df["team"] == team.opp_name) | (df["team"] == team.name)
                ].index
                df.drop(filt, inplace=True)

        df["i_z"] = FDSlate.df_z_score(df, "exp_inn")
        df["f_z"] = FDSlate.df_z_score(df, "fav")
        # df['vt_z'] = FDSlate.df_z_score(df, 'venue_temp', mult = -1)
        df["v_z"] = FDSlate.df_z_score(df, "venue_avg", mult=-1)
        df["u_z"] = FDSlate.df_z_score(df, "ump_avg", mult=-1)
        df["p_z"] = FDSlate.df_z_score(df, "exp_ps_raw")
        df["rmu_z"] = FDSlate.df_z_score(df, "raw_mu")
        df["mu_z"] = FDSlate.df_z_score(df, "mu")
        df["kp_z"] = FDSlate.df_z_score(df, "k_pred")
        df["rk_z"] = FDSlate.df_z_score(df, "k_pred_raw")
        df["s_z"] = FDSlate.df_z_score(df, "fd_salary", mult=-1)
        df["pps_z"] = FDSlate.df_z_score(df, "pitches_start")
        df["e_z"] = FDSlate.df_z_score(df, "env_points", mult=-1)
        df["z"] = (
            (df["f_z"] * 0)
            + (df["rmu_z"] * 0)
            + (df["i_z"] * 0)
            + (df["p_z"] * 7.5)
            + (df["kp_z"] * 0)
            + (df["s_z"] * 2.5)
            + (df["e_z"] * 0)
        ) / 10
        df["mz"] = (
            (df["f_z"] * 0)
            + (df["rmu_z"] * 0)
            + (df["i_z"] * 0)
            + (df["p_z"] * 10)
            + (df["kp_z"] * 0)
            + (df["e_z"] * 0)
        ) / 10
        return df

    def points_df(self, refresh=False):
        file = pickle_path(
            name=f"team_points_{tf.today}_{self.slate_number}",
            directory=settings.FD_DIR,
        )
        path = settings.FD_DIR.joinpath(file)
        if not path.exists() or refresh:
            self.get_hitters()
        df = pandas.read_pickle(path).apply(pandas.to_numeric)
        for team in self.team_instances:
            if team.ppd and team.name in self.active_teams:
                filt = df[(df.index == team.name) | (df.index == team.name)].index

                df.drop(filt, inplace = True)
        df['rp_z'] = FDSlate.df_z_score(df, 'raw_points')
        df['p_z'] = FDSlate.df_z_score(df, 'points')
        df['v_z'] = FDSlate.df_z_score(df, 'venue_avg')
        df['s_z'] = FDSlate.df_z_score(df, 'salary', mult = -1)
        df['mu_z'] = FDSlate.df_z_score(df, 'sp_mu')
        df['t_z'] = FDSlate.df_z_score(df, 'raw_talent')
        df['u_z'] = FDSlate.df_z_score(df, 'ump_avg')
        df['e_z'] = FDSlate.df_z_score(df, 'env_avg')
        df['sa_z'] = FDSlate.df_z_score(df, 'sp_avg')
        df['z'] = (((df['p_z'] * self.stack_wgts['points']) + (df['s_z'] * self.stack_wgts['salary']) + (df['sa_z'] * self.stack_wgts['opp_avg_vs']) + (df['mu_z'] * self.stack_wgts['matchup']) + (df['t_z'] * self.stack_wgts['lu_talent']) + (df['v_z'] * self.stack_wgts['venue']) + (df['e_z'] * self.stack_wgts['environment'])) / self.tot_stack_wgt)
        df['mz'] = (((df['p_z'] * (self.stack_wgts['points'] + self.stack_wgts['salary'])) + (df['sa_z'] * self.stack_wgts['opp_avg_vs']) + (df['mu_z'] * self.stack_wgts['matchup']) + (df['t_z'] * self.stack_wgts['lu_talent']) + (df['v_z'] * self.stack_wgts['venue']) + (df['e_z'] * self.stack_wgts['environment'])) / self.tot_stack_wgt)

        return df

    @cached_property
    def util_df(self):
        df = self.h_df()

        df = df[(~df['team'].isin(self.h_fades)) & (~df['fd_id'].isin(self.h_fades)) & (~df['mlb_id'].isin(self.h_fades))]
        return df[df['fd_r_position'].apply(lambda x: 'util' in x)]
    @cached_property
    def first_df(self):
        df = self.h_df()
        df = df[(~df['team'].isin(self.h_fades)) & (~df['fd_id'].isin(self.h_fades)) & (~df['mlb_id'].isin(self.h_fades))]
        return df[df['fd_position'].apply(lambda x: '1b' in x or 'c' in x)]
    
    @cached_property
    def second_df(self):
        df = self.h_df()
        df = df[(~df['team'].isin(self.h_fades)) & (~df['fd_id'].isin(self.h_fades)) & (~df['mlb_id'].isin(self.h_fades))]
        return df[df['fd_position'].apply(lambda x: '2b' in x)]
    @cached_property
    def ss_df(self):
        df = self.h_df()
        df = df[(~df['team'].isin(self.h_fades)) & (~df['fd_id'].isin(self.h_fades)) & (~df['mlb_id'].isin(self.h_fades))]
        return df[df['fd_position'].apply(lambda x: 'ss' in x)]
    @cached_property
    def third_df(self):
        df = self.h_df()
        df = df[(~df['team'].isin(self.h_fades)) & (~df['fd_id'].isin(self.h_fades)) & (~df['mlb_id'].isin(self.h_fades))]
        return df[df['fd_position'].apply(lambda x: '3b' in x)]

    @cached_property
    def of_df(self):
        df = self.h_df()
        df = df[(~df["team"].isin(self.h_fades)) & (~df["fd_id"].isin(self.h_fades))]
        return df[df["fd_position"].apply(lambda x: "of" in x)]

    @staticmethod

    def filter_df(df, column, maximum):
        return df[df[column] <= maximum]
    
    # FDSlate.sort_list_by_list(FDSlate.order_of_rarity_dict(self.positional_counts()))

    def positional_counts(self, max_order=5):
        first = len(FDSlate.filter_df(self.first_df, 'order', max_order).index)
        second = len(FDSlate.filter_df(self.second_df, 'order', max_order).index)
        ss = len(FDSlate.filter_df(self.ss_df, 'order', max_order).index)
        third = len(FDSlate.filter_df(self.third_df, 'order', max_order).index)
        of = (len(FDSlate.filter_df(self.of_df, 'order', max_order).index)) / 3
        position_count_dict = {'1b': first,
                  '2b': second,
                  'ss': ss,
                  '3b': third,
                  'of': of,
                  'of.1': of,
                  'of.2': of,
                  'util': 99999}
    
        return position_count_dict
    def order_pos_importance(self):
        order = sort_k_by_v(self.positional_counts())
        # order.append('util')
        return order
        
    @staticmethod
    def df_z_score(df, column, mult = 1):
        return ((df[column] - df[column].mean()) / df[column].std(ddof = 0)) * mult
        

    def stacks_df(self):
        lineups = self.lineups
        df = self.points_df()
        i = df[(df.index.isin(self.no_stack))].index
        df.drop(index=i, inplace=True)
        df["stacks"] = 1000
        increment = 0
        for team in self.team_instances:
            if team.ppd and team.name in self.active_teams:
                filt = df[(df.index == team.name) | (df.index == team.name)].index

                df.drop(filt, inplace = True)
        attempts = 0
        while df['stacks'].max() > self.stack_threshold:
            attempts += 1
            if attempts > 2:
                raise ValueError('cannot generate stacks_df, enter custom_stacks')
            df_c = df.copy()
            if self.stack_salary_factor:
                df_c['z'] = (((df['p_z'] * self.stack_wgts['points']) + (df['s_z'] * self.stack_wgts['salary']) + (df['sa_z'] * self.stack_wgts['opp_avg_vs']) + (df['mu_z'] * self.stack_wgts['matchup']) + (df['t_z'] * self.stack_wgts['lu_talent']) + (df['v_z'] * self.stack_wgts['venue']) + (df['e_z'] * self.stack_wgts['environment'])) / self.tot_stack_wgt) + increment
            else:
                df_c['z'] = (((df['p_z'] * (self.stack_wgts['points'] + self.stack_wgts['salary'])) + (df['sa_z'] * self.stack_wgts['opp_avg_vs']) + (df['mu_z'] * self.stack_wgts['matchup']) + (df['t_z'] * self.stack_wgts['lu_talent']) + (df['v_z'] * self.stack_wgts['venue']) + (df['e_z'] * self.stack_wgts['environment'])) / self.tot_stack_wgt) + increment
            df_c = df_c[df_c['z'] > 0]
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

        i = df[df["stacks"] <= 0].index
        df.drop(index=i, inplace=True)
        return df

    def p_lu_df(self):
        lineups = self.lineups
        df = self.p_df()

        drop_stat = ((df[self.drop_p_stat] <= self.min_proj_p_stat) | (df[self.drop_p_stat] == 0))
        drop_p_fade = (df['team'].isin(self.p_fades)) | (df['fd_id'].isin(self.p_fades)) | (df['mlb_id'].isin(self.p_fades))
        i = df[drop_stat | drop_p_fade].index
        df.drop(index = i, inplace=True)
        df['lus'] = 1000
        increment = 0
        attempts = 0
        while df['lus'].max() > self.pitcher_threshold:
            attempts += 1
            if attempts > 2:
                raise ValueError('could not generate p_lu_df, enter custom_pitchers.')

            df_c = df.copy()
            df_c["p_z"] = (
                (df_c["mz"] - df_c["mz"].mean()) / df_c["mz"].std()
            ) + increment
            i = df_c[df_c["p_z"] <= 0].index
            df_c.drop(index=i, inplace=True)
            lu_base = lineups / len(df_c.index)
            df_c["lus"] = lu_base * df_c["p_z"]
            if df_c["lus"].max() > self.pitcher_threshold:
                increment += 0.01
                continue
            diff = lineups - df_c["lus"].sum()
            df_c["lus"] = round(df_c["lus"])
            while df_c["lus"].sum() < lineups:
                if self.heavy_weight_p:
                    df_c["lus"] = df_c["lus"] + np.ceil(
                        ((diff / len(df_c.index)) * df_c["p_z"])
                    )
                else:
                    df_c["lus"] = df_c["lus"] + np.ceil((diff / len(df_c.index)))
                df_c["lus"] = round(df_c["lus"])
            while df_c["lus"].sum() > lineups:
                for idx in df_c.index:
                    if df_c["lus"].sum() == lineups:
                        break
                    else:
                        df_c.loc[idx, "lus"] -= 1
            if df_c["lus"].max() > self.pitcher_threshold:
                increment += 0.01
                continue
            df = df_c
        i = df[df["lus"] <= 0].index
        df.drop(index=i, inplace=True)
        return df

    def p_counts(self):
        file = pickle_path(
            name=f"p_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(file)
        if path.exists():
            df = pandas.read_pickle(path)
            return df
        else:
            return "No pitchers stored yet."

    def h_counts(self):
        file = pickle_path(
            name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR
        )
        path = settings.FD_DIR.joinpath(file)
        if path.exists():
            df = pandas.read_pickle(path)
            return df
        else:
            return "No lineups stored yet."

    @staticmethod

    def get_p_info(df, p_id, columns=['fd_id', 'fd_salary', 'opp', 'team', 'name']):
        pitcher_info = df.loc[p_id, columns].values
        p_info = {}
        p_info['current_pitcher_fanduel_id'] = pitcher_info[0]
        p_info['current_pitcher_fanduel_salary'] = pitcher_info[1]
        p_info['current_pitcher_opponent'] = pitcher_info[2]
        p_info['current_pitcher_team'] = pitcher_info[3]
        p_info['current_pitcher_name'] = pitcher_info[4]
        return p_info
        
     
    
    def build_lineups(self, info_only = False,
                      lus = 150, 
                      index_track = 0, 
                      non_random_stack_start=0,
                      max_lu_total = 70,
                      variance = 0, 
                      below_avg_count = 25,
                      below_order_count = 60,
                      risk_limit = 125,
                      of_count_adjust = 5,
                      max_lu_stack = 150, 
                      max_sal = 35000,
                      initial_salary = 0,
                      min_avg_post_stack = 2700,
                      stack_size = 4,
                      stack_sample = 4, 
                      stack_salary_pairing_cutoff=4,
                      stack_max_pair = 4,
                      fallback_stack_sample = 5,
                      stack_expand_limit = 10000,
                      max_order=9,
                      all_in_replace_order_stack = 7,
                      all_in_replace_order_sec = 7,
                      supreme_replace_order = 4,
                      non_stack_max_order=9,
                      first_max_order = 9,
                      second_max_order = 9,
                      ss_max_order = 9,
                      third_max_order = 9,
                      of_max_order = 9,
                      util_max_order = 9,
                      util_replace_filt = 300,
                      all_in_replace_filt = 300,
                      all_in_diff_filt = 300,
                      non_stack_quantile = .9, 
                      high_salary_quantile = .9,
                      secondary_stack_cut = 0,
                      no_surplus_cut = 0,
                      single_stack_surplus = 500,
                      double_stack_surplus = 1000,
                      pitcher_surplus = 1000,
                      median_pitcher_salary = None,
                      low_pitcher_salary = None,
                      high_pitcher_salary = None,
                      max_pitcher_salary = None,
                      median_team_salary = None,
                      low_stack_salary = None,
                      high_stack_salary = None,
                      max_stack_salary = None,
                      no_surplus_secondary_stacks=True,
                      find_cheap_secondary = True,
                      always_pitcher_first=True,
                      enforce_pitcher_surplus = True,
                      enforce_hitter_surplus = False,
                      filter_last_replaced=False,
                      always_replace_pitcher_lock=False,
                      p_sal_stack_filter = False,
                      replace_secondary_all_in = True,
                      replace_primary_all_in = True,
                      never_replace_primary_supreme = False,
                      expand_utility = False,
                      decrease_stack_salary = True,
                      factor_salary_secondary = False,
                      increase_stack_salary = True,
                      increase_secondary_salary = True,
                      all_in_auto = True,
                      all_in_auto_max_order = 5,
                      exempt=[],
                      all_in=[],
                      supreme_all_in=[],
                      no_supreme_replace=[],
                      supreme_all_in_pos=['1b', 'util', '2b', 'of', 'of.1', 'of.2', '3b', 'ss'],
                      lock = [],
                      no_combos = [],
                      never_replace=[],
                      never_replace_secondary=[],
                      never_replace_primary=[],
                      x_fallback = [],
                      stack_only = [],
                      no_stack_salary_decrease = [],
                      limit_risk = [],
                      no_combine = [],
                      always_replace_first = [],
                      no_replace = [],
                      custom_counts={},
                      always_find_cheap = [],
                      no_secondary_replace = [],
                      no_stack_replace = [],
                      always_replace = [],
                      never_fill = [],
                      no_utility = [],
                      complete_fade = [],
                      custom_stack_size = {},
                      remove_positions = None,
                      custom_stacks = None,
                      custom_pitchers = None,
                      custom_secondary = None,
                      custom_stacks_info = None,
                      fill_high_salary_first = True,
                      select_max_pitcher = True,
                      last_lu = [],
                      filter_last_lu = False,
                      select_max_stack = True,
                      select_non_random_stack = False,
                      shuffle_positions_stack = False,
                      shuffle_positions_non_stack = False,
                      custom_stack_order = {},
                      always_pair_first = {},
                      always_pair_first_sec = {},
                      never_pair = {},
                      never_pair_sec = {},
                      locked_lineup = None,
                      expand_pitchers = False,
                      find_cheap_primary = False,
                      while_loop_secondary = True,
                      stack_top_order = False,
                      info_stack_key = 'points',
                      stack_info_order = 7,
                      stack_info_size = 4,
                      stack_info_salary_factor = False,
                      dupe_lu_replacement_pos = ['1b', 'util', '2b', 'of', 'of.1', 'of.2', '3b', 'ss']
                      ):

        complete_fade.extend(settings.ppd)
        original_cheap_primary = find_cheap_primary
        original_cheap_stacks = find_cheap_secondary
        original_all_in = all_in.copy()
        original_stack_size = stack_size
        # all hitters in slate

        
        all_hitters_df = self.h_df()
        #dropping faded, platoon, and low order (max_order) players.
        h_fade_filt = ((all_hitters_df['team'].isin(complete_fade)) | (all_hitters_df['fd_id'].isin(complete_fade)) | (all_hitters_df['mlb_id'].isin(complete_fade)))
        h_order_filt = (all_hitters_df['order'] > max_order)
        h_exempt_filt = (~all_hitters_df['fd_id'].isin(exempt))
        h_drop_stat_filt = ((all_hitters_df[self.h_drop['drop_h_stat']] <= self.h_drop['min_proj_h_stat']) | (all_hitters_df[self.h_drop['drop_h_stat']] == 0))
        hfi = all_hitters_df[(h_fade_filt | h_order_filt | h_drop_stat_filt) & h_exempt_filt].index
        all_hitters_df.drop(index=hfi,inplace=True)
        no_utility_filt = (~all_hitters_df['fd_id'].isin(no_utility))
        #count each players entries
        all_hitters_df['t_count'] = 0
        #count non_stack
        all_hitters_df['ns_count'] = 0
        h_count_df = all_hitters_df.copy()
        #risk_limit should always be >= below_avg_count
        all_hitters_df.loc[(all_hitters_df['sp_split'] < all_hitters_df['sp_split'].median()),'t_count'] = below_avg_count
        all_hitters_df.loc[(all_hitters_df['team'].isin(stack_only)) | (all_hitters_df['fd_id'].isin(stack_only)), 't_count'] = 1000
        exempt_filt = (all_hitters_df['fd_id'].isin(exempt))
        all_hitters_df.loc[exempt_filt, 't_count'] = 0
        #in the case a player's batting order is above the max_order, but is exempted from drop.
        all_hitters_df.loc[h_order_filt, 't_count'] = below_order_count
        #allowing outfields, as there are 3 of in each LU, to be in more lineups than non-of
        all_hitters_df.loc[(all_hitters_df['fd_position'].apply(lambda x: 'of' in x)), 't_count'] -= of_count_adjust
        #if a game is a threat to be postponed, limit exposure to that team
        all_hitters_df.loc[(all_hitters_df['team'].isin(limit_risk)), 't_count'] = risk_limit
        #ids or teams in all_in have no limit to allowed number of lineups.
        all_in.extend(supreme_all_in)
        all_hitters_df.loc[((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in))), 't_count'] = -1000
        #limit a player by fanduel id in custom_counts dict {'id': original count}
        if custom_counts:
            for k,v in custom_counts.items():
                all_hitters_df.loc[all_hitters_df['fd_id'] == k, 't_count'] = v
        #all pitchers...
        all_pitchers_df = self.p_df()
        #drop pitchers in class p_fades (__init__)
        p_fade_filt = (all_pitchers_df['team'].isin(self.p_fades))
        pfi = all_pitchers_df[p_fade_filt].index
        all_pitchers_df.drop(index=pfi,inplace=True)
        #count a pitcher's entries
        p_count_df = all_pitchers_df.copy()
        p_count_df['t_count'] = 0
        #dict p_df.index: p_df: lineups to be in
        if not custom_pitchers:
            pitcher_count_dict = self.p_lu_df()['lus'].to_dict()

        else:
            pitcher_count_dict = custom_pitchers
        # team: stacks to build
        if not custom_stacks:
            s = self.stacks_df()["stacks"].to_dict()
        else:
            s = custom_stacks
        if not custom_secondary:
            secondary = self.stacks_df()["stacks"].to_dict()
        else:
            secondary = custom_secondary
        stack_list = list(s)

        # keep track of each team's pairing with another team, pitcher, both.
        stack_track = {}
        for team in self.active_teams:
            try:
                stack_track[team] = {}
                stack_track[team]["pitchers"] = {}
                stack_track[team]["secondary"] = {}
                stack_track[team]["salary"] = (
                    self.points_df().loc[team, "salary"].item()
                )
                stack_track[team]["combos"] = []
            except KeyError:
                print(f"{team} not included in list, ppd should be True.")
                continue
        # append each completed lineup, sorted, as to assure unique each time.
        sorted_lus = []

        #keys are positions, values are indexes that correspond to those positions.
        position_index_map = {
            'p': 0,
            '1b': 1,
            '2b': 2,
            '3b': 3,
            'ss': 4,
            'of': 5,
            'of.1': 6,
            'of.2': 7,
            'util': 8
            }

        """
        if filter_last_replaced == True:
            each player that is inserted in a lineup, that is not part
            of a stack, will be filtered the next time that position
            is needed.
        """
        last_replaced = position_index_map.copy()
        """
        if playing a player only at a specific position, 
        use remove_position dict.
        in {'id1': '1b'}, player with 'id1' will no-longer be eligible at 1b.
        Will throw error if all player's positions are removed. Or if player
        isn't eligible at specified position.'
        """
        if remove_positions:

            for k,v in remove_positions.items():
                position_id_filt = (all_hitters_df['fd_id'] == k)
                all_hitters_df.loc[position_id_filt, 'fd_position'].apply(lambda x: x.remove(v))
                

        """
        if not specified as KW args., filter out team's teams not
        being used, and gather salary information for them. This can
        be used to pair team's with high salary with lower one's
        """
        points_df_filt = (self.points_df().index.isin(s.keys())) | (
            self.points_df().index.isin(secondary.keys())
        )
        filtered_points_df = self.points_df()[points_df_filt]

        pitcher_points_filt = (self.p_df().index.isin(pitcher_count_dict))

        filtered_pitcher_df = self.p_df()[pitcher_points_filt]
        if not median_team_salary:
            median_team_salary = filtered_points_df["salary"].median()
        if not low_stack_salary:
            low_stack_salary = filtered_points_df["salary"].quantile(0.25)
        if not high_stack_salary:
            high_stack_salary = filtered_points_df["salary"].quantile(0.75)
        if not max_stack_salary:
            max_stack_salary = filtered_points_df["salary"].max()
        if not median_pitcher_salary:
            median_pitcher_salary = filtered_pitcher_df["fd_salary"].median()
        if not high_pitcher_salary:
            high_pitcher_salary = filtered_pitcher_df["fd_salary"].quantile(0.75)
        if not low_pitcher_salary:
            low_pitcher_salary = filtered_pitcher_df["fd_salary"].quantile(0.25)
        if not max_pitcher_salary:
            max_pitcher_salary = filtered_pitcher_df["fd_salary"].max()
        """
        if info_only == True, return a dict with some information regarding
        salaries and possible combinations of provided teams. Also,
        print stack rankings based on talent/salary, selecting 
        stack size(stack_info_size), maximum stack order (stack_info_order),
        and stack filter(info_stack_key).
        Half the weight will be applied to salary if stack_info_salary_factor == True
        """

        if info_only:
            information = {}

            information['low_stack_salary'] = low_stack_salary
            information['median_team_salary'] = median_team_salary
            information['high_stack_salary'] = high_stack_salary
            information['max_stack_salary'] = max_stack_salary
            information['low_pitcher_salary'] = low_pitcher_salary
            information['median_pitcher_salary'] = median_pitcher_salary
            information['high_pitcher_salary'] = high_pitcher_salary
            information['max_pitcher_salary'] = max_pitcher_salary
            information['high_salary_quantile'] = all_hitters_df['fd_salary'].quantile(high_salary_quantile)
            information['avg_stack_per_team'] = lus / len(list(s))
            stack_info_columns = ['salary', 'points', 'points_std', 'sp_split', 'sp_split_std', 'hr_weight', 'hr_weight_std',
                                  'stack_exp_pa']
            stack_info_df = pandas.DataFrame(columns = stack_info_columns)
            for team in self.active_teams:
                if team not in self.h_fades:
                    team_order_filt = (all_hitters_df['order'] <= stack_info_order)
                    x_team_order_filt = (all_hitters_df['order'] <= stack_info_order + 10)
                    is_team_filt = (all_hitters_df['team'] == team)
                    # plat_filt = (all_hitters_df['is_platoon'] != True)
                    min_abs_filt = (all_hitters_df['pa'] >= settings.MIN_PA_HITTER)
                    team_copy = all_hitters_df[is_team_filt & team_order_filt & min_abs_filt]
                    if len(team_copy.index) != stack_info_order:
                        team_copy = all_hitters_df[is_team_filt & x_team_order_filt & min_abs_filt]
                        
                    team_copy = team_copy[['name','points', 'fd_salary', 'order', 'sp_split', 'exp_ps_sp_pa',
                                           'fd_hr_weight', 'is_platoon']]

                    indices = team_copy[info_stack_key].nlargest(stack_info_size).index
                    team_high_points = team_copy.loc[indices]
                    stack_info_salary = team_high_points["fd_salary"].sum()
                    stack_info_points = team_high_points["points"].sum()
                    stack_info_split = team_high_points["sp_split"].sum()
                    # stack_info_salary_std = team_high_points['fd_salary'].std(ddof = 0)
                    stack_info_points_std = team_high_points["points"].std(ddof=0)
                    stack_info_split_std = team_high_points["sp_split"].std(ddof=0)
                    hr_weight = team_high_points["fd_hr_weight"].sum()
                    hr_weight_std = team_high_points["fd_hr_weight"].std(ddof=0)
                    stack_exp_pa = team_high_points["exp_ps_sp_pa"].sum()
                    stack_info_df.loc[team, stack_info_columns] = [
                        stack_info_salary,
                        stack_info_points,
                        stack_info_points_std,
                        stack_info_split,
                        stack_info_split_std,
                        hr_weight,
                        hr_weight_std,
                        stack_exp_pa,
                    ]
                    print(team)
                    print(f"{team}: {stack_info_points}")
                    print(team_high_points)
            stack_info_df["s_z"] = FDSlate.df_z_score(stack_info_df, "salary", mult=-1)
            stack_info_df["p_z"] = FDSlate.df_z_score(stack_info_df, "points")
            stack_info_df["p_z_s"] = FDSlate.df_z_score(
                stack_info_df, "points_std", mult=-1
            )
            stack_info_df["sp_z"] = FDSlate.df_z_score(stack_info_df, "sp_split")
            stack_info_df["sp_z_s"] = FDSlate.df_z_score(
                stack_info_df, "sp_split_std", mult=-1
            )
            stack_info_df["hr_z"] = FDSlate.df_z_score(stack_info_df, "hr_weight")
            stack_info_df["hr_z_s"] = FDSlate.df_z_score(
                stack_info_df, "hr_weight_std", mult=-1
            )
            stack_info_df["exp_p_z"] = FDSlate.df_z_score(stack_info_df, "stack_exp_pa")
            if stack_info_salary_factor:
                stack_info_df["score"] = (
                    (stack_info_df["s_z"] * 5)
                    + (stack_info_df["p_z"] * 0)
                    + (stack_info_df["p_z_s"] * 0)
                    + (stack_info_df["sp_z"] * 5)
                    + (stack_info_df["sp_z_s"] * 0)
                    + (stack_info_df["hr_z"] * 0)
                    + (stack_info_df["hr_z_s"] * 0)
                    + (stack_info_df["exp_p_z"] * 0)
                ) / 10

            else:
                stack_info_df["score"] = (
                    (stack_info_df["s_z"] * 0)
                    + (stack_info_df["p_z"] * 0)
                    + (stack_info_df["p_z_s"] * 0)
                    + (stack_info_df["sp_z"] * 10)
                    + (stack_info_df["sp_z_s"] * 0)
                    + (stack_info_df["hr_z"] * 0)
                    + (stack_info_df["hr_z_s"] * 0)
                    + (stack_info_df["exp_p_z"] * 0)
                ) / 10

            if not custom_stacks_info:
                information["stack_combos"] = len(list(combinations(list(s), 2)))

            else:

                information['stack_combos'] = len(list(combinations(list(custom_stacks_info), 2)))
            information['pitcher_combos'] = len(list(combinations(list(s), 2))) * len(list(pitcher_count_dict))
            print(all_pitchers_df.loc[all_pitchers_df.index.isin(pitcher_count_dict.keys()), 'name'])
            stack_info_df = stack_info_df.apply(pandas.to_numeric, errors = 'ignore')
            print(stack_info_df.loc[stack_info_df['score'].nlargest(30).index, ['score', 'points', 'salary']])

            print(information)
            return information

        def determine_filter(position, df, filt="position"):
            if filt == "position":
                if "of" in position:
                    return df["fd_position"].apply(lambda x: "of" in x)
                elif "util" in position:
                    return df["fd_r_position"].apply(lambda x: position in x)
                else:
                    return df["fd_position"].apply(lambda x: position in x)
            elif filt == "order":
                if "of" in position:
                    return (df["order"] <= of_max_order) | exempt_filt
                elif "1b" in position:
                    return (df["order"] <= first_max_order) | exempt_filt
                elif "2b" in position:
                    return (df["order"] <= second_max_order) | exempt_filt
                elif "ss" in position:
                    return (df["order"] <= ss_max_order) | exempt_filt
                elif "3b" in position:
                    return (df["order"] <= third_max_order) | exempt_filt
                elif "util" in position:
                    return (df["order"] <= util_max_order) | exempt_filt

            elif filt == "last_replaced":
                if filter_last_replaced:
                    if "of" in position:
                        return df["fd_id"] != last_replaced["of"]
                    else:
                        return df["fd_id"] != last_replaced[position]
                else:
                    return df["fd_id"] != 1
            else:
                raise ValueError("Problem with function: determine_filter")

        #lineups to build
        print('prior to building')

        while lus > 0:
            print('building')
            #if lineup fails requirements, reset will be set to true.
            reset = False
            non_random_stack = stack_list[non_random_stack_start]
            # filter for not inserting
            if filter_last_lu:

                last_lu_filt = (~all_hitters_df['fd_id'].isin(last_lu))
            else:
                last_lu_filt = (all_hitters_df['fd_id'] != 1)
            #pitchers expected lineups will be reduced by 1 for each successful lineup insertion
            if select_max_pitcher:
                pitchers = {k:v for k,v in pitcher_count_dict.items() if v == max(pitcher_count_dict.values())}
            else:
                pitchers = {k:v for k,v in pitcher_count_dict.items() if v > 0}
            #randomly select a pitcher in pool, keys are == p.index
            
            selected_pitcher = random.choice(list(pitchers.keys()))
            #lookup required pitcher info. by index (pi)
            p_info = FDSlate.get_p_info(all_pitchers_df, selected_pitcher)
            
            
            if never_pair.get(p_info['current_pitcher_fanduel_id']):
                avoid_stack = never_pair[p_info['current_pitcher_fanduel_id']]
            else:
                avoid_stack = []
            #start with the pitchers salary
            salary = initial_salary + p_info['current_pitcher_fanduel_salary']
            if p_info['current_pitcher_fanduel_id'] in always_find_cheap:
                # find_cheap_secondary = True
                find_cheap_primary = True
            else:
                find_cheap_primary = original_cheap_primary
                find_cheap_secondary = original_cheap_stacks
            #choose a stack that hasn't exceed its insertion limit and is not playing the current pitcher
            stacks = {}
            if select_max_stack:
                select_non_random_stack = False
                if always_pair_first.get(p_info['current_pitcher_fanduel_id']):
                    stacks = {k:v for k,v in s.items() if v > 0 
                              and p_info['current_pitcher_fanduel_id'] not in stack_track[k]['pitchers'] 
                              and k in always_pair_first[p_info['current_pitcher_fanduel_id']]}
                    if len(stacks.keys()) == 0:
                        {k:v for k,v in s.items() if v > 0 and k in always_pair_first[p_info['current_pitcher_fanduel_id']]}
                if len(stacks.keys()) == 0:
                    filtered_stacks = {k:v for k,v in s.items() if k != p_info['current_pitcher_opponent']
                                       and k not in avoid_stack}
                    stacks = {k:v for k,v in filtered_stacks.items() if v == max(filtered_stacks.values()) 
                              and k != p_info['current_pitcher_opponent'] 
                              and p_info['current_pitcher_fanduel_id'] not in stack_track[k]['pitchers']}
                    if len(stacks.keys()) == 0:
                        stacks = {k:v for k,v in filtered_stacks.items() if v > 0 and k != p_info['current_pitcher_opponent'] and p_info['current_pitcher_fanduel_id'] not in stack_track[k]['pitchers']} 
                    if len(stacks.keys()) == 0:
                        stacks = {k:v for k,v in filtered_stacks.items() if v == max(filtered_stacks.values()) and k != p_info['current_pitcher_opponent']}
                    
            elif select_non_random_stack:    
                stacks = {k:v for k,v in s.items() if v > 0 and k != p_info['current_pitcher_opponent'] and k == non_random_stack}

                if len(stacks.keys()) > 0:
                    non_random_stack_start += 1
                    if non_random_stack_start > len(stack_list) - 1:
                        non_random_stack_start = 0
                else:
                    # original_non_random_stack_start = non_random_stack_start
                    non_random_stack_attempts = 0
                    while len(stacks.keys()) < 1 and non_random_stack_attempts < len(
                        stack_list
                    ):
                        non_random_stack_start += 1
                        if non_random_stack_start > len(stack_list) - 1:
                            non_random_stack_start = 0
                        non_random_stack = stack_list[non_random_stack_start]

                        stacks = {k:v for k,v in s.items() if v > 0 and k != p_info['current_pitcher_opponent'] and k == non_random_stack}
                        non_random_stack_attempts += 1
                    if non_random_stack_attempts >= len(stack_list):
                        stacks = {k:v for k,v in s.items() if v > 0 and k != p_info['current_pitcher_opponent']}
                    

                    non_random_stack_start += 1
                    if non_random_stack_start > len(stack_list) - 1:
                        non_random_stack_start = 0

            # randomly select an eligible stack. keys == team names.
            try:
                stack = random.choice(list(stacks.keys()))

            except IndexError:
                print("Error....")
                print(lus)
                continue
            if stack in always_find_cheap:
                find_cheap_primary = True
            elif p_info['current_pitcher_fanduel_id'] not in always_find_cheap:
                find_cheap_primary = original_cheap_primary
                find_cheap_secondary = original_cheap_stacks
            remaining_stacks = stacks[stack]

            #lookup players on the team for the selected stack
            stack_df = all_hitters_df[all_hitters_df['team'] == stack]
            

            if remaining_stacks % 2 == 0:
                stack_key = settings.STACK_KEY_1
            else:
                stack_key = settings.STACK_KEY_2
            if lus % 2 == 0:
                non_stack_key = settings.FD_NON_STACK_KEY_1
                pitcher_replace_key = settings.PITCHER_REPLACE_KEY_1
            else:
                non_stack_key = settings.FD_NON_STACK_KEY_2
                pitcher_replace_key = settings.PITCHER_REPLACE_KEY_2
            # filter the selected stack by stack_sample arg.
            if custom_stack_order.get(stack):
                # modify_stack = False
                if type(custom_stack_order[stack][0]) == list:
                    
                    custom_stack_sample = random.choice(custom_stack_order[stack])
                    print(custom_stack_sample)
                else:
                    
                    custom_stack_sample = custom_stack_order[stack].copy()

                    print(custom_stack_sample)
                highest = stack_df[stack_df['order'].isin(custom_stack_sample)]

            else:
                if find_cheap_primary:
                    highest = stack_df.loc[
                        stack_df["fd_salary"].nsmallest(stack_sample).index
                    ]
                elif stack_top_order:
                    highest = stack_df.loc[
                        stack_df["order"].nsmallest(stack_sample).index
                    ]

                else:
                    if remaining_stacks > stack_expand_limit:
                        highest = stack_df.loc[
                            stack_df[stack_key].nlargest(stack_sample + 1).index
                        ]
                    else:

                        highest = stack_df.loc[stack_df[stack_key].nlargest(stack_sample).index]
                        
            #array of fanduel ids of the selected hitters
            stack_ids = highest['fd_id'].values
            
            #initial empty lineup, ordered by fanduel structed and mapped by position_index_map
            lineup = [None, None, None, None, None, None, None, None, None]
            #insert current pitcher into lineup
            lineup[0] = p_info['current_pitcher_fanduel_id']
            #try to create a 4-man stack that satifies position requirements 5 times, else 3-man stack.
            attempts_to_insert_stack = 0
            positions_to_be_filled_by_stack = []
            if custom_stack_size.get(stack):
                stack_size = custom_stack_size.get(stack)
            else:
                stack_size = original_stack_size
            while len(positions_to_be_filled_by_stack) != stack_size:
                attempts_to_insert_stack +=1
                if attempts_to_insert_stack > 10:
                    break
                #empty the list of positions each mapped player will fill
                positions_to_be_filled_by_stack = []
                #take an initial random sample of 4
                try:
                    #must sort array to avoid TypeError
                    ids_of_players_to_stack = random.sample(sorted(stack_ids), stack_size)

                except ValueError:
                    attempts_to_insert_stack = 999999
                    continue

                #list of lists of the sample's eligible positions
                eligible_position_list_of_lists = [player for player in all_hitters_df.loc[all_hitters_df['fd_id'].isin(ids_of_players_to_stack), 'fd_position'].values]
                for eligible_position_list in eligible_position_list_of_lists:
                    #iterate each player's list of positions, append if not already filled
                    if eligible_position_list[0] not in positions_to_be_filled_by_stack and not lineup[position_index_map.get(eligible_position_list[0])]:
                        positions_to_be_filled_by_stack.append(eligible_position_list[0])
                    elif eligible_position_list[-1] not in positions_to_be_filled_by_stack and not lineup[position_index_map.get(eligible_position_list[-1])]:
                        positions_to_be_filled_by_stack.append(eligible_position_list[-1])
                    #index 1 because if len == 2, would've already checked
                    elif len(eligible_position_list) > 2 and eligible_position_list[1] not in positions_to_be_filled_by_stack and not lineup[position_index_map.get(eligible_position_list[1])]:
                        positions_to_be_filled_by_stack.append(eligible_position_list[1])
                    #if player is eligible outfielder and first of slot taken
                    elif 'of' in eligible_position_list and positions_to_be_filled_by_stack.count('of') == 1 and 'of.1' not in positions_to_be_filled_by_stack and not lineup[position_index_map['of.1']]:
                        positions_to_be_filled_by_stack.append('of.1')
                    #if already 2 outfielders and player is eligible outfielder
                    elif 'of' in eligible_position_list and positions_to_be_filled_by_stack.count('of.1') == 1 and 'of.2' not in positions_to_be_filled_by_stack and not lineup[position_index_map['of.2']]:
                        positions_to_be_filled_by_stack.append('of.2')
                    elif 'util' not in positions_to_be_filled_by_stack and not lineup[position_index_map['util']]:
                         positions_to_be_filled_by_stack.append('util')
                
                        
            #if couldn't create a 4-man stack, create a 3-man stack        
            # if a > 10:
            #     a = 0
            #     print(f"Using fallback_stack_sample for {stack}.")
            #     while len(pl2) != stack_size:
            #         a +=1
            #         if a > 10:
            #             raise Exception(f"Could not create 4-man stack for {stack}.")
            #         pl2 = []
            #         if find_cheap_primary:
            #             highest = stack_df.loc[stack_df['fd_salary'].nsmallest(fallback_stack_sample).index]
            #         elif stack_top_order:
            #             highest = stack_df.loc[stack_df['order'].nsmallest(fallback_stack_sample).index] 
            #         else:
            #             if stack in x_fallback:
            #                 highest = stack_df.loc[stack_df[stack_key].nlargest(fallback_stack_sample+1).index]
            #             else:
            #                 highest = stack_df.loc[stack_df[stack_key].nlargest(fallback_stack_sample).index]
            #         #array of fanduel ids of the selected hitters
            #         stack_ids = highest['fd_id'].values
            #         samp = random.sample(sorted(stack_ids), stack_size)
            #         pl1 = [x for x in all_hitters_df.loc[all_hitters_df['fd_id'].isin(samp), 'fd_position'].values]                
            #         for x in pl1:
            #             if x[0] not in pl2:
            #                 pl2.append(x[0])
            #             elif x[-1] not in pl2:
            #                 pl2.append(x[-1])
            #             elif len(x) > 2 and x[1] not in pl2:
            #                 pl2.append(x[1])
            #             elif 'of' in x and pl2.count('of.1') == 1 and 'of.2' not in pl2:
            #                 pl2.append('of.2')
            #             elif 'of' in x and pl2.count('of') == 1 and 'of.1' not in pl2:
            #                 pl2.append('of.1')
            #             elif 'util' not in pl2:
            #                 pl2.append('util')
                            
            def get_stack_map(player_df, iter_filt, iter_zip, select_by = 'fd_id', info_cols = ['fd_salary', 'fd_id']):
                stack_info = player_df.loc[player_df[select_by].isin(iter_filt), info_cols].values
                stack_map = dict(zip(iter_zip, stack_info))
                return stack_map
            stack_map = get_stack_map(all_hitters_df, ids_of_players_to_stack, positions_to_be_filled_by_stack)
            # calculate used salary and remaining salary after selecting pitcher/stack
            for player_information in stack_map.values():
                salary += player_information[0]
            remaining_salary = max_sal - salary
            #use position of each player to find corresponding value/index in position_index_map dict to insert fanduel ID.
            for fanduel_id, player_information in stack_map.items():
                idx = position_index_map[fanduel_id]
                lineup[idx] = player_information[1]
            #create list of lineup indexes that need to be filled.
            np_list = [idx for idx, spot in enumerate(lineup) if not spot]
            s_list = [idx for idx, spot in enumerate(lineup) if spot and idx != 0]
            stacked_pos = [x for x, y in position_index_map.items() if y in s_list]
            if not shuffle_positions_stack:
                stacked_pos = sort_list_by_list(stacked_pos, list(reversed(self.order_pos_importance())))
            
            needed_pos = [x for x, y in position_index_map.items() if y in np_list]
            if not shuffle_positions_non_stack:
                needed_pos = sort_list_by_list(needed_pos, list(reversed(self.order_pos_importance())))
                
            #get the ammount of roster spots that need filling.
            npl = len([idx for idx, spot in enumerate(lineup) if not spot])
            #the avgerage salary remaining for each empty lineup spot
            avg_sal = remaining_salary / npl
            if avg_sal < min_avg_post_stack and stack not in no_stack_salary_decrease and decrease_stack_salary:

                if shuffle_positions_stack:
                    random.shuffle(stacked_pos)
                for position in stacked_pos:
                    print(avg_sal)
                    if avg_sal >= min_avg_post_stack:
                        break

                    index_of_position = position_index_map[position]
                    if lineup[index_of_position] not in never_replace:
                        player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                        s_sal = player_being_replaced['fd_salary'].item()
                        sal_filt = (all_hitters_df['fd_salary'] < s_sal)
                        dupe_filt = (~all_hitters_df['fd_id'].isin(lineup))
                        stack_only_filt = (all_hitters_df['team'] == stack)
                        pos_filt =  determine_filter(position, all_hitters_df)
                        hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & stack_only_filt]
                        if len(hitters.index) > 0:
                            hitter = hitters.loc[hitters['fd_salary'].idxmin()]
                            selected_name = player_being_replaced['name'].max()
                            hitter_name = hitter['name']
                            print(f"Replacing {selected_name} with {hitter_name}.(salary)")
                            n_sal = hitter['fd_salary'].item()
                            salary -= s_sal
                            salary += n_sal
                            remaining_salary = max_sal - salary
                            lineup[index_of_position] = hitter['fd_id']
                            avg_sal = remaining_salary / npl
                            
                
            #shuffle the list of positions to encourage variance of best players at each position
            if shuffle_positions_non_stack:
                random.shuffle(needed_pos)
            #filter out hitters going against selected pitcher
            opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
            #filter out hitters on the team of the current stack, as they are already in the lineup.
            stack_filt = (all_hitters_df['team'] != stack)
            #filter out players hitting below specified lineup spot
            order_filt = ((all_hitters_df['order'] <= non_stack_max_order) | exempt_filt)
            #filter out players not on a team being stacked on slate and proj. points not in 90th percentile.
            fade_filt = ((all_hitters_df['team'].isin(stacks.keys())) | ((all_hitters_df[non_stack_key] >= all_hitters_df[non_stack_key].quantile(non_stack_quantile)) & order_filt))
            #filter players out with a current count above the highest value of remaining stacks
            max_stack = max(stacks.values())
            #variance default is 0
            count_filt = (all_hitters_df['t_count'] < ((max_lu_total - max_stack) - variance))
            plat_filt = (all_hitters_df['is_platoon'] != True)
            value_filt = ((all_hitters_df['fd_salary'] <= all_hitters_df['fd_salary'].median()) & (all_hitters_df['points'] >= all_hitters_df['points'].median()))

            new_combo = False

            if lus > secondary_stack_cut:
                if no_surplus_secondary_stacks and lus > no_surplus_cut:
                    enforce_hitter_surplus = False
                else:
                    enforce_hitter_surplus = True
                stack_salary = stack_track[stack]["salary"]
                avoid_secondary = []

                if never_pair_sec.get(p_info['current_pitcher_fanduel_id']):
                    avoid_secondary = avoid_secondary +  never_pair_sec[p_info['current_pitcher_fanduel_id']]

                if never_pair_sec.get(stack):
                    avoid_secondary = avoid_secondary + never_pair_sec[stack]
                secondary_stacks = {}

                if always_pair_first_sec.get(p_info['current_pitcher_fanduel_id']):
                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                        and k != p_info['current_pitcher_opponent'] 
                                        and k != stack 
                                        and [p_info['current_pitcher_fanduel_id'], k] not in stack_track[stack]['combos']
                                        and [p_info['current_pitcher_fanduel_id'], stack] not in stack_track[k]['combos'] 
                                        and k in always_pair_first_sec[p_info['current_pitcher_fanduel_id']] 
                                        }
                    
                if stack not in no_combine and p_info['current_pitcher_fanduel_id'] not in no_combos and len(list(secondary_stacks)) == 0:
                
                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                        and k != p_info['current_pitcher_opponent'] 
                                        and k != stack 
                                        and [p_info['current_pitcher_fanduel_id'], k] not in stack_track[stack]['combos']
                                        and [p_info['current_pitcher_fanduel_id'], stack] not in stack_track[k]['combos'] 
                                        and k not in avoid_secondary}

                if len(list(secondary_stacks)) > 0:
                    new_combo = True

                else:
                    new_combo = False
                    if factor_salary_secondary:
                        if stack_salary >= median_team_salary:

                            if p_sal_stack_filter and p_info['current_pitcher_fanduel_salary'] >= high_pitcher_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] <= low_stack_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] <= low_stack_salary }
                            elif p_sal_stack_filter and p_info['current_pitcher_fanduel_salary'] <= low_pitcher_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] >= median_team_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] >= median_team_salary }
                                
                                
                            elif stack_salary >= high_stack_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] <= low_stack_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] <= low_stack_salary }
                            elif stack_salary < high_stack_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] <= median_team_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] <= median_team_salary }
                        else:
                            ####
                            if p_sal_stack_filter and p_info['current_pitcher_fanduel_salary'] >= high_pitcher_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] <= median_team_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] <= median_team_salary }
                            elif p_sal_stack_filter and p_info['current_pitcher_fanduel_salary'] <= low_pitcher_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] > median_team_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] > median_team_salary }
                                
                            elif stack_salary <= low_stack_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] >= high_stack_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] >= high_stack_salary }
                            
                            elif stack_salary > low_stack_salary:
                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                and k != p_info['current_pitcher_opponent'] 
                                                and k != stack 
                                                and k not in stack_track[stack]['secondary']
                                                and stack_track[k]['salary'] >= median_team_salary }
                                if len(list(secondary_stacks)) < 1:
                                    secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                                    and k != p_info['current_pitcher_opponent'] 
                                                    and k != stack 
                                                    and stack_track[stack]['secondary'].get(k, 0) < stack_salary_pairing_cutoff
                                                    and stack_track[k]['salary'] >= median_team_salary }
                                
                    
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                        and k != p_info['current_pitcher_opponent'] 
                                        and k != stack 
                                        and [p_info['current_pitcher_fanduel_id'], k] not in stack_track[stack]['combos']
                                        and k not in avoid_secondary}
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info['current_pitcher_opponent'] 
                                    and k != stack 
                                    and k not in stack_track[stack]['secondary']
                                    and k not in avoid_secondary}
                    if len(list(secondary_stacks)) < 1:
                        secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info['current_pitcher_opponent'] 
                                    and k != stack 
                                    and stack_track[stack]['secondary'].get(k, 0) < stack_max_pair
                                    and k not in avoid_secondary}
                        
                    
                        

                    if len(list(secondary_stacks)) < 1:
                        if while_loop_secondary:
                            secondary_count = 1
                            stop_while = 0
                            while len(list(secondary_stacks)) < 1 and stop_while < 10:

                                secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info['current_pitcher_opponent'] 
                                    and k != stack 
                                    and stack_track[stack]['secondary'].get(k, 0) < secondary_count
                                    and k not in avoid_secondary}

                                secondary_count += 1
                                stop_while += 1

                        else:

                             secondary_stacks = {k:v for k,v in secondary.items() if v > 0 
                                    and k != p_info['current_pitcher_opponent'] 
                                    and k != stack 
                                    and k not in avoid_secondary
                                    }
                    

                if len(list(secondary_stacks)) != 0:

                    secondary_stack = random.choice(list(secondary_stacks.keys()))
                    max_surplus = double_stack_surplus

                else:
                    enforce_hitter_surplus = True
                    secondary_stack = None
                    max_surplus = single_stack_surplus

            else:
                enforce_hitter_surplus = True
                secondary_stack = None
                max_surplus = single_stack_surplus

            secondary_stack_filt = (all_hitters_df['team'] == secondary_stack)

            if all_in_auto:
                all_to_append = []
                position_dfs = [
                    self.first_df,
                    self.second_df,
                    self.ss_df,
                    self.third_df,
                    self.of_df,
                    self.util_df,
                ]
                for df in position_dfs:

                    order = all_in_auto_max_order
                    order_df = df[df['order'] <= order]
                    order_df = order_df[order_df['is_platoon'] !=True]
                    order_df = order_df[(~order_df['team'].isin(self.h_fades)) & (~order_df['fd_id'].isin(self.h_fades))]
                    order_df = order_df[(~order_df['fd_id'].isin(all_to_append))]
                    salary_mean = order_df['fd_salary'].mean()
                    points_mean_cheap = order_df[non_stack_key].mean()
                    q_salary = (df['fd_salary'] <= salary_mean)
                    q_points = (df[non_stack_key] > points_mean_cheap)
                    q_order = (df['order'] < all_in_auto_max_order + 1)
                    q_platoon = (df['is_platoon'] != True)
                    result_cheap_order = df[q_salary & q_points & q_order & q_platoon]    
                    if len(result_cheap_order.index) > 0:
                        maximum = result_cheap_order[non_stack_key].idxmax()
                        to_append = result_cheap_order.loc[maximum, 'fd_id']
                        all_to_append.append(to_append)
                    if len(order_df.index) > 0:
                        order_df = order_df[(~order_df['fd_id'].isin(all_to_append))]
                        order_df = order_df[(~order_df['team'].isin(self.h_fades)) & (~order_df['fd_id'].isin(self.h_fades))]
                        maximum = order_df[non_stack_key].idxmax()
                        to_append = order_df.loc[maximum, 'fd_id']

                        all_to_append.append(to_append)
                all_to_append = list(set(all_to_append))
                all_in = original_all_in.copy()
                all_in.extend(all_to_append)
            if shuffle_positions_non_stack:
                random.shuffle(needed_pos)

            for position in needed_pos:
                order_filt = determine_filter(position, all_hitters_df, filt = 'order')
                last_replaced_filt = determine_filter(position, all_hitters_df, filt = 'last_replaced')
                #filter out players already in lineup, lineup will change with each iteration
                h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                team_counts = dict(h_df['team'].value_counts())
                team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                dupe_filt = ((~all_hitters_df['fd_id'].isin(lineup)) & (~all_hitters_df['team'].isin(team_counts_list)))
                # dupe_filt = (~all_hitters_df['fd_id'].isin(lineup))
                #filter out players not eligible for the current position being filled.
                pos_filt =  determine_filter(position, all_hitters_df)
                #get the ammount of roster spots that need filling.
                npl = len([idx for idx, spot in enumerate(lineup) if not spot])
                #the avgerage salary remaining for each empty lineup spot
                avg_sal = remaining_salary / npl
                #filter out players with a salary greater than the average avg_sal above
                if npl > 1 and avg_sal >= min_avg_post_stack:
                    sal_filt = (all_hitters_df['fd_salary'] <= (avg_sal + (npl * 100)))
                sal_filt = (all_hitters_df['fd_salary'] <= avg_sal)
                try:
                    update_last_replaced = False
                    if not find_cheap_secondary and npl > 1 or ((npl == 4 and avg_sal > all_hitters_df['fd_salary'].median())
                                                             or (npl == 3 and avg_sal > all_hitters_df['fd_salary'].quantile(.75))
                                                             or (npl == 2 and avg_sal > all_hitters_df['fd_salary'].quantile(.90))):
                         hitters = all_hitters_df[pos_filt & dupe_filt & secondary_stack_filt]
                    else:
                        hitters = all_hitters_df[pos_filt & dupe_filt & secondary_stack_filt & sal_filt]
                        

                    hitter = hitters.loc[hitters[stack_key].idxmax()]

                except (KeyError, ValueError):
                    try:
                        if len(all_in) == 0:
                            raise ValueError("No all ins.")

                        update_last_replaced = True

                        all_in_filt = ((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in)))
                        hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt
                                    & opp_filt & sal_filt & count_filt
                                    & last_replaced_filt & last_lu_filt
                                    & all_in_filt & order_filt]

                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                    except (KeyError, ValueError):

                        try:
                            update_last_replaced = True

                            hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & (fade_filt | value_filt) 
                                        & opp_filt & sal_filt & count_filt & order_filt 
                                        & plat_filt & last_replaced_filt & last_lu_filt]
                            hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                        except (KeyError, ValueError):
                            try:
                                hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                            except (KeyError, ValueError):
                                try:
                                    hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & opp_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                    hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                except (KeyError, ValueError):
                                    try:
                                        hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                        print(f"Trouble with {position}")
                                    except:
                                        
                                        hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & sal_filt & count_filt & order_filt & plat_filt]
                                        hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                            
                if update_last_replaced and filter_last_replaced:
                    
                    if 'of' in position:
                        last_replaced['of'] = hitter['fd_id']
                    else:
                        last_replaced[position] = hitter['fd_id']
                        
                salary += hitter['fd_salary'].item()
                remaining_salary = max_sal - salary
                #if the selected hitter's salary put the lineup over the max. salary, try to find replacement.
                if remaining_salary < 0:
                     r_sal = hitter['fd_salary'].item()
                     try:
                         
                         salary_df = hitters[((hitters['fd_salary'] <= (r_sal + remaining_salary)) & hitters['team'] == secondary_stack)]
                         hitter = salary_df.loc[hitters[non_stack_key].idxmax()]
                         salary += hitter['fd_salary']
                         salary -= r_sal
                         remaining_salary = max_sal - salary
                     except(ValueError,KeyError):
                         try:
                             salary_df = hitters[(hitters['fd_salary'] <= (r_sal + remaining_salary)) ]
                             hitter = salary_df.loc[hitters[non_stack_key].idxmax()]
                             salary += hitter['fd_salary']
                             salary -= r_sal
                             remaining_salary = max_sal - salary
                             if filter_last_replaced:
                                if 'of' in position:
                                    last_replaced['of'] = hitter['fd_id']
                                else:
                                    last_replaced[position] = hitter['fd_id']
                         
                         #if could not find replacment hitter, swap the pitcher for a lower salaried one.
                         except(ValueError,KeyError):
                             avoid_pitchers = []
                             if never_pair and stack:
                                 for k in never_pair.keys():
                                     if stack in never_pair[k]:
                                         avoid_pitchers.append(k)
                             if never_pair_sec and secondary_stack:
                                 for k in never_pair_sec.keys():
                                     if secondary_stack in never_pair_sec[k]:
                                         avoid_pitchers.append(k)
                             try:
                                 new_combo = False
                                 current_pitcher = all_pitchers_df.loc[selected_pitcher]
                                 cp_sal = current_pitcher['fd_salary'].item()
                                 #filter out pitchers too expensive for current lineup
                                 p_sal_filt = (all_pitchers_df['fd_salary'] <= (remaining_salary + cp_sal))
                                 #filter out pitchers not being used for slate. 
                                 p_teams = all_pitchers_df.loc[pitcher_count_dict.keys(), 'team'].values
                                 p_team_filt = ((all_pitchers_df['team'].isin(p_teams)) & (~all_pitchers_df['fd_id'].isin(no_replace)))
                                 #filter out pitchers going against any team in current lineup
                                 h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                                 used_teams=h_df['team'].unique()
                                 p_opp_filt = (~all_pitchers_df['opp'].isin(used_teams))
                                 avoid_pitcher_filt = (~all_pitchers_df['fd_id'].isin(avoid_pitchers))
                                 #potential replacement pitchers
                                 replacements = all_pitchers_df[p_sal_filt & p_team_filt & p_opp_filt & avoid_pitcher_filt]
                                 #use the pitcher with the most projected points in above rps
                                 new_pitcher = replacements.loc[replacements[pitcher_replace_key].idxmax()]
                                 #reset the index and information for the pi/p_info variables
                                 selected_pitcher = replacements[pitcher_replace_key].idxmax()
                                 p_info = FDSlate.get_p_info(all_pitchers_df, selected_pitcher)
                                 np_id = new_pitcher['fd_id']
                                 np_sal = new_pitcher['fd_salary']
                                 #replacement found, subtract replaced pitchers salary and add in new p's.
                                 salary -= cp_sal
                                 salary += np_sal
                                 remaining_salary = max_sal - salary
                                 #insert new pitcher into the static lineup pitcher spot
                                 lineup[0] = np_id
                             #same as above exception, but try with pitchers not being used in slate.   
                             except(ValueError,KeyError):
                                 if not expand_pitchers:
                                     reset = True
                                     print(f"Problem combining stack: {stack} secondary: {secondary_stack} pitcher:{p_info['current_pitcher_team']}")
                                     break
                                 else:
                                     try:
                                         current_pitcher = all_pitchers_df.loc[selected_pitcher]
                                         cp_sal = current_pitcher['fd_salary'].item()
                                         p_sal_filt = (all_pitchers_df['fd_salary'] <= (remaining_salary + cp_sal))
                                         h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                                         used_teams=h_df['team'].unique()
                                         p_opp_filt = (~all_pitchers_df['opp'].isin(used_teams))
                                         replacements = all_pitchers_df[p_sal_filt & p_opp_filt] 
                                         new_pitcher = replacements.loc[replacements[pitcher_replace_key].idxmax()]
                                         selected_pitcher = replacements[pitcher_replace_key].idxmax()
                                         p_info = FDSlate.get_p_info(all_pitchers_df, selected_pitcher)
                                         np_id = new_pitcher['fd_id']
                                         np_sal = new_pitcher['fd_salary']
                                         salary -= cp_sal
                                         salary += np_sal
                                         remaining_salary = max_sal - salary
                                         lineup[0] = np_id
                                         if not pitchers.get(selected_pitcher):
                                             pitcher_count_dict[selected_pitcher] = -1
                                     except(KeyError, ValueError):
                                         reset = True
                                         print(f"Problem combining stack: {stack} secondary: {secondary_stack} pitcher:{p_info['current_pitcher_team']}")
                                         break
                #at this point the selected hitter's salary has not put the salary over max_sal
                h_id = hitter['fd_id']
                index_of_position = position_index_map[position]
                lineup[index_of_position] = h_id
            if not reset and stack and stack not in no_stack_replace and increase_stack_salary:

                if shuffle_positions_stack:
                    random.shuffle(stacked_pos)
                for position in stacked_pos:
                    if remaining_salary == 0:
                        break

                    index_of_position = position_index_map[position]
                    if lineup[index_of_position] not in never_replace:
                        player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                        s_sal = player_being_replaced['fd_salary'].item()
                        s_order = player_being_replaced['order'].item()
                        s_points = player_being_replaced['sp_split'].item()
                        higher_points_filt = (all_hitters_df['sp_split'] > s_points)
                        higher_order_filt = (all_hitters_df['order'] < s_order)
                        if lineup[index_of_position] in always_replace:
                            sal_filt = (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal))
                        else:
                            sal_filt = (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal))
                            # sal_filt = ((all_hitters_df['fd_salary'] >= s_sal) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                        dupe_filt = (~all_hitters_df['fd_id'].isin(lineup))
                        stack_only_filt = (all_hitters_df['team'] == stack)
                        all_in_filt = ((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in)))
                        never_fill_filt = (~all_hitters_df['fd_id'].isin(never_fill))
                        pos_filt =  determine_filter(position, all_hitters_df)
                        hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & stack_only_filt & all_in_filt & higher_order_filt & never_fill_filt & higher_points_filt]
                        if len(hitters.index) == 0:
                            hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & stack_only_filt & higher_order_filt & never_fill_filt & higher_points_filt]
                        if len(hitters.index) > 0:
                            hitter = hitters.loc[hitters['fd_salary'].idxmax()]
                            selected_name = player_being_replaced['name'].max()
                            hitter_name = hitter['name']
                            print(f"Replacing {selected_name} with {hitter_name}.(stack-up)")
                            n_sal = hitter['fd_salary'].item()
                            salary -= s_sal
                            salary += n_sal
                            remaining_salary = max_sal - salary
                            lineup[index_of_position] = hitter['fd_id']
            if secondary_stack and not reset and secondary_stack not in no_secondary_replace and increase_secondary_salary:

                if shuffle_positions_non_stack:
                    random.shuffle(needed_pos)
                for position in needed_pos:
                    if remaining_salary == 0:
                        break

                    index_of_position = position_index_map[position]
                    if lineup[index_of_position] not in never_replace:
                        player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                        s_sal = player_being_replaced['fd_salary'].item()
                        s_team = player_being_replaced['team'].item()
                        if lineup[index_of_position] in always_replace or s_team != secondary_stack:
                            sal_filt = (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal))
                        else:
                            sal_filt = (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal))
                            # sal_filt = ((all_hitters_df['fd_salary'] >= s_sal) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                        h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                        team_counts = dict(h_df['team'].value_counts())
                        team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                        
                        dupe_filt = (~all_hitters_df['fd_id'].isin(lineup) & ((~all_hitters_df['team'].isin(team_counts_list)) | (all_hitters_df['team'] == s_team)))
                        
                        opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                        s_order = player_being_replaced['order'].item()
                        s_points = player_being_replaced['sp_split'].item()
                        higher_points_filt = (all_hitters_df['sp_split'] > s_points)
                        # print(all_hitters_df.loc[higher_points_filt, ['name', 'sp_split']])
                        higher_order_filt = (all_hitters_df['order'] < s_order)
                        all_in_filt = ((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in)))
                        never_fill_filt = (~all_hitters_df['fd_id'].isin(never_fill))
                        pos_filt =  determine_filter(position, all_hitters_df)
                        hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & secondary_stack_filt & all_in_filt & higher_order_filt & never_fill_filt & higher_points_filt]

                        # print(s_team)
                        # print(secondary_stack)
                        # print(s_points)
                        if len(hitters.index) == 0:
                            if s_team != secondary_stack:

                                
                                
                                
                                hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & secondary_stack_filt]
                            else:   
                                hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & secondary_stack_filt & higher_order_filt & never_fill_filt & higher_points_filt]
                                
                        if len(hitters.index) > 0:
                            hitter = hitters.loc[hitters['fd_salary'].idxmax()]
                            selected_name = player_being_replaced['name'].max()
                            hitter_name = hitter['name']
                            print(f"Replacing {selected_name} with {hitter_name}.(sec. up)")
                            
                            n_sal = hitter['fd_salary'].item()
                            salary -= s_sal
                            salary += n_sal
                            remaining_salary = max_sal - salary
                            lineup[index_of_position] = hitter['fd_id']

            if shuffle_positions_non_stack:
                random.shuffle(needed_pos)
            if (
                not reset
                and (replace_secondary_all_in or secondary_stack in always_replace)
                and secondary_stack not in never_replace_secondary
            ):
                if shuffle_positions_non_stack:
                    random.shuffle(needed_pos)

                for position in needed_pos:
                    # if remaining_salary == 0 or len(all_in) == 0:
                    #     break
                    index_of_position = position_index_map[position]
                    if lineup[index_of_position] not in never_replace:
                        index_of_position = position_index_map[position]
                        player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                        s_team = player_being_replaced['team'].item()
                        s_order = player_being_replaced['order'].item()
                        if s_order > all_in_replace_order_sec or s_team != secondary_stack or lineup[index_of_position] in always_replace:
                            s_sal = player_being_replaced['fd_salary'].item()
                            if lineup[index_of_position] in always_replace:
                                sal_filt = (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal))
                            else:
                                sal_filt = ((all_hitters_df['fd_salary'] >= s_sal - all_in_diff_filt) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                            h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                            team_counts = dict(h_df['team'].value_counts())
                            team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                            dupe_filt = dupe_filt = (~all_hitters_df['fd_id'].isin(lineup) & ((~all_hitters_df['team'].isin(team_counts_list)) | (all_hitters_df['team'] == s_team)))
                            opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                            all_in_filt = ((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in)))
                            supreme_filt = ((all_hitters_df['fd_id'].isin(supreme_all_in)) | (all_hitters_df['team'].isin(supreme_all_in)))
                            pos_filt =  determine_filter(position, all_hitters_df)
                            hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & supreme_filt & opp_filt & stack_filt]
                            if len(hitters.index) == 0:
                                hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & all_in_filt & opp_filt & stack_filt]
                            if len(hitters.index) > 0:
                                qualified_sal = hitters['fd_salary'].max() - all_in_replace_filt
                                qualified_sal_hitters = hitters[hitters['fd_salary'] >= qualified_sal]
                                hitter = qualified_sal_hitters.loc[qualified_sal_hitters['t_count'].idxmin()]
                                selected_name = player_being_replaced['name'].max()
                                hitter_name = hitter['name']
                                print(f"Replacing {selected_name} with {hitter_name}.(all in sec.)")
                                
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                remaining_salary = max_sal - salary
                                lineup[index_of_position] = hitter['fd_id']
                    
                        
            if not reset and (replace_primary_all_in or stack in always_replace) and stack not in never_replace_primary:
                for position in stacked_pos:
                    # if remaining_salary == 0 or len(all_in) == 0:
                    #     break
                    index_of_position = position_index_map[position]
                    h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                    team_counts = dict(h_df['team'].value_counts())
                    team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                    if lineup[index_of_position] not in never_replace:
                        index_of_position = position_index_map[position]
                        player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                        s_order = player_being_replaced['order'].item()
                        s_team = player_being_replaced['team'].item()
                        if s_order > all_in_replace_order_stack or lineup[index_of_position] in always_replace:
                            s_sal = player_being_replaced['fd_salary'].item()
                            if lineup[index_of_position] in always_replace:
                                sal_filt = (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal))
                            else:
                                sal_filt = ((all_hitters_df['fd_salary'] >= s_sal - all_in_diff_filt) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                            # dupe_filt = (~all_hitters_df['fd_id'].isin(lineup))
                            dupe_filt = dupe_filt = (~all_hitters_df['fd_id'].isin(lineup) & ((~all_hitters_df['team'].isin(team_counts_list)) | (all_hitters_df['team'] == s_team)))
                            opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                            all_in_filt = ((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in)))
                            supreme_filt = ((all_hitters_df['fd_id'].isin(supreme_all_in)) | (all_hitters_df['team'].isin(supreme_all_in)))
                            pos_filt =  determine_filter(position, all_hitters_df)
                            hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & supreme_filt & opp_filt & stack_filt]
                            if len(hitters.index) == 0:
                                hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & all_in_filt & opp_filt & stack_filt]
                            if len(hitters.index) > 0:
                                qualified_sal = hitters['fd_salary'].max() - all_in_replace_filt
                                qualified_sal_hitters = hitters[hitters['fd_salary'] >= qualified_sal]
                                hitter = qualified_sal_hitters.loc[qualified_sal_hitters['t_count'].idxmin()]
                                selected_name = player_being_replaced['name'].max()
                                hitter_name = hitter['name']
                                print(f"Replacing {selected_name} with {hitter_name}.(all in stack.)")
                                n_sal = hitter['fd_salary'].item()
                                salary -= s_sal
                                salary += n_sal
                                remaining_salary = max_sal - salary
                                lineup[index_of_position] = hitter['fd_id']
                                    
                                    
                                    

            if not reset and supreme_all_in:
                if stack in no_supreme_replace or never_replace_primary_supreme:
                    all_positions = needed_pos.copy()
                else:
                    all_positions = needed_pos + stacked_pos
                for position in all_positions:
                    if position in supreme_all_in_pos:
                        if remaining_salary == 0:
                            break

                        index_of_position = position_index_map[position]
                        h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                        team_counts = dict(h_df['team'].value_counts())
                        team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                        if lineup[index_of_position] not in no_supreme_replace:
                            index_of_position = position_index_map[position]
                            player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                            s_order = player_being_replaced['order'].item()
                            s_team = player_being_replaced['team'].item()
                            if s_order > supreme_replace_order:
                                s_sal = player_being_replaced['fd_salary'].item()
                                
                                if lineup[index_of_position] in always_replace:
                                    sal_filt = (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal))
                                else:
                                    sal_filt = ((all_hitters_df['fd_salary'] >= s_sal - all_in_diff_filt) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                                # dupe_filt = (~all_hitters_df['fd_id'].isin(lineup))
                                dupe_filt = dupe_filt = (~all_hitters_df['fd_id'].isin(lineup) & ((~all_hitters_df['team'].isin(team_counts_list)) | (all_hitters_df['team'] == s_team)))
                                opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                                all_in_filt = ((all_hitters_df['fd_id'].isin(supreme_all_in)) | (all_hitters_df['team'].isin(supreme_all_in)))
                                pos_filt =  determine_filter(position, all_hitters_df)
                                hitters = all_hitters_df[pos_filt & dupe_filt & sal_filt & all_in_filt & opp_filt]
                                if len(hitters.index) > 0:
                                    qualified_sal = hitters['fd_salary'].max() - all_in_replace_filt
                                    qualified_sal_hitters = hitters[hitters['fd_salary'] >= qualified_sal]
                                    hitter = qualified_sal_hitters.loc[qualified_sal_hitters['t_count'].idxmin()]
                                    selected_name = player_being_replaced['name'].max()
                                    hitter_name = hitter['name']
                                    print(f"Replacing {selected_name} with {hitter_name}.(supreme all in.)")
                                    n_sal = hitter['fd_salary'].item()
                                    salary -= s_sal
                                    salary += n_sal
                                    remaining_salary = max_sal - salary
                                    lineup[index_of_position] = hitter['fd_id']        
                    
                        
            #if remaining salary is greater than specified arg max_surplus
            if lus % 2 == 0 and not always_pitcher_first:
                if not reset and (enforce_hitter_surplus or (p_info['current_pitcher_fanduel_id'] in lock and always_replace_pitcher_lock)) and remaining_salary > max_surplus and (not new_combo or p_info['current_pitcher_fanduel_id'] in no_combos or not no_surplus_secondary_stacks):
                    if shuffle_positions_non_stack:
                        random.shuffle(needed_pos)    
                    for position in needed_pos:
                        if remaining_salary <= max_surplus:
                            break
                        order_filt = determine_filter(position, all_hitters_df, filt = 'order')
                        index_of_position = position_index_map[position]
                        player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                        s_sal = player_being_replaced['fd_salary']
                        s_team = player_being_replaced['team'].item()
                        if s_sal < all_hitters_df['fd_salary'].quantile(high_salary_quantile) and position not in never_replace and lineup[index_of_position] not in never_replace:
                            #filter out players already in lineup, lineup will change with each iteration
                            h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                            team_counts = dict(h_df['team'].value_counts())
                            team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                            dupe_filt = dupe_filt = (~all_hitters_df['fd_id'].isin(lineup) & ((~all_hitters_df['team'].isin(team_counts_list)) | (all_hitters_df['team'] == s_team)))
                            #filter out players going against current lineup's pitcher
                            opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                            last_replaced_filt = determine_filter(position, all_hitters_df, filt = 'last_replaced')
                            #filter out players not eligible for the current position being filled.
                            pos_filt =  determine_filter(position, all_hitters_df)
                            #filter out players with a salary less than high_salary_quantile arg
                            sal_filt = ((all_hitters_df['fd_salary'] > s_sal) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                            hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & secondary_stack_filt]
                            if len(hitters.index) == 0:
                                if fill_high_salary_first:
                                    sal_filt = ((all_hitters_df['fd_salary'] >= np.floor(all_hitters_df['fd_salary'].quantile(high_salary_quantile))) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                                hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]

                            if len(hitters.index) > 0:
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                n_sal = hitter["fd_salary"].item()
                                salary -= s_sal
                                salary += n_sal

                                remaining_salary = max_sal - salary
                                lineup[index_of_position] = hitter['fd_id']
                            else:
                                sal_filt = ((all_hitters_df['fd_salary'] > s_sal) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                                hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & order_filt & secondary_stack_filt]
                                if len(hitters.index) == 0:
                                    hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]

                                if len(hitters.index) > 0:
                                    hitter = hitters.loc[
                                        hitters[non_stack_key].idxmax()
                                    ]
                                    n_sal = hitter["fd_salary"].item()
                                    salary -= s_sal
                                    salary += n_sal

                                    remaining_salary = max_sal - salary
                                    lineup[index_of_position] = hitter['fd_id']
                            if len(hitters.index) > 0:
                                if 'of' in position:
                                     last_replaced['of'] = hitter['fd_id']
                                else:
                                    last_replaced[position] = hitter['fd_id']
                    
                if not reset and enforce_pitcher_surplus and remaining_salary > pitcher_surplus and p_info['current_pitcher_fanduel_id'] not in lock and (not new_combo or p_info['current_pitcher_fanduel_id'] in no_combos):
                    #filter out pitchers who would put the salary over the max_sal if inserted
                    current_pitcher = all_pitchers_df.loc[selected_pitcher]
                    cp_sal = current_pitcher['fd_salary'].item()
                    p_sal_filt = (all_pitchers_df['fd_salary'] <= max_sal - (salary - cp_sal))
                    #filter out pitchers not being used for slate.
                    p_teams = all_pitchers_df.loc[pitcher_count_dict.keys(), 'team'].values
                    p_team_filt = (all_pitchers_df['team'].isin(p_teams))
                    #filter out pitchers going against teams already in lineup
                    h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                    used_teams=h_df['team'].unique()
                    p_opp_filt = (~all_pitchers_df['opp'].isin(used_teams))
                    top_pitcher_filt = (all_pitchers_df['fd_id'].isin(always_replace_first))
                    replacements = all_pitchers_df[p_sal_filt & p_team_filt & p_opp_filt & top_pitcher_filt]
                    if len(replacements.index) == 0:
                        replacements = all_pitchers_df[p_sal_filt & p_team_filt & p_opp_filt]
                    #if a pitcher meeting parameters exists, insert in lineup, else leave lineup as is.
                    if len(replacements.index) > 1:
                        new_pitcher = replacements.loc[replacements['fd_salary'].idxmax()]
                        selected_pitcher = replacements['fd_salary'].idxmax()
                        p_info = FDSlate.get_p_info(all_pitchers_df, selected_pitcher)
                        np_id = new_pitcher['fd_id']
                        np_sal = new_pitcher['fd_salary']
                        #subtract replaced pitcher's salary, add new pitcher's salary

                        salary -= cp_sal
                        salary += np_sal
                        remaining_salary = max_sal - salary
                        lineup[0] = np_id
            else:

                
                if not reset and enforce_pitcher_surplus and remaining_salary > pitcher_surplus and p_info['current_pitcher_fanduel_id'] not in lock and (not new_combo or p_info['current_pitcher_fanduel_id'] in no_combos or not no_surplus_secondary_stacks):
                    #filter out pitchers who would put the salary over the max_sal if inserted
                    current_pitcher = all_pitchers_df.loc[selected_pitcher]
                    cp_sal = current_pitcher['fd_salary'].item()
                    p_sal_filt = (all_pitchers_df['fd_salary'] <= max_sal - (salary - cp_sal))
                    #filter out pitchers not being used for slate.
                    p_teams = all_pitchers_df.loc[pitcher_count_dict.keys(), 'team'].values
                    p_team_filt = (all_pitchers_df['team'].isin(p_teams))
                    #filter out pitchers going against teams already in lineup
                    h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                    used_teams=h_df['team'].unique()
                    p_opp_filt = (~all_pitchers_df['opp'].isin(used_teams))
                    top_pitcher_filt = (all_pitchers_df['fd_id'].isin(always_replace_first))
                    replacements = all_pitchers_df[p_sal_filt & p_team_filt & p_opp_filt & top_pitcher_filt]
                    if len(replacements.index) == 0:
                        replacements = all_pitchers_df[p_sal_filt & p_team_filt & p_opp_filt]
                    #if a pitcher meeting parameters exists, insert in lineup, else leave lineup as is.
                    if len(replacements.index) > 1:
                        new_pitcher = replacements.loc[replacements['fd_salary'].idxmax()]
                        selected_pitcher = replacements['fd_salary'].idxmax()
                        p_info = FDSlate.get_p_info(all_pitchers_df, selected_pitcher)
                        np_id = new_pitcher['fd_id']
                        np_sal = new_pitcher['fd_salary']
                        #subtract replaced pitcher's salary, add new pitcher's salary

                        salary -= cp_sal
                        salary += np_sal
                        remaining_salary = max_sal - salary
                        lineup[0] = np_id

                if not reset and (enforce_hitter_surplus or (p_info['current_pitcher_fanduel_id'] in lock and always_replace_pitcher_lock)) and remaining_salary > max_surplus and (not new_combo or p_info['current_pitcher_fanduel_id'] in no_combos):
                    if shuffle_positions_non_stack:
                        random.shuffle(needed_pos)    
                    for position in needed_pos:
                        if remaining_salary <= max_surplus:
                            break
                        order_filt = determine_filter(position, all_hitters_df, filt = 'order')
                        index_of_position = position_index_map[position]
                        player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                        s_sal = player_being_replaced['fd_salary'].item()
                        s_team = player_being_replaced['team'].item()
                        if s_sal < all_hitters_df['fd_salary'].quantile(high_salary_quantile) and position not in never_replace and lineup[index_of_position] not in never_replace:
                            #filter out players already in lineup, lineup will change with each iteration
                            h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                            team_counts = dict(h_df['team'].value_counts())
                            team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                            dupe_filt = dupe_filt = (~all_hitters_df['fd_id'].isin(lineup) & ((~all_hitters_df['team'].isin(team_counts_list)) | (all_hitters_df['team'] == s_team)))
                            #filter out players going against current lineup's pitcher
                            opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                            last_replaced_filt = determine_filter(position, all_hitters_df, filt = 'last_replaced')
                            #filter out players not eligible for the current position being filled.
                            pos_filt =  determine_filter(position, all_hitters_df)
                            #filter out players with a salary less than high_salary_quantile arg
                            sal_filt = ((all_hitters_df['fd_salary'] > s_sal) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                            hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & secondary_stack_filt]
                            if len(hitters.index) == 0:
                                if fill_high_salary_first:
                                    sal_filt = ((all_hitters_df['fd_salary'] >= np.floor(all_hitters_df['fd_salary'].quantile(high_salary_quantile))) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                                hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt] 

                            if len(hitters.index) > 0:
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                n_sal = hitter["fd_salary"].item()
                                salary -= s_sal
                                salary += n_sal

                                remaining_salary = max_sal - salary
                                lineup[index_of_position] = hitter['fd_id']
                            else:
                                sal_filt = ((all_hitters_df['fd_salary'] > s_sal) & (all_hitters_df['fd_salary'] <= (remaining_salary + s_sal)))
                                hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & secondary_stack_filt]
                                if len(hitters.index) == 0:
                                    hitters = all_hitters_df[pos_filt & stack_filt & dupe_filt & fade_filt & opp_filt & sal_filt & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]

                                if len(hitters.index) > 0:
                                    hitter = hitters.loc[
                                        hitters[non_stack_key].idxmax()
                                    ]
                                    n_sal = hitter["fd_salary"].item()
                                    salary -= s_sal
                                    salary += n_sal

                                    remaining_salary = max_sal - salary
                                    lineup[index_of_position] = hitter['fd_id']
                            if len(hitters.index) > 0:
                                if 'of' in position:
                                     last_replaced['of'] = hitter['fd_id']
                                else:
                                    last_replaced[position] = hitter['fd_id']
                
                
            #if there aren't enough teams being used, replace the utility player with a team not in use.    
            h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
            used_teams=h_df['team'].unique()
            
            if not reset and (len(used_teams) < 3 and p_info['current_pitcher_team'] in used_teams):

                random.shuffle(needed_pos)
                for position in needed_pos:
                    if position in dupe_lu_replacement_pos:
                        try:

                            index_of_position = position_index_map[position]
                            if lineup[index_of_position] not in never_replace:
                                player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                                s_order = player_being_replaced['order'].item()
                                
                                r_salary = player_being_replaced['fd_salary'].item()
                                last_replaced_filt = determine_filter(position, all_hitters_df, filt = 'last_replaced')
                                order_filt = determine_filter(position, all_hitters_df, filt = 'order')
                                pos_filt =  determine_filter(position, all_hitters_df)
                                #redeclare use_teams each pass
                                h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                                team_counts = dict(h_df['team'].value_counts())
                                team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                                used_teams = h_df['team'].unique()
                                #append players already attempted to used_players and filter them out each loop
                                
                                dupe_filt = ((~all_hitters_df['fd_id'].isin(lineup)) & (~all_hitters_df['team'].isin(used_teams)))
                                #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                                sal_filt = (all_hitters_df['fd_salary'].between((r_salary-util_replace_filt), (remaining_salary + r_salary)))
                                #don't use players on team against current pitcher
                                opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                                all_in_filt = ((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in)))
                                if position == 'util':
                                    hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & all_in_filt & no_utility_filt]
                                else:
                                    hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & all_in_filt & pos_filt]
                                if len(hitters.index) == 0:
                                    hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & (fade_filt | value_filt) & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt & no_utility_filt & pos_filt]
                                    if position == 'util' and len(hitters.index) == 0 and expand_utility:
                                        hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & (fade_filt | value_filt) & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                lu_copy = lineup.copy()
                                lu_copy[index_of_position] = hitter['fd_id']

                                if sorted(lu_copy) in sorted_lus:
                                    if position == needed_pos[-1]:
                                        reset = True
                                        break
                                    else:
                                        continue

                                salary += hitter['fd_salary'].item()
                                salary -= player_being_replaced['fd_salary'].item()
                                remaining_salary = max_sal - salary
                                lineup[index_of_position] = hitter['fd_id']
                                h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                                used_teams = h_df['team'].unique()
                                if not (len(used_teams) < 3 and p_info['current_pitcher_team'] in used_teams):

                                    break
                            # same as above, but no fade_filt and increasing minimum thresehold by 100.

                        except (KeyError, ValueError):
                            if position == needed_pos[-1]:
                                reset = True

                                print('resetting, no replacement for dupe lu. Try increasing util_replace_filt.' f"or try increase max order for {position}")

                                break
                            else:
                                continue
                        if filter_last_replaced:

                            if 'of' in position:
                                last_replaced['of'] = hitter['fd_id']
                            else:
                                last_replaced[position] = hitter['fd_id']
            
       
            #don't insert lineup unless it's not already inserted        
            used_players = []
            h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
            used_teams = h_df['team'].unique()
            update_last_replaced = False
            if shuffle_positions_non_stack:
                random.shuffle(needed_pos)
            while not reset and (sorted(lineup) in sorted_lus or (len(used_teams) < 3 and p_info['current_pitcher_team'] in used_teams)):
                for position in needed_pos:
                    if position in dupe_lu_replacement_pos:
                        try:
                            index_of_position = position_index_map[position]
                            if lineup[index_of_position] not in never_replace:
                                player_being_replaced = all_hitters_df[all_hitters_df['fd_id'] == lineup[index_of_position]]
                                s_order = player_being_replaced['order'].item()
                                used_players.append(player_being_replaced['fd_id'])
                                r_salary = player_being_replaced['fd_salary'].item()
                                s_team = player_being_replaced['team'].item()
                                last_replaced_filt = determine_filter(position, all_hitters_df, filt = 'last_replaced')
                                order_filt = determine_filter(position, all_hitters_df, filt = 'order')
                                pos_filt =  determine_filter(position, all_hitters_df)
                                #redeclare use_teams each pass
                                h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                                team_counts = dict(h_df['team'].value_counts())
                                team_counts_list = [z for z, y in team_counts.items() if y > 3 ]
                                used_teams = h_df['team'].unique()
                                #append players already attempted to used_players and filter them out each loop
                                used_filt = (~all_hitters_df['fd_id'].isin(used_players))
                                dupe_filt = dupe_filt = (~all_hitters_df['fd_id'].isin(lineup) & ((~all_hitters_df['team'].isin(team_counts_list)) | (all_hitters_df['team'] == s_team)))
                                #only use players with a salary between the (util's salary - util_replace_filt) and maxiumum salary.
                                sal_filt = (all_hitters_df['fd_salary'].between((r_salary-util_replace_filt), (remaining_salary + r_salary)))
                                #don't use players on team against current pitcher
                                opp_filt = (all_hitters_df['opp'] != p_info['current_pitcher_team'])
                                all_in_filt = ((all_hitters_df['fd_id'].isin(all_in)) | (all_hitters_df['team'].isin(all_in)))
                                if position == 'util':
                                    hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & used_filt & secondary_stack_filt & no_utility_filt & order_filt]
                                    
                                    if len(hitters.index) == 0:
                                        hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & used_filt & all_in_filt & no_utility_filt]
                                    if len(hitters.index) == 0:
                                        hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & used_filt & secondary_stack_filt & no_utility_filt]
                                        
                                        
                                else:
                                     hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & used_filt & secondary_stack_filt & pos_filt & order_filt]
                                     
                                     if len(hitters.index) == 0:
                                         hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & used_filt & all_in_filt & pos_filt]
                                       
                                        
                                if len(hitters.index) == 0:
                                    hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & used_filt & (fade_filt | value_filt) & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt & no_utility_filt & pos_filt]
                                    if position == 'util' and len(hitters.index) == 0 and expand_utility:
                                        hitters = all_hitters_df[dupe_filt & sal_filt & opp_filt & used_filt & (fade_filt | value_filt) & count_filt & order_filt & plat_filt & last_replaced_filt & last_lu_filt]
                                hitter = hitters.loc[hitters[non_stack_key].idxmax()]
                                lu_copy = lineup.copy()
                                lu_copy[index_of_position] = hitter['fd_id']

                                if sorted(lu_copy) in sorted_lus:
                                    if position == needed_pos[-1]:
                                        reset = True
                                        break
                                    else:

                                        continue

                                used_players.append(hitter['fd_id'])
                                salary += hitter['fd_salary'].item()
                                salary -= player_being_replaced['fd_salary'].item()
                                remaining_salary = max_sal - salary
                                lineup[index_of_position] = hitter['fd_id']
                                h_df = all_hitters_df[all_hitters_df['fd_id'].isin(lineup)]
                                used_teams = h_df['team'].unique()

                                if not sorted(lineup) in sorted_lus:
                                    break

                        except (KeyError, ValueError):

                            print('error')
                            print(f"{position} {needed_pos[-1]}")
                            print(needed_pos)
                            if position == needed_pos[-1]:
                                print('reset')
                                reset = True
                                print('resetting, no replacement for dupe lu. Try increasing util_replace_filt.' f"or try increase max order for {position}")

                                break
                            else:
                                print("continuing")
                                continue

            if reset == True:
                continue
            if update_last_replaced:
                last_replaced["util"] = lineup[8]
            #!!the lineup meets all parameters at this point.!!
            # decrease lus arg, loop ends at 0.
            lus -= 1
            if secondary_stack:
                secondary[secondary_stack] -= 1
            # append the new lineup to the sorted lus list for next loop.
            sorted_lus.append(sorted(lineup))
            last_lu = lineup.copy()
            # insert the lineup in the lineup df
            self.insert_lineup(index_track, lineup)
            # increase index for next lineup insertion.
            index_track += 1
            # decrease the current stack's value, so it won't be attempted once it reaches 0.
            s[stack] -= 1

            #decrease the pitcher's pitcher_count_dict value, so it won't be used once expected lus filled.
            pitcher_count_dict[selected_pitcher] -= 1
            #keep track of total insertions and stack insertions, players exceeding max_lu_total will be dropped next loop.
            lu_filt = (all_hitters_df['fd_id'].isin(lineup))
            all_hitters_df.loc[lu_filt, 't_count'] += 1
            all_hitters_df.loc[lu_filt & stack_filt, 'ns_count'] += 1

            # print(lus)
            # print(index_track)
            print(salary)
            print(lus)
            if not stack_track[stack]["secondary"].get(secondary_stack):
                stack_track[stack]["secondary"][secondary_stack] = 1
            else:

                stack_track[stack]['secondary'][secondary_stack] += 1
            if not stack_track[stack]['pitchers'].get(p_info['current_pitcher_name']):
                stack_track[stack]['pitchers'][p_info['current_pitcher_name']] = 1
            else:
                stack_track[stack]['pitchers'][p_info['current_pitcher_name']] += 1
            p_stack_combo = [p_info['current_pitcher_name'], secondary_stack]
            if p_stack_combo not in stack_track[stack]['combos'] and secondary_stack:
                stack_track[stack]['combos'].append(p_stack_combo)
            #keep track of players counts, regardless if they're eventually dropped.
            h_count_df.loc[(h_count_df['fd_id'].isin(lineup)), 't_count'] += 1
            p_count_df.loc[(p_count_df['fd_id']) == lineup[0], 't_count'] += 1
        #dump the counts into pickled df for analysis    
        h_count_file = pickle_path(name=f"h_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)   
        p_count_file = pickle_path(name=f"p_counts_{tf.today}_{self.slate_number}", directory=settings.FD_DIR)

        with open(h_count_file, "wb") as f:
            pickle.dump(h_count_df, f)
        with open(p_count_file, "wb") as f:

                pickle.dump(p_count_df, f)
        
        print(secondary)
        return stack_track
        

