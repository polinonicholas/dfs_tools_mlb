import pandas as pd
from difflib import SequenceMatcher


def sm_merge(
    df1,
    df2,
    columns=[],
    ratios=[],
    prefix="m_",
    reset_index=True,
    post_drop=True,
    suffixes=("_a", "_b"),
):
    df1_c = df1.copy()
    df2_c = df2.copy()
    if reset_index:
        df1_c.reset_index(inplace=True, drop=True)
        df2_c.reset_index(inplace=True, drop=True)
    df1_c[prefix + columns[0]] = df1_c[columns[0]]
    df1_c[prefix + columns[1]] = df1_c[columns[1]]
    merge_columns = [prefix + columns[0], prefix + columns[1]]
    for col_1, col_2 in df1_c[[columns[0], columns[1]]].values:
        for index, (col_3, col_4) in enumerate(df2_c[[columns[0], columns[1]]].values):
            if SequenceMatcher(None, str(col_1), str(col_3)).ratio() >= ratios[0]:
                df2_c.loc[index, merge_columns[0]] = col_1
            if SequenceMatcher(None, str(col_2), str(col_4)).ratio() >= ratios[1]:
                df2_c.loc[index, merge_columns[1]] = col_2
    df = pd.merge(df1_c, df2_c, on=merge_columns, suffixes=suffixes)
    if post_drop:
        df.drop(columns=merge_columns, inplace=True)
    return df


def sm_merge_arb(
    df1,
    df2,
    columns=[],
    ratios=[],
    prefix="m_",
    reset_index=True,
    post_drop=True,
    suffixes=("_a", "_b"),
):
    df1_c = df1.copy()
    df2_c = df2.copy()
    if reset_index:
        df1_c.reset_index(inplace=True)
        df2_c.reset_index(inplace=True)
    flag = 0
    merge_columns = []
    r = len(columns)
    for f in range(r):
        df1_c[prefix + columns[flag]] = df1_c[columns[flag]]
        merge_columns.append(prefix + columns[flag])
        flag = +1
    flag = 0
    for f in range(r):
        for col_1 in df1_c[columns[flag]].values:
            for index, col_2 in enumerate(df2_c[columns[flag]].values):
                if (
                    SequenceMatcher(None, str(col_1), str(col_2)).ratio()
                    >= ratios[flag]
                ):
                    df2_c.loc[index, merge_columns[flag]] = col_1
        flag = +1
    df = pd.merge(df1_c, df2_c, on=merge_columns, suffixes=suffixes)
    if post_drop:
        df.drop(columns=merge_columns, inplace=True)
    return df


def modify_team_name(df, columns=["team"]):
    from dfs_tools_mlb.compile.static_mlb import team_info

    for column in columns:
        name = df[column]
        name = name.str.casefold()
        for team in team_info:
            a = team_info[team]["abbreviations"]
            df.loc[(name.isin(a) | name.str.contains(team)), [column]] = team
    return df


def sm_merge_single(
    df1,
    df2,
    column="name",
    ratio=0.63,
    prefix="m_",
    reset_index=True,
    post_drop=True,
    suffixes=("_a", "_b"),
):
    df1_c = df1.copy()
    df2_c = df2.copy()
    if reset_index:
        df1_c.reset_index(inplace=True, drop=True)
        df2_c.reset_index(inplace=True, drop=True)
    df1_c[prefix + column] = df1_c[column]
    merge_column = [prefix + column]
    for col in df1_c[column].values:
        for index, (col_2) in enumerate(df2_c[column].values):
            if SequenceMatcher(None, str(col), str(col_2)).ratio() >= ratio:
                df2_c.loc[index, merge_column[0]] = col

    df = pd.merge(df1_c, df2_c, on=merge_column, suffixes=suffixes)
    if post_drop:
        df.drop(columns=merge_column, inplace=True)
    return df
