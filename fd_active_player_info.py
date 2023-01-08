from dfs_tools_mlb import settings
from dfs_tools_mlb.compile.fanduel import FDSlate as fds

def get_fd_ranks(position='UTIL', largest=15, filtered_cols = True, sort_by = 'points',
                 **kwargs):
    f = fds()
    position = position.casefold()
    if not position or position == 'util' or position == 'utility':
        df = f.util_df
    elif position == 'first' or position == '1b':
        df = f.first_df
    elif position == 'second' or position == '2b':
        df = f.second_df
    elif position == 'shortstop' or position == 'ss':
        df = f.ss_df
    elif position == 'third' or position == '3b':
        df = f.third_df
    elif position == 'outfield' or position == 'of':
        df = f.of_df
    else:
        df = f.util_df
    df = df.sort_values(sort_by, ascending = False)
    if filtered_cols:
        df = df[settings.FD_PLAYER_RANK_COLS]
    if largest:
        idx = df[sort_by].nlargest(largest).index
        df = df.loc[idx]
    if kwargs:
        for column,iterable in kwargs.items():
            df = df[(df[column].isin(iterable))]
            
    return df

    
    
    