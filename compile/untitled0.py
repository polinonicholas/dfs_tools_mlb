dupe_filt = (~h['fd_id'].isin(lineup) & ((~h['team'].isin(team_counts_list)) | (h['team'] == s_team)))

dupe_filt = (((~h['fd_id'].isin(lineup)) & (~h['team'].isin(team_counts_list))) | (h['team'] == s_team))