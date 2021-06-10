from dfs_tools_mlb.compile import teams
import pandas
pandas.set_option('display.max_columns', None)
# test = 'home'
t = teams.mariners
o = teams.tigers 
p_hand = None
p_name = None 
if not p_name:
    p_name = t.opp_sp['lastName']
if not p_hand:
    p_hand = t.opp_sp_hand
hitters = t.hitter
lu = t.lineup
pitchers = o.pitcher
pitcher = pitchers[pitchers['name'].str.contains(p_name)]
print(pitcher[['name','fd_wps_b_vr', 'fd_wps_b_vl', 'batters_faced_vl', 'pitches_start']])
if p_hand == 'R':
    print(t.lineup_df()[['name', 'fd_wpa_pa_vr', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']])

elif p_hand == 'L':
    print(t.lineup_df()[['name', 'fd_wpa_pa_vl', 'pa_vl', 'fd_hr_weight_vl', 'bat_side']])
else:
    print(t.lineup_df()[['name', 'fd_wpa_pa', 'pa', 'fd_hr_weight', 'bat_side']])
print(f"{p_name} is {p_hand}")
print(pitcher[['name','fd_wpa_b_vr', 'fd_wpa_b_vl', 'batters_faced_vl', 'pitches_start']])
if p_hand == 'R':
    print(t.lineup_df()[['name', 'fd_wps_pa_vr','fd_wps_pa_vl', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']])

elif p_hand == 'L':
    print(t.lineup_df()[['name', 'fd_wps_pa_vl', 'fd_wps_pa_vr', 'pa_vl', 'fd_hr_weight_vl', 'bat_side']])
else:
    print(t.lineup_df()[['name', 'fd_wps_pa', 'pa', 'fd_hr_weight', 'bat_side']])

print(f"{o.name} bp ovr:{o.proj_opp_bp['fd_wpa_b_rp'].mean()}")
print(f"{o.name} bp vr:{o.proj_opp_bp['fd_wpa_b_vr'].mean()}")
print(f"{o.name} bp vl:{o.proj_opp_bp['fd_wpa_b_vl'].mean()}")
print(len(o.used_rp))
print(lu)


pitchers[['name', 'mlb_id']]
hitters[['name', 'mlb_id', 'fd_wps_pa_vr']]
t.pitcher[['name', 'mlb_id']]
t.rested_bullpen[['name', 'batters_faced_sp', 'mlb_id']]
o.sp_df()[['name','exp_inn', 'points', 'ump_points', 'venue_points', 'temp_points', 'pitches_start', 'mlb_id']]

angels = [457708, 660271, 543685, 665120, 501571, 578428, 621493, 435559, 664058]
royals = [593160, 467793, 643217, 521692, 642721, 624585, 572191, 641531, 670032]


# test = t.lineup_df()[['name', 'fd_wpa_pa_vr', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']]
# test2 =  t.lineup_df()[['name', 'fd_wpa_pa_vr', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']]
# test['fd_wpa_pa_vr'].describe()
# test2['fd_wpa_pa_vr'].describe()

# test['fd_wpa_pa_vr'].sum()
# test2['fd_wpa_pa_vr'].sum()
