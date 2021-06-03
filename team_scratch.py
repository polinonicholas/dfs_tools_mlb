from dfs_tools_mlb.compile import teams
import pandas
pandas.set_option('display.max_columns', None)
# test = 'home'
t = teams.dodgers
o = teams.cardinals 
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




pitchers[['name', 'mlb_id']]
hitters[['name', 'mlb_id', 'fd_wps_pa_vr']]
t.pitcher[['name', 'mlb_id']]
t.rested_bullpen[['name', 'batters_faced_sp', 'mlb_id']]
o.sp_df()[['name','exp_inn', 'points', 'ump_points', 'venue_points', 'temp_points', 'pitches_start', 'mlb_id']]

twins = [593871, 518626, 666135, 443558, 593934, 666163, 663616, 608701, 592743]
blue_jays = [543760, 666182, 665489, 606192, 545341, 642133, 666971, 605412, 643376]
mariners = [672284, 571745, 572122, 664034, 641487, 608596, 666211, 641924, 608686]
athletics = [592192, 621566, 476704, 664913, 669221, 519048, 656305, 462101, 643393]
dodgers = [605141, 571970, 457759, 641355, 669257, 666158, 621035, 656716, 621111]
cardinals = [572761, 666185, 502671, 571448, 425877, 641933, 624641, 642211, 593372]
astros = [514888, 621043, 608324, 670541, 493329, 663656, 676801, 664702, 455117]
red_sox = [571771, 657077, 502110, 593428, 646240, 592669, 666915, 543877, 624414]
nationals = [607208, 543281, 665742, 475582, 516770, 656941, 543228, 452657, 645302]
rangers = 'good'

# test = t.lineup_df()[['name', 'fd_wpa_pa_vr', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']]
# test2 =  t.lineup_df()[['name', 'fd_wpa_pa_vr', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']]
# test['fd_wpa_pa_vr'].describe()
# test2['fd_wpa_pa_vr'].describe()

# test['fd_wpa_pa_vr'].sum()
# test2['fd_wpa_pa_vr'].sum()
