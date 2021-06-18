from dfs_tools_mlb.compile import teams
import pandas
pandas.set_option('display.max_columns', None)
# pandas.set_option('display.max_rows', None)

def team_notes(team, opponent,  p_hand = None, p_name = None, extend = False):
    if not p_name:
        p_name = team.opp_sp['lastName']
    if not p_hand:
        p_hand = team.opp_sp_hand
    hitters = team.hitter
    lu = team.lineup
    pitchers = opponent.pitcher
    pitcher = pitchers[pitchers['name'].str.contains(p_name)]
    print('START')
    print(t.weather)
    print(pitcher[['name','fd_wps_b_vr', 'fd_wps_b_vl', 'batters_faced_vl', 'pitches_start']])
    print(t.sp_avg(return_full_dict=True))
    if p_hand == 'R':
        print(team.lineup_df()[['name', 'fd_wpa_pa_vr', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']])
        print(team.lineup_df()['fd_wpa_pa_vr'].describe())
    
    elif p_hand == 'L':
        print(team.lineup_df()[['name', 'fd_wpa_pa_vl', 'pa_vl', 'fd_hr_weight_vl', 'bat_side']])
        print(team.lineup_df()['fd_wpa_pa_vl'].describe())
    else:
        print(team.lineup_df()[['name', 'fd_wpa_pa', 'pa', 'fd_hr_weight', 'bat_side']])
        print(team.lineup_df()['fd_wpa_pa'].describe())
    print(f"{p_name} is {p_hand}")
    print(pitcher[['name','fd_wpa_b_vr', 'fd_wpa_b_vl', 'batters_faced_vl', 'pitches_start']])
    if p_hand == 'R':
        print(team.lineup_df()[['name', 'fd_wps_pa_vr','fd_wps_pa_vl', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']])
        print(team.lineup_df()['fd_wps_pa_vr'].describe())
    elif p_hand == 'L':
        print(team.lineup_df()[['name', 'fd_wps_pa_vl', 'fd_wps_pa_vr', 'pa_vl', 'fd_hr_weight_vl', 'bat_side']])
        print(team.lineup_df()['fd_wps_pa_vl'].describe())
    else:
        print(team.lineup_df()[['name', 'fd_wps_pa', 'pa', 'fd_hr_weight', 'bat_side']])
        print(team.lineup_df()['fd_wps_pa'].describe())
    
    print(f"{opponent.name} bp ovr:{opponent.proj_opp_bp['fd_wpa_b_rp'].mean()}")
    print(f"{opponent.name} bp vr:{opponent.proj_opp_bp['fd_wpa_b_vr'].mean()}")
    print(f"{opponent.name} bp vl:{opponent.proj_opp_bp['fd_wpa_b_vl'].mean()}")
    
    print(len(opponent.used_rp))
    print(lu)
    if extend:
        print(hitters[['name', 'mlb_id', 'fd_wps_pa_vr']])
    return None
    
t = teams.blue_jays
o = teams.yankees 
team_notes(t, o, extend = True)
# t.bullpen[['name', 'status']]

test = t.lineup_df()
test.columns.tolist()

t.hitter[['name', 'mlb_id']]


angels = [457708, 660271, 578428, 665120, 545358, 621493, 594838, 501571, 664058]
blue_jays = [543760, 666182, 665489, 545341, 624415, 666971, 642133, 669289, 624512]