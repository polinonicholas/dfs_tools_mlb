from dfs_tools_mlb.compile import teams
import pandas
pandas.set_option('display.max_columns', None)
# test = 'home'
t = teams.rockies
o = teams.mets 
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
print(f"{p_name} is {p_hand}")
print(pitcher[['name','fd_wpa_b_vr', 'fd_wpa_b_vl', 'batters_faced_vl', 'pitches_start']])
if p_hand == 'R':
    print(t.lineup_df()[['name', 'fd_wps_pa_vr', 'pa_vr', 'fd_hr_weight_vr', 'bat_side']])

elif p_hand == 'L':
    print(t.lineup_df()[['name', 'fd_wps_pa_vl', 'pa_vl', 'fd_hr_weight_vl', 'bat_side']])
else:
    print(t.lineup_df()[['name', 'fd_wps_pa', 'pa', 'fd_hr_weight', 'bat_side']])

print(f"{o.name} bp ovr:{o.proj_opp_bp['fd_wpa_b_rp'].mean()}")
print(f"{o.name} bp vr:{o.proj_opp_bp['fd_wpa_b_vr'].mean()}")
print(f"{o.name} bp vl:{o.proj_opp_bp['fd_wpa_b_vl'].mean()}")
print(len(o.used_rp))

hitters[['name', 'mlb_id', 'fd_wps_pa_vl']]
o.sp_df()[['name','k_b', 'points', 'ump_points', 'venue_points', 'temp_points', 'pitches_start', 'mlb_id']]





orioles = [656775, 669720, 641820, 596748, 621466, 663624, 600474, 520471, 547004]

rays = [668227, 640457, 622534, 664040, 650490, 621563, 572287, 595281, 642715]
cardinals = [669242, 666185, 502671, 571448, 425877, 657557, 641933, 664056, 593372]
mets = [542340, 596019, 457727, 642086, 543510, 606299, 641561, 666137, 656849]
twins = [650333, 518626, 443558, 596146, 593871, 663616, 641598, 593934, 592743]
nationals = [607208, 665742, 605137, 656941, 516770, 543281, 543228, 453286, 645302]


athletics = [592192, 640461, 657656, 621566, 476704, 656305, 669221, 572039, 462101]
angels = [621493, 660271, 543685, 665120, 457708, 501571, 578428, 435559, 664058]
brewers = [543939, 456715, 596129, 543768, 541645, 598265, 649966, 641924, 605288]
marlins = [500743, 542583, 572816, 594807, 605119, 656371, 506702, 642423, 656548]
giants = [643565, 573262, 457763, 474832, 543105, 446334, 543063, 456781, 622072]
rockies = [606132, 656582, 453568, 596115, 641857, 658069, 641658, 624513, 592351]
padres = [663757, 592518, 630105, 502054, 657434, 672779, 673490, 605170, 506433]


mariners = [672284, 571745, 572122, 641786, 641487, 608596, 641924, 664059, 622268]

blue_jays = [543760, 666182, 665489, 606192, 545341,624415 , 666971, 642133, 643376]

cubs = [592626, 592178, 595879, 519203, 575929, 664023, 518792, 663538, 592866]
yankees = [518934, 572228, 592450, 570482, 650402, 596142, 640449, 609280, 458731]


dodgers = [605141, 457759, 571970, 405395, 669257, 621035, 641914, 666158, 545333]

diamondbacks = [656976, 606466, 500871, 444482, 642736, 605113, 519390, 605113, 518876]


red_sox = [542454, 657077, 502110, 593428, 646240, 543877, 503556, 571771, 527048]
