from dfs_tools_mlb.compile import teams
import pandas
pandas.set_option('display.max_columns', None)

t = teams.red_sox
# t.projected_sp()
hitters = t.hitter
lu = t.lineup
# x = t.full_roster
pitchers = t.pitcher
pitchers[['name', 'pitches_start']]
pitchers[['name','fd_wps_b_vr', 'fd_wpa_b_vr','fd_wps_b_vl', 'fd_wpa_b_vl', 'mlb_id']]
hitters[['name', 'mlb_id', 'fd_wps_pa_vr', 'fd_wpa_pa_vr']]
hitters[['name', 'mlb_id', 'fd_wps_pa_vl', 'ab_vl']]
hitters[['name', 'mlb_id', 'fd_wps_pa_vr', 'ab_vr']]

t.lineup_df()[['name', 'fd_wps_pa_vr', 'fd_wpa_pa_vr', 'pa_vr', 'position']]
t.lineup_df()[['name', 'fd_wps_pa_vl', 'ab_vl']]
hitters.columns.tolist()

t.first_base[['name', 'bat_side']]
t.bullpen[['name', 'mlb_id']]
t.lineup_df()[['name', 'bat_side', 'points']]


t.sp_df().columns.tolist()
t.sp_df()[['name','k_b', 'points', 'ump_points', 'venue_points', 'temp_points', 'pitches_start']]
t.weather

t.shorstop

orioles = [656775, 669720, 641820, 596748, 621466, 663624, 600474, 520471, 547004]

rays = [668227, 640457, 622534, 664040, 650490, 621563, 572287, 595281, 642715]
cardinals = [669242, 666185, 502671, 571448, 425877, 657557, 641933, 664056, 593372]
mets = [607043, 596019, 643446, 624413, 624424, 542340, 642086, 543510, 612434]
twins = [650333, 518626, 443558, 596146, 593871, 663616, 641598, 593934, 592743]
nationals = [607208, 543281, 475582, 516770, 543228, 656941, 645302, 474568, 452657]


athletics = [592192, 657656, 621566, 669221, 656305, 476704, 572039, 462101, 643393]
angels = [664058, 660271, 545361, 665120, 578428, 457708, 501571, 594838, 435559]
brewers = [543939, 456715, 596129, 543768, 541645, 598265, 649966, 641924, 605288]
marlins = [500743, 542583, 572816, 594807, 605119, 656371, 506702, 642423, 656548]
giants = [643565, 573262, 457763, 474832, 446334, 543063, 527038, 621453, 622072]
rockies = [606132, 656582, 453568, 596115, 641857, 658069, 641658, 624513, 592351]
padres = [663757, 592518, 630105, 502054, 657434, 672779, 673490, 605170, 506433]

dodgers = [605141, 571970, 457759, 669257, 621035, 641914, 670042, 656847, 621111]
mariners = [641487, 571745, 641786, 572122, 664034, 620443, 664238, 657108, 672284]
diamondbacks = [656976, 668942, 452678, 444482, 519390, 500871, 605113, 662139, 676840]

blue_jays = [543760, 666182, 665489, 606192, 545341, 624415, 666971, 642133, 643376]

