from dfs_tools_mlb.compile import teams

t = teams.diamondbacks
# t.projected_sp()

hitters = t.hitter
lu = t.lineup
# x = t.full_roster
pitchers = t.pitcher

pitchers[['name','fd_wps_b_vr', 'fd_wpa_b_vr','fd_wps_b_vl', 'fd_wpa_b_vl', 'mlb_id']]
hitters[['name', 'mlb_id', 'fd_wps_pa_vr', 'fd_wpa_pa_vr']]

t.first_base[['name', 'bat_side']]
t.bullpen[['name', 'mlb_id']]
t.lineup_df()[['name', 'bat_side']]

t.sp_df()

orioles = [656775, 669720, 641820, 596748, 621466, 663624, 600474, 520471, 547004]
angels = [664058, 660271, 545361, 665120, 578428, 457708, 405395, 594838, 545358]
rays = [668227, 640457, 622534, 664040, 650490, 621563, 572287, 595281, 642715]
cardinals = [669242, 666185, 502671, 571448, 425877, 657557, 641933, 664056, 593372]
mets = [607043, 596019, 643446, 624413, 624424, 542340, 642086, 543510, 612434]
twins = [621439, 518626, 443558, 596146, 666135, 593871, 595909, 641598, 592743]
nationals = [607208, 543281, 475582, 516770, 543228, 656941, 645302, 474568, 452657]
indians = [514917, 656669, 608070, 614177, 592696, 623912, 642708, 644374, 595978]
red_sox = [571771, 657077, 502110, 593428, 646240, 543877, 503556, 592669, 666915]
athletics = [592192, 657656, 621566, 669221, 656305, 476704, 572039, 462101, 643393]
angels = [664058, 660271, 545361, 665120, 578428, 457708, 501571, 594838, 435559]
brewers = [543939, 456715, 596129, 543768, 541645, 598265, 649966, 641924, 605288]
marlins = [500743, 542583, 572816, 594807, 605119, 656371, 621446, 506702, 656548]
giants = [643565, 573262, 457763, 474832, 446334, 543063, 527038, 621453, 622072]
rockies = [606132, 596115, 641857, 453568, 641658, 571431, 624513, 658069, 622608]
padres = [663757, 502054, 592518, 543333, 571976, 630105, 543592, 673490, 659275]

dodgers = [605141, 608369, 457759, 571970, 669257, 621035, 607461, 666158, 628711]
mariners = [571745, 664034, 572122, 641786, 620443, 657108, 664238, 641487, 643290]
diamondbacks = [656976, 668942, 452678, 444482, 519390, 500871, 605113, 662139, 676840]