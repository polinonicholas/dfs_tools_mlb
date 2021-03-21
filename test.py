import statsapi
import pandas as pd
from dfs_tools_mlb import settings
from dfs_tools_mlb.compile.mlb import get_splits_p, get_splits_h
import pandas as pd
# file_path = settings.ARCHIVE_DIR.joinpath('weighted_runs_df.pickle')
# weights = pd.read_pickle(file_path)
# weights = weights[weights['Season'] > 2015]
# weight_bb = weights['wBB'].mean()
# weight_hbp = weights['wHBP'].mean()
# weight_1b = weights['w1B'].mean()
# weight_2b = weights['w2B'].mean()
# weight_3b = weights['w3B'].mean()
# weight_hr = weights['wHR'].mean()
# weight_sb = weights['runSB'].mean()
# weight_cs = weights['runCS'].mean()
# weight_pa = weights['R/PA'].mean()

fd_scoring = {
    'hitters': {
        'base': 3,
        'rbi' : 3.5,
        'run' : 3.2,
        'sb' : 6,
        'hr': 6.7,
        
        },
    'pitchers': {
        'inning': 3,
        'out': 1,
        'k' : 3,
        'er': -3,
        'qs': 4,
        'win': 6
        }
    }

i = fd_scoring['pitchers']['inning']
k = fd_scoring['pitchers']['k']
r = fd_scoring['pitchers']['er']
b = fd_scoring['hitters']['base']
rbi = fd_scoring['hitters']['rbi']
er = fd_scoring['hitters']['run']
s = fd_scoring['hitters']['sb']
hr = fd_scoring['hitters']['hr']
o = fd_scoring['pitchers']['out']

test = get_splits_p([2019,2020,2021])
test_h = get_splits_h([2019,2020,2021])



df = pd.DataFrame(test)
h = pd.DataFrame(test_h)

h.columns.tolist()
h['gb'] = h['gb_vl'] + h['gb_vr']
h['sac_flies'] = h['sac_flies_vl'] + h['sac_flies_vr']
h['sac_bunts'] = h['sac_bunts_vl'] + h['sac_bunts_vr']
h['sacs'] = h['sac_flies'] + h['sac_bunts']
h['rbi'] = h['rbi_vl'] + h['rbi_vr']
h['total_bases'] = h['total_bases_vl'] + h['total_bases_vr']
h['pa'] = h['pa_vl'] + h['pa_vr']
h['total_pitches'] = h['total_pitches_vl'] + h['total_pitches_vr']
h['gidp'] = h['gidp_vl'] + h['gidp_vr']
h['sb'] = h['sb_vl'] + h['sb_vr']
h['cs'] = h['cs_vl'] + h['cs_vr']
h['ab'] = h['ab_vl'] + h['ab_vr']
h['hbp'] = h['hbp_vl'] + h['hbp_vr']
h['hits'] = h['hits_vl'] + h['hits_vr']
h['ibb'] = h['ibb_vl'] + h['ibb_vr']
h['bb'] = h['bb_vl'] + h['bb_vr']
h['k'] = h['k_vl'] + h['k_vr']
h['runs'] = h['runs_vl'] + h['runs_vr']
h['fb'] = h['fb_vl'] + h['fb_vr']
h['sb_pa'] = h['sb'] / h['pa']
h['hr'] = h['hr_vr'] + h['hr_vl']
h['outs-'] = (h['gb'] + h['fb'] + h['cs'] + h['k']) - h['sacs']


h['gb_fb'] = h['gb'] / h['fb']
h['k_pa'] = h['k'] / h['pa']
h['bb_pa'] = h['bb'] / h['pa']
h['k-bb'] = h['k_pa'] - h['bb_pa']
h['runs_pa'] = h['runs'] / h['pa']
h['hits_pa'] = h['hits'] / h['pa']
h['bases+'] = h['total_bases'] + h['bb'] + h['hbp']
h['bases+_pa'] = h['bases+'] / h['pa']
h['pitches_pa'] = h['total_pitches'] / h['pa']
h['sb_pa_vl'] = h['sb_vl'] / h['pa_vl']

h['gb_fb_vr'] = h['gb_vr'] / h['fb_vr']
h['k_pa_vr'] = h['k_vr'] / h['pa_vr']
h['bb_pa_vr'] = h['bb_vr'] / h['pa_vr']
h['k-bb_vr'] = h['k_pa_vr'] - h['bb_pa_vr']
h['runs_pa_vr'] = h['runs_vr'] / h['pa_vr']
h['hits_pa_vr'] = h['hits_vr'] / h['pa_vr']
h['bases+_vr'] = h['total_bases_vr'] + h['bb_vr'] + h['hbp_vr']
h['bases+_pa_vr'] = h['bases+_vr'] / h['pa_vr']
h['pitches_pa_vr'] = h['total_pitches_vr'] / h['pa_vr']
h['sb_pa_vr'] = h['sb_vr'] / h['pa_vr']
h['sacs_vr'] = h['sac_flies_vr'] + h['sac_bunts_vr']
h['outs-_vr'] = (h['gb_vr'] + h['fb_vr'] + h['cs_vr'] + h['k_vr']) - h['sacs_vr']



h['gb_fb_vl'] = h['gb_vl'] / h['fb_vl']
h['k_pa_vl'] = h['k_vl'] / h['pa_vl']
h['bb_pa_vl'] = h['bb_vl'] / h['pa_vl']
h['k-bb_vl'] = h['k_pa_vl'] - h['bb_pa_vl']
h['runs_pa_vl'] = h['runs_vl'] / h['pa_vl']
h['hits_pa_vl'] = h['hits_vl'] / h['pa_vl']
h['bases+_vl'] = h['total_bases_vl'] + h['bb_vl'] + h['hbp_vl']
h['bases+_pa_vl'] = h['bases+_vl'] / h['pa_vl']
h['pitches_pa_vl'] = h['total_pitches_vl'] / h['pa_vl']
h['sb_pa_vl'] = h['sb_vl'] / h['pa_vl']
h['sacs_vl'] = h['sac_flies_vl'] + h['sac_bunts_vl']
h['outs-_vl'] = (h['gb_vl'] + h['fb_vl'] + h['cs_vl'] + h['k_vl']) - h['sacs_vl']


#points/pa - vs. right
h['fd_ps_pa_vr'] = ((h['bases+_vr'] * b) + (h['sb_vr'] * s) + (h['hr_vr'] * hr)) / h['pa_vr']
#points/pa - vs. left
h['fd_ps_pa_vl'] = ((h['bases+_vl'] * b) + (h['sb_vl'] * s) + (h['hr_vl'] * hr)) / h['pa_vl']
#points/pa
h['fd_ps_pa'] = ((h['bases+'] * b) + (h['sb'] * s) + (h['hr'] * hr)) / h['pa']

#points conceded/pa vs. right
h['fd_pa_pa_vr'] = ((h['k_vr'] * k) + (h['outs-_vr'] * o) + (h['hr_vr'] * r)) / h['pa_vr']
#points conceded/pa vs. left
h['fd_pa_pa_vl'] = ((h['k_vl'] * k) + (h['outs-_vl'] * o) + (h['hr_vl'] * r)) / h['pa_vl']
#points conceded/pa
h['fd_pa_pa'] = ((h['k'] * k) + (h['outs-'] * o) + (h['hr'] * r)) / h['pa']

h_q = h[h['pa'] >= h['pa'].median()]
h_q_vr = h[h['pa_vr'] >= h['pa_vr'].median()]
h_q_vl = h[h['pa_vl'] >= h['pa_vl'].median()]

h_q.loc[h_q['fd_pa_pa_vr'].nlargest(50).index, ['name', 'fd_pa_pa_vr', 'outs-_vr', 'hr_vr','k_vr', 'pa_vr']]


# df.fillna(0, inplace = True)
if not df.get('games_sp_21'):
    df['games_sp_21'] = 0
    df['games_21'] = 0
    df['wins_21'] = 0
    df['losses_21'] = 0
    df['saves_21'] = 0
    df['save_chances_21'] = 0
    df['holds_21'] = 0
    df['complete_games_21'] = 0
    df['shutouts_21'] = 0
df['games_sp'] = df['games_sp_19'] + df['games_sp_20'] + df['games_sp_21']
df['games'] = df['games_19'] + df['games_20'] + df['games_21']
df['games_rp'] = df['games'] - df['games_sp']
df['wins'] = df['wins_19'] + df['wins_20'] + df['wins_21']
df['losses'] = df['losses_19'] + df['losses_20'] + df['losses_21']
df['saves'] = df['saves_19'] + df['saves_20'] + df['saves_21']
df['holds'] = df['holds_19'] + df['holds_20'] + df['holds_21']
df['complete_games'] = df['complete_games_19'] + df['complete_games_20'] + df['complete_games_21']
df['shutouts'] = df['shutouts_19'] + df['shutouts_20'] + df['shutouts_21']
df['pickoffs'] = df['pickoffs_vr'] + df['pickoffs_vl']
df['gidp'] = df['gidp_vr'] + df['gidp_vl']
df['hits'] = df['hits_vr'] + df['hits_vl']
df['k'] = df['k_vr'] + df['k_vl']
df['bb'] = df['bb_vr'] + df['bb_vl']
df['ibb'] = df['ibb_vr'] + df['ibb_vl']
df['runs'] = df['runs_vr'] + df['runs_vl']
df['fb'] = df['fb_vr'] + df['fb_vl']
df['gb'] = df['gb_vr'] + df['gb_vl']
df['ab'] = df['ab_vr'] + df['ab_vl']
df['sb'] = df['sb_vr'] + df['sb_vl']
df['cs'] = df['cs_vr'] + df['cs_vl']
df['balls'] = df['balls_vr'] + df['balls_vl']
df['strikes'] = df['strikes_vr'] + df['strikes_vl']
df['balks'] = df['balks_vr'] + df['balks_vl']
df['wild_pitches'] = df['wild_pitches_vr'] + df['wild_pitches_vl']
df['total_bases'] = df['total_bases_vr'] + df['total_bases_vl']
df['total_pitches'] = df['total_pitches_vr'] + df['total_pitches_vl']
df['hb'] = df['hb_vr'] + df['hb_vl']
df['outs'] = df['outs_vr'] + df['outs_vl']
df['ip'] = df['outs'] / 3
df['batters_faced'] = df['batters_faced_vr'] + df['batters_faced_vl']
df['er'] = df['er_vr'] + df['er_vl']
df['cs+'] = df['cs'] + df['pickoffs']
df['sb+'] = df['sb'] + df['wild_pitches'] + df['balks']
df['cs_sb'] = df['cs'] / df['sb']
df['cs+_sb+'] = df['cs+'] /df['sb+']
df['bases+'] = df['total_bases'] + df['bb'] + df['hb']

df['pitches_inning'] = df['total_pitches'] / (df['outs'] / 3)
df['pitches_batter'] = df['total_pitches'] / df['batters_faced']
df['gb_fb'] = df['gb'] / df['fb']
df['k_b'] = df['k'] / df['batters_faced']
df['bb_b'] = df['bb'] / df['batters_faced']
df['k-bb'] = df['k_b'] - df['bb_b']
df['runners_allowed'] = df['hits'] + df['bb'] + df['hb']
df['dp_ra'] = df['gidp'] / df['runners_allowed']
df['strike_ball'] = df['strikes'] / df['balls']

df['pitches_start'] = df['total_pitches_sp'] / df['games_sp']
df['innings_start'] = (df['outs_sp'] / 3) / (df['games_sp'])
df['batters_start'] = df['batters_faced_sp'] / df['games_sp']
df['er_start'] = df['er_sp'] / df['games_sp']



df['ppi_sp'] = df['total_pitches_sp'] / (df['outs_sp'] / 3)
df['ppb_sp'] = df['total_pitches_sp'] / df['batters_faced_sp']
df['gb_fb_sp'] = df['gb_sp'] / df['fb_sp']
df['k_b_sp'] = df['k_sp'] / df['batters_faced_sp']
df['bb_b_sp'] = df['bb_sp'] / df['batters_faced_sp']
df['k-bb_sp'] = df['k_b_sp'] - df['bb_b_sp']
df['runners_allowed_sp'] = df['hits_sp'] + df['bb_sp'] + df['hb_sp']
df['dp_ro_sp'] = df['gidp_sp'] / df['runners_allowed_sp']
df['strike_ball_sp'] = df['strikes_sp'] / df['balls_sp']
df['ip_sp'] = df['outs_sp'] / 3
df['bases+_sp'] = df['total_bases_sp'] + df['bb_sp'] + df['hb_sp']



df['ppi_rp'] = df['total_pitches_rp'] / (df['outs_rp'] / 3)
df['ppb_rp'] = df['total_pitches_rp'] / df['batters_faced_rp']
df['gb_fb_rp'] = df['gb_rp'] / df['fb_rp']
df['k_b_rp'] = df['k_rp'] / df['batters_faced_rp']
df['bb_b_rp'] = df['bb_rp'] / df['batters_faced_rp']
df['k-bb_rp'] = df['k_b_rp'] - df['bb_b_rp']
df['runners_allowed_rp'] = df['hits_rp'] + df['bb_rp'] + df['hb_rp']
df['dp_ro_rp'] = df['gidp_rp'] / df['runners_allowed_rp']
df['strike_ball_rp'] = df['strikes_rp'] / df['balls_rp']
df['ip_rp'] = df['outs_rp'] / 3
df['bases+_rp'] = df['total_bases_rp'] + df['bb_rp'] + df['hb_rp']


df['ppi_vr'] = df['total_pitches_vr'] / (df['outs_vr'] / 3)
df['ppb_vr'] = df['total_pitches_vr'] / df['batters_faced_vr']
df['gb_fb_vr'] = df['gb_vr'] / df['fb_vr']
df['k_b_vr'] = df['k_vr'] / df['batters_faced_vr']
df['bb_b_vr'] = df['bb_vr'] / df['batters_faced_vr']
df['k-bb_vr'] = df['k_b_vr'] - df['bb_b_vr']
df['runners_allowed_vr'] = df['hits_vr'] + df['bb_vr'] + df['hb_vr']
df['dp_ro_vr'] = df['gidp_vr'] / df['runners_allowed_vr']
df['strike_ball_vr'] = df['strikes_vr'] / df['balls_vr']
df['ip_vr'] = df['outs_vr'] / 3
df['bases+_vr'] = df['total_bases_vr'] + df['bb_vr'] + df['hb_vr']

df['ppi_vl'] = df['total_pitches_vl'] / (df['outs_vl'] / 3)
df['ppb_vl'] = df['total_pitches_vl'] / df['batters_faced_vl']
df['gb_fb_vl'] = df['gb_vl'] / df['fb_vl']
df['k_b_vl'] = df['k_vl'] / df['batters_faced_vl']
df['bb_b_vl'] = df['bb_vl'] / df['batters_faced_vl']
df['k-bb_vl'] = df['k_b_vl'] - df['bb_b_vl']
df['runners_allowed_vl'] = df['hits_vl'] + df['bb_vl'] + df['hb_vl']
df['dp_ro_vl'] = df['gidp_vl'] / df['runners_allowed_vl']
df['strike_ball_vl'] = df['strikes_vl'] / df['balls_vl']  
df['ip_vl'] = df['outs_vl'] / 3
df['bases+_vl'] = df['total_bases_vl'] + df['bb_vl'] + df['hb_vl'] 




#points scored/batter faced - in starts
df['fd_ps_b_sp'] = ((df['ip_sp'] * i) + (df['k_sp'] * k) + (df['er_sp'] * r)) / df['batters_faced_sp']
#points scored/start
df['fd_ps_s'] = (df['fd_ppb_sp'] * df['batters_faced_sp']) / df['games_sp']
# points scored/left-handed batter faced
df['fd_ps_b_vl'] = ((df['ip_vl'] * i) + (df['k_vl'] * k) + (df['er_vl'] * r)) / df['batters_faced_vl']
#points score/right-handed batter faced
df['fd_ps_b_vr'] = ((df['ip_vr'] * i) + (df['k_vr'] * k) + (df['er_vr'] * r)) / df['batters_faced_vr']


#points allowed/start
df['fd_pa_s'] = ((df['bases+_sp'] * b) + (df['er_sp'] * (er + rbi)) + (df['sb_sp'] * s)) / df['games_sp']
#points allowed/batter faced - in starts
df['fd_pa_b_sp'] = ((df['bases+_sp'] * b) + (df['er_sp'] * (er + rbi)) + (df['sb_sp'] * s)) / df['batters_faced_sp']
#total points allowed - in starts
df['fd_pa_sp'] = ((df['bases+_sp'] * b) + (df['er_sp'] * (er + rbi)) + (df['sb_sp'] * s))
#points allowed/batter faced - in relief
df['fd_pa_b_rp'] = ((df['bases+_rp'] * b) + (df['er_rp'] * (er + rbi)) + (df['sb_rp'] * s)) / df['batters_faced_rp']
#points allowed/relief appearence
df['fd_pa_ra'] = ((df['bases+_rp'] * b) + (df['er_rp'] * (er + rbi)) + (df['sb_rp'] * s)) / df['games_rp']
#total points allowed - in relief
df['fd_pa_rp'] = ((df['bases+_rp'] * b) + (df['er_rp'] * (er + rbi)) + (df['sb_rp'] * s))
#total points allowed
df['fd_pa'] = ((df['bases+'] * b) + (df['er'] * (er + rbi)) + (df['sb'] * s))
#points allowed/ right-handed batter faced
df['fd_pa_b_vr'] = ((df['bases+_vr'] * b) + (df['er_vr'] * (er + rbi)) + (df['sb_vr'] * s)) / df['batters_faced_vr']
#points allowed/ left-handed batter faced
df['fd_pa_b_vl'] = ((df['bases+_vl'] * b) + (df['er_vl'] * (er + rbi)) + (df['sb_vl'] * s)) / df['batters_faced_vl']

q_rp = df[df['batters_faced_rp'] >= df['batters_faced_rp'].median()]
q_sp = df[df['batters_faced_sp'] >= df['batters_faced_sp'].median()]
q_vl = df[df['batters_faced_vl'] >= df['batters_faced_vl'].median()]
q_vr = df[df['batters_faced_vr'] >= df['batters_faced_vr'].median()]




q_vl.loc[q_vl['fd_ps_b_vl'].nlargest(50).index, ['fd_ps_b_vl', 'fd_pa_b_vl','batters_faced_vl','name']]



