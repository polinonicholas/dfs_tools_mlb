from dfs_tools_mlb import settings
from dfs_tools_mlb.compile.stats_mlb import get_splits_h, get_splits_p
from dfs_tools_mlb.utils.storage import clean_directory
import pandas as pd
from dfs_tools_mlb.utils.time import time_frames as tf
import pickle
from dfs_tools_mlb.compile.static_mlb import mlb_api_codes as mac

data_path_h = settings.STORAGE_DIR.joinpath(f'player_data_h_{tf.today}.pickle')
data_path_p = settings.STORAGE_DIR.joinpath(f'player_data_p_{tf.today}.pickle')
if not data_path_h.exists() or not data_path_p.exists():
    fd_scoring = {
    'hitters': {
        'base': 3,
        'rbi' : 3.5,
        'run' : 3.2,
        'sb' : 6,
        'hr': 18.7,
        
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
    er = fd_scoring['pitchers']['er']
    b = fd_scoring['hitters']['base']
    rbi = fd_scoring['hitters']['rbi']
    r = fd_scoring['hitters']['run']
    s = fd_scoring['hitters']['sb']
    hr = fd_scoring['hitters']['hr']
    o = fd_scoring['pitchers']['out']
    x = (r + rbi) / 2
    
    weights_path = settings.ARCHIVE_DIR.joinpath('weighted_runs_df.pickle')
    weights = pd.read_pickle(weights_path)
    weights = weights[weights['Season'] > 2015]
    weight_bb = weights['wBB'].mean()
    weight_hbp = weights['wHBP'].mean()
    weight_1b = weights['w1B'].mean()
    weight_2b = weights['w2B'].mean()
    weight_3b = weights['w3B'].mean()
    weight_hr = weights['wHR'].mean()
    weight_sb = weights['runSB'].mean()
    weight_cs = weights['runCS'].mean()
    weight_pa = weights['R/PA'].mean()
if not data_path_h.exists():
    h_splits = pd.DataFrame(get_splits_h(range(settings.stat_range['player_start'], settings.stat_range['end']),sport=1,pool='ALL',get_all=True))
    
    h_splits['1b_vl'] = h_splits['hits_vl'] - (h_splits['hr_vl'] + h_splits['3b_vl'] + h_splits['2b_vl'])
    h_splits['1b_vr'] = h_splits['hits_vr'] - (h_splits['hr_vr'] + h_splits['3b_vr'] + h_splits['2b_vr'])
    h_splits['1b'] = h_splits['1b_vl'] + h_splits['1b_vr']
    h_splits['2b'] = h_splits['2b_vl'] + h_splits['2b_vr']
    h_splits['3b'] = h_splits['3b_vl'] + h_splits['3b_vr']
    
    h_splits['gb'] = h_splits['gb_vl'] + h_splits['gb_vr']
    h_splits['fb'] = h_splits['fb_vl'] + h_splits['fb_vr']
    h_splits['sac_flies'] = h_splits['sac_flies_vl'] + h_splits['sac_flies_vr']
    h_splits['sac_bunts'] = h_splits['sac_bunts_vl'] + h_splits['sac_bunts_vr']
    h_splits['sacs'] = h_splits['sac_flies'] + h_splits['sac_bunts']
    h_splits['rbi'] = h_splits['rbi_vl'] + h_splits['rbi_vr']
    h_splits['total_bases'] = h_splits['total_bases_vl'] + h_splits['total_bases_vr']
    h_splits['pa'] = h_splits['pa_vl'] + h_splits['pa_vr']
    h_splits['total_pitches'] = h_splits['total_pitches_vl'] + h_splits['total_pitches_vr']
    h_splits['gidp'] = h_splits['gidp_vl'] + h_splits['gidp_vr']
    h_splits['sb'] = h_splits['sb_vl'] + h_splits['sb_vr']
    h_splits['cs'] = h_splits['cs_vl'] + h_splits['cs_vr']
    h_splits['ab'] = h_splits['ab_vl'] + h_splits['ab_vr']
    h_splits['hbp'] = h_splits['hbp_vl'] + h_splits['hbp_vr']
    h_splits['hits'] = h_splits['hits_vl'] + h_splits['hits_vr']
    h_splits['ibb'] = h_splits['ibb_vl'] + h_splits['ibb_vr']
    h_splits['bb'] = h_splits['bb_vl'] + h_splits['bb_vr']
    h_splits['k'] = h_splits['k_vl'] + h_splits['k_vr']
    h_splits['runs'] = h_splits['runs_vl'] + h_splits['runs_vr']
    h_splits['sb_pa'] = h_splits['sb'] / h_splits['pa']
    h_splits['hr'] = h_splits['hr_vr'] + h_splits['hr_vl']
    h_splits['outs-'] = (h_splits['gb'] + h_splits['fb'] + h_splits['cs'] + h_splits['k'] + h_splits['gidp']) - h_splits['sacs']
    h_splits['gb_fb'] = h_splits['gb'] / h_splits['fb']
    h_splits['k_pa'] = h_splits['k'] / h_splits['pa']
    h_splits['bb_pa'] = h_splits['bb'] / h_splits['pa']
    h_splits['k-bb'] = h_splits['k_pa'] - h_splits['bb_pa']
    h_splits['runs_pa'] = h_splits['runs'] / h_splits['pa']
    h_splits['hits_pa'] = h_splits['hits'] / h_splits['pa']
    h_splits['bases+'] = h_splits['total_bases'] + h_splits['bb'] + h_splits['hbp']
    h_splits['bases+_pa'] = h_splits['bases+'] / h_splits['pa']
    h_splits['pitches_pa'] = h_splits['total_pitches'] / h_splits['pa']
    
    h_splits['gb_fb_vr'] = h_splits['gb_vr'] / h_splits['fb_vr']
    h_splits['k_pa_vr'] = h_splits['k_vr'] / h_splits['pa_vr']
    h_splits['bb_pa_vr'] = h_splits['bb_vr'] / h_splits['pa_vr']
    h_splits['k-bb_vr'] = h_splits['k_pa_vr'] - h_splits['bb_pa_vr']
    h_splits['runs_pa_vr'] = h_splits['runs_vr'] / h_splits['pa_vr']
    h_splits['hits_pa_vr'] = h_splits['hits_vr'] / h_splits['pa_vr']
    h_splits['bases+_vr'] = h_splits['total_bases_vr'] + h_splits['bb_vr'] + h_splits['hbp_vr']
    h_splits['bases+_pa_vr'] = h_splits['bases+_vr'] / h_splits['pa_vr']
    h_splits['pitches_pa_vr'] = h_splits['total_pitches_vr'] / h_splits['pa_vr']
    h_splits['sb_pa_vr'] = h_splits['sb_vr'] / h_splits['pa_vr']
    h_splits['sacs_vr'] = h_splits['sac_flies_vr'] + h_splits['sac_bunts_vr']
    h_splits['outs-_vr'] = (h_splits['gb_vr'] + h_splits['fb_vr'] + h_splits['cs_vr'] + h_splits['k_vr'] + h_splits['gidp_vr']) - h_splits['sacs_vr']
    
    h_splits['gb_fb_vl'] = h_splits['gb_vl'] / h_splits['fb_vl']
    h_splits['k_pa_vl'] = h_splits['k_vl'] / h_splits['pa_vl']
    h_splits['bb_pa_vl'] = h_splits['bb_vl'] / h_splits['pa_vl']
    h_splits['k-bb_vl'] = h_splits['k_pa_vl'] - h_splits['bb_pa_vl']
    h_splits['runs_pa_vl'] = h_splits['runs_vl'] / h_splits['pa_vl']
    h_splits['hits_pa_vl'] = h_splits['hits_vl'] / h_splits['pa_vl']
    h_splits['bases+_vl'] = h_splits['total_bases_vl'] + h_splits['bb_vl'] + h_splits['hbp_vl']
    h_splits['bases+_pa_vl'] = h_splits['bases+_vl'] / h_splits['pa_vl']
    h_splits['pitches_pa_vl'] = h_splits['total_pitches_vl'] / h_splits['pa_vl']
    h_splits['sb_pa_vl'] = h_splits['sb_vl'] / h_splits['pa_vl']
    h_splits['sacs_vl'] = h_splits['sac_flies_vl'] + h_splits['sac_bunts_vl']
    h_splits['outs-_vl'] = (h_splits['gb_vl'] + h_splits['fb_vl'] + h_splits['cs_vl'] + h_splits['k_vl'] + h_splits['gidp_vl']) - h_splits['sacs_vl']
    
    #weight points scored/pa by type.
    h_splits['fd_bb_weight_vr'] = ((h_splits['bb_vr'] * b) + ((h_splits['bb_vr'] * weight_bb) * r)) / h_splits['pa_vr']
    h_splits['fd_hbp_weight_vr'] = ((h_splits['hbp_vr'] * b) + ((h_splits['hbp_vr'] * weight_hbp) * x)) / h_splits['pa_vr']
    h_splits['fd_1b_weight_vr'] = ((h_splits['1b_vr'] * b) + ((h_splits['1b_vr'] * weight_1b) * x)) / h_splits['pa_vr']
    h_splits['fd_2b_weight_vr'] = ((h_splits['2b_vr'] * (b * 2)) + ((h_splits['2b_vr'] * weight_2b) * x)) / h_splits['pa_vr']
    h_splits['fd_3b_weight_vr'] = ((h_splits['3b_vr'] * (b * 3)) + ((h_splits['3b_vr'] * weight_3b) * x)) / h_splits['pa_vr']
    h_splits['fd_hr_weight_vr'] = ((h_splits['hr_vr'] * 18.7) + ((h_splits['hr_vr'] * (weight_hr - 1)) * rbi)) / h_splits['pa_vr']
    h_splits['fd_sb_weight_vr'] = ((h_splits['sb_vr'] * s) + ((h_splits['sb_vr'] * weight_sb) * r)) / h_splits['pa_vr']
    #weighted points/pa - vs. right
    h_splits['fd_wps_pa_vr'] = h_splits['fd_bb_weight_vr'] + h_splits['fd_hbp_weight_vr'] + \
        h_splits['fd_1b_weight_vr'] + h_splits['fd_2b_weight_vr'] + h_splits['fd_3b_weight_vr'] + \
            h_splits['fd_hr_weight_vr'] + h_splits['fd_sb_weight_vr']
            
    h_splits['fd_bb_weight_vl'] = ((h_splits['bb_vl'] * b) + ((h_splits['bb_vl'] * weight_bb) * r)) / h_splits['pa_vl']
    h_splits['fd_hbp_weight_vl'] = ((h_splits['hbp_vl'] * b) + ((h_splits['hbp_vl'] * weight_hbp) * x)) / h_splits['pa_vl']
    h_splits['fd_1b_weight_vl'] = ((h_splits['1b_vl'] * b) + ((h_splits['1b_vl'] * weight_1b) * x)) / h_splits['pa_vl']
    h_splits['fd_2b_weight_vl'] = ((h_splits['2b_vl'] * (b * 2)) + ((h_splits['2b_vl'] * weight_2b) * x)) / h_splits['pa_vl']
    h_splits['fd_3b_weight_vl'] = ((h_splits['3b_vl'] * (b * 3)) + ((h_splits['3b_vl'] * weight_3b) * x)) / h_splits['pa_vl']
    h_splits['fd_hr_weight_vl'] = ((h_splits['hr_vl'] * 18.7) + ((h_splits['hr_vl'] * (weight_hr - 1)) * rbi)) / h_splits['pa_vl']
    h_splits['fd_sb_weight_vl'] = ((h_splits['sb_vl'] * s) + ((h_splits['sb_vl'] * weight_sb) * r)) / h_splits['pa_vl']
    #weighted points/pa - vs. left
    h_splits['fd_wps_pa_vl'] = h_splits['fd_bb_weight_vl'] + h_splits['fd_hbp_weight_vl'] + \
        h_splits['fd_1b_weight_vl'] + h_splits['fd_2b_weight_vl'] + h_splits['fd_3b_weight_vl'] + \
            h_splits['fd_hr_weight_vl'] + h_splits['fd_sb_weight_vl']
    
    h_splits['fd_bb_weight'] = ((h_splits['bb'] * b) + ((h_splits['bb'] * weight_bb) * r)) / h_splits['pa']
    h_splits['fd_hbp_weight'] = ((h_splits['hbp'] * b) + ((h_splits['hbp'] * weight_hbp) * x)) / h_splits['pa']
    h_splits['fd_1b_weight'] = ((h_splits['1b'] * b) + ((h_splits['1b'] * weight_1b) * x)) / h_splits['pa']
    h_splits['fd_2b_weight'] = ((h_splits['2b'] * (b * 2)) + ((h_splits['2b'] * weight_2b) * x)) / h_splits['pa']
    h_splits['fd_3b_weight'] = ((h_splits['3b'] * (b * 3)) + ((h_splits['3b'] * weight_3b) * x)) / h_splits['pa']
    h_splits['fd_hr_weight'] = ((h_splits['hr'] * 18.7) + ((h_splits['hr'] * (weight_hr - 1)) * rbi)) / h_splits['pa']
    h_splits['fd_sb_weight'] = ((h_splits['sb'] * s) + ((h_splits['sb'] * weight_sb) * r)) / h_splits['pa']
    #weighted points/pa - total
    h_splits['fd_wps_pa'] = h_splits['fd_bb_weight'] + h_splits['fd_hbp_weight'] + \
        h_splits['fd_1b_weight'] + h_splits['fd_2b_weight'] + h_splits['fd_3b_weight'] + \
            h_splits['fd_hr_weight'] + h_splits['fd_sb_weight']
    
    #points/pa - vs. right
    h_splits['fd_ps_pa_vr'] = ((h_splits['bases+_vr'] * b) + (h_splits['sb_vr'] * s) + (h_splits['hr_vr'] * (r + rbi))) / h_splits['pa_vr']
    #points/pa - vs. left
    h_splits['fd_ps_pa_vl'] = ((h_splits['bases+_vl'] * b) + (h_splits['sb_vl'] * s) + (h_splits['hr_vl'] * (r + rbi))) / h_splits['pa_vl']
    #points/pa
    h_splits['fd_ps_pa'] = ((h_splits['bases+'] * b) + (h_splits['sb'] * s) + (h_splits['hr'] * (r + rbi))) / h_splits['pa']
    #weighted points conceded/pa - right
    h_splits['fd_wpa_pa_vr'] = ((((h_splits['k_vr'] * k) + (h_splits['outs-_vr'] * o)) +\
        ((h_splits['hr_vr'] * weight_hr) * er) + ((h_splits['3b_vr'] * weight_3b) * er) + \
            ((h_splits['2b_vr'] * weight_2b) * er) +  ((h_splits['1b_vr'] * weight_1b) * er) + \
                ((h_splits['bb_vr'] * weight_bb) * er) + ((h_splits['hbp_vr'] * weight_hbp) * er) + \
                    ((h_splits['sb_vr'] * weight_sb) * er)) / h_splits['pa_vr'])
    #weighted points conceded/pa - left
    h_splits['fd_wpa_pa_vl'] = ((((h_splits['k_vl'] * k) + (h_splits['outs-_vl'] * o)) +\
        ((h_splits['hr_vl'] * weight_hr) * er) + ((h_splits['3b_vl'] * weight_3b) * er) + \
            ((h_splits['2b_vl'] * weight_2b) * er) +  ((h_splits['1b_vl'] * weight_1b) * er) + \
                ((h_splits['bb_vl'] * weight_bb) * er) + ((h_splits['hbp_vl'] * weight_hbp) * er) + \
                    ((h_splits['sb_vl'] * weight_sb) * er)) / h_splits['pa_vl'])
    #weighted points conceded/pa
    h_splits['fd_wpa_pa'] = ((((h_splits['k'] * k) + (h_splits['outs-'] * o)) +\
        ((h_splits['hr'] * weight_hr) * er) + ((h_splits['3b'] * weight_3b) * er) + \
            ((h_splits['2b'] * weight_2b) * er) +  ((h_splits['1b'] * weight_1b) * er) + \
                ((h_splits['bb'] * weight_bb) * er) + ((h_splits['hbp'] * weight_hbp) * er) + \
                    ((h_splits['sb'] * weight_sb) * er)) / h_splits['pa'])
    
    
    
    #points conceded/pa vs. right
    h_splits['fd_pa_pa_vr'] = ((h_splits['k_vr'] * k) + (h_splits['outs-_vr'] * o) + (h_splits['hr_vr'] * er)) / h_splits['pa_vr']
    #points conceded/pa vs. left
    h_splits['fd_pa_pa_vl'] = ((h_splits['k_vl'] * k) + (h_splits['outs-_vl'] * o) + (h_splits['hr_vl'] * er)) / h_splits['pa_vl']
    #points conceded/pa
    h_splits['fd_pa_pa'] = ((h_splits['k'] * k) + (h_splits['outs-'] * o) + (h_splits['hr'] * er)) / h_splits['pa']
    
    #mark platoon players
    vr_vl_pa_ratio = h_splits['pa_vr'].sum() / h_splits['pa_vl'].sum()
    vl_vr_pa_ratio = h_splits['pa_vl'].sum() / h_splits['pa_vr'].sum()
    platoon_filt = (((h_splits['pa_vr'] / h_splits['pa_vl'] > (vr_vl_pa_ratio * 2)) | (h_splits['pa_vl'] / h_splits['pa_vr'] > (vl_vr_pa_ratio * 2))) & (h_splits['pa'] > h_splits['pa'].median()))
    h_splits.loc[platoon_filt, 'is_platoon'] = True
    h_splits[h_splits['is_platoon'] == True]

    with open(data_path_h, "wb") as file:
        pickle.dump(h_splits, file)
    clean_directory(settings.STORAGE_DIR)
else:
    h_splits = pd.read_pickle(data_path_h)
h_q = h_splits[h_splits['pa'] >= h_splits['pa'].mean()]
h_q_vr = h_splits[h_splits['pa_vr'] >= h_splits['pa_vr'].mean()]
h_q_vl = h_splits[h_splits['pa_vl'] >= h_splits['pa_vl'].mean()]

h_r_filt = (h_splits['bat_side'] == 'R')
h_l_filt = (h_splits['bat_side'] == 'L')
hq_r_filt = (h_splits['pa_vr'] >= h_splits['pa_vr'].mean())
hq_l_filt = (h_splits['pa_vl'] >= h_splits['pa_vl'].mean())

h_l_vl = h_splits[h_l_filt & hq_l_filt]
h_r_vl = h_splits[h_r_filt & hq_l_filt]
h_r_vr = h_splits[h_r_filt & hq_r_filt]
h_l_vr = h_splits[h_l_filt & hq_r_filt]

hp_filt = (h_splits['position_code'].isin(mac.players.p))
hp = h_splits[hp_filt]
hp_q_filt = (hp['pa'] >= hp['pa'].mean())
hp_q = hp[hp_q_filt]



if not data_path_p.exists():

    p_splits = pd.DataFrame(get_splits_p(range(settings.stat_range['player_start'], settings.stat_range['end']),sport=1,pool='ALL',get_all=True))
    
    
    values = {'games_sp_21': 0, 'games_21': 0, 'wins_21': 0, 'losses_21': 0, 'saves_21': 0, 
              'save_chances_21': 0, 'holds_21': 0, 'complete_games_21': 0, 'shutouts_21': 0,
              'games_sp_19': 0, 'games_19': 0, 'wins_19': 0, 'losses_19': 0, 'saves_19': 0, 
              'save_chances_19': 0, 'holds_19': 0, 'complete_games_19': 0, 'shutouts_19': 0,
              'games_sp_20': 0, 'games_20': 0, 'wins_20': 0, 'losses_20': 0, 'saves_20': 0, 
              'save_chances_20': 0, 'holds_20': 0, 'complete_games_20': 0, 'shutouts_20': 0}
    p_splits.fillna(value=values, inplace=True)
    
        
    
    p_splits['1b_vl'] = p_splits['hits_vl'] - (p_splits['hr_vl'] + p_splits['3b_vl'] + p_splits['2b_vl'])
    p_splits['1b_vr'] = p_splits['hits_vr'] - (p_splits['hr_vr'] + p_splits['3b_vr'] + p_splits['2b_vr'])
    p_splits['1b_sp'] = p_splits['hits_sp'] - (p_splits['hr_sp'] + p_splits['3b_sp'] + p_splits['2b_sp'])
    p_splits['1b_rp'] = p_splits['hits_rp'] - (p_splits['hr_rp'] + p_splits['3b_rp'] + p_splits['2b_rp'])
    p_splits['1b'] = p_splits['1b_vl'] + p_splits['1b_vr']
    p_splits['2b'] = p_splits['2b_vl'] + p_splits['2b_vr']
    p_splits['3b'] = p_splits['3b_vl'] + p_splits['3b_vr']
    p_splits['hr'] = p_splits['hr_vl'] + p_splits['hr_vr']
    p_splits['games_sp'] = p_splits['games_sp_19'] + p_splits['games_sp_20'] + p_splits['games_sp_21']
    p_splits['games'] = p_splits['games_19'] + p_splits['games_20'] + p_splits['games_21']
    p_splits['games_rp'] = p_splits['games'] - p_splits['games_sp']
    p_splits['wins'] = p_splits['wins_19'] + p_splits['wins_20'] + p_splits['wins_21']
    p_splits['losses'] = p_splits['losses_19'] + p_splits['losses_20'] + p_splits['losses_21']
    p_splits['saves'] = p_splits['saves_19'] + p_splits['saves_20'] + p_splits['saves_21']
    p_splits['holds'] = p_splits['holds_19'] + p_splits['holds_20'] + p_splits['holds_21']
    p_splits['complete_games'] = p_splits['complete_games_19'] + p_splits['complete_games_20'] + p_splits['complete_games_21']
    p_splits['shutouts'] = p_splits['shutouts_19'] + p_splits['shutouts_20'] + p_splits['shutouts_21']
    p_splits['pickoffs'] = p_splits['pickoffs_vr'] + p_splits['pickoffs_vl']
    p_splits['gidp'] = p_splits['gidp_vr'] + p_splits['gidp_vl']
    p_splits['hits'] = p_splits['hits_vr'] + p_splits['hits_vl']
    p_splits['k'] = p_splits['k_vr'] + p_splits['k_vl']
    p_splits['bb'] = p_splits['bb_vr'] + p_splits['bb_vl']
    p_splits['ibb'] = p_splits['ibb_vr'] + p_splits['ibb_vl']
    p_splits['runs'] = p_splits['runs_vr'] + p_splits['runs_vl']
    p_splits['fb'] = p_splits['fb_vr'] + p_splits['fb_vl']
    p_splits['gb'] = p_splits['gb_vr'] + p_splits['gb_vl']
    p_splits['ab'] = p_splits['ab_vr'] + p_splits['ab_vl']
    p_splits['sb'] = p_splits['sb_vr'] + p_splits['sb_vl']
    p_splits['cs'] = p_splits['cs_vr'] + p_splits['cs_vl']
    p_splits['balls'] = p_splits['balls_vr'] + p_splits['balls_vl']
    p_splits['strikes'] = p_splits['strikes_vr'] + p_splits['strikes_vl']
    p_splits['balks'] = p_splits['balks_vr'] + p_splits['balks_vl']
    p_splits['wild_pitches'] = p_splits['wild_pitches_vr'] + p_splits['wild_pitches_vl']
    p_splits['total_bases'] = p_splits['total_bases_vr'] + p_splits['total_bases_vl']
    p_splits['total_pitches'] = p_splits['total_pitches_vr'] + p_splits['total_pitches_vl']
    p_splits['hb'] = p_splits['hb_vr'] + p_splits['hb_vl']
    p_splits['outs'] = p_splits['outs_vr'] + p_splits['outs_vl']
    p_splits['ip'] = p_splits['outs'] / 3
    p_splits['batters_faced'] = p_splits['batters_faced_vr'] + p_splits['batters_faced_vl']
    p_splits['er'] = p_splits['er_vr'] + p_splits['er_vl']
    p_splits['cs+'] = p_splits['cs'] + p_splits['pickoffs']
    p_splits['sb+'] = p_splits['sb'] + p_splits['wild_pitches'] + p_splits['balks']
    p_splits['cs_sb'] = p_splits['cs'] / p_splits['sb']
    p_splits['cs+_sb+'] = p_splits['cs+'] /p_splits['sb+']
    p_splits['bases+'] = p_splits['total_bases'] + p_splits['bb'] + p_splits['hb']
    
    p_splits['pitches_inning'] = p_splits['total_pitches'] / (p_splits['outs'] / 3)
    p_splits['pitches_batter'] = p_splits['total_pitches'] / p_splits['batters_faced']
    p_splits['gb_fb'] = p_splits['gb'] / p_splits['fb']
    p_splits['k_b'] = p_splits['k'] / p_splits['batters_faced']
    p_splits['bb_b'] = p_splits['bb'] / p_splits['batters_faced']
    p_splits['k-bb'] = p_splits['k_b'] - p_splits['bb_b']
    p_splits['runners_allowed'] = p_splits['hits'] + p_splits['bb'] + p_splits['hb']
    p_splits['dp_ra'] = p_splits['gidp'] / p_splits['runners_allowed']
    p_splits['strike_ball'] = p_splits['strikes'] / p_splits['balls']
    p_splits['ra-_b'] = (p_splits['runners_allowed'] - (p_splits['gidp'] + p_splits['cs+'])) / p_splits['batters_faced']
    
    p_splits['pitches_start'] = p_splits['total_pitches_sp'] / p_splits['games_sp']
    p_splits['innings_start'] = (p_splits['outs_sp'] / 3) / (p_splits['games_sp'])
    p_splits['batters_start'] = p_splits['batters_faced_sp'] / p_splits['games_sp']
    p_splits['er_start'] = p_splits['er_sp'] / p_splits['games_sp']
    p_splits['ppa'] = p_splits['total_pitches'] / p_splits['games']
    
    p_splits['ppi_sp'] = p_splits['total_pitches_sp'] / (p_splits['outs_sp'] / 3)
    p_splits['ppb_sp'] = p_splits['total_pitches_sp'] / p_splits['batters_faced_sp']
    p_splits['gb_fb_sp'] = p_splits['gb_sp'] / p_splits['fb_sp']
    p_splits['k_b_sp'] = p_splits['k_sp'] / p_splits['batters_faced_sp']
    p_splits['bb_b_sp'] = p_splits['bb_sp'] / p_splits['batters_faced_sp']
    p_splits['k-bb_sp'] = p_splits['k_b_sp'] - p_splits['bb_b_sp']
    p_splits['runners_allowed_sp'] = p_splits['hits_sp'] + p_splits['bb_sp'] + p_splits['hb_sp']
    p_splits['dp_ro_sp'] = p_splits['gidp_sp'] / p_splits['runners_allowed_sp']
    p_splits['strike_ball_sp'] = p_splits['strikes_sp'] / p_splits['balls_sp']
    p_splits['ip_sp'] = p_splits['outs_sp'] / 3
    p_splits['bases+_sp'] = p_splits['total_bases_sp'] + p_splits['bb_sp'] + p_splits['hb_sp']
    p_splits['sb+_sp'] = p_splits['sb_sp'] + p_splits['wild_pitches_sp'] + p_splits['balks_sp']
    p_splits['cs+_sp'] = p_splits['cs_sp'] + p_splits['pickoffs_sp']
    p_splits['ra-_b_sp'] = (p_splits['runners_allowed_sp'] - (p_splits['gidp_sp'] + p_splits['cs+_sp'])) / p_splits['batters_faced_sp']
    
    
    p_splits['ppi_rp'] = p_splits['total_pitches_rp'] / (p_splits['outs_rp'] / 3)
    p_splits['ppb_rp'] = p_splits['total_pitches_rp'] / p_splits['batters_faced_rp']
    p_splits['gb_fb_rp'] = p_splits['gb_rp'] / p_splits['fb_rp']
    p_splits['k_b_rp'] = p_splits['k_rp'] / p_splits['batters_faced_rp']
    p_splits['bb_b_rp'] = p_splits['bb_rp'] / p_splits['batters_faced_rp']
    p_splits['k-bb_rp'] = p_splits['k_b_rp'] - p_splits['bb_b_rp']
    p_splits['runners_allowed_rp'] = p_splits['hits_rp'] + p_splits['bb_rp'] + p_splits['hb_rp']
    p_splits['dp_ro_rp'] = p_splits['gidp_rp'] / p_splits['runners_allowed_rp']
    p_splits['strike_ball_rp'] = p_splits['strikes_rp'] / p_splits['balls_rp']
    p_splits['ip_rp'] = p_splits['outs_rp'] / 3
    p_splits['bases+_rp'] = p_splits['total_bases_rp'] + p_splits['bb_rp'] + p_splits['hb_rp']
    p_splits['sb+_rp'] = p_splits['sb_rp'] + p_splits['wild_pitches_rp'] + p_splits['balks_rp']
    p_splits['cs+_rp'] = p_splits['cs_rp'] + p_splits['pickoffs_rp']
    p_splits['ra-_b_rp'] = (p_splits['runners_allowed_rp'] - (p_splits['gidp_rp'] + p_splits['cs+_rp'])) / p_splits['batters_faced_rp']
  
    
    
    p_splits['ppi_vr'] = p_splits['total_pitches_vr'] / (p_splits['outs_vr'] / 3)
    p_splits['ppb_vr'] = p_splits['total_pitches_vr'] / p_splits['batters_faced_vr']
    p_splits['gb_fb_vr'] = p_splits['gb_vr'] / p_splits['fb_vr']
    p_splits['k_b_vr'] = p_splits['k_vr'] / p_splits['batters_faced_vr']
    p_splits['bb_b_vr'] = p_splits['bb_vr'] / p_splits['batters_faced_vr']
    p_splits['k-bb_vr'] = p_splits['k_b_vr'] - p_splits['bb_b_vr']
    p_splits['runners_allowed_vr'] = p_splits['hits_vr'] + p_splits['bb_vr'] + p_splits['hb_vr']
    p_splits['dp_ro_vr'] = p_splits['gidp_vr'] / p_splits['runners_allowed_vr']
    p_splits['strike_ball_vr'] = p_splits['strikes_vr'] / p_splits['balls_vr']
    p_splits['ip_vr'] = p_splits['outs_vr'] / 3
    p_splits['bases+_vr'] = p_splits['total_bases_vr'] + p_splits['bb_vr'] + p_splits['hb_vr']
    p_splits['sb+_vr'] = p_splits['sb_vr'] + p_splits['wild_pitches_vr'] + p_splits['balks_vr']
    p_splits['cs+_vr'] = p_splits['cs_vr'] + p_splits['pickoffs_vr']
    p_splits['ra-_b_vr'] = (p_splits['runners_allowed_vr'] - (p_splits['gidp_vr'] + p_splits['cs+_vr'])) / p_splits['batters_faced_vr']
    
    p_splits['ppi_vl'] = p_splits['total_pitches_vl'] / (p_splits['outs_vl'] / 3)
    p_splits['ppb_vl'] = p_splits['total_pitches_vl'] / p_splits['batters_faced_vl']
    p_splits['gb_fb_vl'] = p_splits['gb_vl'] / p_splits['fb_vl']
    p_splits['k_b_vl'] = p_splits['k_vl'] / p_splits['batters_faced_vl']
    p_splits['bb_b_vl'] = p_splits['bb_vl'] / p_splits['batters_faced_vl']
    p_splits['k-bb_vl'] = p_splits['k_b_vl'] - p_splits['bb_b_vl']
    p_splits['runners_allowed_vl'] = p_splits['hits_vl'] + p_splits['bb_vl'] + p_splits['hb_vl']
    p_splits['dp_ro_vl'] = p_splits['gidp_vl'] / p_splits['runners_allowed_vl']
    p_splits['strike_ball_vl'] = p_splits['strikes_vl'] / p_splits['balls_vl']  
    p_splits['ip_vl'] = p_splits['outs_vl'] / 3
    p_splits['bases+_vl'] = p_splits['total_bases_vl'] + p_splits['bb_vl'] + p_splits['hb_vl'] 
    p_splits['sb+_vl'] = p_splits['sb_vl'] + p_splits['wild_pitches_vl'] + p_splits['balks_vl']
    p_splits['cs+_vl'] = p_splits['cs_vl'] + p_splits['pickoffs_vl']
    p_splits['ra-_b_vl'] = (p_splits['runners_allowed_vl'] - (p_splits['gidp_vl'] + p_splits['cs+_vl'])) / p_splits['batters_faced_vl']
    
    #weighted by catergoy/batter faced - right
    p_splits['fd_bb_weight_vr'] = (((p_splits['bb_vr'] * weight_bb) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_hb_weight_vr'] = (((p_splits['hb_vr'] * weight_hbp) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_1b_weight_vr'] = (((p_splits['1b_vr'] * weight_1b) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_2b_weight_vr'] = (((p_splits['2b_vr'] * weight_2b) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_3b_weight_vr'] = (((p_splits['3b_vr'] * weight_3b) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_hr_weight_vr'] = (((p_splits['hr_vr'] * weight_hr) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_sb_weight_vr'] = (((p_splits['sb_vr'] * weight_sb) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_sb+_weight_vr'] = (((p_splits['sb+_vr'] * weight_sb) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_balk_weight_vr'] = (((p_splits['balks_vr'] * weight_sb) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_wp_weight_vr'] = (((p_splits['wild_pitches_vr'] * weight_sb) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_cs_weight_vr'] = (((p_splits['cs_vr'] * weight_cs) * er) / p_splits['batters_faced_vr'])
    p_splits['fd_ip_weight_vr'] = ((p_splits['ip_vr'] * i) / p_splits['batters_faced_vr'])
    p_splits['fd_k_weight_vr'] = ((p_splits['k_vr'] * k) / p_splits['batters_faced_vr'])
    
    #weighted points scored/batter faced - right
    p_splits['fd_wps_b_vr'] = p_splits['fd_bb_weight_vr'] + p_splits['fd_hb_weight_vr'] + p_splits['fd_1b_weight_vr'] + \
        p_splits['fd_2b_weight_vr'] + p_splits['fd_3b_weight_vr'] + p_splits['fd_hr_weight_vr'] + p_splits['fd_sb+_weight_vr'] + \
            p_splits['fd_cs_weight_vr'] + p_splits['fd_ip_weight_vr'] + p_splits['fd_k_weight_vr']
                
    #weighted by catergoy/batter faced - left
    p_splits['fd_bb_weight_vl'] = (((p_splits['bb_vl'] * weight_bb) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_hb_weight_vl'] = (((p_splits['hb_vl'] * weight_hbp) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_1b_weight_vl'] = (((p_splits['1b_vl'] * weight_1b) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_2b_weight_vl'] = (((p_splits['2b_vl'] * weight_2b) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_3b_weight_vl'] = (((p_splits['3b_vl'] * weight_3b) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_hr_weight_vl'] = (((p_splits['hr_vl'] * weight_hr) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_sb_weight_vl'] = (((p_splits['sb_vl'] * weight_sb) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_sb+_weight_vl'] = (((p_splits['sb+_vl'] * weight_sb) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_balk_weight_vl'] = (((p_splits['balks_vl'] * weight_sb) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_wp_weight_vl'] = (((p_splits['wild_pitches_vl'] * weight_sb) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_cs_weight_vl'] = (((p_splits['cs_vl'] * weight_cs) * er) / p_splits['batters_faced_vl'])
    p_splits['fd_ip_weight_vl'] = ((p_splits['ip_vl'] * i) / p_splits['batters_faced_vl'])
    p_splits['fd_k_weight_vl'] = ((p_splits['k_vr'] * k) / p_splits['batters_faced_vl'])
    
    #weighted points scored/batter faced - left
    p_splits['fd_wps_b_vl'] = p_splits['fd_bb_weight_vl'] + p_splits['fd_hb_weight_vl'] + p_splits['fd_1b_weight_vl'] + \
        p_splits['fd_2b_weight_vl'] + p_splits['fd_3b_weight_vl'] + p_splits['fd_hr_weight_vl'] + p_splits['fd_sb+_weight_vl'] + \
            p_splits['fd_cs_weight_vl'] + p_splits['fd_ip_weight_vl'] + p_splits['fd_k_weight_vl']
    
    #weighted by catergoy/batter faced - starting
    p_splits['fd_bb_weight_sp'] = (((p_splits['bb_sp'] * weight_bb) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_hb_weight_sp'] = (((p_splits['hb_sp'] * weight_hbp) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_1b_weight_sp'] = (((p_splits['1b_sp'] * weight_1b) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_2b_weight_sp'] = (((p_splits['2b_sp'] * weight_2b) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_3b_weight_sp'] = (((p_splits['3b_sp'] * weight_3b) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_hr_weight_sp'] = (((p_splits['hr_sp'] * weight_hr) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_sb_weight_sp'] = (((p_splits['sb_sp'] * weight_sb) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_sb+_weight_sp'] = (((p_splits['sb+_sp'] * weight_sb) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_balk_weight_sp'] = (((p_splits['balks_sp'] * weight_sb) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_wp_weight_sp'] = (((p_splits['wild_pitches_sp'] * weight_sb) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_cs_weight_sp'] = (((p_splits['cs_sp'] * weight_cs) * er) / p_splits['batters_faced_sp'])
    p_splits['fd_ip_weight_sp'] = ((p_splits['ip_sp'] * i) / p_splits['batters_faced_sp'])
    p_splits['fd_k_weight_sp'] = ((p_splits['k_sp'] * k) / p_splits['batters_faced_sp'])
    
    #weighted points scored/batter faced - starting
    p_splits['fd_wps_b_sp'] = p_splits['fd_bb_weight_sp'] + p_splits['fd_hb_weight_sp'] + p_splits['fd_1b_weight_sp'] + \
        p_splits['fd_2b_weight_sp'] + p_splits['fd_3b_weight_sp'] + p_splits['fd_hr_weight_sp'] + p_splits['fd_sb+_weight_sp'] + \
            p_splits['fd_cs_weight_sp'] + p_splits['fd_ip_weight_sp'] + p_splits['fd_k_weight_sp']
    
    #weighted by catergoy/batter faced
    p_splits['fd_bb_weight'] = (((p_splits['bb'] * weight_bb) * er) / p_splits['batters_faced'])
    p_splits['fd_hb_weight'] = (((p_splits['hb'] * weight_hbp) * er) / p_splits['batters_faced'])
    p_splits['fd_1b_weight'] = (((p_splits['1b'] * weight_1b) * er) / p_splits['batters_faced'])
    p_splits['fd_2b_weight'] = (((p_splits['2b'] * weight_2b) * er) / p_splits['batters_faced'])
    p_splits['fd_3b_weight'] = (((p_splits['3b'] * weight_3b) * er) / p_splits['batters_faced'])
    p_splits['fd_hr_weight'] = (((p_splits['hr'] * weight_hr) * er) / p_splits['batters_faced'])
    p_splits['fd_sb_weight'] = (((p_splits['sb'] * weight_sb) * er) / p_splits['batters_faced'])
    p_splits['fd_sb+_weight'] = (((p_splits['sb+'] * weight_sb) * er) / p_splits['batters_faced'])
    p_splits['fd_balk_weight'] = (((p_splits['balks'] * weight_sb) * er) / p_splits['batters_faced'])
    p_splits['fd_wp_weight'] = (((p_splits['wild_pitches'] * weight_sb) * er) / p_splits['batters_faced'])
    p_splits['fd_cs_weight'] = (((p_splits['cs'] * weight_cs) * er) / p_splits['batters_faced'])
    p_splits['fd_ip_weight'] = ((p_splits['ip'] * i) / p_splits['batters_faced'])
    p_splits['fd_k_weight'] =((p_splits['k'] * k )/ p_splits['batters_faced'])
    
    #weighted points scored/batter faced
    p_splits['fd_wps_b'] = p_splits['fd_bb_weight'] + p_splits['fd_hb_weight'] + p_splits['fd_1b_weight'] + \
        p_splits['fd_2b_weight'] + p_splits['fd_3b_weight'] + p_splits['fd_hr_weight'] + p_splits['fd_sb+_weight'] + \
            p_splits['fd_cs_weight'] + p_splits['fd_ip_weight'] + p_splits['fd_k_weight']    
    
    
    
    #points scored/batter faced - in starts
    p_splits['fd_ps_b_sp'] = ((p_splits['ip_sp'] * i) + (p_splits['k_sp'] * k) + (p_splits['er_sp'] * er)) / p_splits['batters_faced_sp']
    #points scored/start
    p_splits['fd_ps_s'] = (p_splits['fd_ps_b_sp'] * p_splits['batters_faced_sp']) / p_splits['games_sp']
    # points scored/left-handed batter faced
    p_splits['fd_ps_b_vl'] = ((p_splits['ip_vl'] * i) + (p_splits['k_vl'] * k) + (p_splits['er_vl'] * er)) / p_splits['batters_faced_vl']
    #points score/right-handed batter faced
    p_splits['fd_ps_b_vr'] = ((p_splits['ip_vr'] * i) + (p_splits['k_vr'] * k) + (p_splits['er_vr'] * er)) / p_splits['batters_faced_vr']
    
    #weight points allowed/batter - vs. right
    p_splits['fd_wpa_b_vr'] = ((((p_splits['bb_vr'] * b) + ((p_splits['bb_vr'] * weight_bb) * (r + rbi))) + \
        ((p_splits['hb_vr'] * b) + ((p_splits['hb_vr'] * weight_hbp) * (r + rbi))) + \
            ((p_splits['1b_vr'] * b) + ((p_splits['1b_vr'] * weight_1b) * (r + rbi))) + \
                ((p_splits['2b_vr'] * (b * 2)) + ((p_splits['2b_vr'] * weight_2b) * (r + rbi))) + \
                    ((p_splits['3b_vr'] * (b * 3)) + ((p_splits['3b_vr'] * weight_3b) * (r + rbi))) + \
                        ((p_splits['hr_vr'] * hr) + ((p_splits['hr_vr'] * (weight_hr - 1)) * (r + rbi))) + \
                            ((p_splits['sb_vr'] * s) + (((p_splits['sb+_vr'] - p_splits['cs_vr']) * weight_sb) * (r + rbi)))) \
        / p_splits['batters_faced_vr'])
                                
    #weight points allowed/batter - vs. left
    p_splits['fd_wpa_b_vl'] = ((((p_splits['bb_vl'] * b) + ((p_splits['bb_vl'] * weight_bb) * (r + rbi))) + \
        ((p_splits['hb_vl'] * b) + ((p_splits['hb_vl'] * weight_hbp) * (r + rbi))) + \
            ((p_splits['1b_vl'] * b) + ((p_splits['1b_vl'] * weight_1b) * (r + rbi))) + \
                ((p_splits['2b_vl'] * (b * 2)) + ((p_splits['2b_vl'] * weight_2b) * (r + rbi))) + \
                    ((p_splits['3b_vl'] * (b * 3)) + ((p_splits['3b_vl'] * weight_3b) * (r + rbi))) + \
                        ((p_splits['hr_vl'] * hr) + ((p_splits['hr_vl'] * (weight_hr - 1)) * (r + rbi))) + \
                            ((p_splits['sb_vl'] * s) + (((p_splits['sb+_vl'] - p_splits['cs_vl']) * weight_sb) * (r + rbi)))) \
        / p_splits['batters_faced_vl'])
        
    #weight points allowed/batter - as starter
    p_splits['fd_wpa_b_sp'] = ((((p_splits['bb_sp'] * b) + ((p_splits['bb_sp'] * weight_bb) * (r + rbi))) + \
        ((p_splits['hb_sp'] * b) + ((p_splits['hb_sp'] * weight_hbp) * (r + rbi))) + \
            ((p_splits['1b_sp'] * b) + ((p_splits['1b_sp'] * weight_1b) * (r + rbi))) + \
                ((p_splits['2b_sp'] * (b * 2)) + ((p_splits['2b_sp'] * weight_2b) * (r + rbi))) + \
                    ((p_splits['3b_sp'] * (b * 3)) + ((p_splits['3b_sp'] * weight_3b) * (r + rbi))) + \
                        ((p_splits['hr_sp'] * hr) + ((p_splits['hr_sp'] * (weight_hr - 1)) * (r + rbi))) + \
                            ((p_splits['sb_sp'] * s) + (((p_splits['sb+_sp'] - p_splits['cs_sp']) * weight_sb) * (r + rbi)))) \
        / p_splits['batters_faced_sp'])
    
    #weight points allowed/batter - as reliever
    p_splits['fd_wpa_b_rp'] = ((((p_splits['bb_rp'] * b) + ((p_splits['bb_rp'] * weight_bb) * (r + rbi))) + \
        ((p_splits['hb_rp'] * b) + ((p_splits['hb_rp'] * weight_hbp) * (r + rbi))) + \
            ((p_splits['1b_rp'] * b) + ((p_splits['1b_rp'] * weight_1b) * (r + rbi))) + \
                ((p_splits['2b_rp'] * (b * 2)) + ((p_splits['2b_rp'] * weight_2b) * (r + rbi))) + \
                    ((p_splits['3b_rp'] * (b * 3)) + ((p_splits['3b_rp'] * weight_3b) * (r + rbi))) + \
                        ((p_splits['hr_rp'] * hr) + ((p_splits['hr_rp'] * (weight_hr - 1)) * (r + rbi))) + \
                            ((p_splits['sb_rp'] * s) + (((p_splits['sb+_rp'] - p_splits['cs_rp']) * weight_sb) * (r + rbi)))) \
        / p_splits['batters_faced_rp'])
        
    #weight points allowed/batter - as reliever
    p_splits['fd_wpa_b'] = ((((p_splits['bb'] * b) + ((p_splits['bb'] * weight_bb) * (r + rbi))) + \
        ((p_splits['hb'] * b) + ((p_splits['hb'] * weight_hbp) * (r + rbi))) + \
            ((p_splits['1b'] * b) + ((p_splits['1b'] * weight_1b) * (r + rbi))) + \
                ((p_splits['2b'] * (b * 2)) + ((p_splits['2b'] * weight_2b) * (r + rbi))) + \
                    ((p_splits['3b'] * (b * 3)) + ((p_splits['3b'] * weight_3b) * (r + rbi))) + \
                        ((p_splits['hr'] * hr) + ((p_splits['hr'] * (weight_hr - 1)) * (r + rbi))) + \
                            ((p_splits['sb'] * s) + (((p_splits['sb+'] - p_splits['cs']) * weight_sb) * (r + rbi)))) \
        / p_splits['batters_faced'])
    
    #points allowed/start
    p_splits['fd_pa_s'] = ((p_splits['bases+_sp'] * b) + (p_splits['er_sp'] * (r + rbi)) + (p_splits['sb_sp'] * s)) / p_splits['games_sp']
    #points allowed/batter faced - in starts
    p_splits['fd_pa_b_sp'] = ((p_splits['bases+_sp'] * b) + (p_splits['er_sp'] * (r + rbi)) + (p_splits['sb_sp'] * s)) / p_splits['batters_faced_sp']
    #total points allowed - in starts
    p_splits['fd_pa_sp'] = ((p_splits['bases+_sp'] * b) + (p_splits['er_sp'] * (r + rbi)) + (p_splits['sb_sp'] * s))
    #points allowed/batter faced - in relief
    p_splits['fd_pa_b_rp'] = ((p_splits['bases+_rp'] * b) + (p_splits['er_rp'] * (r + rbi)) + (p_splits['sb_rp'] * s)) / p_splits['batters_faced_rp']
    #points allowed/relief appearence
    p_splits['fd_pa_ra'] = ((p_splits['bases+_rp'] * b) + (p_splits['er_rp'] * (r + rbi)) + (p_splits['sb_rp'] * s)) / p_splits['games_rp']
    #total points allowed - in relief
    p_splits['fd_pa_rp'] = ((p_splits['bases+_rp'] * b) + (p_splits['er_rp'] * (r + rbi)) + (p_splits['sb_rp'] * s))
    #total points allowed
    p_splits['fd_pa'] = ((p_splits['bases+'] * b) + (p_splits['er'] * (r + rbi)) + (p_splits['sb'] * s))
    #points allowed/ right-handed batter faced
    p_splits['fd_pa_b_vr'] = ((p_splits['bases+_vr'] * b) + (p_splits['er_vr'] * (r + rbi)) + (p_splits['sb_vr'] * s)) / p_splits['batters_faced_vr']
    #points allowed/ left-handed batter faced
    p_splits['fd_pa_b_vl'] = ((p_splits['bases+_vl'] * b) + (p_splits['er_vl'] * (r + rbi)) + (p_splits['sb_vl'] * s)) / p_splits['batters_faced_vl']
    with open(data_path_p, "wb") as file:
        pickle.dump(p_splits, file)
    clean_directory(settings.STORAGE_DIR)
else:
    p_splits = pd.read_pickle(data_path_p)
    
p_q_rp = p_splits[p_splits['batters_faced_rp'] >= p_splits['batters_faced_rp'].mean()]
p_q_sp = p_splits[p_splits['batters_faced_sp'] >= p_splits['batters_faced_sp'].mean()]
p_q_vl = p_splits[p_splits['batters_faced_vl'] >= p_splits['batters_faced_vl'].mean()]
p_q_vr = p_splits[p_splits['batters_faced_vr'] >= p_splits['batters_faced_vr'].mean()]
p_q = p_splits[p_splits['batters_faced'] >= p_splits['batters_faced'].mean()]

l_filt = (p_splits['pitch_hand'] == 'L')
r_filt = (p_splits['pitch_hand'] == 'R')
l_q_filt = (p_splits['batters_faced_vl'] >= p_splits['batters_faced_vl'].mean())
r_q_filt = (p_splits['batters_faced_vr'] >= p_splits['batters_faced_vr'].mean())

p_q_l_vl = p_splits[l_filt & l_q_filt]
p_q_r_vl = p_splits[r_filt & l_q_filt]
p_q_l_vr = p_splits[l_filt & r_q_filt]
p_q_r_vr = p_splits[r_filt & r_q_filt]


