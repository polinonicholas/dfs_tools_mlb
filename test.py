

def lineup_df(self):
        data_path = pickle_path(name=f"{self.name}_lu_{tf.today}", directory=settings.LINEUP_DIR)
        if not data_path.exists():
            lineup_ids = self.lineup
            lineup = pd.DataFrame(lineup_ids, columns= ["mlb_id"])
            roster = pd.DataFrame(self.full_roster)
            merged = pd.merge(lineup, roster, on="mlb_id")
            h_df = merged.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            roster = roster.join(h_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            if settings.use_fangraphs:
                def drop(df):
                    df = df.loc[~df.duplicated(subset=['mlb_id'])]
                    df = df[df['mlb_id'].isin(lineup_ids)]
                    return df
                from dfs_tools_mlb.compile.stats_fangraphs import Stats
                fg_stats = Stats.current_stats()
                h_df = sm_merge(h_df, fg_stats, columns=['name', 'team'], ratios=[.63, 1], prefix='m_', reset_index=False, post_drop=True, suffixes=('', '_fg'))
                h_df = drop(h_df)
            h_df = h_df[~h_df['position'].astype(str).isin(mac.players.p)]
            if not self.is_dh and len(h_df.index) == 9:
                h_key = 'fd_wps_pa_' + self.o_split
                i_d = h_df.loc[h_df[h_key].idxmin(), 'mlb_id'].item()
                h_df.drop(h_df[h_key].idxmin(), inplace=True)
                new_row = self.get_replacement('S', 'pitcher', lineup_ids)
                h_df = h_df.append(new_row, ignore_index = True)
                lineup_ids.remove(i_d)
                lineup_ids.append(int(new_row['mlb_id']))
            if len(h_df.index) < 9:
                for i_d in lineup_ids:
                    if i_d not in h_df['mlb_id'].values:
                        new_row = roster[roster['mlb_id'] == i_d]
                        new_row = new_row[~new_row['position'].astype(str).isin(mac.players.p)]
                        if len(new_row.index) == 0:
                            player = statsapi.get('people', {'personIds': i_d})
                            position = player['people'][0]['primaryPosition']['code']
                            position_type = player['people'][0]['primaryPosition']['type']
                            new_row = self.get_replacement(position, position_type, lineup_ids)
                            index = lineup_ids.index(i_d)
                            lineup_ids[index] = int(new_row['mlb_id'])
                            self.update_lineup(lineup_ids, self.opp_sp_hand)
                        h_df = h_df.append(new_row, ignore_index = True)
            sorter = dict(zip(lineup_ids, range(len(lineup_ids))))
            h_df['order'] = h_df['mlb_id'].map(sorter)
            h_df.sort_values(by='order',inplace=True, kind='mergesort ')
            h_df['order'] = h_df['order'] + 1
            h_df.reset_index(inplace=True, drop=True)
            h_df.loc[((h_df['pa'] < 100) | (h_df['pitches_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa'] = h_q['pitches_pa'].median() - h_q['pitches_pa'].std()
            h_df.loc[((h_df['pa_vr'] < 100) | (h_df['pitches_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa_vr'] = h_q_vr['pitches_pa_vr'].median() - h_q_vr['pitches_pa_vr'].std()
            h_df.loc[((h_df['pa_vl'] < 100) | (h_df['pitches_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'pitches_pa_vl'] = h_q_vl['pitches_pa_vl'].median() - h_q_vl['pitches_pa_vl'].std()
            h_df.loc[(h_df['pa'] < 100) | (h_df['pitches_pa'].isna()), 'pitches_pa'] = h_q['pitches_pa'].median()
            h_df.loc[(h_df['pa_vr'] < 100) | (h_df['pitches_pa_vr'].isna()), 'pitches_pa_vr'] = h_q_vr['pitches_pa_vr'].median()
            h_df.loc[(h_df['pa_vl'] < 100) | (h_df['pitches_pa_vl'].isna()), 'pitches_pa_vl'] = h_q_vl['pitches_pa_vl'].median()
            h_df.loc[((h_df['pa'] < 100) | (h_df['fd_wps_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa'] = h_q['fd_wps_pa'].median() - h_q['fd_wps_pa'].std()
            h_df.loc[((h_df['pa_vr'] < 100) | (h_df['fd_wps_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa_vr'] = h_q_vr['fd_wps_pa_vr'].median() - h_q_vr['fd_wps_pa_vr'].std()
            h_df.loc[((h_df['pa_vl'] < 100) | (h_df['fd_wps_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wps_pa_vl'] = h_q_vl['fd_wps_pa_vl'].median() - h_q_vl['fd_wps_pa_vl'].std()
            h_df.loc[(h_df['pa'] < 100) | (h_df['fd_wps_pa'].isna()), 'fd_wps_pa'] = h_q['fd_wps_pa'].median()
            h_df.loc[(h_df['pa_vr'] < 100) | (h_df['fd_wps_pa_vr'].isna()), 'fd_wps_pa_vr'] = h_q_vr['fd_wps_pa_vr'].median()
            h_df.loc[(h_df['pa_vl'] < 100) | (h_df['fd_wps_pa_vl'].isna()), 'fd_wps_pa_vl'] = h_q_vl['fd_wps_pa_vl'].median()
            h_df.loc[((h_df['pa'] < 100) | (h_df['fd_wpa_pa'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa'] = h_q['fd_wpa_pa'].median() + h_q['fd_wpa_pa'].std()
            h_df.loc[((h_df['pa_vr'] < 100) | (h_df['fd_wpa_pa_vr'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa_vr'] = h_q_vr['fd_wpa_pa_vr'].median() + h_q_vr['fd_wpa_pa_vr'].std()
            h_df.loc[((h_df['pa_vl'] < 100) | (h_df['fd_wpa_pa_vl'].isna())) & (h_df['position'].isin(mac.players.p)), 'fd_wpa_pa_vl'] = h_q_vl['fd_wpa_pa_vl'].median() + h_q_vl['fd_wpa_pa_vl'].std()
            h_df.loc[(h_df['pa'] < 100) | (h_df['fd_wpa_pa'].isna()), 'fd_wpa_pa'] = h_q['fd_wpa_pa'].median()
            h_df.loc[(h_df['pa_vr'] < 100) | (h_df['fd_wpa_pa_vr'].isna()), 'fd_wpa_pa_vr'] = h_q_vr['fd_wpa_pa_vr'].median()
            h_df.loc[(h_df['pa_vl'] < 100) | (h_df['fd_wpa_pa_vl'].isna()), 'fd_wpa_pa_vl'] = h_q_vl['fd_wpa_pa_vl'].median()
            if self.opp_sp_hand == 'R':
                h_df.loc[h_df['bat_side'] == 'S', 'bat_side'] = 'L'
            else:
                h_df.loc[h_df['bat_side'] == 'S', 'bat_side'] = 'R'
            
            try:
                p_info = self.opp_sp
                player = {'mlb_id': p_info['id'],
                          'name': p_info.get('fullName', ''),
                          'mlb_api': p_info.get('link', ''),
                          'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                          'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                          'weight': p_info.get('weight', ''),
                          'nickname': p_info.get('nickName', ''),
                          'debut': p_info.get('mlbDebutDate', ''),
                          'bat_side': p_info.get('batSide', {}).get('code', ''),
                          'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                          'age': p_info.get('currentAge', ''),
                          'team': self.opp_name
                              }
                p_df = pd.DataFrame([player]).join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
            except TypeError:
                p_df = Team.default_sp()
            p_df['pitches_start'].fillna(p_q_sp['pitches_start'].median(), inplace = True)
            key = 'pitches_pa_' + self.o_split
            p_df['exp_x_lu'] = p_df['pitches_start'] / h_df[key].sum()
            p_df['exp_bf'] = round((p_df['exp_x_lu'] * 9))
            sp_rollover = floor((p_df['exp_x_lu'] % 1) * 9)
            h_df.loc[h_df['order'] <= sp_rollover, 'exp_pa_sp'] = ceil(p_df['exp_x_lu'])
            h_df.loc[h_df['order'] > sp_rollover, 'exp_pa_sp'] = floor(p_df['exp_x_lu'])
            p_df.loc[(p_df['batters_faced_vr'] < 100) | (p_df['fd_wpa_b_vr'].isna()), 'fd_wpa_b_vr'] = p_q_vr['fd_wpa_b_vr'].median()
            p_df.loc[(p_df['batters_faced_vl'] < 100) | (p_df['fd_wpa_b_vl'].isna()), 'fd_wpa_b_vl'] = p_q_vl['fd_wpa_b_vl'].median()
            lefties = (h_df['bat_side'] == 'L')
            righties = (h_df['bat_side'] == 'R')
            key = 'fd_wps_pa_' + self.o_split
            h_df.loc[lefties, 'exp_ps_sp'] = ((p_df['fd_wpa_b_vl'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df.loc[righties, 'exp_ps_sp'] = ((p_df['fd_wpa_b_vr'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df.loc[lefties, 'sp_mu'] = p_df['fd_wpa_b_vl'].max()
            h_df.loc[righties, 'sp_mu'] = p_df['fd_wpa_b_vr'].max()
            #points conceded
            key = 'fd_wpa_pa_' + self.o_split
            h_df.loc[lefties, 'exp_pc_sp'] = ((p_df['fd_wps_b_vl'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df.loc[righties, 'exp_pc_sp'] = ((p_df['fd_wpa_b_vr'].max() + h_df[key]) / 2) * h_df['exp_pa_sp']
            h_df['exp_pc_sp_raw'] = h_df[key] * h_df['exp_pa_sp']
            p_df.loc[(p_df['batters_faced_vr'] < 100) | (p_df['ra-_b_vr'].isna()), 'ra-_b_vr'] = p_q_vr['ra-_b_vr'].median()
            p_df.loc[(p_df['batters_faced_vl'] < 100) | (p_df['ra-_b_vl'].isna()), 'ra-_b_vl'] = p_q_vl['ra-_b_vl'].median()
            exp_pa_r_sp = h_df.loc[righties, 'exp_pa_sp'].sum()
            exp_pa_l_sp = h_df.loc[lefties, 'exp_pa_sp'].sum()
            p_df['exp_ra'] = floor((exp_pa_r_sp * p_df['ra-_b_vr'].max()) + (exp_pa_l_sp * p_df['ra-_b_vl'].max()))
            p_df['exp_inn'] = (p_df['exp_bf'].max() - p_df['exp_ra'].max()) / 3
            if self.is_home:
                exp_bp_inn = 8.5 - p_df['exp_inn'].max()
            else:
                exp_bp_inn = 9 - p_df['exp_inn'].max()
            bp = self.proj_opp_bp
            bp.loc[(bp['batters_faced_vr'] < 100) | (bp['fd_wpa_b_vr'].isna()), 'fd_wpa_b_vr'] = p_q_vr['fd_wpa_b_vr'].median()
            bp.loc[(bp['batters_faced_vl'] < 100) | (bp['fd_wpa_b_vl'].isna()), 'fd_wpa_b_vl'] = p_q_vl['fd_wpa_b_vl'].median()
            bp.loc[(bp['batters_faced_vr'] < 100) | (bp['ra-_b_vr'].isna()), 'ra-_b_vr'] = p_q_vr['ra-_b_vr'].median()
            bp.loc[(bp['batters_faced_vl'] < 100) | (bp['ra-_b_vl'].isna()), 'ra-_b_vl'] = p_q_vl['ra-_b_vl'].median()
            bp.loc[(bp['batters_faced_rp'] < 100) | (bp['fd_wpa_b_rp'].isna()), 'fd_wpa_b_rp'] = p_q_rp['fd_wpa_b_rp'].median()
            bp.loc[(bp['batters_faced_rp'] < 100) | (bp['ra-_b_rp'].isna()), 'ra-_b_rp'] = p_q_rp['ra-_b_rp'].median()
            exp_bf_bp = floor((exp_bp_inn * 3) + ((exp_bp_inn * 3) * bp['ra-_b_rp'].mean()))
            first_bp_pa = h_df.loc[(h_df['exp_pa_sp'] == floor(p_df['exp_x_lu'])), 'order'].idxmin()
            order = h_df.loc[first_bp_pa, 'order'].item()
            print(order)
            h_df['exp_pa_bp'] = 0
            print(exp_bf_bp)
            while exp_bf_bp > 0:
                if order == 10:
                    order = 1
                h_df.loc[h_df['order'] == order, 'exp_pa_bp'] += 1
                order += 1
                exp_bf_bp -= 1
            h_df['exp_ps_bp'] = h_df['exp_pa_bp'] * ((bp['fd_wpa_b_rp'].mean() + h_df['fd_wps_pa']) / 2)
            h_df['exp_ps_raw'] = h_df['exp_ps_sp'] + h_df['exp_ps_bp']
            self.exp_ps_raw = h_df['exp_ps_raw'].sum()
            h_df.loc[h_df['is_platoon'] == True, 'exp_ps_raw'] = h_df['exp_ps_sp']
            h_df['venue_points'] = h_df['exp_ps_raw'] * self.next_venue_boost
            h_df['temp_points'] = h_df['exp_ps_raw'] * self.temp_boost
            h_df['ump_points'] = h_df['exp_ps_raw'] * self.ump_boost
            h_df['points'] = (h_df['venue_points'] * fw['venue_h']) + \
                (h_df['temp_points'] * fw['temp_h']) + (h_df['ump_points'] * fw['ump_h'])
            h_df['points'] = (h_df['venue_points'] + h_df['temp_points'] + h_df['ump_points']) / 3
            self.venue_points = self.exp_ps_raw * self.next_venue_boost
            self.temp_points = self.exp_ps_raw * self.temp_boost
            self.ump_points = self.exp_ps_raw * self.ump_boost
            self.points= (self.venue_points + self.temp_points + self.ump_points) / 3
            if self.confirmed_lu:
                with open(data_path, "wb") as file:
                    pickle.dump(h_df, file)
        else:
            h_df = pd.read_pickle(data_path)
        return h_df
     
def sp_df(self):
        if self.confirmed_sp:
            slug = self.projected_sp['nameSlug']
            data_path = pickle_path(name=f"{slug}_{tf.today}", directory=settings.LINEUP_DIR)
            data_path = settings.SP_DIR.joinpath(f"{slug}_{tf.today}.pickle")
            if data_path.exists():
                p_df = pd.read_pickle(data_path)
                return p_df
        try:
            p_info = self.projected_sp
            player = {'mlb_id': p_info['id'],
                      'name': p_info.get('fullName', ''),
                      'mlb_api': p_info.get('link', ''),
                      'born': p_info.get('birthCity', '') + ', ' + p_info.get('birthStateProvince', '') ,
                      'height': '.'.join(re.findall('[0-9]', p_info.get('height', ''))),
                      'weight': p_info.get('weight', ''),
                      'nickname': p_info.get('nickName', ''),
                      'debut': p_info.get('mlbDebutDate', ''),
                      'bat_side': p_info.get('batSide', {}).get('code', ''),
                      'pitch_hand': p_info.get('pitchHand', {}).get('code', ''),
                      'age': p_info.get('currentAge', ''),
                      'team': self.opp_name
                          }
            p_df = pd.DataFrame([player]).join(p_splits.set_index('mlb_id'), on='mlb_id', rsuffix='_drop')
        except TypeError:
            p_df = Team.default_sp()
        h_df = self.opp_instance.lineup_df()
        p_df['exp_ps_raw'] = h_df['exp_pc_sp'].sum()
        p_df['venue_points'] = p_df['exp_ps_raw'] * self.next_venue_boost
        p_df['temp_points'] = p_df['exp_ps_raw'] * self.temp_boost
        p_df['ump_points'] = p_df['exp_ps_raw'] * self.ump_boost
        p_df['points'] = \
            (p_df['venue_points'] * fw['venue_p']) + \
                (p_df['temp_points'] * fw['temp_p']) + (p_df['ump_points'] * fw['ump_p'])
        p_df['lu_mu'] = h_df['exp_pc_sp_raw'].sum()
        if self.confirmed_sp:
            with open(data_path, "wb") as file:
                pickle.dump(p_df, file)
        return p_df
    
    
x = 2
try:
    x = 'x'
    int('x')
    
    
    print(x)
except StopIteration:
    print('error')