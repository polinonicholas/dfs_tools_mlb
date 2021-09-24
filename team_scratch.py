from dfs_tools_mlb.compile import teams
import pandas
pandas.set_option('display.max_columns', None)
t = teams.royals
o = teams.white_sox
teams.Team.team_notes(t, o, extend = True)

active = ['indians', 'mets', 'brewers', 'dodgers', 'yankees', 'blue jays',
       'rays', 'white sox', 'red sox', 'marlins', 'astros', 'cardinals',
       'athletics', 'phillies', 'padres', 'angels', 'reds', 'braves',
       'rockies', 'tigers', 'diamondbacks', 'mariners', 'twins', 'royals',
       'orioles', 'rangers']
    

opposing_sp = {}
for t in teams.Team:
    if t.name in active:
        try:
            opposing_sp[t.opp_sp['fullName']] = {}
            opposing_sp[t.opp_sp['fullName']]['id'] = t.opp_sp['id']
        except:
            continue
bp_used = {}
for t in teams.Team:
    if t.name in active:
        bp_used[t.name] = len(t.used_rp)
        
for t in teams.Team:
    if t.name in active:
        print(t.lineup_df()[['name', 'mlb_id']])
  
no_rest = {}
no_rest['home'] = []
no_rest['away'] = []
for t in teams.Team:
    if t.name in active and t.no_rest_travel:
        if t.is_home:
            no_rest['home'].append(t.name)
        else:
            no_rest['away'].append(t.name)
            
         
# sea = [641487, 571745, 572122, 664034, 647351, 620443, 672284, 608596, 664238]

 