from dfs_tools_mlb.compile import teams
import pandas
pandas.set_option('display.max_columns', None)
t = teams.angels
o = teams.angels
from dfs_tools_mlb.compile.teams import Team
Team.team_notes(t, o, extend = True)

active = ['blue jays', 'padres', 'astros', 'brewers', 'marlins', 'mariners',
       'white sox', 'orioles', 'rays', 'twins', 'reds', 'yankees',
       'nationals', 'red sox', 'royals', 'tigers', 'phillies', 'rockies',
       'rangers', 'pirates']

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
            
         

custom_stacks = {}

custom_pitchers = {}


never_pair_sec = {'yankees': ['angels', 'phillies','rangers'],
                  'angels': ['yankees', 'phillies','rangers'],
                  'phillies': ['angels', 'yankees','rangers'],
                  'rangers': ['yankees', 'angels', 'phillies', 'blue jays']}

no_combine = {}
always_pair_first_sec = {}

never_pair = {}
no_combos = {}
always_pair_first = {'74502-68523': ['royals', 'marlins'],
                     }

all_in = ['74998-163666', '74998-79082',
          '74998-21853', '74998-60672',
          '74998-12113', '74998-82536',
          '74998-12948', '74998-13303',
          '74998-14000',]

supreme_all_in = {}

exempt = []

custom_counts={}

never_fill = [],
no_utility = ['74998-163666'],
complete_fade = ['rockies', 'tigers', 'red sox'],
stack_only = []
never_replace_secondary=[],
never_replace_primary=[],
remove_positions = {'74998-13567': '1b',
                    '74998-79082': 'of',
                    
                    }

no_stack_replace = [],
always_replace = [],

custom_stack_order = {},

factor_salary_secondary = False,
stack_salary_pairing_cutoff=2,
high_pitcher_salary = 0
low_pitcher_salary = 0
median_team_salary=0

0       Shohei Ohtani
1          Mike Trout
2         Jared Walsh
3      Anthony Rendon
4       Brandon Marsh
5          Max Stassi
6          Jose Rojas
7          Tyler Wade
8    Andrew Velazquez

[660271, 545361, 665120, 543685, 669016, 435559, 642180, 642180, 664058]

games = []
games[-1]