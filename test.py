from dfs_tools_mlb.compile.fanduel import FDSlate
import pandas as pd
expensive = []

cheap = []

nev = {}

playing = []

for t in expensive:
    nev[t] = expensive
for t in cheap:
    nev[t]= cheap

# nev[] = cheap
# nev[] = expensive

always_pair_first_sec = {
    }

lus=23
all_in = []
supreme_all_in = []
no_supreme_replace = []
never_pair = {}
never_pair_sec = nev

complete_fade = []
h_fades = []
stack_only = []
no_combos = []
always_replace = []
p_fades = []
no_utility = []
custom_stack_order = {
                      }
remove_positions = {}
custom_counts={}
custom_stack_size = {}
custom_stacks = {'giants': 23
 }
custom_secondary =  {'twins': 8, 'pirates': 7, 'rangers': 8}

custom_pitchers = {4: 23}
supreme_all_in_pos=["1b", "util", "2b", "of", "of.1", "of.2", "3b", "ss"]
dupe_lu_replacement_pos=["1b", "util", "2b", "of", "of.1", "of.2", "3b", "ss"]
index_track = 0
max_lu_total = 70
of_count_adjust = 5
stack_size = 4
stack_sample = 5
stack_max_pair = 4
min_avg_post_stacks = 2600
supreme_replace_order = 0
max_order = 9
all_in_replace_order_stack = 6
all_in_replace_order_sec = 5
all_in_auto = False
all_in_auto_max_order = 4
replace_sec_all_in = True
replace_primary_all_in = True
first_max_order = 5
second_max_order = 9
ss_max_order = 9
third_max_order = 9
of_max_order = 9
util_max_order = 4
non_stack_max_order = 7
single_surplus = 0
double_surplus = 0
pitcher_surplus = 0
enforce_pitcher_surplus = True
enforce_hitter_surplus = False
low_pitcher_salary=None
median_pitcher_salary=None
high_pitcher_salary=None
max_pitcher_salary=None
median_team_salary=None
low_stack_salary=None
high_stack_salary=None
max_stack_salary=None
no_surplus_secondary_stacks=True
stack_sal_pair_cutoff = 4
p_sal_stack_filter=False
factor_salary_secondary=True
info_only = False

slate_number = 1

stack_threshold = 50
pitcher_threshold = 40
heavy_weight_stack = True
heavy_weight_p = True
stack_salary_factor = True
salary_batting_order = True

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
s = FDSlate(
    slate_number=slate_number,
    lineups=lus,
    p_fades=p_fades,
    h_fades=h_fades,
    stack_threshold=stack_threshold,
    pitcher_threshold=pitcher_threshold,
    heavy_weight_stack=heavy_weight_stack,
    heavy_weight_p=heavy_weight_p,
    stack_salary_factor=stack_salary_factor,
    salary_batting_order=salary_batting_order,

)

hitters = s.get_hitters()
pitchers = s.get_pitchers()


# pitchers.columns.tolist()
lineups = s.build_lineups(
    info_only=info_only,
    index_track = index_track,
    lus=lus,
    max_lu_total=max_lu_total,
    of_count_adjust=of_count_adjust,
    min_avg_post_stack=min_avg_post_stacks,
    stack_size=stack_size,
    stack_sample=stack_sample,
    stack_max_pair=stack_max_pair,
    max_order=max_order,
    all_in_replace_order_stack=all_in_replace_order_stack,
    all_in_replace_order_sec=all_in_replace_order_sec,
    all_in_auto=all_in_auto,
    all_in_auto_max_order = all_in_auto_max_order,
    replace_secondary_all_in=replace_sec_all_in,
    replace_primary_all_in=replace_primary_all_in,
    supreme_replace_order=supreme_replace_order,
    non_stack_max_order=non_stack_max_order,
    first_max_order=first_max_order,
    second_max_order=second_max_order,
    ss_max_order=ss_max_order,
    third_max_order=third_max_order,
    of_max_order=of_max_order,
    util_max_order=util_max_order,
    single_stack_surplus=single_surplus,
    double_stack_surplus=double_surplus,
    pitcher_surplus=pitcher_surplus,
    enforce_pitcher_surplus=enforce_pitcher_surplus,
    enforce_hitter_surplus=enforce_hitter_surplus,
    median_pitcher_salary=median_pitcher_salary,
    low_pitcher_salary=low_pitcher_salary,
    high_pitcher_salary=high_pitcher_salary,
    max_pitcher_salary=max_pitcher_salary,
    median_team_salary=median_team_salary,
    low_stack_salary=low_stack_salary,
    high_stack_salary=high_stack_salary,
    max_stack_salary=max_stack_salary,
    no_surplus_secondary_stacks=no_surplus_secondary_stacks,
    p_sal_stack_filter=p_sal_stack_filter,
    stack_salary_pairing_cutoff=stack_sal_pair_cutoff,
    factor_salary_secondary=factor_salary_secondary,
    all_in=all_in,
    supreme_all_in=supreme_all_in,
    no_supreme_replace=no_supreme_replace,
    no_utility=no_utility,
    complete_fade=complete_fade,
    remove_positions=remove_positions,
    custom_stacks=custom_stacks,
    custom_pitchers=custom_pitchers,
    custom_secondary=custom_secondary,
    custom_stack_size = custom_stack_size,
    custom_stack_order=custom_stack_order,
    never_pair_sec=never_pair_sec,
    never_pair = never_pair,
    supreme_all_in_pos=supreme_all_in_pos,
    dupe_lu_replacement_pos=dupe_lu_replacement_pos,
    stack_only = stack_only,
    always_pair_first_sec = always_pair_first_sec,
    no_combos = no_combos,
    always_replace = always_replace,
    fill_high_salary_first = False
)

try:
    track_df = pd.DataFrame(lineups)
    track_df.loc["combos", "twins"]
except:
    pass
# AFTER BUILDING LINEUPS
pc_df = s.p_counts()
pc_index = pc_df["t_count"].nlargest(60).index
hc_df = s.h_counts()
hc_index = hc_df["t_count"].nlargest(60).index
if not info_only:
    pitcher_counts = pc_df.loc[
        pc_index,
        [
            "name",
            "t_count",
            "fd_salary",
            "team",
            "fd_id",
            "points",
            "pitches_start",
            "batters_faced_sp",
            "exp_ps_raw",
        ],
    ]
    hitter_counts = hc_df.loc[
        hc_index,
        [
            "name",
            "t_count",
            "fd_salary",
            "order",
            "fd_position",
            "fd_id",
            "team",
            "points",
            "fd_wps_pa",
        ],
    ].set_index("name")
    
    
    
    
    s.finalize_entries()
s.active_teams
s.positional_counts()




all_stacks = s.points_df()[
[
    "raw_points",
    "points",
    "salary",
    "sp_mu",
    "raw_talent",
    "ump_avg",
    "venue_avg",
    "env_avg",
    "sp_avg",
    "mz",
    "z",
]]

pitchers.columns.tolist()
pitchers['name']


s.stacks_df()['stacks']
