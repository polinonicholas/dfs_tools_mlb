from dfs_tools_mlb.compile import teams
from dfs_tools_mlb.compile.fanduel import FDSlate as fds
from dfs_tools_mlb import settings
f = fds(slate_number = settings.current_fd_slate_number)
active = f.active_teams
active_teams = [t for t in teams.Team if t.name in active and not t.ppd]
opposing_sp = {}
bp_used = {}
no_rest = {}
no_rest["home"] = []
no_rest["away"] = []
for t in active_teams:
    try:
        opposing_sp[t.opp_sp["fullName"]] = {}
        opposing_sp[t.opp_sp["fullName"]]["id"] = t.opp_sp["id"]
        opposing_sp[t.opp_sp["fullName"]]["notes"] = None
        
        
    except:
        continue

for t in active_teams:
    bp_used[t.name] = len(t.used_rp)

def print_all_active_lu():
    for t in active_teams:
            print(t.lineup_df()[["name", "mlb_id"]])

for t in active_teams:
    if t.no_rest_travel:
        if t.is_home:
            no_rest["home"].append(t.name)
        else:
            no_rest["away"].append(t.name)

