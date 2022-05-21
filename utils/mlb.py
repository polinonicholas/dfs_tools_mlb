from dfs_tools_mlb.compile.static_mlb import team_info
import json
from dfs_tools_mlb.utils.storage import json_path
from json import JSONDecodeError

def generate_team_instances():
    instances = set()
    for k, v in team_info.items():
        value = k.replace(' ', '_') + ' = ' + f"Team(mlb_id = {v['mlb_id']}, name = '{k}')"
        print(value)
        instances.add(value)
    return instances

def team_lineups():
    path = json_path(name='team_lineups')
    try:
        with open(path) as file:
            team_lineups = json.load(file)
            file.close()
        return team_lineups
    except (FileNotFoundError, JSONDecodeError):
        team_lineups = {}
        for team in team_info.keys():
            team_lineups[team] = {'L': [], 'R': []}
        with open(path, "w+") as file:
            json.dump(team_lineups, file)
            file.close()
        return team_lineups
    
