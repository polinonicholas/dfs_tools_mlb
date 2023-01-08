from dfs_tools_mlb.compile.teams import Team
from dfs_tools_mlb import settings
from pathlib import Path
from dfs_tools_mlb.settings import reset_team_vars, reset_all_team_vars
for team in Team:
    team_file = Path(f"{settings.TEAM_DIR.joinpath(team.name)}.json")
    exists_bool = team_file.exists()
    team_file.touch(exist_ok=True)
    if reset_all_team_vars or not exists_bool or team.name in reset_team_vars:
        import shutil
        default_vars_path = Path(f"{settings.TEAM_DIR}/default_team_vars.json")
        shutil.copyfile(default_vars_path, team_file)
        
        

        
        
    
