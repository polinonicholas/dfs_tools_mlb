from dfs_tools_mlb.compile.teams import Team
from dfs_tools_mlb import settings
from pathlib import Path

for team in Team:
    myfile = Path(f"{settings.TEAM_DIR.joinpath(team.name)}.py")
    myfile.touch(exist_ok=True)
