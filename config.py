import os
import glob
from dfs_tools_mlb import settings


def get_fd_file(DL_FOLDER=settings.DL_FOLDER):
    FD_FILE_MATCH = DL_FOLDER + "/FanDuel-MLB*entries-upload-template*"
    FD_FILES = glob.glob(FD_FILE_MATCH)
    try:
        FD_FILE = max(FD_FILES, key=os.path.getctime)
        return FD_FILE
    except ValueError:
        return f"There are no fanduel entries files in specified DL_FOLDER: {DL_FOLDER}, obtain one at fanduel.com/upcoming"

