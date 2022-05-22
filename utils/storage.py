from dfs_tools_mlb import settings


def json_path(name="untitled.json", directory=settings.ARCHIVE_DIR):
    directory.mkdir(exist_ok=True, parents=True)
    path = str(directory.joinpath(name))
    if not path.endswith(".json"):
        path += ".json"
    return path


def pickle_path(name="untitled.pickle", directory=settings.ARCHIVE_DIR):
    try:
        directory.mkdir(exist_ok=True, parents=True)
    except PermissionError:
        pass
    path = str(directory.joinpath(name))
    if not path.endswith(".pickle"):
        path += ".pickle"
    return path


from pathlib import Path
from dfs_tools_mlb.utils.time import time_frames as tf
import os


def clean_directory(directory, force_delete=False):
    if not directory.exists():
        directory.mkdir(parents=True)
    for file in Path.iterdir(directory):
        if not file.is_dir() and not str(tf.today) in str(file):
            if (
                not settings.storage_settings.get("archive_stats", False)
                or force_delete
            ):
                file.unlink()
            else:
                settings.ARCHIVE_DIR.mkdir(exist_ok=True, parents=True)
                old_path = file
                new_path = settings.ARCHIVE_DIR.joinpath(file.name)
                os.replace(old_path, new_path)
    return "Directory has been cleaned."
