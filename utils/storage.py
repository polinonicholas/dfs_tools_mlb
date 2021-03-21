from dfs_tools_mlb import settings


def json_path(name='untitled.json'):
    settings.ARCHIVE_DIR.mkdir(exist_ok=True, parents=True)
    path = str(settings.ARCHIVE_DIR.joinpath(name))
    if not path.endswith('.json'):
        path += '.json'
    return path
def pickle_path(name='untitled.pickle'):
    settings.ARCHIVE_DIR.mkdir(exist_ok=True, parents=True)
    path = str(settings.ARCHIVE_DIR.joinpath(name))
    if not path.endswith('.pickle'):
        path += '.pickle'
    return path
    

from pathlib import Path
from dfs_tools_mlb.utils.time import time_frames as tf
import os
def clean_storage():
    for file in Path.iterdir(settings.STORAGE_DIR):
        if not file.is_dir() and not str(file).endswith(str(tf.today)):
            if not settings.storage_settings.get('archive_stats', False):
                    file.unlink()
            else:
                settings.ARCHIVE_DIR.mkdir(exist_ok=True, parents=True)
                old_path = file
                new_path = settings.ARCHIVE_DIR.joinpath(file.name)
                os.replace(old_path, new_path)
    return "Storage directory has been cleaned."
    



            
            
    