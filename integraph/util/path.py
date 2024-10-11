from pathlib import Path


def ensure_path(path):
    if isinstance(path, str):
        return Path(path)
    else:
        return path
