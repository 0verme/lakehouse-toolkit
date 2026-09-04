import os
import sys


def ensure_project_root_on_path() -> str:
    # Support running cron scripts directly with `python path/to/script.py`.
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    return project_root
