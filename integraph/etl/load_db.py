from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from ..lib.load import connect, get_db
from ..util.path import ensure_path


class LoadDB:
    def __init__(self, root_dir, config):
        self.tg_id = "load"
        self.root_dir = ensure_path(root_dir)
        self.cfg = config

    def load(self, filepath):
        with TaskGroup(group_id=self.tg_id):
            connect_task = task(connect)(self.cfg)
            load_task = task(get_db(self.cfg))(filepath, connect_task, self.cfg)
            return load_task
