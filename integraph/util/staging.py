from .path import ensure_path
import os
import shutil


class StagingHelper:
    def __init__(self, root_dir):
        self.root_dir = ensure_path(root_dir)
        self.registry = {"root": self.root_dir}

    def __getitem__(self, dir_name):
        return self.get(dir_name)

    def get(self, dir_name):
        return self.registry[dir_name]

    def register(self, dir_name):
        self.registry[dir_name] = self.root_dir / dir_name
        return self

    def unregister(self, dir_name):
        if dir_name in self.registry:
            self.remove(dir_name)
            self.registry.pop(dir_name)
        return self

    def create(self, dir_name):
        if dir_name in self.registry:
            os.makedirs(self.registry[dir_name])
        return self

    def remove(self, dir_name):
        if dir_name in self.registry:
            if os.path.exists(self.registry[dir_name]):
                shutil.rmtree(self.registry[dir_name])
        return self

    def clean(self, dir_name):
        self.remove(dir_name)
        self.create(dir_name)
        return self
