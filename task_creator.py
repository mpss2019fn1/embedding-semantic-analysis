import csv
import os
import itertools

from abc import ABC, abstractmethod


class TaskCreator(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def process_node(self, path, node, entities):
        pass

    @staticmethod
    def save_to_file(filename, content):
        if '/' in filename:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, mode="w+") as f:
            writer = csv.writer(f)
            writer.writerows(content)


class NeighborhoodTaskCreator(TaskCreator):

    def __init__(self):
        super().__init__()
        self._HEADER = ["entity", "group_id", "is_similar"]
        self._PREFIX = "neighborhood"

    def process_node(self, path, node, entities):
        cluster_id = 0
        is_similar = True

        content = [self._HEADER]

        for entity in entities:
            content.append([entity, cluster_id, is_similar])

        filename = path.split("/")[-1]
        split_path = path.split("/")[:-1]
        path = ""
        if split_path:
            path = "/".join(split_path) + "/"

        path = path + self._PREFIX + "_" + filename + ".csv"

        TaskCreator.save_to_file(path, content)
