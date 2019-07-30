import csv
import os

from abc import ABC, abstractmethod

from hierarchy_traversal import HierarchyTraversal


class TaskCreator(ABC):

    def __init__(self):
        self._PREFIX = ""

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

    def filename_from_path(self, path):
        filename = path.split("/")[-1]
        split_path = path.split("/")[:-1]
        path = ""
        if split_path:
            path = "/".join(split_path) + "/"

        path = path + self._PREFIX + "_" + filename + ".csv"

        return path


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

        TaskCreator.save_to_file(self.filename_from_path(path), content)


class SimilarityTaskCreator(TaskCreator):
    pass


class OutlierTaskCreator(TaskCreator):

    def __init__(self, hierarchy, max_group_size=5):
        super().__init__()
        self._HEADER = ["entity", "group_id", "is_similar"]
        self._PREFIX = "outlier"
        self.max_group_size = max_group_size
        self.root_node = hierarchy.root_node

    def process_node(self, path, node, entities):
        cluster_id = 0
        is_similar = True

        if not node or not node.is_leaf():
            return

        content = [self._HEADER]

        entities_group = []
        for entity in entities:

            if len(entities_group) < self.max_group_size:
                entities_group.append([entity, cluster_id, is_similar])
            else:
                entities_group.append([self.get_outlier(path), cluster_id, not is_similar])
                content.extend(entities_group)
                entities_group.clear()
                cluster_id += 1

        if len(content) > 1:
            TaskCreator.save_to_file(self.filename_from_path(path), content)

    def get_outlier(self, path):
        split_path = path.split('/')
        level = 1
        stack = [self.root_node]
        outlier_exists = False
        while stack:
            current_node = stack.pop()
            if current_node.is_leaf():
                if outlier_exists:
                    for entity in current_node.values:
                        break
                    wikidata_id = HierarchyTraversal.extract_wikidata_id(entity.value)
                    return wikidata_id
                else:
                    return None

            for child in current_node.children:
                rdf_object = HierarchyTraversal.extract_wikidata_id(child.label[1].value)
                if level + 1 >= len(split_path) or rdf_object != split_path[level+1]:
                    outlier_exists = True
                    stack.append(child)
                    break

            if not stack and outlier_exists:
                stack.append(current_node.children[0])
            level += 2
        return None
