import csv
import os

from abc import ABC, abstractmethod

from hierarchy_traversal import HierarchyTraversal


class TaskCreator(ABC):

    ANOLOGY_TASK_PREFIX = "anology"
    NEIGHBORHOOD_TASK_PREFIX = "neighborhood"
    OUTLIER_TASK_PREFIX = "outlier"

    def __init__(self):
        self._PREFIX = ""

    @abstractmethod
    def process_node(self, path, node, entities, is_predicate):
        """

        :param path: path of node (or property if is_property = true
        :param node: node currently be processed by the hierarchy_traversal
        :param entities: Contains all entities in path
        :param is_predicate: True, if a specific property of node has been selected
        :return: nothing
        """
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
        self._PREFIX = TaskCreator.NEIGHBORHOOD_TASK_PREFIX

    def process_node(self, path, node, entities, is_predicate):
        cluster_id = 0
        is_similar = True

        content = [self._HEADER]

        for entity in entities:
            content.append([entity, cluster_id, is_similar])

        TaskCreator.save_to_file(self.filename_from_path(path), content)


class SimilarityTaskCreator(TaskCreator):

    def __init__(self):
        super.__init__()
        self._PREFIX = "similarity"

    def process_node(self, path, node, entities, is_predicate):
        pass


class OutlierTaskCreator(TaskCreator):

    def __init__(self, hierarchy, max_group_size=5):
        super().__init__()
        self._HEADER = ["entity", "group_id", "is_similar"]
        self._PREFIX = TaskCreator.OUTLIER_TASK_PREFIX
        self.max_group_size = max_group_size
        self.root_node = hierarchy.root_node

    def process_node(self, path, node, entities, is_predicate):
        cluster_id = 0
        is_similar = True

        # node none means that we are being passed all entities to a predicate (for example: all entities having a
        # sex or gender,
        # We have to identify predicate calls
        if is_predicate or not node.is_leaf():
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


class AnalogyTaskCreator(TaskCreator):

    def __init__(self):
        super().__init__()
        self._PREFIX = TaskCreator.ANOLOGY_TASK_PREFIX
        self._HEADER = ["a", "b"]

    def process_node(self, path, node, entities, is_predicate):
        if not is_predicate:
            return

        predicate = path.split("/")[-1]

        # selektiere alle Kinder mit property
        # Matche alle Kinder
        # Bekomme alle Entitäten mit sex or gender. Das hilft mir nur nicht, weil cih nicht weiß, welche nun mails sind.
        # Also muss ich ggf. alle Males selektieren
        # bekomme ich das leichter hin an einer anderen Stelle? Ich denke nicht.

        anology_test_set = [self._HEADER]

        for child in node.children:
            child_predicate = HierarchyTraversal.extract_wikidata_id(child.label[0].value)
            child_object = HierarchyTraversal.extract_wikidata_id(child.label[1].value)
            if child_predicate == predicate:
                for entity in child.values:
                    anology_test_set.append([HierarchyTraversal.extract_wikidata_id(entity.value),
                                             HierarchyTraversal.extract_wikidata_id(child_object)])

        if len(anology_test_set) > 1:
            self.save_to_file(self.filename_from_path(path), anology_test_set)
