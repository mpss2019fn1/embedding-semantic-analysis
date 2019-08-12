import csv
import os
import re
import random

from abc import ABC, abstractmethod

from hierarchy_traversal import HierarchyTraversal

random.seed(42)


class TaskCreator(ABC):
    ANOLOGY_TASK_PREFIX = "analogy"
    NEIGHBORHOOD_TASK_PREFIX = "neighborhood"
    OUTLIER_TASK_PREFIX = "outlier"
    SIMILARITY_TASK_PREFIX = "similarity"
    ENTITY_COLLECTOR_TASK_PREFIX = "entities"

    def __init__(self, output_dir):
        self._PREFIX = ""
        self._output_dir = output_dir

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

        return os.path.join(self._output_dir, path)


class NeighborhoodTaskCreator(TaskCreator):

    def __init__(self, output_dir):
        super().__init__(output_dir)
        self._HEADER = ["entity", "group_id", "is_similar"]
        self._PREFIX = TaskCreator.NEIGHBORHOOD_TASK_PREFIX
        self._MAX_NEIGHBORHOOD_SIZE = 10

    def process_node(self, path, node, entities, is_predicate):
        cluster_id = 0
        is_similar = True

        content = [self._HEADER]

        shuffled_entities = []
        if len(entities) > self._MAX_NEIGHBORHOOD_SIZE:
            shuffled_entities.extend(entities)
        else:
            shuffled_entities = entities

        for entity in shuffled_entities:
            content.append([entity, cluster_id, is_similar])
            if (len(content) - 1) % self._MAX_NEIGHBORHOOD_SIZE == 0:
                cluster_id += 1

        if len(content) > 2:
            TaskCreator.save_to_file(self.filename_from_path(path), content)

    @staticmethod
    def _shuffle_entities(entities):
        for i in range(0, len(entities)):
            pos1 = random.randint(0, len(entities) - 1)
            pos2 = random.randint(0, len(entities) - 1)
            tmp = entities[pos2]
            entities[pos2] = entities[pos1]
            entities[pos1] = tmp


class SimilarityTaskCreator(TaskCreator):

    def __init__(self, output_dir, hierarchy):
        super().__init__(output_dir)
        self._HEADER = ["a", "b", "group_id", "rank"]
        self._PREFIX = TaskCreator.SIMILARITY_TASK_PREFIX
        self.root_node = hierarchy.root_node

    def process_node(self, path, node, entities, is_predicate):
        group_id = 0
        rank = 0

        if is_predicate:
            return

        if not node.is_leaf():
            return

        if len(node.values) < 2:
            return

        content = [self._HEADER]
        entity1 = HierarchyTraversal.extract_wikidata_id(node.element_at(0).value)
        entity2 = HierarchyTraversal.extract_wikidata_id(node.element_at(1).value)
        content.append([entity1, entity2, group_id, rank])
        split_path = path.split('/')
        path_length = len(split_path)
        for i in range(2, path_length, 2):
            rank += 1
            upper_path_list = split_path[:path_length - i]
            lower_path_list = split_path[path_length - i + 1: path_length]
            node = self.get_similarity_node(upper_path_list, lower_path_list)
            if node:
                entity2 = HierarchyTraversal.extract_wikidata_id(node.element_at(0).value)
                content.append([entity1, entity2, group_id, rank])

        if len(content) > 2:
            TaskCreator.save_to_file(self.filename_from_path(path), content)

    def get_similarity_node(self, upper_path_list, lower_path_list):
        level = 1
        stack = [self.root_node]
        while stack:
            if level >= len(upper_path_list):
                break
            current_node = stack.pop()
            for child in current_node.children:
                child_predicate = HierarchyTraversal.extract_wikidata_id(child.label[0].value)
                child_object = HierarchyTraversal.extract_wikidata_id(child.label[1].value)
                if child_predicate == upper_path_list[level] and child_object == upper_path_list[level + 1]:
                    stack.append(child)
                    level += 2
                    break

        lower_path_object_set = set(lower_path_list[::2])
        while stack:
            current_node = stack.pop()
            if current_node.is_leaf():
                return current_node
            for child in current_node.children:
                child_object = HierarchyTraversal.extract_wikidata_id(child.label[1].value)
                if child_object not in lower_path_object_set:
                    stack.append(child)

        return None


class OutlierTaskCreator(TaskCreator):

    def __init__(self, output_dir, hierarchy, max_group_size=5):
        super().__init__(output_dir)
        self._HEADER = ["entity", "group_id", "is_outlier"]
        self._PREFIX = TaskCreator.OUTLIER_TASK_PREFIX
        self.max_group_size = max_group_size
        self.root_node = hierarchy.root_node

    def process_node(self, path, node, entities, is_predicate):
        cluster_id = 0

        # node none means that we are being passed all entities to a predicate (for example: all entities having a
        # sex or gender,
        # We have to identify predicate calls
        if is_predicate or not node.is_leaf():
            return

        content = [self._HEADER]

        entities_group = []
        for entity in entities:
            entities_group.append([entity, cluster_id, False])  # entity, group_id, is_outlier
            if len(entities_group) == self.max_group_size - 1:
                outlier = self.get_outlier(path)
                if outlier:
                    entities_group.append([outlier, cluster_id, True])  # entity, group_id, is_outlier
                    content.extend(entities_group)
                    cluster_id += 1
                entities_group.clear()

        if len(content) > 3:
            TaskCreator.save_to_file(self.filename_from_path(path), content)

    def get_outlier(self, path):
        split_path = path.split('/')
        level = 1  # level of first predicate by which was split
        stack = [self.root_node]
        outlier_exists = False
        while stack:
            current_node = stack.pop()
            if current_node.is_leaf():
                if outlier_exists:
                    # select random outlier
                    values_as_list = list(current_node.values)
                    i = random.randint(0, len(values_as_list) - 1)
                    wikidata_id = HierarchyTraversal.extract_wikidata_id(values_as_list[i].value)
                    return wikidata_id
                else:
                    return None

            # try to select a child along a different path
            # select random child with object != split_path[level + 1]
            object_to_exclude = ""
            if level < len(split_path):
                object_to_exclude = split_path[level + 1]  # object in path
            child, on_different_path = OutlierTaskCreator._select_random_child(current_node, object_to_exclude)
            outlier_exists = outlier_exists or on_different_path
            stack.append(child)

            level += 2

        raise Exception("This line should never be reached")

    @staticmethod
    def _select_random_child(node, object_to_exclude):
        child_count = len(node.children)
        assert child_count > 0, "node must have at least one child"

        rdf_objects = [HierarchyTraversal.extract_wikidata_id(child.label[1].value) for child in node.children]

        selected_index = -1

        if child_count == 1:
            selected_index = 0
        else:
            while selected_index < 0:
                i = random.randint(0, child_count - 1)
                if rdf_objects[i] != object_to_exclude:
                    selected_index = i

        return node.children[selected_index], rdf_objects[selected_index] != object_to_exclude


class EntityCollectorTaskCreator(TaskCreator):

    def __init__(self, output_dir):
        super().__init__(output_dir)
        self._PREFIX = TaskCreator.ENTITY_COLLECTOR_TASK_PREFIX

    def process_node(self, path, node, entities, is_predicate):
        if is_predicate:
            return

        if node.is_leaf():
            return

        content = []

        for entity in node.values:
            content.append([HierarchyTraversal.extract_wikidata_id(entity.value)])

        TaskCreator.save_to_file(self.filename_from_path(path), content)


class AnalogyTaskCreator(TaskCreator):

    def __init__(self, output_dir, wikidata_ids):
        super().__init__(output_dir)
        self._PREFIX = TaskCreator.ANOLOGY_TASK_PREFIX
        self._HEADER = ["a", "b"]
        self.wikidata_id_set = set(wikidata_ids)
        self._is_entity_pattern = re.compile("^Q[0-9]+$")
        self._MAX_ENTITIES_PER_OBJECT = 5

    def process_node(self, path, node, entities, is_predicate):
        if not is_predicate:
            return

        predicate = path.split("/")[-1]

        # selektiere alle Kinder mit property
        # Matche alle Kinder
        # Bekomme alle Entitäten mit sex or gender. Das hilft mir nur nicht, weil cih nicht weiß, welche nun mails sind.
        # Also muss ich ggf. alle Males selektieren
        # bekomme ich das leichter hin an einer anderen Stelle? Ich denke nicht.

        object_subjects = dict()

        for child in node.children:
            child_predicate = HierarchyTraversal.extract_wikidata_id(child.label[0].value)
            if child_predicate != predicate:
                continue
            child_object = HierarchyTraversal.extract_wikidata_id(child.label[1].value)

            # match to pattern Q[0-9]+
            if not self._is_entity_pattern.match(child_object):
                continue
            if int(child_object[1:]) not in self.wikidata_id_set:
                continue

            for entity in child.values:
                subjects = object_subjects.get(child_object, None)
                if not subjects:
                    subjects = []
                    object_subjects[child_object] = subjects
                subjects.append(HierarchyTraversal.extract_wikidata_id(entity.value))

        # select at most self._MAX_ENTITIES_PER_OBJECT entities
        analogy_test_set = [self._HEADER]
        for object_, subjects in object_subjects.items():
            start_index = random.randint(0, len(subjects) - 1)
            start_index = max(0, start_index - self._MAX_ENTITIES_PER_OBJECT)
            for i in range(start_index, min(start_index + self._MAX_ENTITIES_PER_OBJECT, len(subjects))):
                analogy_test_set.append([subjects[i], object_])

        if len(analogy_test_set) > 2:
            self.save_to_file(self.filename_from_path(path), analogy_test_set)
