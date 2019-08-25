import csv
import os
import re
import random

from abc import ABC, abstractmethod

from hierarchy_traversal import HierarchyTraversal

random.seed(42)


class TaskCreator(ABC):
    ANALOGY_TASK_PREFIX = "analogy"
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

    @staticmethod
    def get_node(root_node, path):
        """
            Traverses the path from the given node
        :param root_node: must be root node of graph
        :param path: path to traverse from root node
        :return: None, if node could not be found
        """
        split_path = path.split("/")
        return TaskCreator.get_node_split_path(root_node, split_path)

    @staticmethod
    def get_node_split_path(root_node, split_path):
        """
            Traverses the path from the given node
        :param root_node: must be root node of graph
        :param path: path to traverse from root node
        :return: None, if node could not be found
        """
        stack = [root_node]
        level = 1

        assert split_path[0] == "root", "split_path must start at root"
        if len(split_path) == 1:
            return root_node

        while stack:
            node = stack.pop()
            for child in node.children:
                predicate = HierarchyTraversal.extract_wikidata_id(child.label[0].value)
                object_ = HierarchyTraversal.extract_wikidata_id(child.label[1].value)

                if predicate == split_path[level] and object_ == split_path[level + 1]:
                    stack.append(child)
                    break
            if not stack:
                return None
            if level + 2 == len(split_path):
                return stack.pop()
            level += 2

        return None

    @staticmethod
    def get_random_entity(node, objects_to_exclude, entities_to_exclude):
        """
        Returns a random entity from a random leaf. The leaf is searched starting from from node
        :param node:
        :param objects_to_exclude: (set) Object entities along path to be excluded
        :param entities_to_exclude: (set) Returned entity must not be in entities_to_exclude of UriReturnType
        :return: Entity
        """
        stack = [node]
        while stack:
            current_node = stack.pop()
            if current_node.is_leaf():
                # select random outlier
                values = list(current_node.values - entities_to_exclude)
                if values:
                    return HierarchyTraversal.extract_wikidata_id(values[random.randint(0, len(values) - 1)].value)
                else:
                    continue
            # try to select a child along a different path
            # select random child with object != split_path[level + 1]
            random_children = TaskCreator.select_random_children(current_node, objects_to_exclude)
            if random_children:
                stack.extend(random_children)

        return None

    @staticmethod
    def select_random_children(node, objects_to_exclude):
        rdf_objects = [child for child in node.children if
                       HierarchyTraversal.extract_wikidata_id(child.label[1].value) not in objects_to_exclude]
        if not rdf_objects:
            return None
        random.shuffle(rdf_objects)
        return rdf_objects


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
            parent = TaskCreator.get_node_split_path(self.root_node, split_path[:path_length - i])
            entity2 = TaskCreator.get_random_entity(parent, split_path[path_length - i + 1::2], node.values)
            if entity2:
                content.append([entity1, entity2, group_id, rank])

        if len(content) > 2:
            TaskCreator.save_to_file(self.filename_from_path(path), content)


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
                outlier = TaskCreator.get_random_entity(self.root_node, set(path.split("/")[2::2]), node.values)
                if outlier:
                    entities_group.append([outlier, cluster_id, True])  # entity, group_id, is_outlier
                    content.extend(entities_group)
                    cluster_id += 1
                entities_group.clear()

        if len(content) > 3:
            TaskCreator.save_to_file(self.filename_from_path(path), content)


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

    def __init__(self, output_dir, wikidata_ids, max_analogies=40):
        super().__init__(output_dir)
        self._PREFIX = TaskCreator.ANALOGY_TASK_PREFIX
        self._HEADER = ["a", "b"]
        self.wikidata_id_set = set(wikidata_ids)
        self._is_entity_pattern = re.compile("^Q[0-9]+$")
        self._MAX_ENTITIES_PER_OBJECT = 5
        self._max_analogies = max_analogies

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
        analogy_test_set = []
        for object_, subjects in object_subjects.items():
            start_index = random.randint(0, len(subjects) - 1)
            start_index = max(0, start_index - self._MAX_ENTITIES_PER_OBJECT)
            for i in range(start_index, min(start_index + self._MAX_ENTITIES_PER_OBJECT, len(subjects))):
                analogy_test_set.append([subjects[i], object_])

        if len(analogy_test_set) > 1:
            random.shuffle(analogy_test_set)
            self.save_to_file(self.filename_from_path(path), [self._HEADER] + analogy_test_set[:self._max_analogies])
