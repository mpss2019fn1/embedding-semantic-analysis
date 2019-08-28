from attr import dataclass
from task_creator import TaskCreator
from argparse import ArgumentParser

import yaml
import os
import re


@dataclass
class TaskConfiguration:
    type: str
    enabled: bool

    def to_yaml_entry(self):
        yaml_dict = dict()
        yaml_dict["type"] = self.type
        yaml_dict["enabled"] = self.enabled

        return {"task_configuration": yaml_dict}


@dataclass
class Task:
    name: str
    type: str
    test_set: str

    def to_yaml_entry(self):
        yaml_dict = dict()
        yaml_dict["name"] = self.name
        yaml_dict["type"] = self.type
        yaml_dict["test_set"] = self.test_set

        return {"task": yaml_dict}


@dataclass
class Category:
    name: str
    enabled: bool
    categories: dict
    tasks: list
    entities: str

    def to_yaml_entry(self):

        task_entries = []
        for task in self.tasks:
            task_entries.append(task.to_yaml_entry())

        category_entries = []
        for category in self.categories.values():
            if category.entities:
                category_entries.append(category.to_yaml_entry())

        yaml_dict = dict()
        yaml_dict["name"] = self.name
        yaml_dict["enabled"] = self.enabled
        yaml_dict["tasks"] = task_entries
        yaml_dict["categories"] = category_entries
        yaml_dict["entities"] = self.entities

        return {"category": yaml_dict}


class EvaluationSetConfigGenerator:

    def __init__(self):
        pass

    @staticmethod
    def default_task_configurations():
        task_configurations = [TaskConfiguration(type="cosine_similarity", enabled=True),
                               TaskConfiguration(type="euclidean_similarity", enabled=True),
                               TaskConfiguration(type="analogy", enabled=True),
                               TaskConfiguration(type="cosine_neighborhood", enabled=True),
                               TaskConfiguration(type="euclidean_neighborhood", enabled=True),
                               TaskConfiguration(type="cosine_outlier_detection", enabled=True),
                               TaskConfiguration(type="euclidean_outlier_detection", enabled=True)]
        return task_configurations

    @staticmethod
    def create_neighborhood_tasks(task_name, file_path):
        return [Task(name=f"{task_name}", type="cosine_neighborhood", test_set=file_path),
                Task(name=f"{task_name}", type="euclidean_neighborhood", test_set=file_path)]

    @staticmethod
    def create_similarity_tasks(task_name, file_path):
        return [Task(name=f"{task_name}", type="cosine_similarity", test_set=file_path),
                Task(name=f"{task_name}", type="euclidean_similarity", test_set=file_path)]

    @staticmethod
    def create_outlier_tasks(task_name, file_path):
        return [Task(name=f"{task_name}", type="cosine_outlier_detection", test_set=file_path),
                Task(name=f"{task_name}", type="euclidean_outlier_detection", test_set=file_path)]

    @staticmethod
    def create_analogy_task(task_name, file_path):
        return [Task(name=f"{task_name}", type="analogy", test_set=file_path)]

    @staticmethod
    def build_category_tree(root_dir):
        root_category = Category(name="root", enabled=True, categories={}, tasks=[], entities="")
        entitiy_files = []
        # dict_hierarchy = dict()
        for root, dirs, files in os.walk(root_dir, topdown=False):
            for file in files:
                if re.match("[a-zA-Z]+_([P|Q][0-9]+|root).csv$", file):
                    path = os.path.join(root, file)
                    # path = root + '/' + file
                    print(path)
                    split_path = path.split('/')
                    previous_category = root_category
                    for i in range(1, len(split_path) - 1):
                        # name P123(Q666)
                        category_key = split_path[i]
                        current_category = previous_category.categories.get(category_key, None)
                        if current_category is None:
                            current_category = Category(name=category_key, enabled=True, categories={}, tasks=[],
                                                        entities="")
                            previous_category.categories[category_key] = current_category
                        previous_category = current_category

                    filename = split_path[-1].split('.csv')[0]
                    task_name = filename.split('_')[1]
                    key = filename.split('_')[1]

                    if TaskCreator.OUTLIER_TASK_PREFIX in file:
                        tasks = EvaluationSetConfigGenerator.create_outlier_tasks(task_name, path)
                    elif TaskCreator.NEIGHBORHOOD_TASK_PREFIX in file:
                        tasks = EvaluationSetConfigGenerator.create_neighborhood_tasks(task_name, path)
                    elif TaskCreator.ANALOGY_TASK_PREFIX in file:
                        tasks = EvaluationSetConfigGenerator.create_analogy_task(task_name, path)
                    elif TaskCreator.SIMILARITY_TASK_PREFIX in file:
                        tasks = EvaluationSetConfigGenerator.create_similarity_tasks(task_name, path)
                    elif TaskCreator.ENTITY_COLLECTOR_TASK_PREFIX in file:
                        # hole Kategorie, zu der entity file gehört.
                        entitiy_files.append(path)
                        continue
                    else:
                        continue

                    # Kategorie, zu der das testset hinzugefügt werden soll
                    deepest_category = previous_category.categories.get(key, None)
                    if deepest_category is None:
                        deepest_category = Category(name=key, enabled=True, categories={}, tasks=tasks,
                                                    entities="")
                        previous_category.categories[key] = deepest_category
                    else:
                        if len(tasks) > 0:
                            previous_category.categories[key].tasks.extend(tasks)


        # set entity files
        for entity_file_path in entitiy_files:
            index_of_root = entity_file_path.find("root")
            assert index_of_root >= 0
            split_path = entity_file_path[31:].split('/')
            split_path[-1] = split_path[-1][9:-4]
            category = root_category
            for category_name in split_path:
                category = category.categories[category_name]
            EvaluationSetConfigGenerator._set_entities_file(category, entity_file_path)

        return root_category

    @staticmethod
    def _set_entities_file(parent, entities_file_path):
        for category1 in parent.categories.values():
            assert category1.name[0] == "P", "category1 should correspond to a property"
            for category2 in category1.categories.values():
                # assert category2.name[0] == "Q" or category2.name[0] == "M", "category2 should correspond to an entity"
                category2.entities = entities_file_path
            category1.entities = entities_file_path

    @staticmethod
    def build_from_file_system(evaluation_data_dir, filename):
        """

        :param evaluation_data_dir: root directory of directory containing test sets
        :param filename: name of yaml file to save configuration to
        :return: Nothing
        """
        task_configurations = EvaluationSetConfigGenerator.default_task_configurations()
        root_node = EvaluationSetConfigGenerator.build_category_tree(evaluation_data_dir)

        yaml_dict = dict()

        yaml_dict["configuration"] = dict()
        yaml_dict["configuration"]["task_configurations"] = \
            [task_configuration.to_yaml_entry() for task_configuration in task_configurations]
        yaml_dict["configuration"]["categories"] = []

        root_node.categories['root'].entities = os.path.join(evaluation_data_dir, "entities_root.csv")

        for category in root_node.categories.values():
            yaml_dict["configuration"]["categories"].append(category.to_yaml_entry())

        with open(filename, "w+") as file:
            yaml.dump(yaml_dict, file, default_flow_style=False)


def setup_arguments(parser):
    parser.add_argument('--evaluation-data-dir', type=str, required=True)
    parser.add_argument('--save-to-config', type=str, required=True)


def main():
    parser = ArgumentParser()
    setup_arguments(parser)
    args = parser.parse_args()

    EvaluationSetConfigGenerator.build_from_file_system(args.evaluation_data_dir, args.save_to_config)


if __name__ == '__main__':
    main()
