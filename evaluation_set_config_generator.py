from attr import dataclass
from task_creator import TaskCreator
from argparse import ArgumentParser

import yaml
import os


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

    def to_yaml_entry(self):

        task_entries = []
        for task in self.tasks:
            task_entries.append(task.to_yaml_entry())

        category_entries = []
        for category in self.categories.values():
            category_entries.append(category.to_yaml_entry())

        yaml_dict = dict()
        yaml_dict["name"] = self.name
        yaml_dict["enabled"] = self.enabled
        yaml_dict["tasks"] = task_entries
        yaml_dict["categories"] = category_entries

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
        return [Task(name=f"Analogy: {task_name}", type="analogy", test_set=file_path)]

    @staticmethod
    def build_category_tree(root_dir):
        root_category = Category(name="root", enabled=True, categories={}, tasks=[])
        # dict_hierarchy = dict()
        for root, dirs, files in os.walk(root_dir, topdown=False):
            for file in files:
                if file.endswith('.csv'):
                    path = root + '/' + file
                    print(path)
                    split_path = path.split('/')
                    previous_category = root_category
                    for i in range(1, len(split_path) - 1):
                        # name P123(Q666)
                        category_key = split_path[i]
                        current_category = previous_category.categories.get(category_key, None)
                        if current_category is None:
                            current_category = Category(name=category_key, enabled=True, categories={}, tasks=[])
                            previous_category.categories[category_key] = current_category
                        previous_category = current_category

                    filename = split_path[-1].split('.csv')[0]
                    task_name = filename.split('_')[1]
                    key = filename

                    if TaskCreator.OUTLIER_TASK_PREFIX in file:
                        tasks = EvaluationSetConfigGenerator.create_outlier_tasks(task_name, path)
                    elif TaskCreator.NEIGHBORHOOD_TASK_PREFIX in file:
                        tasks = EvaluationSetConfigGenerator.create_neighborhood_tasks(task_name, path)
                    elif TaskCreator.ANOLOGY_TASK_PREFIX in file:
                        tasks = EvaluationSetConfigGenerator.create_analogy_task(task_name, path)
                    else:
                        continue

                    # Kategorie, zu der das testset hinzugefügt werden soll
                    deepest_category = previous_category.categories.get(key, None)
                    if deepest_category is None:
                        deepest_category = Category(name=key, enabled=True, categories={}, tasks=tasks)
                        previous_category.categories[key] = deepest_category
                    else:
                        previous_category.categories[key].tasks.extend(tasks)

        return root_category

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

        for category in root_node.categories.values():
            yaml_dict["configuration"]["categories"].append(category.to_yaml_entry())

        with open(filename, "w+") as file:
            yaml.dump(yaml_dict, file, default_flow_style=False)


def setup_arguments(parser):
    parser.add_argument('--evaluation_data_dir', type=str, required=True)
    parser.add_argument('--save_to_config', type=str, required=True)


if __name__ == '__main__':
    parser = ArgumentParser()
    setup_arguments(parser)
    args = parser.parse_args()

    # EvaluationSetConfigGenerator.build_from_file_system('__living_people_10000__')
    EvaluationSetConfigGenerator.build_from_file_system(args.evaluation_data_dir, args.save_to_config)
