from attr import dataclass

import yaml
import os


@dataclass
class Task:
    name: str
    type: str
    test_set: str


@dataclass
class Category:
    name: str
    enabled: bool
    categories: dict
    tasks: list

    def to_yaml(self):
        return 'ok'


class EvaluationSetConfigGenerator:

    def __init__(self):
        pass

    @staticmethod
    def build_from_file_system(root_dir):
        root_category = Category(name="root", enabled=True, categories={}, tasks=[])
        # dict_hierarchy = dict()
        for root, dirs, files in os.walk(root_dir, topdown=False):
            for file in files:
                if file.endswith('.csv'):
                    path = root + '/' + file
                    #print(path)
                    split_path = path.split('/')
                    previous_category = root_category
                    for i in range(1, len(split_path) - 2):
                        category_key = f'{split_path[i]}({split_path[i + 1]})'
                        current_category = previous_category.categories.get(category_key, None)
                        if current_category is None:
                            current_category = Category(name=category_key, enabled=True, categories={}, tasks=[])
                            previous_category.categories[category_key] = current_category
                        previous_category = current_category

                    filename = split_path[-1].split('.csv')[0]
                    key = f'{split_path[-2]}({filename})'
                    previous_category.categories[key] = Category(name=key, enabled=True, categories={}, tasks=[])
        #print(yaml.dump(root_category))

        print(yaml.dump({"tasks": [{"task": {"name": "hung", "name2": "mathias"}}, {"task": {"name": "chick1", "name2": "chick2"}}]}))
        #
        # with open('test.yaml', "r") as stream:
        #     configuration = yaml.safe_load(stream)
        # breakpoint()breakpoint



if __name__ == '__main__':
    EvaluationSetConfigGenerator.build_from_file_system('root_2')

    # yaml.dump({'name': 'human',
    #            'enabled': True,
    #            'tasks': {name: 'male <-> female (Analogy Task)', type: 'analogy' , test_set: path}
    #            'categories': { categiry ; {'name': 'stuff',
    #                            'enabled': True,
    #                            'tasks': {},
    #                            'categories': }},
    #
    #
    # }})
