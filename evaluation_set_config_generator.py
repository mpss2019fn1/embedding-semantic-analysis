from attr import dataclass

import yaml
import os


@dataclass
class Task:
    name : str
    type : str
    test_set : str

@dataclass
class Category:
    name : str
    enabled : bool
    categories : dict
    tasks : list


class EvaluationSetConfigGenerator:

    def __init__(self):
        pass

    @staticmethod
    def build_from_file_system(root_dir):
        dict_hierarchy = dict()
        for root, dirs, files in os.walk(root_dir, topdown=False):
            for file in files:
                if file.endswith('.csv'):
                    path = root + '/' + file
                    split_path = path.split('/')
                    previous_hierarchy_level = dict_hierarchy
                    for i in range(1,len(split_path)-2):
                        key = f'{split_path[i]}({split_path[i+1]})'
                        current_hierarchy_level = previous_hierarchy_level.get(key, None)
                        if current_hierarchy_level is None:
                            current_hierarchy_level = dict()
                        previous_hierarchy_level = current_hierarchy_level

                    filename = split_path[-1].split('.csv')[0]
                    value = f'{split_path[-2]}({filename})'
                    #previous_hierarchy_level[]





    def save_config(self):
        pass


if __name__ == '__main__':
    EvaluationSetConfigGenerator.build_from_file_system('root_2')