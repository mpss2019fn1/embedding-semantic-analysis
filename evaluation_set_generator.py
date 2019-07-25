import csv
import os

class EvaluationSetGenerator:

    def __init__(self, graph):
        self.graph = graph

    def build(self):
        clusters = {}
        stack = []
        path_stack = []
        root = self.graph.root_node
        # (node, level)
        stack.append((self.graph.root_node, 0))
        cluster_id = 0
        while stack:
            # hier gehen wir ggf. wieder eine Ebene nach oben.
            current_node, level = stack.pop()
            while len(path_stack) > max(level * 2 - 1, 0):
                path_stack.pop()

            if level == 0:
                path_stack.append("root")
            else:
                path_stack.append(self.extract_wikidata_id(current_node.label[0].value))
                path_stack.append(self.extract_wikidata_id(current_node.label[1].value))
            # refactor
            # magic number
            # müssen quasi noch eine Ebene haben
            if current_node.children and len(current_node.values) > 15:
                for child in current_node.children:
                    stack.append((child, level + 1))
            else:
                # speichere die childen entsprechend in den path
                # hier muss geprüft werden, ob es sich um eine
                print(path_stack)
                path = self.build_path(path_stack) + ".csv"
                neighborhood_task_entities = [["entity", "group_id", "is_similar"]]
                for entity in current_node.values:
                    wikidata_id = self.extract_wikidata_id(entity.value)
                    neighborhood_task_entities.append([wikidata_id, cluster_id, True])
                self.save_to_file(path, neighborhood_task_entities)
                cluster_id += 1

    @staticmethod
    def extract_wikidata_id(uri):
        return uri.split('/')[-1]

    @staticmethod
    def build_path(stack):
        return '/'.join(stack)

    @staticmethod
    def save_to_file(filename, content):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, mode="w+") as f:
            writer = csv.writer(f)
            writer.writerows(content)
