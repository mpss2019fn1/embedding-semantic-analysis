import csv


class EvaluationSetGenerator:

    def __init__(self, graph):
        self.graph = graph
        self.neighborhood_task_entities = [["entity", "group_id", "is_similar"]]

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
            while len(path_stack) > level:
                path_stack.pop()

            if level == 0:
                path_stack.append("root")
            else:
                path_stack.append(self.extract_wikidata_id(current_node.label[1].value))
            # refactor
            # magic number
            # müssen quasi noch eine Ebene haben
            if current_node.children and len(current_node.values) > 15:
                for child in current_node.children:
                    stack.append((child, level + 1))
            else:
                print(path_stack)
                for entity in current_node.values:
                    wikidata_id = self.extract_wikidata_id(entity.value)
                    self.neighborhood_task_entities.append([wikidata_id, cluster_id, True])
                    # self.save(path, self.neighborhood_task_entities)
                cluster_id += 1

    @staticmethod
    def extract_wikidata_id(uri):
        return uri.split('/')[-1]

    def save(self, filename, content):
        with open(filename + "csv", "w") as f:
            writer = csv.writer(f)
            writer.writerows(content)
