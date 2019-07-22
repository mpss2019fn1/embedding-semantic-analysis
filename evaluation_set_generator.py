import csv


class EvaluationSetGenerator():

    def __init__(self, graph):
        self.graph = graph
        self.neighborhood_task_entities = [["entity","group_id","is_similar"]]

    def build(self):
        clusters = {}
        stack = []
        stack.append((self.graph.root_node, [self.graph.root_node.label.split('/')[-1]]))
        cluster_id = 0
        while stack:
            current_node, path = stack.pop()
            if current_node.children and len(current_node.values) > 15:
                for child in current_node.children:
                    temp_list = path.append(child.label.value.split('/')[-1]).copy()
                    stack.append((child, temp_list))
            else:
                for entity in current_node.values:
                    wikidata_id = entity.value.split('/')[-1]
                    self.neighborhood_task_entities.append([wikidata_id, cluster_id, True])
                    self.save(path, self.neighborhood_task_entities)
                cluster_id += 1


    def save(self, filename, content):
        with open(filename+"csv", "w") as f:
            writer = csv.writer(f)
            writer.writerows(content)

