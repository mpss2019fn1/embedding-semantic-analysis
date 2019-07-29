import csv
import os


class EvaluationSetGenerator:

    def __init__(self, graph):
        self.graph = graph

    def build_recursive1(self):
        root = self.graph.root_node
        EvaluationSetGenerator.build_recursive(node=root, path="root")

    @staticmethod
    def build_recursive(node, path):
        is_leaf = len(node.children) == 0 or len(node.values) <= 15
        is_similar = True
        cluster_id = 0


        # pfad sieht wie folgt aus
        # root/P/Q/P/Q

        # kommen rein mit Root
        #   | 2 Kinder
        #       | sex(male)
        #       | sex(female)


        # bin ich ein Blatt? -> abspeichern
        # reiche Elemente hoch
        # we can chain lists
        entities = [["entity", "group_id", "is_similar"]]

        if is_leaf:
            for entity in node.values:
                wikidata_id = EvaluationSetGenerator.extract_wikidata_id(entity.value)
                entities.append([wikidata_id, cluster_id, is_similar])
        else:
            property_entity_dict = {}

            for child in node.children:
                child_predicate = EvaluationSetGenerator.extract_wikidata_id(child.label[0].value)
                child_object = EvaluationSetGenerator.extract_wikidata_id(child.label[1].value)
                child_entities = EvaluationSetGenerator.build_recursive(child, path + "/" + child_predicate + "/" + child_object)
                property_entities = property_entity_dict.get(child_predicate, None)
                if property_entities is None:
                    property_entities = [["entity", "group_id", "is_similar"]]
                    property_entity_dict[child_predicate] = property_entities
                property_entities.extend(child_entities[1:])

            for predicate, predicate_entities in property_entity_dict.items():
                EvaluationSetGenerator.save_to_file(path + '/' + predicate + ".csv", predicate_entities)
                entities.extend(predicate_entities[1:])

        EvaluationSetGenerator.save_to_file(path + ".csv", entities)
        return entities


    def build(self):
        # contains nodes for depth first search. also contains node level and maybe list?
        stack = []
        # contains
        path_stack = []
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

            # An dieser Stelle den kompletten Knoten wegschreiben
            # Was ist der Pfad?

            # Todo
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
        if '/' in filename:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, mode="w+") as f:
            writer = csv.writer(f)
            writer.writerows(content)
