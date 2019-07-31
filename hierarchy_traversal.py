import csv
import os


class HierarchyTraversal:

    @staticmethod
    def traverse(hierarchy, task_creator):
        HierarchyTraversal._traverse(node=hierarchy.root_node, path="root", task_creator=task_creator)

    @staticmethod
    def _traverse(node, path, task_creator):
        entities = []

        if node.is_leaf():
            for entity in node.values:
                wikidata_id = HierarchyTraversal.extract_wikidata_id(entity.value)
                entities.append(wikidata_id)
        else:
            property_entity_dict = {}

            for child in node.children:
                child_predicate = HierarchyTraversal.extract_wikidata_id(child.label[0].value)
                child_object = HierarchyTraversal.extract_wikidata_id(child.label[1].value)
                child_entities = HierarchyTraversal._traverse(child, path + "/" + child_predicate + "/" + child_object, task_creator)
                property_entities = property_entity_dict.get(child_predicate, None)
                if property_entities is None:
                    property_entities = []
                    property_entity_dict[child_predicate] = property_entities
                property_entities.extend(child_entities)

            # für jedes P
            for predicate, predicate_entities in property_entity_dict.items():
                task_creator.process_node(path + '/' + predicate, node, predicate_entities, True)
                entities.extend(predicate_entities)

        # für jedes Q
        task_creator.process_node(path, node, entities, False)
        return entities

    @staticmethod
    def extract_wikidata_id(uri):
        return uri.split('/')[-1]

    @staticmethod
    def build_path(stack):
        return '/'.join(stack)

