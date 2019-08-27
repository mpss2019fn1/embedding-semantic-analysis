from attr import dataclass
import csv
import typing

from itertools import repeat
from multiprocessing import Pool

from relation_selector import RelationSelector


@dataclass
class Node:
    label: str
    values: 'typing.Any'
    children: 'typing.Any'
    splits: 'typing.Any' = []
    is_root: bool = False

    def is_leaf(self):
        return len(self.children) == 0 or len(self.values) <= 15

    def element_at(self, index):
        for value in self.values:
            index -= 1
            if index < 0:
                return value
        return None


def split_node_on_predicate(node, property_mapping, metric_config_path):
    local_property_mapping = {property_: (property_group & node.values) for property_, property_group in
                              property_mapping.items()}
    local_property_mapping = {property_: property_group for property_, property_group in
                              local_property_mapping.items() if
                              len(property_group) > 0}
    if len(local_property_mapping) < 1:
        return
    relation_selector = RelationSelector(local_property_mapping, metric_config_path)
    relation = relation_selector.top_property(not_include=node.splits)
    if not relation:
        return
    relation_targets = relation_selector.relation_groups()[relation]
    if len(relation_targets) < 2:
        return
    next_nodes = []
    for relation_target in relation_targets:
        property_ = (relation, relation_target)
        if local_property_mapping[property_] == node.values:
            continue
        next_nodes.append(build_node(node, property_, local_property_mapping))
    return next_nodes


def build_node(node, property_, property_mapping):
    # ToDo: & node.values necessary?
    relation_groups = property_mapping[property_] & node.values
    if len(relation_groups) > 0:
        child_node = Node(property_, relation_groups, [], node.splits + [property_[0]])
        return child_node


class HierarchyBuilder:
    def __init__(self, relation_selector):
        self.relation_selector = relation_selector
        self.property_mapping = relation_selector.property_mapping
        self.relation_groups = relation_selector.relation_groups
        self.root_node = Node('root',
                              {relation_source for sublist in self.property_mapping.values() for relation_source in
                               sublist}, [], is_root=True)

    def build(self, number_processes=25):
        nodes_to_process = [self.root_node]
        with Pool(number_processes) as pool:
            while len(nodes_to_process) > 0:
                print(len(nodes_to_process))
                next_nodes = pool.starmap(split_node_on_predicate, list(
                    zip(nodes_to_process, repeat(self.property_mapping),
                        repeat(self.relation_selector.metric_config_path))))
                assert len(nodes_to_process) == len(next_nodes)
                for parent, children in zip(nodes_to_process, next_nodes):
                    parent.children = children if children else []
                nodes_to_process = list(
                    filter(None.__ne__, [item for sublist in filter(None.__ne__, next_nodes) for item in sublist]))

    def save_to_file(self, filename):
        # dfs
        print("Saving to file")
        file_data = {}
        stack = [self.root_node]
        while stack:
            current_node = stack.pop()
            # print(current_node.values)
            if current_node.children and len(current_node.values) > 15:
                for child in current_node.children:
                    stack.append(child)
            else:
                # print(f"labels: {current_node.label}")
                # print(f"values: {current_node.values}")
                file_data[current_node.label] = current_node.values

        w = csv.writer(open(filename, "w"))
        for key, val in file_data.items():
            w.writerow([key, val])
