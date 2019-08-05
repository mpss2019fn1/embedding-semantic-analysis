from attr import dataclass
import csv
import typing

import networkx as nx

from relation_selector import RelationSelector

tree = nx.DiGraph()


@dataclass
class Node:
    label: str
    values: 'typing.Any'
    children: 'typing.Any'
    is_root: bool = False

    def is_leaf(self):
        return len(self.children) == 0 or len(self.values) <= 15


class HierachyBuilder:
    def __init__(self, property_mapping, relation_groups):
        self.property_mapping = property_mapping
        self.relation_groups = relation_groups
        self.root_node = Node('root',
                              {relation_source for sublist in self.property_mapping.values() for relation_source in
                               sublist}, [], is_root=True)
        tree.add_node(str(self.root_node.label))

    def build_node(self, node, property_):
        relation_groups = self.property_mapping[property_] & node.values
        if len(relation_groups) > 0:
            child_node = Node(property_, relation_groups, [])
            tree.add_node(str(child_node.label))
            tree.add_edge(str(node.label), str(child_node.label))
            node.children.append(child_node)
            return child_node

    def build(self):
        current_nodes = [self.root_node]
        while current_nodes:
            current_node = current_nodes.pop()
            current_nodes.extend(list(x for x in self.split_node_on_predicate(current_node) if x))

    def split_node_on_predicate(self, node):
        local_property_mapping = {property_: (property_group & node.values) for property_, property_group in
                                  self.property_mapping.items()}
        local_property_mapping = {property_: property_group for property_, property_group in
                                  local_property_mapping.items() if
                                  len(property_group) > 0}
        relation_selector = RelationSelector(local_property_mapping)

        relation_selector.remove_unique_relations()
        relation_selector.remove_rare_relations(0.1)
        relation_selector.remove_overlapping_relation_groups()

        relation_groups = relation_selector.relation_groups()
        if len(relation_groups) < 1:
            return
        relation, relation_targets = next(iter(relation_groups.items()))
        if len(relation_targets) < 2:
            return
        for relation_target in relation_targets:
            property_ = (relation, relation_target)
            yield self.build_node(node, property_)

    def save_to_file(self, filename):
        # dfs
        print("HERE I AM")
        file_data = {}
        stack = []
        stack.append(self.root_node)
        cluster_id = 0
        while stack:
            current_node = stack.pop()
            # print(current_node.values)
            if current_node.children and len(current_node.values) > 15:
                for child in current_node.children:
                    stack.append(child)
            else:
                print(f"labels: {current_node.label}")
                print(f"values: {current_node.values}")
                file_data[current_node.label] = current_node.values

        w = csv.writer(open(filename, "w"))
        for key, val in file_data.items():
            w.writerow([key, val])
