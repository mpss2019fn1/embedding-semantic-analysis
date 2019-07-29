from attr import dataclass
import typing
import csv
import networkx as nx
import matplotlib.pyplot as plt
import mpld3


tree = nx.DiGraph()


@dataclass
class Node:
    label: str
    values: 'typing.Any'
    children: 'typing.Any'
    is_root: bool = False


class HierachyBuilder:
    def __init__(self, property_mapping, relation_groups):
        self.property_mapping = property_mapping
        self.relation_groups = relation_groups
        self.root_node = Node('root', {relation_source for sublist in self.property_mapping.values() for relation_source in sublist}, [], is_root=True)
        tree.add_node(str(self.root_node.label))

    def build_node(self, node, property):
        relation_groups = self.property_mapping[property] & node.values
        if len(relation_groups) > 0:
            child_node = Node(property, relation_groups, [])
            tree.add_node(str(child_node.label))
            tree.add_edge(str(node.label), str(child_node.label))
            node.children.append(child_node)

    def build(self):
        previous_nodes = [self.root_node]
        for relation, relation_targets in self.relation_groups.items():
            for relation_target in relation_targets:
                property = (relation, relation_target)
                self.build_next_level(previous_nodes, property)
            children = [node.children for node in previous_nodes]
            previous_nodes = [node for sublist in children for node in sublist]

        # pos = nx.spring_layout(tree)
        # fig, ax = plt.subplots()
        # scatter = nx.draw_networkx_nodes(tree, pos, ax=ax)
        # nx.draw_networkx_edges(tree, pos, ax=ax)
        #
        # labels = tree.nodes()
        # tooltip = mpld3.plugins.PointLabelTooltip(scatter, labels=labels)
        # mpld3.plugins.connect(fig, tooltip)

        # mpld3.show()

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






    def build_next_level(self, previous_nodes, property):
        for child_node in previous_nodes:
            self.build_node(child_node, property)

