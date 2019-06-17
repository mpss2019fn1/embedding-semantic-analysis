import networkx as nx
import matplotlib.pyplot as plt
from argparse import ArgumentParser
from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration
from pathlib import Path

member = dict()
DG = nx.DiGraph()
root = 'ROOT'
DG.add_node(root)
stack = []


def graph_insertion(current_set_name, root_name):
    sub_children = [child for child in DG.successors(root_name) if member[child].issuperset(member[current_set_name])]
    if len(sub_children) == 0:
        DG.add_edge(root_name, current_set_name)
    else:
        for sub_child in sub_children:
            stack.append((current_set_name, sub_child))

def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = args.wikidata_ids
    endpoint = WikidataEndpoint(WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    query = open('resources/query.rq').read() % '\n'.join(f'wd:Q{wikidata_id}' for wikidata_id in wikidata_ids)
    member['ROOT'] = set(f'http://www.wikidata.org/entity/Q{wikidata_id}'for wikidata_id in wikidata_ids)

    with endpoint.request() as request:
        for answer in request.post(query):
            current_set = set(answer['member'].split(', '))
            current_set_name = answer['object'].split('/Q')[-1]
            member[current_set_name] = current_set
            DG.add_node(current_set_name)
            stack.append((current_set_name, root))
            while stack:
                graph_insertion(*stack.pop())
    print(list(DG.nodes(data=True)))
    nx.draw_networkx(DG, with_labels=True)
    plt.show()


if __name__ == '__main__':
    main()
