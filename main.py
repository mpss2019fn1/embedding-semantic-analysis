import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from argparse import ArgumentParser
from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration
from pathlib import Path

member = dict()
DG = nx.DiGraph()
root = 'ROOT'
DG.add_node(root)


def graph_insertion(current_set_name, root_name):
    if member[root_name].issuperset(member[current_set_name]):
        already_inserted = False
        for child in DG.neighbors(root_name):
            already_inserted = already_inserted or graph_insertion(current_set_name, child)
        if not already_inserted:
            DG.add_edge(root_name, current_set_name)
        return True
    else:
        return False


def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = args.wikidata_ids
    endpoint = WikidataEndpoint(WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    query = open('resources/query.rq').read() % '\n'.join(f'wd:Q{wikidata_id}' for wikidata_id in wikidata_ids)
    member['ROOT'] = set(wikidata_ids)

    with endpoint.request() as request:
        for answer in request.post(query):
            current_set = (answer['member'].split(', '))
            current_set_name = answer['object'].split('/Q')[-1]
            member[current_set_name] = current_set
            DG.add_node(current_set_name)
            graph_insertion(current_set_name, root)
    print(list(DG.nodes(data=True)))
    nx.draw(DG)
    plt.show()


if __name__ == '__main__':
    main()
