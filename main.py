import asyncio
from argparse import ArgumentParser
from pathlib import Path
import pickle
from itertools import combinations

from relation_fetcher import RelationFetcher
from relation_selector import RelationSelector, overlap_coefficient


async def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = range(1000)
    if Path('relation_mapping.pickle').exists():
        with open('relation_mapping.pickle', 'rb') as handle:
            relation_mapping = pickle.load(handle)
    else:
        relation_fetcher = RelationFetcher(wikidata_ids)
        relation_mapping = await relation_fetcher.fetch()
    relation_selector = RelationSelector(relation_mapping)
    print(len(relation_selector.relations_mapping))
    relation_selector.remove_unique_relations()
    print(len(relation_selector.relations_mapping))
    relation_selector.remove_rare_relations(0.3)
    print(len(relation_selector.relations_mapping))
    relation_selector.remove_unique_relations()
    print(len(relation_selector.relations_mapping))
    breakpoint()

    # selected_predicates = [x[0] for x in relation_selector.group_counter().items() if 3 < x[1]]
    # selected_predicate_groups = {
    #     selected_predicate: {x[1] for x in relation_selector.relations_mapping if x[0] == selected_predicate} for
    #     selected_predicate in selected_predicates}
    # for predicate, objects in selected_predicate_groups.items():
    #     print('==========================')
    #     print(predicate)
    #     groups = (relation_selector.relations_mapping[(predicate, object_)] for object_ in objects)
    #     for (group1, label1), (group2, label2) in combinations(zip(groups, objects), 2):
    #         if len(group1) == 1 or len(group2) == 1:
    #             continue
    #         overlap_coefficient_value = overlap_coefficient(group1, group2)
    #         if overlap_coefficient_value == 0.0:
    #             print(label1.sparql_escape() + ' ' + label2.sparql_escape())
    #             print(f'{predicate} {len(group1)} {len(group2)} {overlap_coefficient_value}')


if __name__ == '__main__':
    asyncio.run(main())
