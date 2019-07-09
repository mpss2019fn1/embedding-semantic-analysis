import asyncio
from argparse import ArgumentParser
from pathlib import Path
import pickle

from relation_fetcher import RelationFetcher
from relation_selector import RelationSelector
from hierachy_builder import HierachyBuilder


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
    print(len(relation_selector.property_mapping))
    relation_selector.remove_unique_relations()
    print(len(relation_selector.property_mapping))
    relation_selector.remove_rare_relations(0.1)
    print(len(relation_selector.property_mapping))
    relation_selector.remove_overlapping_relation_groups()
    print(len(relation_selector.property_mapping))
    hierachy_builder = HierachyBuilder(relation_selector.property_mapping, relation_selector.relation_groups())
    hierachy_builder.build()
    breakpoint()

if __name__ == '__main__':
    asyncio.run(main())
