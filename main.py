import asyncio
from argparse import ArgumentParser
from pathlib import Path
import pickle

from relation_fetcher import RelationFetcher
from relation_selector import RelationSelector
from hierachy_builder import HierachyBuilder

from wikidata_endpoint.return_types import UriReturnType

PICKLE_FIlE = 'people_relations.pickle'

async def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    # args = open('living_people_ids.txt').read().split()
    # wikidata_ids = (arg.split('Q')[1] for arg in args)
    wikidata_ids = range(1000)
    if Path(PICKLE_FIlE).exists():
        with open(PICKLE_FIlE, 'rb') as handle:
            relation_mapping = pickle.load(handle)
    else:
        relation_fetcher = RelationFetcher(wikidata_ids)
        relation_mapping = await relation_fetcher.fetch()
    relation_selector = RelationSelector(relation_mapping)

    #Popularity
    r = relation_selector.predicate_popularity()
    for key, value in r.most_common(10):
        print(key, value)

    r = relation_selector.group_sizes()

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
