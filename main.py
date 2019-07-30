import asyncio
from argparse import ArgumentParser
from pathlib import Path
import pickle

from relation_fetcher import RelationFetcher
from relation_selector import RelationSelector
from hierachy_builder import HierachyBuilder
from hierarchy_traversal import HierarchyTraversal
from task_creator import OutlierTaskCreator
from task_creator import NeighborhoodTaskCreator

from wikidata_endpoint.return_types import UriReturnType

PICKLE_FILE = 'people_relations.pickle'


async def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    # args = open('living_people_ids.txt').read().split()
    # wikidata_ids = (arg.split('Q')[1] for arg in args)

    # first 100
    # wikidata_ids = range(100)

    # 100 peopl
    wikidata_ids = [9142456, 18011133, 4419464, 6387504, 55622219, 630905, 18921439, 27894490, 16205515, 2268624,
                    6799591, 5524044, 436700, 293117, 7179079, 21284920, 1450681, 27917799, 4984948, 7644111, 2289124,
                    18684184, 7335274, 55641017, 11641887, 817411, 2102795, 1175130, 7357858, 7659849, 7789024,
                    24006015, 6701943, 6558576, 15407093, 317209, 2346692, 4519585, 22670958, 5263122, 8061105, 8073192,
                    1738429, 6891154, 7698554, 18127720, 7818007, 231327, 5132633, 5526082, 60736561, 21858356,
                    62078645, 1256398, 3436464, 4701181, 6702666, 29033508, 5616244, 5293052, 507894, 946712, 5044692,
                    15296200, 11476388, 19308413, 48800769, 57242423, 6100039, 980021, 3672886, 6395925, 19800980,
                    19880599, 5568709, 5181263, 7926623, 7581446, 1603859, 62020451, 61988696, 58008406, 1188298,
                    3308977, 1673575, 7860003, 919837, 30055929, 7347091, 12612041, 4739662, 6778291, 6551436, 7192104,
                    4894446, 4777241, 2810518, 211065, 6988882, 3535137]

    if Path(PICKLE_FILE).exists():
        with open(PICKLE_FILE, 'rb') as handle:
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
    #print(len(relation_selector.property_mapping))
    relation_selector.remove_rare_relations(0.1)
    #print(len(relation_selector.property_mapping))
    relation_selector.remove_overlapping_relation_groups()
    #print(len(relation_selector.property_mapping))
    hierachy_builder = HierachyBuilder(relation_selector.property_mapping, relation_selector.relation_groups())
    hierachy_builder.build()
    hierachy_builder.save_to_file('hierachy_leaf_data.csv')

    neighborhood_task_creator = NeighborhoodTaskCreator()
    outlier_task_creator = OutlierTaskCreator(hierachy_builder, 2)
    HierarchyTraversal.traverse(hierachy_builder, neighborhood_task_creator)
    HierarchyTraversal.traverse(hierachy_builder, outlier_task_creator)
    breakpoint()

if __name__ == '__main__':
    asyncio.run(main())
