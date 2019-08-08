import asyncio
import csv
import pickle
from argparse import ArgumentParser
from pathlib import Path

from hierarchy_builder import HierarchyBuilder
from hierarchy_traversal import HierarchyTraversal
from relation_fetcher import RelationFetcher
from relation_selector import RelationSelector
from task_creator import AnalogyTaskCreator
from task_creator import NeighborhoodTaskCreator
from task_creator import OutlierTaskCreator
from task_creator import EntityCollectorTaskCreator

PICKLE_FILE = 'people_relations.pickle'


def setup_arguments(parser):
    parser.add_argument('--wikidata-ids', type=list, required=False)
    parser.add_argument('--linking-file', type=str, required=False)
    parser.add_argument('--output-dir', type=str, required=True)
    parser.add_argument('--top-count', type=int, required=False, default=float('inf'))
    parser.add_argument('--entities-per-query', type=int, required=False, default=250)
    parser.add_argument('--relation-selection-config', type=Path, required=True)


def read_ids_from_linking_file(filename, rows_to_read):
    wikidata_ids = []
    with open(filename, "r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            wikidata_ids.append(int(row[1].split('Q')[1]))
            if len(wikidata_ids) == rows_to_read:
                break
    return wikidata_ids


async def main():
    parser = ArgumentParser()
    setup_arguments(parser)
    args = parser.parse_args()

    if args.linking_file:
        wikidata_ids = read_ids_from_linking_file(args.linking_file, args.top_count)
    elif args.wikidata_ids:
        wikidata_ids = (arg.split('Q')[1] for arg in args.wikidata_ids)
    else:
        wikidata_ids = []

    if Path(PICKLE_FILE).exists():
        with open(PICKLE_FILE, 'rb') as handle:
            relation_mapping = pickle.load(handle)
    else:
        relation_fetcher = RelationFetcher(wikidata_ids, args.entities_per_query)
        relation_mapping = await relation_fetcher.fetch()
    relation_selector = RelationSelector(relation_mapping, args.relation_selection_config)
    hierachy_builder = HierarchyBuilder(relation_selector)
    hierachy_builder.build()
    hierachy_builder.save_to_file('hierarchy_leaf_data.csv')

    neighborhood_task_creator = NeighborhoodTaskCreator(args.output_dir)
    outlier_task_creator = OutlierTaskCreator(args.output_dir, hierachy_builder, 2)
    analogy_task_creator = AnalogyTaskCreator(args.output_dir, wikidata_ids)
    get_entities_task_creator = EntityCollectorTaskCreator(args.output_dir)
    HierarchyTraversal.traverse(hierachy_builder, neighborhood_task_creator)
    HierarchyTraversal.traverse(hierachy_builder, outlier_task_creator)
    HierarchyTraversal.traverse(hierachy_builder, analogy_task_creator)
    HierarchyTraversal.traverse(hierachy_builder, get_entities_task_creator)


if __name__ == '__main__':
    # wikidata_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    # Fahrenheit = map(lambda x: wikidata_ids[x:x+2], range(0, 10, 2))
    #
    # print(list(Fahrenheit))
    asyncio.run(main())
