import asyncio
from argparse import ArgumentParser

from relation_fetcher import RelationFetcher
from relation_selector import RelationSelector


async def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = range(20)

    relation_fetcher = RelationFetcher(wikidata_ids)
    relation_mapping = await relation_fetcher.fetch()
    relation_selector = RelationSelector(relation_mapping)
    print(relation_selector.global_relation_counter())


if __name__ == '__main__':
    asyncio.run(main())
