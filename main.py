import asyncio
from argparse import ArgumentParser

from relation_fetcher import RelationFetcher


async def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = args.wikidata_ids
    relation_fetcher = RelationFetcher(wikidata_ids, redis_config={'host': 'localhost', 'port': 6379})
    x = await relation_fetcher.fetch()


if __name__ == '__main__':
    asyncio.run(main())
