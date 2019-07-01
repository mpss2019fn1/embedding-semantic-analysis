import asyncio
from argparse import ArgumentParser

from relation_fetcher import RelationFetcher


async def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = list(range(10))

    relation_fetcher = RelationFetcher(wikidata_ids)
    x = await relation_fetcher.fetch()
    for e in x:
        print(e)


if __name__ == '__main__':
    asyncio.run(main())
