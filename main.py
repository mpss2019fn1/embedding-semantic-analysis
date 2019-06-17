from argparse import ArgumentParser
from pathlib import Path
from collections import defaultdict
from aiostream import stream
import asyncio

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration


async def get_relations(wikidata_id: int, endpoint: WikidataEndpoint):
    query = open('resources/get_relations.rq').read() % f'wd:Q{wikidata_id}'

    with endpoint.request() as request:
        for results in request.post(query):
            subject, predicate, object_ = [result.split('/entity/')[-1] for result in results.values()]
            yield (subject, predicate, object_)


async def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = args.wikidata_ids
    endpoint = WikidataEndpoint(WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    relations_entity_map = defaultdict(set)

    async with stream.chain(
            *[get_relations(wikidata_id, endpoint) for wikidata_id in wikidata_ids]).stream() as relations:
        async for relation in relations:
            subject, predicate, object_ = relation
            relations_entity_map[(predicate, object_)].add(subject)


if __name__ == '__main__':
    asyncio.run(main())
