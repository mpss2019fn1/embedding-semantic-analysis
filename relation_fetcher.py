from collections import defaultdict
from pathlib import Path

from aiostream import stream
from redis import Redis

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration


class RelationFetcher:

    def __init__(self, wikidata_ids, endpoint=None):
        self.wikidata_ids = wikidata_ids
        self.redis = Redis(host='localhost', port=6379)
        self.endpoint = endpoint or WikidataEndpoint(
            WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    async def get_relations(self, wikidata_id: int):
        query = open('resources/get_relations.rq').read() % f'wd:Q{wikidata_id}'
        with self.endpoint.request() as request:
            for results in request.post(query):
                subject, predicate, object_ = [result.split('/entity/')[-1] for result in results.values()]
                yield (subject, predicate, object_)

    async def fetch(self):
        relations_entity_map = defaultdict(set)

        async with stream.chain(
                *[self.get_relations(wikidata_id) for wikidata_id in self.wikidata_ids]).stream() as relations:
            async for relation in relations:
                subject, predicate, object_ = relation
                relations_entity_map[(predicate, object_)].add(subject)

        return relations_entity_map
