from collections import Counter
from functools import lru_cache
from itertools import islice
from pathlib import Path

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration


def chunk_dictionary(dictionary, chunk_size):
    it = iter(dictionary)
    for _ in range(0, len(dictionary), chunk_size):
        yield {k: dictionary[k] for k in islice(it, chunk_size)}


class RelationSelector:

    def __init__(self, relations_mapping, endpoint=None):
        self.relations_mapping = relations_mapping
        self.endpoint = endpoint or WikidataEndpoint(
            WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    @lru_cache(maxsize=None)
    def relation_counter(self, threshold=1):
        return Counter({key: len(value) for key, value in self.relations_mapping.items() if len(value) > threshold})

    @lru_cache(maxsize=None)
    def group_counter(self):
        return Counter(c[0] for c in self.relation_counter())

    @lru_cache(maxsize=None)
    def global_relation_counter(self, chunk_size=800):
        global_count = Counter()
        for chunk in chunk_dictionary(self.relations_mapping, chunk_size):
            global_query = open('resources/global_relation_count.rq').read() % ' '.join(
                f'({x[0].sparql_escape()} {x[1].sparql_escape()})' for x in chunk)
            with self.endpoint.request() as request:
                request_result = request.post(global_query)
                chunk_global_counts = {(x['predicate'], x['object']): int(x['global_count'].value) for x in
                                       request_result}
            global_count.update(chunk_global_counts)
        return global_count

    def score(self, relation):
        pass

