from collections import Counter
from functools import lru_cache
from itertools import chain, islice
from pathlib import Path
import logging

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration


class TimeoutException(Exception):
    pass


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
        chunks = chunk_dictionary(self.relations_mapping, chunk_size)
        while chunks:
            timeout_chunks = []
            for chunk in chunks:
                global_query = open('resources/global_relation_count.rq').read() % ' '.join(
                    f'({x[0].sparql_escape()} {x[1].sparql_escape()})' for x in chunk)
                with self.endpoint.request() as request:
                    try:
                        request_result = request.post(global_query, on_timeout=lambda x: exec('raise(TimeoutException)'))
                    except TimeoutException:
                        logging.error("Request timeout: Will try again with smaller chunks")
                        timeout_chunks.append(chunk_dictionary(chunk, 3))
                        continue
                    chunk_global_counts = {(x['predicate'], x['object']): int(x['global_count'].value) for x in
                                           request_result}
                global_count.update(chunk_global_counts)
            chunks = timeout_chunks
        return global_count

    def score(self, relation):
        pass
