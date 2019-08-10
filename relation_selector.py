import csv

from collections import Counter
from functools import lru_cache
from heapq import heappush, heappushpop, nlargest
from pathlib import Path

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration

import numpy as np
import pandas as pd

metrics = dict()


def metric(func):
    metrics[func.__name__] = func
    return func


class RelationSelector:

    def __init__(self, relations_mapping, metric_config_path: Path, endpoint=None):
        self.property_mapping = relations_mapping
        self.endpoint = endpoint or WikidataEndpoint(
            WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))
        self.metric_data_frame = pd.DataFrame([*self.relation_groups().keys()], columns=['predicate'])
        self.metric_config_path = metric_config_path
        self.number_entities = len(set().union(*self.property_mapping.values()))

    def top_property(self, not_include):
        metric_weights = []
        with self.metric_config_path.open() as metric_csv_file:
            metric_reader = csv.reader(metric_csv_file)
            # skip header
            next(metric_reader)
            for metric_name, metric_weight in metric_reader:
                self.metric_data_frame[metric_name] = self.metric_data_frame['predicate'].map(
                    lambda predicate: metrics[metric_name](self, predicate)).astype('float64')
                metric_weights.append(float(metric_weight))
        self.metric_data_frame['score'] = self.metric_data_frame.drop(['predicate'], axis=1).to_numpy() @ np.array(
            metric_weights)
        self.metric_data_frame = self.metric_data_frame[~self.metric_data_frame['predicate'].isin(not_include)]
        if self.metric_data_frame['score'].size == 0:
            return
        return self.metric_data_frame.loc[self.metric_data_frame['score'].idxmax()]['predicate']

    @metric
    def popularity(self, predicate) -> float:
        objects = self.relation_groups()[predicate]
        predicate_objects = set()
        for object_ in objects:
            predicate_objects = predicate_objects.union(self.property_mapping[(predicate, object_)])
        return len(predicate_objects) / self.number_entities

    @metric
    def big_groups(self, predicate, number_of_big_groups=3) -> float:
        objects = self.relation_groups()[predicate]
        heap = []
        total_sum = 0
        for object_ in objects:
            group_size = len(self.property_mapping[(predicate, object_)])
            total_sum += group_size
            heappushpop(heap, group_size) if len(heap) > number_of_big_groups else heappush(heap, group_size)
        big_groups_sum = sum(nlargest(number_of_big_groups, heap))
        return big_groups_sum / total_sum

    @lru_cache(maxsize=None)
    def relation_groups(self):
        """
        :return: mapping from predicate to connected objects
        """
        selected_relations = self.group_counter().keys()
        selected_relation_group = {
            selected_relation: {x[1] for x in self.property_mapping if x[0] == selected_relation} for
            selected_relation in selected_relations}
        return selected_relation_group

    @lru_cache(maxsize=None)
    def group_counter(self):
        return Counter(c[0] for c in self.property_counter())

    @lru_cache(maxsize=None)
    def property_counter(self, threshold=1):
        """
        :param threshold: values smaller then this value are discarded
        :return: mapping from property to number of entities with this property
        """
        return Counter({key: len(value) for key, value in self.property_mapping.items() if len(value) > threshold})
