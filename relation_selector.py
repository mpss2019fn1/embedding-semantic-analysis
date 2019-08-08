import csv

from collections import Counter
from functools import lru_cache
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
        self.metric_data_frame = pd.DataFrame()
        self.metric_config_path = metric_config_path

    def top_property(self, not_include):
        metric_weights = []
        with self.metric_config_path.open() as metric_csv_file:
            metric_reader = csv.reader(metric_csv_file)
            # skip header
            next(metric_reader)
            metric_name, metric_weight = next(metric_reader)
            self.metric_data_frame = pd.DataFrame(list(metrics[metric_name](self).items()),
                                                  columns=['predicate', metric_name])
            self.metric_data_frame[metric_name] = self.metric_data_frame[metric_name].astype('float64')
            metric_weights.append(float(metric_weight))
            for metric_name, metric_weight in metric_reader:
                self.metric_data_frame[metric_name] = self.metric_data_frame['predicate'].map(
                    metrics[metric_name](self)).astype('float64')
                metric_weights.append(float(metric_weight))
        self.metric_data_frame['score'] = self.metric_data_frame.drop(['predicate'], axis=1).to_numpy() @ np.array(
            metric_weights)
        self.metric_data_frame = self.metric_data_frame[~self.metric_data_frame['predicate'].isin(not_include)]
        if self.metric_data_frame['score'].size == 0:
            return
        return self.metric_data_frame.loc[self.metric_data_frame['score'].idxmax()]['predicate']

    @metric
    def predicate_popularity(self):
        """
        :return: mapping from predicate to proportion of entities, which have a outgoing edge with this predicate
        """
        result = dict()
        for predicate, objects in self.relation_groups().items():
            predicate_objects = set()
            for object_ in objects:
                predicate_objects = predicate_objects.union(self.property_mapping[(predicate, object_)])
            result[predicate] = len(predicate_objects)
        return Counter(result)

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
