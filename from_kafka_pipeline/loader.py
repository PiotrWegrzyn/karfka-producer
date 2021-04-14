from typing import Iterable

from from_kafka_pipeline.gk_connector import GKConnector


class GKMessageLoader:
    def __init__(self):
        self.source_connector = GKConnector()

    def load(self, messages: Iterable[dict]):
        self.source_connector.send(messages)
