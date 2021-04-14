from json import loads
from typing import Iterable


class GKMessageTransformer:

    def transform(self, messages: Iterable[bytes]) -> Iterable[dict]:
        return [self.transform_message(mes) for mes in messages]

    def transform_message(self, message: bytes) -> dict:
        message = loads(message.decode('utf-8'))

        return self.map(message)

    def map(self, message: dict) -> dict:
        return message
