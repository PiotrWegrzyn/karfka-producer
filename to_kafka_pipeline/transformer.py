from json import dumps
from typing import Iterable


class LoaderMessageTransformer:
    def transform_messages(self, messages: Iterable[dict]) -> Iterable[dict]:
        return [self.transform_message(message) for message in messages]

    def transform_message(self, message: dict) -> dict:
        self.censor(message)

        return dumps(message).encode('utf-8')

    def censor(self, message: dict):
        del message['dob']
