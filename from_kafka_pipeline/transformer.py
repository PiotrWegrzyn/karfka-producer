from json import loads


class GKMessageTransformer:
    def transform(self, message: bytes) -> dict:
        message = loads(message.decode('utf-8'))

        return self.map(message)

    def map(self, message: dict) -> dict:
        return message
