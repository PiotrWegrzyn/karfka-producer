import sys

from confluent_kafka.cimpl import Consumer, KafkaError, KafkaException, Message

from settings import BOOTSTRAP_SERVER, TOPIC_NAME


class KafkaMessageExtractor:
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'group.id': "foo",
        'auto.offset.reset': 'smallest'
    }

    def extract(self):
        consumer = Consumer(self.conf)
        consumer.subscribe([TOPIC_NAME])

        messages = []

        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    break

                if msg.error():
                    if self.is_partition_end(msg):
                        sys.stderr.write(f'{msg.topic()} {msg.partition()} {msg.offset()} reached end at offset %d\n')
                        break

                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    messages.append(msg)
        finally:
            print(f'Found {len(messages)} messages.')
            consumer.close()

        return messages

    @staticmethod
    def is_partition_end(msg: Message) -> bool:
        return msg.error().code() == KafkaError._PARTITION_EOF
