import socket
from typing import Iterable

from confluent_kafka.cimpl import Producer

from settings import BOOTSTRAP_SERVER, TOPIC_NAME


class KafkaMessageLoader:
    def __init__(self):

        conf = {
            'bootstrap.servers': BOOTSTRAP_SERVER,
            'client.id': socket.gethostname()
        }

        self.producer = Producer(conf)

    def load(self, messages: Iterable[dict]):
        for mes in messages:
            self.producer.produce(TOPIC_NAME, value=mes, callback=self.produce_cb)
            self.producer.poll(1)

    def produce_cb(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))
