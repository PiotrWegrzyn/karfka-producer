from json import dumps
from typing import Iterable

import pandas as pd
import psycopg2 as psycopg2
from kafka import KafkaProducer


class DatabaseConnector:
    DB_SCHEMA = '?'
    DB_TABLE = '?'

    def __init__(self):
        conn_string = self.create_connection_string()

        # self.conn = psycopg2.connect(conn_string)

    def create_connection_string(self) -> str:
        mock_conn_str = ''

        return mock_conn_str

    def get_messages_mock(self) -> Iterable[dict]:
        mock_messages = [
            {
                'first_name': 'Elon',
                'last_name': 'Musk',
                'dob': '1945-09-02',
                'message_body': 'What is love?'
            },
            {
                'first_name': 'Adam',
                'last_name': 'Eve',
                'dob': '01-01-02',
                'message_body': 'Oh baby, don\'t hurt me '
            },
            {
                'first_name': 'Jacob',
                'last_name': 'Rothschild',
                'dob': '1800-09-02',
                'message_body': 'Dont hurt me'
            },
            {
                'first_name': 'John',
                'last_name': 'Smith',
                'dob': '1980-02-14',
                'message_body': 'No more'
            },

        ]

        return mock_messages

    def get_messages(self) -> Iterable[dict]:
        sql_command = 'SELECT * FROM {}.{};'.format(self.DB_SCHEMA, self.DB_TABLE)

        return pd.read_sql(sql_command, self.conn).to_dict()


class LoaderMessageTransformer:
    def transform_messages(self, messages: Iterable[dict]) -> Iterable[dict]:
        return [self.transform_message(message) for message in messages]

    def transform_message(self, message: dict) -> dict:
        self.censor(message)

        return dumps(message).encode('utf-8')

    def censor(self, message: dict):
        del message['dob']


class KafkaMessageLoader:
    def __init__(self):
        self.message_transformer = LoaderMessageTransformer()
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda m: self.message_transformer.transform_message(m)
        )

    def load(self, messages: Iterable[dict]):
        for message in messages:
            self.producer.send('chat_messages', value=message)
            print(message)


if __name__ == '__main__':
    chat_messages = DatabaseConnector().get_messages_mock()
    KafkaMessageLoader().load(chat_messages)
