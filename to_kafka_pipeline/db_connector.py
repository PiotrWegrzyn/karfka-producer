import pandas as pd
import psycopg2 as psycopg2

from json import dumps
from typing import Iterable


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
                'first_name': 'Elon2',
                'last_name': 'Musk',
                'dob': '1945-09-02',
                'message_body': 'What is love?'
            },
            {
                'first_name': 'Adam2',
                'last_name': 'Eve',
                'dob': '01-01-02',
                'message_body': 'Oh baby, don\'t hurt me '
            },
            {
                'first_name': 'Jacob2',
                'last_name': 'Rothschild',
                'dob': '1800-09-02',
                'message_body': 'Dont hurt me'
            },
            {
                'first_name': 'John2',
                'last_name': 'Smith',
                'dob': '1980-02-14',
                'message_body': 'No more'
            },

        ]

        return mock_messages

    def get_messages(self) -> Iterable[dict]:
        sql_command = 'SELECT * FROM {}.{};'.format(self.DB_SCHEMA, self.DB_TABLE)

        return pd.read_sql(sql_command, self.conn).to_dict()
