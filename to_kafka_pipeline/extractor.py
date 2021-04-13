from to_kafka_pipeline.db_connector import DatabaseConnector


class ChatDatabaseExtractor:
    data_source = DatabaseConnector()

    def get_messages(self):
        return self.data_source.get_messages_mock()
