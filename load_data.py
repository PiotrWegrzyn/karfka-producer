from to_kafka_pipeline.extractor import ChatDatabaseExtractor
from to_kafka_pipeline.loader import KafkaMessageLoader
from to_kafka_pipeline.transformer import LoaderMessageTransformer

if __name__ == '__main__':
    chat_messages = ChatDatabaseExtractor().get_messages()
    transformed_chat_messages = LoaderMessageTransformer().transform_messages(chat_messages)
    KafkaMessageLoader().load(transformed_chat_messages)
