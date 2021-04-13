from from_kafka_pipeline.extractor import KafkaMessageExtractor
from from_kafka_pipeline.loader import GKMessageLoader
from from_kafka_pipeline.transformer import GKMessageTransformer

if __name__ == '__main__':
    chat_messages = KafkaMessageExtractor().extract()
    transformed_chat_messages = GKMessageTransformer().transform(chat_messages)
    GKMessageLoader().load(transformed_chat_messages)
