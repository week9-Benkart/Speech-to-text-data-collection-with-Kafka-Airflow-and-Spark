from app_logger import App_Logger
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import random
import time
from read_from_s3 import *
from topic import Topics


class Producer:
    """ Create a producer and emit events from s3 bucket"""

    def __init__(self, bootstrap='b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092', create_topic=True, topic='transc_data'):
        self.bootstrap = bootstrap
        self.topic = topic
        self.create_topic = create_topic
        self.logger = App_Logger().get_logger(__name__)

    def start_publishing(self):
        """ Start producer by initializing producer"""
        self.get_data()
        self.initialize()
        if self.create_topic:
            tp = Topics('Benkart', [self.topic])
            tp.create_topics()
            self.topic = tp.topics[0]
        self.publish()

    def initialize(self):
        """ Initialize producer with bootstrap server """
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            api_version=(0, 10, 1),
            value_serializer=lambda v: json.dumps(v).encode('UTF-8'),
            key_serializer=lambda v: json.dumps(v).encode('UTF-8')
        )

    def get_data(self):
        """ Get data from created s3 bucket"""
        data = load_data_s3()
        self.data = pd.DataFrame(data).values.tolist()

    def on_send_success(metadata):
        """ Message printed on producer send success"""
        print(metadata.topic)
        print(metadata.partition)
        print(metadata.offset)

    def on_send_error(self, exc):
        """ Message printed on producer send error"""
        self.logger.error(f'error on {exc}.')

    def publish(self):
        """ Publish Events"""
        i = 0
        for val in self.data:
            i += 1
            self.producer.send(self.topic, key=i, value=val).add_callback(
                self.on_send_success).add_errback(self.on_send_error)
           # print("Sent to topic",self.topic,": ",val)


if __name__ == "__main__":
    Producer().start_publishing()
