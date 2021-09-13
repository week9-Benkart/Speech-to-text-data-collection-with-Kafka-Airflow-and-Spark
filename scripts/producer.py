try:
    from app_logger import App_Logger
except:
    from scripts.app_logger import App_Logger

try:
    from topic import Topics
except:
    from scripts.topic import Topics

import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import random
import time

class Producer:
    """ Create a producer and emit events
    :param bootstrap: bootstrap server
    :param create_topic: a variable to create or not to create a new topic
    :param topic: topic name
    """

    def __init__(self,data,
        bootstrap='b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092',
        create_topic=True, topic='test_data',file_type="text"):
        self.bootstrap = bootstrap
        self.create_topic = create_topic
        self.data = data
        self.logger = App_Logger().get_logger(__name__)
        self.file_type = file_type
        self.topic = topic

    def start_publishing(self):
        """ Start producer by initializing producer"""
        self.initialize()            
        if self.create_topic:
            tp = Topics('Benkart', [self.topic])
            tp_dict = tp.create_topics()
            self.topic = tp_dict['created_topics'][0]
        else:
            self.topic = "Benkart" +'_'+ self.topic
            
        self.publish()

    def initialize(self):
        """ Initialize producer with bootstrap server """
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap
        )

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
        if self.file_type == "text":
            for val in self.data["sentence"]:
                self.producer.send(self.topic, bytes(val, 'utf-8')).add_callback(
                    self.on_send_success).add_errback(self.on_send_error)
        elif self.file_type == "audio":
            for val in self.data["audio_file"]:
                self.producer.send(self.topic, bytes(val)).add_callback(
                    self.on_send_success).add_errback(self.on_send_error)



# if __name__ == "__main__":
#     Producer().start_publishing()
