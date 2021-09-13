from kafka import KafkaConsumer
import json
from app_logger import App_Logger
import numpy as np

class Consumer:
    """ Class for Consumer"""

    def __init__(self, bootstrap='b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092', topic='Benkart_test_data',file_type="text"):
        """ Initialize consumer with the given bootstrapserver"""
        self.bootstrap = bootstrap
        self.topic = topic
        self.logger = App_Logger().get_logger(__name__)
        self.file_type = file_type

    def start_consuming(self):
        """ Start Consumer"""
        self.initialize()
        self.print_msg()

    def initialize(self):
        """ Initialize Consumer"""
        self.consumer = KafkaConsumer(self.topic,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=False,
                                      bootstrap_servers=self.bootstrap)

    def print_msg(self):
        """ Print text messages from consumer"""
        for message in self.consumer:
            if self.file_type == "text":
                print ("%s:%d:%d: value=%s" % (message.topic, message.partition,
                                          message.offset,message.value.decode('utf-8')))

            elif self.file_type == "audio":
                print ("%s:%d:%d: value=%s" % (message.topic, message.partition,
                                      message.offset,np.frombuffer(message.value, dtype=np.float32)))

            else:
                print("error")


# if __name__ == "__main__":
#     Consumer().start_consuming()
