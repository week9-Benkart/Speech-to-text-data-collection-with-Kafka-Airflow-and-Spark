from kafka import KafkaConsumer
import json


class Consumer:
    """ Class for Consumer"""

    def __init__(self, bootstrap='b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092', topic='transc_data'):
        """ Initialize consumer with the given bootstrapserver"""
        self.bootstrap = bootstrap
        self.topic = topic

    def start_consuming(self):
        """ Start Consumer"""
        self.initialize()
        self.print_msg()

    def initialize(self):
        """ Initialize Consumer"""
        self.consumer = KafkaConsumer(self.topic,
                                      group_id='my-group',
                                      # value_deserializer=lambda m: json.loads(m.decode('UTF-8')),
                                      bootstrap_servers=self.bootstrap)

    def print_msg(self):
        """ Print messages from consumer"""
        for msg in self.consumer:
            print(msg.value)


if __name__ == "__main__":
    Consumer().start_consuming()
