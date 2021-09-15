import unittest
import pandas as pd
import logging, logging.handlers
from unittest.mock import patch
import os
import sys
sys.path.append(os.path.abspath(os.path.join('../scripts')))
from producer import Producer
from helper import Helper
from consumer import Consumer

class TestDf(unittest.TestCase):
    ''' testing  functons in the helper class
    '''
    def setup(self):
        self.helper = Helper

    def test_to_csv(self):
        df = pd.DataFrame({'col1': range(1,4), 'col2': range(3,6)})
        self.helper = Helper
        csv_path = '../Data/'
        self.helper.save_csv('data.csv',csv_path, False)
        df2 = pd.read_csv('../Data/data.csv')


#     def test_read_csv(self, df):
#         df = self.helper.read_csv('../Data/test.csv')
#         df2 = pd.read_csv('../Data/test.csv')
        
#     @patch('kafka.KafkaProducer')
    
#     def test_producer(KafkaProducerMock):
#         Producer.publishg(self.data)
#         KafkaProducerMock.send.assert_called
if __name__ == '__main__':
    unittest.main()     
   # @patch('kafka.KafkaProducer.send')
    
#     def test_produced_message(KafkaProducerMock):
    
#         Producer.publish({'key1':'i', 'key2': 'i'})
#         publishToKafkaTopic({'key1':'value1', 'key2': 'value2'})
#         args = KafkaProducerMock.call_args
#         assert(args[0] == (TOPIC_NAME,))
#         assert(args[2] == {'value': {'key1':'value1', 'key2': 'value2'}}) 
        
        
#               i = 0
#         for val in self.data:
#             i += 1
#             self.producer.send(self.topic, key=i, value=val).add_callback(
#                 self.on_send_success).add_errback(self.on_send_error)
#            # print("Sent to topic",self.topic,": ",val)
        
#     def test_producer(self):
        
#         publish_topic = producer.start_publishing()
#         if publish_topic == self.publish():
#             return publish_topic
#         else:
#             print("can't generate a topic")
#     def test_consumer(self):
        
