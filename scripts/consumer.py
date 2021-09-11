import time
import threading
import datetime
from kafka import KafkaConsumer
consumer = KafkaConsumer('first_topic',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         consumer_timeout_ms=-1)


def fetch_last_min_requests(next_call_in, is_first_execution=False):
    next_call_in += 60
    counter_requests = 0
    batch = consumer.poll(timeout_ms=100)
    if len(batch) > 0:
        for message in list(batch.values())[0]:
            counter_requests += 1


next_call_in += 60
threading.Timer(time.time(),
                fetch_last_minute_requests,
                [next_call_in]).start()
