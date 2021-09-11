from kafka import KafkaProducer


def ping_kafka_when_request():

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic_name = "first_topic"
    producer.send(topic_name, value="Testing String nnnn".encode())
    producer.flush()


ping_kafka_when_request()
