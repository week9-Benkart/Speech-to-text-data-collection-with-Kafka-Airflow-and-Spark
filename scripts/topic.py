from kafka.admin import KafkaAdminClient, NewTopic
try:
    from app_logger import App_Logger
except:
    from scripts.app_logger import App_Logger


class Topics():
    def __init__(self, group_name: str, topics: list) -> None:
        '''Instantiate a class to create topic on aws. group_name is string, topics is a list of topics to be created'''
        self.group_name = group_name
        self.topics = [group_name + '_' + topic for topic in topics]
        self.logger = App_Logger().get_logger(__name__)

    # modified from code received from https://github.com/Blvisse
    def create_topic(self, topic_name):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092", "b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],
                                            client_id='tests_id',)

            topic_list = []
            topic_list.append(
                NewTopic(name=topic_name, num_partitions=1, replication_factor=2))
            admin_client.create_topics(
                new_topics=topic_list, validate_only=False)

        except Exception:
            self.logger.exception('Error creating topic.')

    def create_topics(self) -> dict:
        '''Iterate thorough topics to be created and create them one after the other. Then return a dictionary of created topics and all topics'''
        for topic in self.topics:
            self.create_topic(topic)

        admin_client = KafkaAdminClient(bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092", "b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],
                                        client_id='tests_id',)
        all_topics = admin_client.list_topics()
        all_topics.sort()
        created_topics = self.topics
        created_topics.sort()
        return {'all_topics': all_topics, 'created_topics': created_topics}
