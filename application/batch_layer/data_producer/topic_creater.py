from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
import logging

def create_producer_topic(bootstrap_server):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_server,
        client_id='test'
    )
    topic_list = []
    topic_list.append(NewTopic(name="producer_topic", num_partitions=10, replication_factor=1)) #init the the conf for new topic
    try:
        admin_client.delete_topics(topics=['producer_topic']) #clean up cached topic
    except:
        logging.info('no producer_topic found')
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    try:
        part=NewPartitions(9) #sometimes the topic will create with 1 partition so we try to manually add 9 if it does this
        admin_client.create_partitions(topic_partitions={'producer_topic' : part})
    except:
        logging.info('topic already has 10 partitions')

def create_consumer_topic(bootstrap_server):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_server,
        client_id='test'
    )
    topic_list = []
    topic_list.append(NewTopic(name="consumer_topic", num_partitions=1, replication_factor=1)) #init the conf for new topic
    try:
        admin_client.delete_topics(topics=['consumer_topic']) #clean up cached topic
    except:
        logging.info('no producer_topic found')
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

def create_topics(bootstrap_server):
    create_producer_topic(bootstrap_server)
    create_consumer_topic(bootstrap_server)
    return True

if __name__ == "__main__":
    #create_consumer_topic()
    bootstrap_server = 'localhost:9092' #Set external bootstrap for testing locally
    create_topics(bootstrap_server)