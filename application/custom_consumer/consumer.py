from kafka import KafkaConsumer
from json import loads
from kafka import KafkaProducer
from json import dumps
import threading
import logging

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

def innit_consumer(bootstrap_server):
    """Creates the custom consumer that will consume from the topic and sends to the orderd one"""
    consumer = KafkaConsumer(
        'producer_topic',
        bootstrap_servers=bootstrap_server,
        auto_offset_reset='earliest', #we set this so the messages will not be lost if the consumer ever goes down
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    return consumer

def sort_messages(bootstrap_server):
    """Consume the unorderd topic and sends to our end topic"""
    consumer = innit_consumer(bootstrap_server)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda x: dumps(x).encode('utf-8'),
    )

    for event in consumer:
        event_data = event.value #read out the messages
        producer.send('consumer_topic', value=event_data) #send them to our end topic

#TODO create the custom partitioner which would use the sorting algorithm to keep ordering our end topic

def sorting_algo(message_batch):
    """implementing a simple bublesort for sorting the array"""
    n = len(message_batch)

    for i in range(n):
        already_sorted = True

        for j in range(n - i - 1):
            if message_batch[j] > message_batch[j + 1]:
                message_batch[j], message_batch[j + 1] = message_batch[j + 1], message_batch[j]
                already_sorted = False

        if already_sorted:
            break

    return message_batch

def end_topic_consumer(bootstrap_server):
    """This consumer is used to move the orderd topic data out of kakfa into another source, like a cloud database"""
    consumer_data = KafkaConsumer(
        'consumer_topic',
        bootstrap_servers=bootstrap_server,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    for event in consumer_data:
        event_data = event.value
        #TODO send data somewhere to be stored


def innit_conusmers(bootstrap_server):
    logging.info('starting raw data consumer')
    consumer_thread = threading.Thread(target=sort_messages, args=(bootstrap_server, ))
    consumer_thread.start()
    logging.info('starting end consumer thread')
    end_consumer_thread = threading.Thread(target=end_topic_consumer, args=(bootstrap_server, ))
    end_consumer_thread.start()
    return 'Threads succesfully started'

if __name__ == "__main__":
    #create_consumer_topic()
    bootstrap_server = 'localhost:9092' #Set external bootstrap for testing locally
    logging.info(innit_conusmers(bootstrap_server))