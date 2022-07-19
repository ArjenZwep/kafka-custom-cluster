import logging
import threading
import time
import random
from kafka import KafkaProducer
from json import dumps

def innit_kafka_broker(parition_count, bootstrap_servers):
    """Creates a producer"""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8'),
    )
    message_sender(parition_count, producer)

def message_sender(partition_count, producer, starting_message = 0):
    """recursive function that sends a orderd message every 10 seconds"""
    time.sleep(10)
    if starting_message == 0:
        starting_message = random.randint(1, 15) #partitions starts a random count to have unorderd data across paritions
    else:
        starting_message += 1 #keeps orderd count when partition already started counting
    message_data = starting_message
    logging.info(f'partition {partition_count} is sending: {starting_message}')
    producer.send('producer_topic', value=message_data, partition=partition_count)
    return message_sender(partition_count, producer, starting_message)

def message_partition(partition_count: int, bootstrap_servers) -> bool:
    """activates 1 partition that will send messages"""
    logging.info(f'starting partition number {partition_count}')
    thread = threading.Thread(target=innit_kafka_broker, args=(partition_count, bootstrap_servers))
    return thread

def start_producers(bootstrap_servers):
    """initialize producer for every parition in the topic"""
    for i in range(1, 10):
        partition_thread = message_partition(i, bootstrap_servers)
        partition_thread.start()

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    bootstrap_servers = 'localhost:9092' #setting bootstrap external for local development
    start_producers(bootstrap_servers)
    # create_topic()
