import threading
import logging
from batch_layer.data_producer.message_producer import start_producers
import time
from serving_layer.custom_consumer.consumer import innit_conusmers
from batch_layer.data_producer.topic_creater import create_topics
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions


#setting up inital global variables
time.sleep(30) #we give time for kafka image to innitalize #TODO ping container instead set waiting time
format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                    datefmt="%H:%M:%S")
bootstrap_server = 'kafka:9093' #setting internal bootstrap for dockerized application

#starting main logic line
try:
    logging.info('trying to create topics in kafka')
    create_topics() #first we create the topics
except:
    logging.info('topics already exsist in current kafka application, starting application')
# try:
#     logging.info('create extra partitions')
#     admin_client = KafkaAdminClient(
#         bootstrap_servers=bootstrap_server,
#         client_id='test'
#     )
#     part = NewPartitions(9)  # sometimes the topic will create with 1 partition so we try to manually add 9 if it does this
#     admin_client.create_partitions(topic_partitions={'producer_topic': part})
# except:
#     logging.info('already has 9 partitions')

time.sleep(20)
#starting up our producer
producer_thread = threading.Thread(target=start_producers, args=(bootstrap_server, ))
logging.info('Starting producer application')
producer_thread.start()

#starting up the consumer
consumer_thread = threading.Thread(target=innit_conusmers, args=(bootstrap_server, ))
consumer_thread.start()