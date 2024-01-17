import threading
import logging
from batch_layer.data_producer.message_producer import start_producers
import time
from serving_layer.custom_consumer.consumer import innit_conusmers
from batch_layer.data_producer.topic_creater import create_topics


#setting up inital global variables
#time.sleep(30) #we give time for kafka image to innitalize #TODO ping container instead set waiting time
format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                    datefmt="%H:%M:%S")
bootstrap_server = 'localhost:9092' #setting internal bootstrap for dockerized application

#starting main logic line
try:
    logging.info('trying to create topics in kafka')
    create_topics() #first we create the topics
except:
    logging.info('topics already exsist in current kafka application, starting application')

time.sleep(20)
#starting up our producer
producer_thread = threading.Thread(target=start_producers, args=(bootstrap_server, ))
logging.info('Starting producer application')
producer_thread.start()

#starting up the consumer
consumer_thread = threading.Thread(target=innit_conusmers, args=(bootstrap_server, ))
consumer_thread.start()