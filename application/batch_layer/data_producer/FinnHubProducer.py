# Main file for Finnhub API & Kafka integration
import os
import json
import websocket
from FinnhubUtility import FinnhubUtility
import threading
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import logging

bootstrap_server = f"localhost:9092"

class FinnhubProducer:
    """The Finnhub producer starts up a websocket API to finhubb, it then retrieves that data with schema and
    loads this to kafka. Invoking start_producer will create the topic and start streaming async"""

    def __init__(self):
        # FinnhubProducer is a compostion of the direct API interface class, a thread and a kafka producer
        # TODO Make the producer better parameterized for starting multipile producers at the same time
        self.finnhub_utility = FinnhubUtility(
            token=os.environ['FINNHUB_API_TOKEN'],
            avro_schema_path='src/schemas/trades.avsc',
            tickers=os.environ['FINNHUB_STOCKS_TICKERS'],
            validate=os.environ['FINNHUB_VALIDATE_TICKERS'])

        # self.producer = KafkaProducer(bootstrap_server=f"{os.environ['KAFKA_SERVER']}:{os.environ['KAFKA_PORT']}")
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server)
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_server)
        self.thread = threading.Thread(target=self.start_producer)
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token=cmjquk1r01qnr60h17k0cmjquk1r01qnr60h17kg',
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

    # top level start-up function
    def start_producer(self):
        #self.create_topic(topic_name='trades')
        self.start_data_load()

    def start_data_load(self):
        # We can invoke the async run forever method to keep polling
        self.ws.run_forever()

    def create_topic(self, topic_name):
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=5, replication_factor=1))
        try:
            self.admin_client.delete_topics(topics=[topic_name])  # clean up cached topic
        except:
            logging.info('no old topic found')
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)

    # Behaviour utility functions for the producer
    def on_message(self, message):
        message = json.loads(message)
        avro_message = self.finnhub_utility.avro_encode(
            {
                'data': message['data'],
                'type': message['type']
            }
        )
        #self.producer.send(os.environ['KAFKA_TOPIC_NAME'], avro_message)
        self.producer.send('trades', avro_message)

    def on_error(self):
        print('sure')

    def on_close(self):
        print("### closed ###")

    def on_open(self):
        #open connection for getting binance finiance data
        self.ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')


if __name__ == "__main__":
    test_producer = FinnhubProducer()
    test_producer.start_producer()
