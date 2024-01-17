# Main file for Finnhub API & Kafka integration
import os
import json
import websocket
import FinnhubUtility
import threading
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import logging


class FinnhubProducer:
    """The Finnhub producer starts up a websocket API to finhubb, it then retrieves that data with schema and
    loads this to kafka. Invoking start_producer will create the topic and start streaming async"""
    def __init__(self):
        #FinnhubProducer is a compostion of the direct API interface class, a thread and a kafka producer
        #TODO Make the producer better parameterized for starting multipile producers at the same time
        self.finnhub_utility = FinnhubUtility(
            token=os.environ['FINNHUB_API_TOKEN'],
            avro_schema_path='src/schemas/trades.avsc',
            tickers=os.environ['FINNHUB_STOCKS_TICKERS'],
            validate=os.environ['FINNHUB_VALIDATE_TICKERS'])
        self.producer = KafkaProducer(bootstrap_server=f"{os.environ['KAFKA_SERVER']}:{os.environ['KAFKA_PORT']}")
        self.admin_client = KafkaAdminClient
        self.thread = threading.Thread(target=self.start_producer)

    #top level start-up function
    def start_producer(self):
        self.create_topic(topic_name='trades')
        self.start_data_load()

    def start_data_load(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token={os.environ["FINNHUB_API_TOKEN"]}',
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        ws.on_open = self.on_open

        # We can invoke the async run forever method to keep polling
        ws.run_forever

    def create_topic(self, topic_name):
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=5, replication_factor=1))
        self.admin_client
        try:
            self.admin_client.delete_topics(topics=[topic_name])  # clean up cached topic
        except:
            logging.info('no old topic found')
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)

    #Behaviour utility functions for the producer
    def on_message(self, message):
        message = json.loads(message)
        avro_message = self.finnhub_utility.avro_encode(
            {
                'data': message['data'],
                'type': message['type']
            },
            self.avro_schema
        )
        self.producer.send(os.environ['KAFKA_TOPIC_NAME'], avro_message)

    def on_error(self, error):
        print(error)


    def on_open(self):
        for ticker in self.finnhub_utility.tickers:
            if self.validate == "1":
                if self.finnhub_utility.ticker_validator(self.finnhub_client, ticker):
                    self.ws.send(f'{"type":"subscribe","symbol":"{ticker}"}')
                    print(f'Subscription for {ticker} succeeded')
                else:
                    print(f'Subscription for {ticker} failed - ticker not found')
            else:
                self.ws.send(f'{"type":"subscribe","symbol":"{ticker}"}')

if __name__ == "__main__":
    FinnhubProducer()
