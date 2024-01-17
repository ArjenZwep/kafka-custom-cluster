# Main file for Finnhub API & Kafka integration
import os
import ast
import json
import websocket
import FinnhubUtility
import threading

class FinnhubProducer:

    def __init__(self):
        print('Environment:')
        for k, v in os.environ.items():
            print(f'{k}={v}')

        #FinnhubProducer is a compostion of the direct API interface class, a thread and a kafka producer
        self.finnhub_utility = FinnhubUtility(
            token = os.environ['FINNHUB_API_TOKEN'],
            avro_schema_path = 'src/schemas/trades.avsc')
        self.thread = threading.Thread(target=self.ws.run_forever)

        self.producer = load_producer(f"{os.environ['KAFKA_SERVER']}:{os.environ['KAFKA_PORT']}")
        self.avro_schema = load_avro_schema('src/schemas/trades.avsc')
        self.tickers = ast.literal_eval(os.environ['FINNHUB_STOCKS_TICKERS'])
        self.validate = os.environ['FINNHUB_VALIDATE_TICKERS']

        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token={os.environ["FINNHUB_API_TOKEN"]}',
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open

        # Create a thread to run the WebSocket in the background
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.start()

    def on_message(self, message):
        message = json.loads(message)
        avro_message = avro_encode(
            {
                'data': message['data'],
                'type': message['type']
            },
            self.avro_schema
        )
        self.producer.send(os.environ['KAFKA_TOPIC_NAME'], avro_message)

    def on_error(self, error):
        print(error)

    def on_close(self):
        print("### closed ###")

    def on_open(self):
        for ticker in self.tickers:
            if self.validate == "1":
                if ticker_validator(self.finnhub_client, ticker):
                    self.ws.send(f'{"type":"subscribe","symbol":"{ticker}"}')
                    print(f'Subscription for {ticker} succeeded')
                else:
                    print(f'Subscription for {ticker} failed - ticker not found')
            else:
                self.ws.send(f'{"type":"subscribe","symbol":"{ticker}"}')

if __name__ == "__main__":
    FinnhubProducer()
