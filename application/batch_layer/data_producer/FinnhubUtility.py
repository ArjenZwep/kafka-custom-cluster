import json
import finnhub
import io
import avro.schema
import avro.io
from kafka import KafkaProducer

class FinnhubUtility:

    def __init__(self, token, avro_schema_path):
        self.finnhub_client = self.load_client(token)
        self.avro_schema = self.load_avro_schema(avro_schema_path)

    def load_client(self, token):
        return finnhub.Client(api_key=token)

    def lookup_ticker(self, ticker):
        return self.finnhub_client.symbol_lookup(ticker)

    def ticker_validator(self, ticker):
        for stock in self.lookup_ticker(ticker)['result']:
            if stock['symbol'] == ticker:
                return True
        return False


    def load_avro_schema(self, schema_path):
        return avro.schema.parse(open(schema_path).read())

    def avro_encode(self, data):
        writer = avro.io.DatumWriter(self.avro_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return bytes_writer.getvalue()

# Example usage:
finnhub_utility = FinnhubUtility(
    token='your_finnhub_api_key',
    kafka_server='your_kafka_server',
    avro_schema_path='path_to_your_avro_schema_file.avsc'
)

# Now you can use the methods of the FinnhubUtility class
# For example:
ticker_exists = finnhub_utility.ticker_validator('AAPL')