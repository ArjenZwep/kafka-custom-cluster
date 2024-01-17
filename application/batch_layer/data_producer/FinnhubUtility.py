import json
import finnhub
import io
import avro.schema
import avro.io
import ast

class FinnhubUtility:

    def __init__(self, token, avro_schema_path, tickers, validate):
        self.finnhub_client = self.load_client(token)
        #self.avro_schema = self.load_avro_schema(avro_schema_path)
        #self.tickers = ast.literal_eval(tickers)
        self.validate = validate

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