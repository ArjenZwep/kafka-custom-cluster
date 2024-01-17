
# technical kafka cluster
Custom Kafka running in python. This can receive different type of avro data from producers and push them to different sources using connectors. All avro type data is universal for this application.
To start the application, clone the reprository and run the following command:


```
  docker-compose up
```

After this wait for all the images to download and start up, this may take several minutes.
After this the whole application will start streaming itself.


## Monitoring the messages

We can see this in the application image where the producer and consumer are running.
In the logs you. We can now look on ```localhost:8084 ``` to see the topics and it's content.
We want to see the end result inside the consumer_topic. You can explore the end data that has been streamed.

### Finnhub producer

One of the custom producer is a connection to the finnhub real time financial data.
You can create a token at finnhub API docs. Input them into the .env to receive real time data and load it to kafka.