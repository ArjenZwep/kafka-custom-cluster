
# technical kafka cluster

This application is made for the technical challenge.
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
