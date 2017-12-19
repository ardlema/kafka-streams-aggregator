# Kafka streams aggregator

## Description

This is a proof of concept of aggregating data using the Kafka Streams API.
 
This project is meant to be used along the kafka-data-pipeline project.

## Building and run the example using docker

To build the image you can run:

```
docker build -t kafka-streams-aggregator:latest .
```

And we run the docker with these parameters:

```
docker run -v $PWD:/workspace --network deveventbus_default -it simple-event-transformer
```

So current working directory will be mounted on container in "/workspace" folder and uses the same network as event bus container in compose.

Entrypoint is bash for now, so you have to run application with:

```
sbt "run <broker-list> <zookeeper> <schema-registry-url> <quotes-topic> <policies-topic> <output-topic>"
```

Where:

* broker-list: list of kafka brokers separated by commas, see following an example: 172.22.0.3:9092,172.22.0.4:9092
* zookeeper: zookeeper instance, see following an example: 172.22.0.2:2181
* quotes-topic: the name of the topic where the quotes events will be written (see below for examples on how to create these events)
* output-topic: the name of the topic where the transformation events will be written

## Testing the service

We suggest to run the kafka-data-pipeline project and then start the kafka-streams-aggregator.

If you don't want to start the kafka-data-pipeline project and test the service standalone you should [download the confluent kafka distribution](http://packages.confluent.io/archive/3.0/confluent-3.0.1-2.11.tar.gz) which includes command line scripts to both produce and consume messages in avro format.

Once you have download and unpackage the confluent distro, you should go to the bin directory and execute the following commands to start producing events:

* To start the kafka-avro-console-producer for producing quote events execute the following command:
 
```
./kafka-avro-console-producer --broker-list 172.22.0.3:9092 --topic feed-quotes --property value.schema='{"type":"record","name":"myrecord","fields":[{"name": "date_of_birth", "type": "string"}, {"name": "years_licence", "type": "int"}, {"name": "country_licence_issued", "type": "string"}, {"name": "driver_licence_type", "type": "string"}, {"name": "price_estimation", "type": "double"}]}' --property parse.key='true' --property key.separator='|' --property key.schema='{"type":"record","name":"mykey","fields":[{"name": "key", "type": "string"}]}'
```
 And then produce quote events as following:
 
 ```
 {"key":"17"}|{"date_of_birth":"17/08/1978","years_licence":15,"country_licence_issued":"SPAIN","driver_licence_type":"B2","price_estimation":1200.00}
 ```

 
 * To start the kafka-avro-console-consumer to start consuming the policy-quote events produced by the transformation execute the following command:
 
 ```
 ./kafka-avro-console-consumer --topic output-topic --zookeeper 172.22.0.2:2181 --from-beginning
 ```
 
You should see messages in the consumer console like the following:
 
 ```
 {"driver":"Maria Perez","nif":"02327463X","finalprice":1400.0,"difference":200.0}
 
 
 
 ```
 
 ./kafka-avro-console-producer --broker-list localhost:9092 --topic input-topic --property value.schema='{"type":"record","name":"aggregator","fields":[{"name": "song", "type": "string"}, {"name": "artist","type": "string"}, {"name": "album","type": "string"}, {"name": "genre","type": "string"}, {"name": "playduration","type": "int"}, {"name": "rating","type": "int"}, {"name": "user","type": "string"}, {"name": "usertype","type": "string"}, {"name": "city","type": "string"}, {"name": "location","type": "string"}, {"name": "starttime","type": "string"}]}' --property parse.key='true' --property key.separator='|' --property key.schema='{"type":"record","name":"mykey","fields":[{"name": "key", "type": "string"}]}'
 
 
 {"key":"1"}|{"song":"Enter Sandman","artist":"Metallica","album":"Metallica","genre":"Metal","playduration":140, "rating":3,"user":"Alberto1234","usertype":"free","city":"Madrid","location":"Madrid","starttime":"20:30:12"}