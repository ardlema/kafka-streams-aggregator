# Kafka streams aggregator

## Description

This is a proof of concept of aggregating data using the Kafka Streams API. It will aggregate by artist a fake stream of songs.


## Building and run the example using docker

To build the image you firstly need to create the artifact, to do so please execute the following command:

```
mvn clean package -DskipTests
```

Then you should build the docker image:

```
docker build -t kafka-streams-aggregator:latest .
```

And finally run the container with these parameters:

```
docker run -e BROKER_LIST=<broker-list> -e ZOOKEEPER=<zookeeper> -e SCHEMA_REGISTRY=http://172.19.16.217:8081 -e INPUT_TOPIC=docker-input-3 -e OUTPUT_TOPIC=docker-output-3 --name streams-aggregator kafka-streams-aggregator:latest
```

Where:


* BROKER_LIST: list of kafka brokers separated by commas, see following an example: 172.22.0.3:9092,172.22.0.4:9092
* ZOOKEEPER: zookeeper instance, see following an example: 172.22.0.2:2181
* SCHEMA_REGISTRY: schema registry url, see following an example: http://172.19.16.217:8081
* INPUT_TOPIC: the name of the topic where the songs events will be written (see below for examples on how to create these events)
* OUTPUT_TOPIC: the name of the topic where the aggregation events will be written

## Testing the application

The application will listen to incoming events from the input-topic and it expects the events to comply with the following format:

```
{"song":"Enter Sandman","artist":"Metallica","album":"Metallica","genre":"Metal","playduration":140, "rating":3,"user":"Alberto1234","usertype":"free","city":"Madrid","location":"Madrid","starttime":"20:30:12"}
```

It will aggregate the songs by artist (generating internally a Ktable) and will output the results to the output topic, with the following format:

```
Metallica     2
Foo fighters  4
Muse          1
```

To manually test the service you should [download the confluent kafka distribution](http://packages.confluent.io/archive/3.0/confluent-3.0.1-2.11.tar.gz) which includes command line scripts to both produce and consume messages in avro format.

Once you have download and unpackage the confluent distro, you should go to the bin directory and execute the following commands to start producing events:

* To start the kafka-avro-console-producer for producing song events execute the following command:
 
```
./kafka-avro-console-producer --broker-list localhost:9092 --topic input-topic --property value.schema='{"type":"record","name":"aggregator","fields":[{"name": "song", "type": "string"}, {"name": "artist","type": "string"}, {"name": "album","type": "string"}, {"name": "genre","type": "string"}, {"name": "playduration","type": "int"}, {"name": "rating","type": "int"}, {"name": "user","type": "string"}, {"name": "usertype","type": "string"}, {"name": "city","type": "string"}, {"name": "location","type": "string"}, {"name": "starttime","type": "string"}]}' --property parse.key='true' --property key.separator='|' --property key.schema='{"type":"record","name":"mykey","fields":[{"name": "key", "type": "string"}]}'
```

And then produce song events as following:
 
```
{"key":"1"}|{"song":"Enter Sandman","artist":"Metallica","album":"Metallica","genre":"Metal","playduration":140, "rating":3,"user":"Alberto1234","usertype":"free","city":"Madrid","location":"Madrid","starttime":"20:30:12"}
```

 
 * To start the kafka-avro-console-consumer to start consuming the aggregate events produced by the transformation execute the following command:
 
 ```
./kafka-console-consumer --topic test-output-20 --bootstrap-server localhost:9092 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --from-beginning
 ```
 
You should see messages in the consumer console like the following:
 
```
 1  Metallica
```