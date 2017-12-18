package org.ust.transformer

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object EventAggregator {

  def main(args: Array[String]): Unit = {

    val kafkaConfig = new Properties()
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-event-transformer")
    kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args(0))
    kafkaConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, args(1))
    kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, args(2))
    val inputTopic = args(3)
    val outputTopic = args(4)
    aggregateByArtist(inputTopic, outputTopic, kafkaConfig)
  }

  def aggregateByArtist(inputTopic: String, outputTopic: String, properties: Properties): KafkaStreams = {
    properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde].getName)
    properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100")
    val builder = new KStreamBuilder

    // read the source stream
    val songs: KStream[String, GenericRecord] = builder.stream(inputTopic)
    // this should be in a separate avsc file
    val jsonSchemaForSongs = """{"namespace": "org.ust.aggregator.schema", "type": "record", "name": "aggregator", "fields": [{"name": "song", "type": "string"}, {"name": "artist","type": "string"}, {"name": "album","type": "string"}, {"name": "genre","type": "string"}, {"name": "playduration","type": "int"}, {"name": "rating","type": "int"}, {"name": "user","type": "string"}, {"name": "usertype","type": "string"}, {"name": "city","type": "string"}, {"name": "location","type": "string"}, {"name": "starttime","type": "string"}]}"""
    val schemaForSongs = new Schema.Parser().parse(jsonSchemaForSongs)

    val keyValueMapper = new KeyValueMapper[String, GenericRecord, String] {
      override def apply(key: String, value: GenericRecord): String = {
          value.get("artist").asInstanceOf[String]
      }
    }

    songs
      .groupBy(keyValueMapper)
      .count("output")
      .to(outputTopic)


    val streams =  new KafkaStreams(builder, properties)
    println("Stream application about to start...")
    streams.start
    streams
  }
}
