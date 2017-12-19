package org.ust.transformer

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import java.lang.{Long => JLong}

import org.apache.jute.compiler
import org.apache.kafka.streams.state.Stores

import scala.collection.mutable

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
    /*val jsonSchemaForSongs = """{"namespace": "org.ust.aggregator.schema", "type": "record", "name": "aggregator", "fields": [{"name": "song", "type": "string"}, {"name": "artist","type": "string"}, {"name": "album","type": "string"}, {"name": "genre","type": "string"}, {"name": "playduration","type": "int"}, {"name": "rating","type": "int"}, {"name": "user","type": "string"}, {"name": "usertype","type": "string"}, {"name": "city","type": "string"}, {"name": "location","type": "string"}, {"name": "starttime","type": "string"}]}"""
    val schemaForSongs = new Schema.Parser().parse(jsonSchemaForSongs)*/

    val keyArtist = new KeyValueMapper[String, GenericRecord, KeyValue[String, JLong]] {
      override def apply(key: String, value: GenericRecord): KeyValue[String, JLong] = {
        new KeyValue[String, JLong](value.get("artist").toString, 1L)
      }
    }

    val keyAndArtist: KStream[String, JLong] = songs.map(keyArtist)

    val stringSerde: Serde[String] = Serdes.String()
    val longSerde: Serde[JLong] = Serdes.Long()


    val temp = keyAndArtist.through(stringSerde, longSerde, "avro-temp-4")
    temp.groupByKey(stringSerde, longSerde).count("artist-aggregation-4").toStream.to(stringSerde, longSerde, outputTopic)


    val streams =  new KafkaStreams(builder, properties)
    println("Stream application started!")
    streams.start
    streams
  }
}
