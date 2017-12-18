package org.ust.transformer

import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import kafka.consumer.ConsumerConfig
import kafka.server.KafkaConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers}
import org.ust.transformer.infra.KafkaInfra

class EventAggregatorTest extends FlatSpec with KafkaInfra with Matchers {

  "The event aggregator" should "aggregate songs by artist" in {

    var result = false

    val kafkaConfig = new Properties()
    //TODO: Extract these properties to a dev environment config file!!
    //for now we ips need to be get from containers with inspect
    
    kafkaConfig.put(bootstrapServerKey, "localhost:9092")
    kafkaConfig.put("zookeeper.host", "localhost")
    kafkaConfig.put("zookeeper.port", "2181")
    kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

    kafkaConfig.put(keySerializerKey, classOf[KafkaAvroSerializer])
    kafkaConfig.put(valueSerializerKey, classOf[KafkaAvroSerializer])
    kafkaConfig.put(keyDeserializerKey, classOf[KafkaAvroDeserializer])
    kafkaConfig.put(valueDeserializerKey, classOf[KafkaAvroDeserializer])
    kafkaConfig.put(groupIdKey, groupIdValue)
    kafkaConfig.put(KafkaConfig.BrokerIdProp, defaultBrokerIdProp)
    kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
    kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
    kafkaConfig.put(KafkaConfig.NumPartitionsProp, defaultPartitions)
    kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, defaultAutoCreateTopics)
    kafkaConfig.put(ConsumerConfig.AutoOffsetReset, "earliest")

    withKafkaServer(Option(kafkaConfig), false) { kafkaServer =>
      val properties = fromKafkaConfigToProps(kafkaServer.config)
      properties.put("application.id", "quote-policy-events-transformer")
      val inputTopic = "songs"
      val outputTopic = "artists-count"


      ThreadRunner.runThread(20000) {
        EventAggregator.aggregateByArtist(inputTopic, outputTopic, properties)
      }


      ThreadRunner.runThread(10000) {
        withKafkaProducer(properties) { kafkaProducer: KafkaProducer[Any, Any] =>
          withKafkaConsumer(properties, List(outputTopic)) { kafkaConsumer =>
            //This should be in an external avsc file!!!
            val jsonSchemaForSongs = """{"namespace": "org.ust.aggregator.schema", "type": "record", "name": "aggregator", "fields": [{"name": "song", "type": "string"}, {"name": "artist","type": "string"}, {"name": "album","type": "string"}, {"name": "genre","type": "string"}, {"name": "playduration","type": "int"}, {"name": "rating","type": "int"}, {"name": "user","type": "string"}, {"name": "usertype","type": "string"}, {"name": "city","type": "string"}, {"name": "location","type": "string"}, {"name": "starttime","type": "string"}]}"""

            val songs = new Schema.Parser().parse(jsonSchemaForSongs)

            val genericSongMetallica1 = new GenericData.Record(songs)
            val genericSongMetallica2 = new GenericData.Record(songs)
            val genericSongPearlJam = new GenericData.Record(songs)

            genericSongMetallica1.put("song", "Enter Sandman")
            genericSongMetallica1.put("artist", "Metallica")
            genericSongMetallica1.put("album", "Metallica")
            genericSongMetallica1.put("genre", "Metal")
            genericSongMetallica1.put("playduration", 140)
            genericSongMetallica1.put("rating", 3)
            genericSongMetallica1.put("user", "Alberto1234")
            genericSongMetallica1.put("usertype", "free")
            genericSongMetallica1.put("city", "Madrid")
            genericSongMetallica1.put("location", "Madrid")
            genericSongMetallica1.put("starttime", "20:30:12")

            genericSongMetallica2.put("song", "Fade to Black")
            genericSongMetallica2.put("artist", "Metallica")
            genericSongMetallica2.put("album", "Live Shit: Binge & Purge")
            genericSongMetallica2.put("genre", "Metal")
            genericSongMetallica2.put("playduration", 160)
            genericSongMetallica2.put("rating", 2)
            genericSongMetallica2.put("user", "Alberto1234")
            genericSongMetallica2.put("usertype", "free")
            genericSongMetallica2.put("city", "Madrid")
            genericSongMetallica2.put("location", "Madrid")
            genericSongMetallica2.put("starttime", "20:32:07")

            genericSongMetallica2.put("song", "Black")
            genericSongMetallica2.put("artist", "Pearl Jam")
            genericSongMetallica2.put("album", "Ten")
            genericSongMetallica2.put("genre", "Rock")
            genericSongMetallica2.put("playduration", 115)
            genericSongMetallica2.put("rating", 4)
            genericSongMetallica2.put("user", "Alberto1234")
            genericSongMetallica2.put("usertype", "free")
            genericSongMetallica2.put("city", "Madrid")
            genericSongMetallica2.put("location", "Madrid")
            genericSongMetallica2.put("starttime", "20:34:07")

            val songId1 = "12345678X"
            val songId2 = "44545423M"
            val songId3 = "34545423M"
            val song1 = new ProducerRecord[Any, Any](inputTopic, songId1, genericSongMetallica1)
            val song2 = new ProducerRecord[Any, Any](inputTopic, songId2, genericSongMetallica2)
            val song3 = new ProducerRecord[Any, Any](inputTopic, songId3, genericSongPearlJam)

            kafkaProducer.send(song1)
            kafkaProducer.send(song2)
            kafkaProducer.send(song3)

            import scala.collection.JavaConversions._

            while (true) {
              val records = kafkaConsumer.poll(10).records(outputTopic)

              if (records.size > 0) {
                val matchingRecord = records.toList(0)
                result = matchingRecord.key.equals("12345678X")
              }
            }
          }
        }
      }

      result shouldBe(true)
    }
  }
}


object ThreadRunner {

  def runThread(sleepInMillis: Int = 5000)(function: => Any) = {
    val thread = new Thread {
      override def run: Unit = {
        function
      }
    }
    thread.start
    Thread.sleep(sleepInMillis)
  }
}