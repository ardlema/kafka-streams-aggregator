package org.ust.transformer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * A schema-registry aware serializer for writing data in "generic Avro" format.
 *
 * <p>This serializer writes data in the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
 * It requires access to a Confluent Schema Registry endpoint, which you must
 * {@link GenericAvroSerializer#configure(Map, boolean)} via the parameter
 * "schema.registry.url".</p>
 *
 * <p>See {@link GenericAvroDeserializer} for its deserializer counterpart.</p>
 */
@InterfaceStability.Unstable
public class GenericAvroSerializer implements Serializer<GenericRecord> {

    private final KafkaAvroSerializer inner;

    public GenericAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    /**
     * For testing purposes only.
     */
    GenericAvroSerializer(final SchemaRegistryClient client) {
        inner = new KafkaAvroSerializer(client);
    }

    @Override
    public void configure(final Map<String, ?> serializerConfig,
                          final boolean isSerializerForRecordKeys) {
        inner.configure(serializerConfig, isSerializerForRecordKeys);
    }

    @Override
    public byte[] serialize(final String topic, final GenericRecord record) {
        return inner.serialize(topic, record);
    }

    @Override
    public void close() {
        inner.close();
    }

}
