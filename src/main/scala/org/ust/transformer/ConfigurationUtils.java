package org.ust.transformer;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

class ConfigurationUtils {

    private ConfigurationUtils() {
        throw new AssertionError("you must not instantiate this class");
    }

    /**
     * Enables the use of Specific Avro.
     *
     * @param config the serializer/deserializer/serde configuration
     * @return a copy of the configuration where the use of specific Avro is enabled
     */
    public static Map<String, Object> withSpecificAvroEnabled(final Map<String, ?> config) {
        Map<String, Object> specificAvroEnabledConfig =
                config == null ? new HashMap<String, Object>() : new HashMap<>(config);
        specificAvroEnabledConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return specificAvroEnabledConfig;
    }

}
