/**
 * 
 */
package com.bcits.responseDataModel;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Tarik Afwas
 * @Date 20th April 2025
 *
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaConfigProperties configProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configProperties.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from the beginning
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configProperties.getMaxPollRecords()); // Configurable batch size
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, configProperties.getFetchMaxWaitMs()); // Configurable fetch wait ms
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, configProperties.getMaxPollIntervalMs()); // Configurable processing window
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, configProperties.getReceiveBufferBytes()); // Configurable socket receive buffer

        if (configProperties.getSecurityProtocol() != null && !configProperties.getSecurityProtocol().isEmpty()) {
            props.put("security.protocol", configProperties.getSecurityProtocol());
        }
        if (configProperties.getSaslMechanism() != null && !configProperties.getSaslMechanism().isEmpty()) {
            props.put("sasl.mechanism", configProperties.getSaslMechanism());
        }
        if (configProperties.getSaslJaasConfig() != null && !configProperties.getSaslJaasConfig().isEmpty()) {
            props.put("sasl.jaas.config", configProperties.getSaslJaasConfig());
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // @SuppressWarnings("deprecation")
    // @Bean
    // public ConcurrentKafkaListenerContainerFactory<String, String>
    // kafkaListenerContainerFactory(
    // ConsumerFactory<String, String> consumerFactory,
    // KafkaConfigProperties configProperties) {
    //
    // ConcurrentKafkaListenerContainerFactory<String, String> factory =
    // new ConcurrentKafkaListenerContainerFactory<>();
    // factory.setConsumerFactory(consumerFactory);
    //
    // // Error Handling with Retry
    // factory.setErrorHandler(new SeekToCurrentErrorHandler(
    // new FixedBackOff(configProperties.getBackoffInterval(),
    // configProperties.getMaxRetryAttempts())
    // ));
    //
    // return factory;
    // }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<? super String, ? super String> consumerFactory,
            KafkaConfigProperties configProperties) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Error Handling with Retry (Modern DefaultErrorHandler)
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(configProperties.getBackoffInterval(),
                        configProperties.getMaxRetryAttempts())));

        return factory;
    }

}
