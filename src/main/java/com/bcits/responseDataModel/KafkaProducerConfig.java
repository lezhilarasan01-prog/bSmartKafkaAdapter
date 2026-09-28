/**
 * 
 */
package com.bcits.responseDataModel;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * @author Tarik Afwas
 * @Date 11th August 2025
 *
 */
@Configuration
public class KafkaProducerConfig {
	
	@Value("${kafka.bootstrap-servers}")
	private String kafkaServers;

	@Value("${kafka.security.protocol:}")
	private String securityProtocol;

	@Value("${kafka.sasl.mechanism:}")
	private String saslMechanism;

	@Value("${kafka.sasl.jaas.config:}")
	private String saslJaasConfig;

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		// Optional: You can add retries, acks, and other producer configs here
		configProps.put(ProducerConfig.ACKS_CONFIG, "all");
		configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
		configProps.put("enable.idempotence", "true");

		if (securityProtocol != null && !securityProtocol.isEmpty()) {
			configProps.put("security.protocol", securityProtocol);
		}
		if (saslMechanism != null && !saslMechanism.isEmpty()) {
			configProps.put("sasl.mechanism", saslMechanism);
		}
		if (saslJaasConfig != null && !saslJaasConfig.isEmpty()) {
			configProps.put("sasl.jaas.config", saslJaasConfig);
		}

		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}
