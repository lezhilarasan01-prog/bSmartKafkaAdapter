/**
 * 
 */
package com.bcits.responseDataModel;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author Tarik Afwas
 * @Date 20th April 2025
 *
 */
@Configuration
public class KafkaConfigProperties {
	
	 	@Value("${kafka.bootstrap-servers}")
	    private String bootstrapServers;

	    @Value("${kafka.group-id}")
	    private String groupId;

	    @Value("${kafka.topics}")
	    private String topics;
	    
	    @Value("${kafka.dlqTopics}")
	    private String dlqTopics;

	    @Value("${kafka.retry.max-attempts}")
	    private int maxRetryAttempts;

	    @Value("${kafka.retry.backoff-interval}")
	    private long backoffInterval;
	    
	    @Value("${project}")
	    private String project ;

	    @Value("${kafka.security.protocol:}")
	    private String securityProtocol;

	    @Value("${kafka.sasl.mechanism:}")
	    private String saslMechanism;

	    @Value("${kafka.sasl.jaas.config:}")
	    private String saslJaasConfig;

	    @Value("${kafka.consumer.max-poll-records:1000}")
	    private int maxPollRecords;

	    @Value("${kafka.consumer.batch-insert-size:100}")
	    private int batchInsertSize;

	    @Value("${kafka.consumer.fetch-max-wait-ms:200}")
	    private int fetchMaxWaitMs;

	    @Value("${kafka.consumer.offset-save-threshold:100}")
	    private int offsetSaveThreshold;

	    @Value("${kafka.consumer.poll-timeout-ms:200}")
	    private int pollTimeoutMs;

	    @Value("${kafka.consumer.max-poll-interval-ms:600000}")
	    private int maxPollIntervalMs;

	    @Value("${kafka.consumer.receive-buffer-bytes:65536}")
	    private int receiveBufferBytes;

	    @Value("${kafka.consumer.watchdog-stall-timeout-ms:180000}")
	    private long watchdogStallTimeoutMs;

	    @Value("${kafka.consumer.watchdog-check-interval-ms:15000}")
	    private long watchdogCheckIntervalMs;

	    public String getProject() { return project; }

		public String getBootstrapServers() { return bootstrapServers; }

	    public String getGroupId() { return groupId; }

	    public List<String> getTopics() { return Arrays.asList(topics.split(",")); }
	    
	    public List<String> getDlqTopics() { return Arrays.asList(dlqTopics.split(",")); }

	    public int getMaxRetryAttempts() { return maxRetryAttempts; }

	    public long getBackoffInterval() { return backoffInterval; }

	    public String getSecurityProtocol() { return securityProtocol; }

	    public String getSaslMechanism() { return saslMechanism; }

	    public String getSaslJaasConfig() { return saslJaasConfig; }

	    public int getMaxPollRecords() { return maxPollRecords; }

	    public int getBatchInsertSize() { return batchInsertSize; }

	    public int getFetchMaxWaitMs() { return fetchMaxWaitMs; }

	    public int getOffsetSaveThreshold() { return offsetSaveThreshold; }

	    public int getPollTimeoutMs() { return pollTimeoutMs; }

	    public int getMaxPollIntervalMs() { return maxPollIntervalMs; }

	    public int getReceiveBufferBytes() { return receiveBufferBytes; }

	    public long getWatchdogStallTimeoutMs() { return watchdogStallTimeoutMs; }

	    public long getWatchdogCheckIntervalMs() { return watchdogCheckIntervalMs; }
}
