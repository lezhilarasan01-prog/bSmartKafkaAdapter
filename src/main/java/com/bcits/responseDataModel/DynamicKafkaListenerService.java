/**
 * 
 */
package com.bcits.responseDataModel;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.bcits.dataModel.DeviceData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

/**
 * @author Tarik Afwas
 * @Date 20th April 2025
 *
 */
@Service
public class DynamicKafkaListenerService {

	private static final Logger log = LoggerFactory.getLogger(DynamicKafkaListenerService.class);

	private static final String DLQ_TOPIC_SUFFIX = "_DLQ";

//	private final Map<String, Integer> retryCountMap = new ConcurrentHashMap<>();
//	private final Map<TopicPartition, Long> offsetStore = new HashMap<>();
	
	private final ConcurrentLinkedQueue<ReSeekRequest> reSeekQueue = new ConcurrentLinkedQueue<>();

	public static class ReSeekRequest {
		public String topic;
		public int partition;
		public long offset;
		public ReSeekRequest(String topic, int partition, long offset) {
			this.topic = topic;
			this.partition = partition;
			this.offset = offset;
		}
	}
	
	public void requestReSeek(String topic, int partition, long offset) {
		reSeekQueue.add(new ReSeekRequest(topic, partition, offset));
	}
	
	private volatile boolean running = true;
	private Thread coordinatorThread;
	private final Map<TopicPartition, Thread> partitionWorkerThreadsMap = new ConcurrentHashMap<>();
	private final Map<TopicPartition, KafkaConsumer<String, String>> activeConsumersMap = new ConcurrentHashMap<>();
	private final Map<TopicPartition, Long> partitionHeartbeats = new ConcurrentHashMap<>();
	private final Set<TopicPartition> runningPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());

	@Autowired
	private KafkaConfigProperties kafkaConfigProperties;
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private ConsumerFactory<String, String> consumerFactory;
	
	@Autowired
	KafkaOffsetService kafkaOffsetService;
	
	@Autowired
	ResponseParser rp;

	@Autowired
	private com.bcits.dao.SchduledMeterTrackDao schMtrTrckDao;

	private final ObjectMapper objectMapper = new ObjectMapper();

	
	
	private static final Gson gson = new com.google.gson.GsonBuilder()
			.registerTypeAdapter(Long.class, new com.google.gson.TypeAdapter<Long>() {
				@Override
				public void write(com.google.gson.stream.JsonWriter out, Long value) throws java.io.IOException {
					if (value == null)
						out.nullValue();
					else
						out.value(value);
				}

				@Override
				public Long read(com.google.gson.stream.JsonReader in) throws java.io.IOException {
					if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
						in.nextNull();
						return 0L;
					}
					String str = in.nextString();
					if (str == null || str.trim().isEmpty() || "null".equalsIgnoreCase(str.trim())) {
						return 0L;
					}
					try {
						if (str.contains(".")) {
							return (long) Double.parseDouble(str.trim());
						}
						return Long.parseLong(str.trim());
					} catch (Exception e) {
						return 0L;
					}
				}
			}).registerTypeAdapter(long.class, new com.google.gson.TypeAdapter<Long>() {
				@Override
				public void write(com.google.gson.stream.JsonWriter out, Long value) throws java.io.IOException {
					if (value == null)
						out.value(0L);
					else
						out.value(value);
				}

				@Override
				public Long read(com.google.gson.stream.JsonReader in) throws java.io.IOException {
					if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
						in.nextNull();
						return 0L;
					}
					String str = in.nextString();
					if (str == null || str.trim().isEmpty() || "null".equalsIgnoreCase(str.trim())) {
						return 0L;
					}
					try {
						if (str.contains(".")) {
							return (long) Double.parseDouble(str.trim());
						}
						return Long.parseLong(str.trim());
					} catch (Exception e) {
						return 0L;
					}
				}
			}).registerTypeAdapter(Integer.class, new com.google.gson.TypeAdapter<Integer>() {
				@Override
				public void write(com.google.gson.stream.JsonWriter out, Integer value) throws java.io.IOException {
					if (value == null)
						out.nullValue();
					else
						out.value(value);
				}

				@Override
				public Integer read(com.google.gson.stream.JsonReader in) throws java.io.IOException {
					if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
						in.nextNull();
						return 0;
					}
					String str = in.nextString();
					if (str == null || str.trim().isEmpty() || "null".equalsIgnoreCase(str.trim())) {
						return 0;
					}
					try {
						if (str.contains(".")) {
							return (int) Double.parseDouble(str.trim());
						}
						return Integer.parseInt(str.trim());
					} catch (Exception e) {
						return 0;
					}
				}
			}).registerTypeAdapter(int.class, new com.google.gson.TypeAdapter<Integer>() {
				@Override
				public void write(com.google.gson.stream.JsonWriter out, Integer value) throws java.io.IOException {
					if (value == null)
						out.value(0);
					else
						out.value(value);
				}

				@Override
				public Integer read(com.google.gson.stream.JsonReader in) throws java.io.IOException {
					if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
						in.nextNull();
						return 0;
					}
					String str = in.nextString();
					if (str == null || str.trim().isEmpty() || "null".equalsIgnoreCase(str.trim())) {
						return 0;
					}
					try {
						if (str.contains(".")) {
							return (int) Double.parseDouble(str.trim());
						}
						return Integer.parseInt(str.trim());
					} catch (Exception e) {
						return 0;
					}
				}
			}).registerTypeAdapter(Double.class, new com.google.gson.TypeAdapter<Double>() {
				@Override
				public void write(com.google.gson.stream.JsonWriter out, Double value) throws java.io.IOException {
					if (value == null)
						out.nullValue();
					else
						out.value(value);
				}

				@Override
				public Double read(com.google.gson.stream.JsonReader in) throws java.io.IOException {
					if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
						in.nextNull();
						return 0.0;
					}
					String str = in.nextString();
					if (str == null || str.trim().isEmpty() || "null".equalsIgnoreCase(str.trim())) {
						return 0.0;
					}
					try {
						return Double.parseDouble(str.trim());
					} catch (Exception e) {
						return 0.0;
					}
				}
			}).registerTypeAdapter(double.class, new com.google.gson.TypeAdapter<Double>() {
				@Override
				public void write(com.google.gson.stream.JsonWriter out, Double value) throws java.io.IOException {
					if (value == null)
						out.value(0.0);
					else
						out.value(value);
				}

				@Override
				public Double read(com.google.gson.stream.JsonReader in) throws java.io.IOException {
					if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
						in.nextNull();
						return 0.0;
					}
					String str = in.nextString();
					if (str == null || str.trim().isEmpty() || "null".equalsIgnoreCase(str.trim())) {
						return 0.0;
					}
					try {
						return Double.parseDouble(str.trim());
					} catch (Exception e) {
						return 0.0;
					}
				}
			}).registerTypeAdapter(java.math.BigDecimal.class, new com.google.gson.TypeAdapter<java.math.BigDecimal>() {
				@Override
				public void write(com.google.gson.stream.JsonWriter out, java.math.BigDecimal value)
						throws java.io.IOException {
					if (value == null)
						out.nullValue();
					else
						out.value(value);
				}

				@Override
				public java.math.BigDecimal read(com.google.gson.stream.JsonReader in) throws java.io.IOException {
					if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
						in.nextNull();
						return null;
					}
					String str = in.nextString();
					if (str == null || str.trim().isEmpty() || "null".equalsIgnoreCase(str.trim())) {
						return null;
					}
					try {
						return new java.math.BigDecimal(str.trim());
					} catch (Exception e) {
						return null;
					}
				}
			}).create();


	@PostConstruct
	public void startKafkaConsumer() {
	    coordinatorThread = new Thread(() -> startPartitionConsumers(kafkaConfigProperties.getProject()));
	    coordinatorThread.setName("DynamicKafkaCoordinator-Thread");
	    coordinatorThread.start();
	}

	
	@PreDestroy
	public void shutdownConsumer() {
	    log.info("Shutting down Kafka consumers...");
	    running = false;

	    // Wake up all active Kafka consumers to cleanly break out of poll()
	    for (KafkaConsumer<String, String> consumer : activeConsumersMap.values()) {
	        try {
	            consumer.wakeup();
	        } catch (Exception e) {
	            // Ignore during shutdown
	        }
	    }

	    if (coordinatorThread != null && coordinatorThread.isAlive()) {
	        try {
	            coordinatorThread.interrupt();
	            coordinatorThread.join(2000);
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }
	    }

	    for (Thread worker : partitionWorkerThreadsMap.values()) {
	        if (worker != null && worker.isAlive()) {
	            try {
	                worker.interrupt();
	                worker.join(2000);
	            } catch (InterruptedException e) {
	                Thread.currentThread().interrupt();
	            }
	        }
	    }
	    partitionWorkerThreadsMap.clear();
	    activeConsumersMap.clear();
	    partitionHeartbeats.clear();
	    runningPartitions.clear();
	    log.info("Kafka consumers shutdown complete.");
	}

	/**
	 * Supervisor coordinator loop:
	 * 1. Watchdog: monitors partition worker threads and auto-recovers any dead or stalled partition consumers.
	 * 2. Discovery: discovers new topic partitions on the broker and starts dedicated worker threads.
	 */
	private void startPartitionConsumers(String hes) {
		List<String> primaryTopics = kafkaConfigProperties.getTopics();
		List<String> dlqTopics = kafkaConfigProperties.getDlqTopics();
		List<String> allTopics = new ArrayList<>();
		allTopics.addAll(primaryTopics);
		allTopics.addAll(dlqTopics);

		Properties props = new Properties();
		props.putAll(consumerFactory.getConfigurationProperties());

		long watchdogCheckInterval = kafkaConfigProperties.getWatchdogCheckIntervalMs() > 0 
				? kafkaConfigProperties.getWatchdogCheckIntervalMs() : 15000;
		long stallTimeout = kafkaConfigProperties.getWatchdogStallTimeoutMs() > 0 
				? kafkaConfigProperties.getWatchdogStallTimeoutMs() : 180000;

		while (running) {
			// 1. WATCHDOG: Check liveness and heartbeat of existing partition workers
			try {
				long now = System.currentTimeMillis();
				for (TopicPartition tp : new ArrayList<>(runningPartitions)) {
					Thread worker = partitionWorkerThreadsMap.get(tp);
					Long lastHb = partitionHeartbeats.get(tp);
					boolean isDead = (worker == null || !worker.isAlive());
					boolean isStalled = (lastHb != null && (now - lastHb) > stallTimeout);

					if (isDead || isStalled) {
						long elapsedSec = lastHb != null ? (now - lastHb) / 1000 : -1;
						log.warn("[WATCHDOG] Alert: Partition {} detected as {}! Auto-recovering and restarting partition consumer...",
								tp, (isDead ? "DEAD" : "STALLED (no poll for " + elapsedSec + "s)"));
						startSinglePartitionWorker(tp, hes);
					}
				}
			} catch (Exception wex) {
				log.error("[WATCHDOG] Error during partition health check: {}", wex.getMessage());
			}

			// 2. DISCOVERY: Discover partitions on broker and launch any missing workers
			try (KafkaConsumer<String, String> discoveryConsumer = new KafkaConsumer<>(props)) {
				for (String topic : allTopics) {
					try {
						List<PartitionInfo> partitionInfos = discoveryConsumer.partitionsFor(topic);
						if (partitionInfos != null) {
							for (PartitionInfo pi : partitionInfos) {
								if (pi.partition() < 100) {
									TopicPartition tp = new TopicPartition(topic, pi.partition());
									if (!runningPartitions.contains(tp)) {
										startSinglePartitionWorker(tp, hes);
									}
								}
							}
						}
					} catch (AuthenticationException ae) {
						log.error("Authentication failed with Kafka broker during partition discovery: {}", ae.getMessage());
						break;
					} catch (Exception e) {
						log.error("Error discovering partitions for topic {}: {}", topic, e.getMessage());
					}
				}
			} catch (Exception ex) {
				log.error("Error in coordinator partition discovery: {}", ex.getMessage());
			}

			// Periodically check every watchdogCheckInterval ms (e.g. 15s)
			try {
				Thread.sleep(watchdogCheckInterval);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	/**
	 * Starts a dedicated consumer thread for a single partition.
	 */
	public synchronized boolean startSinglePartitionWorker(TopicPartition tp, String hes) {
		if (!running) {
			return false;
		}
		// Safely stop and cleanup any existing thread/consumer for this partition first
		stopSinglePartitionWorker(tp);

		runningPartitions.add(tp);
		partitionHeartbeats.put(tp, System.currentTimeMillis());

		Thread worker = new Thread(() -> consumeSinglePartition(tp, hes));
		worker.setName("DynamicConsumer-" + tp.topic() + "-P" + tp.partition());
		partitionWorkerThreadsMap.put(tp, worker);
		worker.start();
		log.info("[KAFKA] Started dedicated consumer thread for partition: {}", tp);
		return true;
	}

	/**
	 * Stops and cleans up a partition consumer thread.
	 */
	public synchronized void stopSinglePartitionWorker(TopicPartition tp) {
		KafkaConsumer<String, String> consumer = activeConsumersMap.remove(tp);
		if (consumer != null) {
			try {
				consumer.wakeup();
				consumer.close(Duration.ofSeconds(2));
			} catch (Exception e) {
				// Ignore on cleanup
			}
		}

		Thread worker = partitionWorkerThreadsMap.remove(tp);
		if (worker != null && worker.isAlive()) {
			try {
				worker.interrupt();
			} catch (Exception e) {
				// Ignore
			}
		}

		runningPartitions.remove(tp);
		partitionHeartbeats.remove(tp);
	}

	/**
	 * Restarts a specific partition (e.g. partition 8) on demand.
	 */
	public boolean restartPartition(String topic, int partition) {
		TopicPartition tp = new TopicPartition(topic, partition);
		log.info("[KAFKA-RESTART] Explicit restart triggered for partition: {}", tp);
		return startSinglePartitionWorker(tp, kafkaConfigProperties.getProject());
	}

	/**
	 * Scans all running partitions and restarts any partition that has stalled.
	 */
	public int restartAllStalledPartitions() {
		long now = System.currentTimeMillis();
		long stallTimeout = kafkaConfigProperties.getWatchdogStallTimeoutMs() > 0 
				? kafkaConfigProperties.getWatchdogStallTimeoutMs() : 180000;
		int restartedCount = 0;

		for (TopicPartition tp : new ArrayList<>(runningPartitions)) {
			Thread worker = partitionWorkerThreadsMap.get(tp);
			Long lastHb = partitionHeartbeats.get(tp);
			boolean isDead = (worker == null || !worker.isAlive());
			boolean isStalled = (lastHb != null && (now - lastHb) > stallTimeout);

			if (isDead || isStalled) {
				long elapsedSec = lastHb != null ? (now - lastHb) / 1000 : -1;
				log.warn("[WATCHDOG-MANUAL] Restarting stalled partition {} (isDead={}, stalledSec={}s)", tp, isDead, elapsedSec);
				startSinglePartitionWorker(tp, kafkaConfigProperties.getProject());
				restartedCount++;
			}
		}
		return restartedCount;
	}

	/**
	 * Returns current health status of all partition threads.
	 */
	public List<Map<String, Object>> getPartitionHealthStatus() {
		List<Map<String, Object>> list = new ArrayList<>();
		long now = System.currentTimeMillis();
		long stallTimeout = kafkaConfigProperties.getWatchdogStallTimeoutMs() > 0 
				? kafkaConfigProperties.getWatchdogStallTimeoutMs() : 180000;

		for (TopicPartition tp : new ArrayList<>(runningPartitions)) {
			Map<String, Object> map = new HashMap<>();
			map.put("topic", tp.topic());
			map.put("partition", tp.partition());
			Thread worker = partitionWorkerThreadsMap.get(tp);
			Long lastHb = partitionHeartbeats.get(tp);

			boolean isAlive = (worker != null && worker.isAlive());
			long elapsedSec = (lastHb != null) ? (now - lastHb) / 1000 : -1;
			boolean isStalled = (lastHb != null && (now - lastHb) > stallTimeout);

			String status = !isAlive ? "DEAD" : (isStalled ? "STALLED" : "HEALTHY");
			map.put("status", status);
			map.put("isAlive", isAlive);
			map.put("lastHeartbeatSecondsAgo", elapsedSec);
			map.put("threadName", worker != null ? worker.getName() : "N/A");
			list.add(map);
		}
		return list;
	}

	/**
	 * Dedicated consumer loop for an individual partition (1 Partition = 1 Consumer Thread).
	 */
	private void consumeSinglePartition(TopicPartition tp, String hes) {
		Properties props = new Properties();
		props.putAll(consumerFactory.getConfigurationProperties());
		props.put("enable.auto.commit", "false");
		props.put("client.id", "consumer-" + tp.topic() + "-p" + tp.partition());

		List<String> dlqTopics = kafkaConfigProperties.getDlqTopics();
		boolean isDlq = dlqTopics.contains(tp.topic());

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			activeConsumersMap.put(tp, consumer);
			partitionHeartbeats.put(tp, System.currentTimeMillis());
			consumer.assign(Collections.singletonList(tp));

			// Initialize offset from DB for this partition
			try {
				long offset = kafkaOffsetService.getSavedOffset(tp.topic(), tp.partition(), hes);
				if (offset == -1) {
					// NEW partition detected → seek to beginning to consume existing/backlog data
					consumer.seekToBeginning(Collections.singletonList(tp));
					long startOffset = consumer.position(tp);
					kafkaOffsetService.saveOffsetInDb(tp, startOffset, hes);
					consumer.seek(tp, startOffset);
					log.info("[KAFKA] New partition detected -> {}-{} starting from earliest offset {}",
							tp.topic(), tp.partition(), startOffset);
				} else {
					consumer.seek(tp, offset);
					log.info("[KAFKA] Consuming from {}-{} at saved offset {}", tp.topic(), tp.partition(), offset);
				}
			} catch (Exception ex) {
				log.error("Error seeking/setting offset for partition {}: {}", tp, ex.getMessage());
			}

			int pollTimeout = kafkaConfigProperties.getPollTimeoutMs() > 0 ? kafkaConfigProperties.getPollTimeoutMs() : 200;
			int batchInsertSize = kafkaConfigProperties.getBatchInsertSize() > 0 ? kafkaConfigProperties.getBatchInsertSize() : 100;

			// Dedicated partition polling loop
			while (running) {
				try {
					// Handle re-seek requests targeted to this specific partition
					handleReSeekForPartition(tp, consumer);

					partitionHeartbeats.put(tp, System.currentTimeMillis());
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
					partitionHeartbeats.put(tp, System.currentTimeMillis());

					if (records == null || records.isEmpty()) {
						continue;
					}

					long startPollMs = System.currentTimeMillis();

					if (isDlq) {
						for (ConsumerRecord<String, String> record : records) {
							handleDlqRecord(record);
						}
					} else {
						// Process polled records in configurable sub-batches (e.g. 100 records per DB bulk insert)
						List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
						for (ConsumerRecord<String, String> r : records) {
							recordList.add(r);
						}

						int totalRecords = recordList.size();
						for (int i = 0; i < totalRecords; i += batchInsertSize) {
							int end = Math.min(i + batchInsertSize, totalRecords);
							List<ConsumerRecord<String, String>> subBatch = recordList.subList(i, end);
							processSubBatchWithRetry(subBatch, tp, hes);
							partitionHeartbeats.put(tp, System.currentTimeMillis());
						}

						long elapsed = System.currentTimeMillis() - startPollMs;
						long firstOff = recordList.get(0).offset();
						long lastOff = recordList.get(totalRecords - 1).offset();
						log.info("[KAFKA] [{}] Partition {} consumed {} records (offsets {}..{}) in {}ms",
								tp.topic(), tp.partition(), totalRecords, firstOff, lastOff, elapsed);
					}

					try {
						consumer.commitSync();
					} catch (Exception commitEx) {
						// Non-fatal since offsets are tracked in DB
						log.debug("[KAFKA] Notice: Kafka commitSync skipped/failed: {}", commitEx.getMessage());
					}
				} catch (WakeupException we) {
					if (!running) {
						break;
					}
				} catch (IllegalStateException e) {
					log.error("IllegalStateException in consumer loop for {}: {}", tp, e.getMessage());
					try {
						Thread.sleep(3000);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						break;
					}
				} catch (Throwable e) {
					log.error("Error in consumer poll loop for {}: {}", tp, e.getMessage());
					try {
						Thread.sleep(3000);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		} catch (Throwable ex) {
			log.error("Critical error in partition consumer {}: {}", tp, ex.getMessage(), ex);
		} finally {
			runningPartitions.remove(tp);
			activeConsumersMap.remove(tp);
			partitionWorkerThreadsMap.remove(tp);
			partitionHeartbeats.remove(tp);
			log.info("[KAFKA] Partition consumer stopped for {}", tp);
		}
	}

	private void processSubBatchWithRetry(List<ConsumerRecord<String, String>> subBatch, TopicPartition tp, String hes) {
		if (subBatch == null || subBatch.isEmpty()) {
			return;
		}

		int maxAttempts = kafkaConfigProperties.getMaxRetryAttempts() > 0 ? kafkaConfigProperties.getMaxRetryAttempts() : 3;
		long backoff = kafkaConfigProperties.getBackoffInterval() > 0 ? kafkaConfigProperties.getBackoffInterval() : 2000;
		long lastOffset = subBatch.get(subBatch.size() - 1).offset();

		int attempt = 0;
		boolean success = false;

		while (attempt < maxAttempts && !success) {
			try {
				attempt++;
				List<DeviceData> batchDeviceDataList = new ArrayList<>();
				for (ConsumerRecord<String, String> record : subBatch) {
					try {
						List<DeviceData> extracted = extractDeviceData(record, hes);
						if (extracted != null && !extracted.isEmpty()) {
							batchDeviceDataList.addAll(extracted);
						}
					} catch (Exception parseEx) {
						log.error("[KAFKA] Record parse error on {}-{}@{}: {}",
								record.topic(), record.partition(), record.offset(), parseEx.getMessage());
						logExceptionToDatabase(record, null, hes, parseEx);
						sendToDlq(record);
					}
				}

				if (!batchDeviceDataList.isEmpty()) {
					rp.parseData(batchDeviceDataList, tp.topic(), hes, tp.partition(), lastOffset);
				}

				// Flush offset for this sub-batch to DB
				kafkaOffsetService.flushOffset(tp, lastOffset + 1, hes);
				success = true;

			} catch (Exception e) {
				log.error("[KAFKA] Sub-batch processing error on {} offsets [{}..{}] Attempt: {}/{} Error: {}",
						tp, subBatch.get(0).offset(), lastOffset, attempt, maxAttempts, e.getMessage());

				if (attempt >= maxAttempts) {
					log.error("[KAFKA] Sub-batch failed after {} attempts. Falling back to single-record retry to isolate faulty records.", maxAttempts);
					for (ConsumerRecord<String, String> record : subBatch) {
						processSingleRecordFallback(record, hes);
					}
					kafkaOffsetService.flushOffset(tp, lastOffset + 1, hes);
					success = true;
				} else {
					try {
						Thread.sleep(backoff);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		}
	}

	private void processSingleRecordFallback(ConsumerRecord<String, String> record, String hes) {
		try {
			List<DeviceData> dataList = extractDeviceData(record, hes);
			if (dataList != null && !dataList.isEmpty()) {
				rp.parseData(dataList, record.topic(), hes, record.partition(), record.offset());
			}
		} catch (Exception e) {
			log.error("[KAFKA-FALLBACK] Single record failed on {}-{}@{}: {}",
					record.topic(), record.partition(), record.offset(), e.getMessage());
			try {
				logExceptionToDatabase(record, null, hes, e);
			} catch (Throwable logEx) {
				log.error("Failed to log fallback exception to DB: {}", logEx.getMessage());
			}
			try {
				sendToDlq(record);
			} catch (Throwable dlqEx) {
				log.error("Failed to send fallback record to DLQ: {}", dlqEx.getMessage());
			}
		}
	}

	public List<DeviceData> extractDeviceData(ConsumerRecord<String, String> record, String hes) {
		String message = record.value();
		if (message == null || message.trim().isEmpty()) {
			throw new IllegalArgumentException("JSON message is null or empty");
		}

		List<DeviceData> smm = new ArrayList<>();
		try {
			JsonNode jsonNode = objectMapper.readTree(message);
			boolean isAlarmTopic = "cyanconnode-hes.sma.meter.alarm".equals(record.topic())
					|| "KIMBAL_ALARM_TOPIC".equals(record.topic());
			String defaultProfile = ResponseParser.resolveProfileCode(null, record.topic());

			if (jsonNode.isArray()) {
				JsonArray jsonArray = JsonParser.parseString(message).getAsJsonArray();
				for (JsonElement element : jsonArray) {
					if (!element.isJsonObject()) continue;
					JsonObject obj = element.getAsJsonObject();
					if (isAlarmTopic) {
						obj.addProperty("profileObisCode", "0.4.25.9.0.255");
					} else if (!"Unknown Profile".equals(defaultProfile) && !obj.has("profileCode") && !obj.has("profileObisCode")) {
						obj.addProperty("profileObisCode", defaultProfile);
					}
					normalizeJsonFields(obj);
				}
				smm = gson.fromJson(jsonArray, new TypeToken<List<DeviceData>>() {}.getType());
			} else if (jsonNode.isObject()) {
				JsonObject obj = JsonParser.parseString(message).getAsJsonObject();
				if (isAlarmTopic) {
					obj.addProperty("profileObisCode", "0.4.25.9.0.255");
				} else if (!"Unknown Profile".equals(defaultProfile) && !obj.has("profileCode") && !obj.has("profileObisCode")) {
					obj.addProperty("profileObisCode", defaultProfile);
				}
				normalizeJsonFields(obj);
				DeviceData data = gson.fromJson(obj, DeviceData.class);
				if (data != null) {
					smm.add(data);
				}
			} else {
				log.warn("[KAFKA] Unexpected message format at offset {}", record.offset());
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to extract DeviceData at offset " + record.offset() + ": " + e.getMessage(), e);
		}
		return smm;
	}

	private void normalizeJsonFields(JsonObject obj) {
		for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
			JsonElement value = entry.getValue();
			String field = entry.getKey();

			if (value != null && value.isJsonArray()) {
				JsonArray arr = value.getAsJsonArray();
				if (arr.size() > 0 && arr.get(0).isJsonPrimitive()) {
					StringBuilder sb = new StringBuilder();
					for (JsonElement e : arr) {
						if (sb.length() > 0) sb.append(",");
						sb.append(e.getAsString());
					}
					obj.addProperty(field, sb.toString());
				} else {
					for (JsonElement arrElem : arr) {
						if (arrElem.isJsonObject()) {
							JsonObject innerObj = arrElem.getAsJsonObject();
							for (Map.Entry<String, JsonElement> inner : innerObj.entrySet()) {
								JsonElement innerVal = inner.getValue();
								if (innerVal != null && innerVal.isJsonPrimitive() && innerVal.getAsJsonPrimitive().isNumber()) {
									innerObj.addProperty(inner.getKey(), innerVal.getAsString());
								}
							}
						}
					}
				}
			} else if (value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber()) {
				obj.addProperty(field, value.getAsString());
			}
		}
	}

	private void logExceptionToDatabase(ConsumerRecord<String, String> record, List<DeviceData> smm, String hes, Exception e) {
		try {
			String topic = record.topic();
			String messagePayload = record.value() != null ? record.value().replace("'", "''") : "";
			String errorMsg = e.getMessage() != null ? e.getMessage().replace("'", "''") : "Unknown processing error";

			if (smm == null && record.value() != null && !record.value().trim().isEmpty()) {
				try {
					JsonNode jsonNode = objectMapper.readTree(record.value());
					if (jsonNode.isArray()) {
						smm = gson.fromJson(record.value(), new TypeToken<List<DeviceData>>() {}.getType());
					} else if (jsonNode.isObject()) {
						DeviceData data = gson.fromJson(record.value(), DeviceData.class);
						smm = new ArrayList<>();
						smm.add(data);
					}
				} catch (Exception parseEx) {
					// Fallback to null smm
				}
			}

			if (smm != null && !smm.isEmpty()) {
				StringBuilder bulkSql = new StringBuilder();
				bulkSql.append("insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type, meter_number, data, exp_remarks, hes_type) values ");

				for (DeviceData scMM : smm) {
					String elementData = gson.toJson(scMM).replace("'", "''");
					bulkSql.append("(now(), '")
					       .append(scMM.getSeqId()).append("', '")
					       .append(topic).append("', '")
					       .append(scMM.getDeviceId() != null ? scMM.getDeviceId().trim() : "").append("', '")
					       .append(elementData).append("', '")
					       .append("Error in batch processing: ").append(errorMsg).append("', '")
					       .append(hes).append("'),");
				}

				if (bulkSql.length() > 0 && bulkSql.charAt(bulkSql.length() - 1) == ',') {
					String finalSql = bulkSql.substring(0, bulkSql.length() - 1) + " ON CONFLICT DO NOTHING";
					schMtrTrckDao.bulkInsert(finalSql);
					log.debug("[KAFKA-CONSUMER-EXCP] Logged exception details for {} samples in exception_seq_tracker", smm.size());
				}
			} else {
				String sql = "insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type, meter_number, data, exp_remarks, hes_type) values "
						+ "(now(), '0', '" + topic + "', '', '" + messagePayload + "', 'Deserialization failed: " + errorMsg + "', '" + hes + "') ON CONFLICT DO NOTHING";
				schMtrTrckDao.bulkInsert(sql);
				log.debug("[KAFKA-CONSUMER-EXCP] Logged raw payload deserialization exception in exception_seq_tracker");
			}
		} catch (Exception ex) {
			log.error("CRITICAL: Failed to write processing exception to database: {}", ex.getMessage(), ex);
		}
	}
	
	private void sendToDlq(ConsumerRecord<String, String> record) {
		String dlqTopic = record.topic().endsWith(DLQ_TOPIC_SUFFIX) ? record.topic() : record.topic() + DLQ_TOPIC_SUFFIX;
		ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, record.key(), record.value());
		dlqRecord.headers().add("original-offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));
		log.warn("Sending failed record to DLQ topic {} (Offset: {})", dlqTopic, record.offset());
		try {
			kafkaTemplate.send(dlqRecord);
			log.info("Sent to DLQ {}@{}", dlqTopic, record.offset());
		} catch (Exception ex) {
			log.error("Error sending to DLQ topic {}: {}", dlqTopic, ex.getMessage());
		}
	}

	private void handleDlqRecord(ConsumerRecord<String, String> record) {
	    log.info("Reprocessing DLQ message {}-{}@{}", record.topic(), record.partition(), record.offset());

	    int maxAttempts = kafkaConfigProperties.getMaxRetryAttempts() > 0 ? kafkaConfigProperties.getMaxRetryAttempts() : 3;
	    long backoff = kafkaConfigProperties.getBackoffInterval() > 0 ? kafkaConfigProperties.getBackoffInterval() : 2000;
	    int attempt = 0;
	    boolean success = false;

	    while (attempt < maxAttempts && !success) {
	        try {
	            attempt++;
	            List<DeviceData> dataList = extractDeviceData(record, kafkaConfigProperties.getProject());
	            if (dataList != null && !dataList.isEmpty()) {
	                rp.parseData(dataList, record.topic(), kafkaConfigProperties.getProject(), record.partition(), record.offset());
	            }
	            kafkaOffsetService.storeReducedOffset(record, kafkaConfigProperties.getProject());
	            success = true;

	        } catch (Exception ex) {
	            log.error("DLQ retry attempt {}/{} failed: {}", attempt, maxAttempts, ex.getMessage());

	            if (attempt >= maxAttempts) {
	                log.warn("DLQ message failed after {} retries -> Advancing DLQ offset.", maxAttempts);
	                kafkaOffsetService.storeReducedOffset(record, kafkaConfigProperties.getProject());
	                success = true;
	            } else {
	                try {
	                    Thread.sleep(backoff);
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                    break;
	                }
	            }
	        }
	    }
	}


	private void handleReSeekForPartition(TopicPartition tp, KafkaConsumer<String, String> consumer) {
		if (reSeekQueue.isEmpty()) {
			return;
		}
		for (ReSeekRequest req : reSeekQueue) {
			if (req.topic.equals(tp.topic()) && req.partition == tp.partition()) {
				if (reSeekQueue.remove(req)) {
					consumer.seek(tp, req.offset);
					log.info("[KAFKA] Dynamic re-seek to {}-{} @ {}", tp.topic(), tp.partition(), req.offset);
				}
			}
		}
	}

	private void handleReSeekRequests(KafkaConsumer<String, String> consumer) {
		ReSeekRequest req;
		while ((req = reSeekQueue.poll()) != null) {
			TopicPartition tpReq = new TopicPartition(req.topic, req.partition);
			consumer.seek(tpReq, req.offset);
			log.info("[KAFKA] Dynamic re-seek to {}-{} @ {}", req.topic, req.partition, req.offset);
		}
	}
}
