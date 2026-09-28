package com.bcits.responseDataModel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.bcits.daoimpl.GenericDaoImpl;
import com.bcits.entity.SchduledMeterTrackEntity;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Tarik Afwas
 * @Date 07-08-2025
 *
 */
@Service
public class KafkaOffsetService extends GenericDaoImpl<SchduledMeterTrackEntity>{

	private static final Logger log = LoggerFactory.getLogger(KafkaOffsetService.class);

	// Counter map: partition -> count
	private final Map<TopicPartition, Integer> offsetCounter = new ConcurrentHashMap<>();

	@Autowired
	private DataSource dataSource;

	@Autowired
	private KafkaConfigProperties kafkaConfigProperties;

	public void storeOffset(ConsumerRecord<String, String> record, String hes) {
	    TopicPartition tp = new TopicPartition(
	            record.topic(),
	            record.partition()
	    );

	    int currentCount = offsetCounter.merge(tp, 1, Integer::sum);
	    int threshold = (kafkaConfigProperties != null && kafkaConfigProperties.getOffsetSaveThreshold() > 0) 
	            ? kafkaConfigProperties.getOffsetSaveThreshold() : 50;

	    if (currentCount >= threshold) {
	        try {
	            saveOffsetInDb(tp, record.offset() + 1, hes);
	            offsetCounter.put(tp, 0);
	        } catch (Exception e) {
	            log.error("[KAFKA-OFFSET] Offset commit failed for {}: {}", tp, e.getMessage());
	        }
	    }
	}

	public void flushOffset(TopicPartition tp, long offset, String hes) {
	    try {
	        saveOffsetInDb(tp, offset, hes);
	        offsetCounter.put(tp, 0);
	    } catch (Exception e) {
	        log.error("[KAFKA-OFFSET] Failed to flush offset in DB for {}: {}", tp, e.getMessage());
	    }
	}

	public void saveOffsetInDb(TopicPartition tp, long offset, String hes) {
	    String sql =
	            "INSERT INTO meter_data.kafka_offsets " +
	            "(topic, partition_id, offset_value, last_updated_time, hes) " +
	            "VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?) " +
	            "ON CONFLICT (topic, partition_id, hes) " +
	            "DO UPDATE SET " +
	            "offset_value = ?, " +
	            "last_updated_time = CURRENT_TIMESTAMP";

	    try (Connection conn = dataSource.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql)) {
	        ps.setString(1, tp.topic());
	        ps.setInt(2, tp.partition());
	        ps.setLong(3, offset);
	        ps.setString(4, hes);
	        ps.setLong(5, offset);
	        ps.executeUpdate();
	    } catch (Exception e) {
	        log.error("[KAFKA-OFFSET] Failed to save offset in DB for {} @ {}: {}", tp, offset, e.getMessage());
	    }
	}
	
	public long getSavedOffset(String topic, int partition, String hes) {
	    String sql =
	            "SELECT offset_value " +
	            "FROM meter_data.kafka_offsets " +
	            "WHERE topic = ? " +
	            "AND partition_id = ? " +
	            "AND hes = ?";

	    try (Connection conn = dataSource.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql)) {
	        ps.setString(1, topic);
	        ps.setInt(2, partition);
	        ps.setString(3, hes);
	        try (ResultSet rs = ps.executeQuery()) {
	            if (rs.next()) {
	                return rs.getLong("offset_value");
	            }
	        }
	    } catch (Exception e) {
	        log.error("[KAFKA-OFFSET] Error fetching saved offset for {}-P{}: {}", topic, partition, e.getMessage());
	    }
	    return -1; // NEW topic
	}

	public void storeReducedOffset(ConsumerRecord<String, String> record, String hes) {
	    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
	    long nextOffset = record.offset() + 1;
	    saveOffsetInDb(tp, nextOffset, hes);

	    log.info("[KAFKA-OFFSET] Offset updated for DLQ message: {}-{} -> {}",
	            tp.topic(), tp.partition(), nextOffset);
	}

}
