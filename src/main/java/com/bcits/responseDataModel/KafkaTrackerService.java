package com.bcits.responseDataModel;

import com.bcits.dao.KafkaTrackerDao;
import com.bcits.dataModel.ProcessingStats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class KafkaTrackerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaTrackerService.class);

    @Autowired
    private KafkaTrackerDao kafkaTrackerDao;

    @Autowired
    private DataSource dataSource;

    public void logOffsetProcessing(String topic, int partition, long offset, String hes, ProcessingStats stats) {
        try {
            int totalRecords = stats != null ? stats.getTotalRecords() : 0;
            int successCount = stats != null ? stats.getSuccessCount() : 0;
            int failureCount = stats != null ? stats.getFailureCount() : 0;
            int exceptionCount = stats != null ? stats.getExceptionCount() : 0;
            String summary = (stats != null && stats.getSummary() != null) ? stats.getSummary() : "";
            
            String status;
            if (stats == null || (failureCount == 0 && exceptionCount == 0)) {
                status = "SUCCESS";
            } else if (successCount == 0) {
                status = "FAILURE";
            } else {
                status = "PARTIAL";
            }

            if ("FAILURE".equals(status) || "PARTIAL".equals(status)) {
                log.warn("[KAFKA-TRACKER] [{}] Partition {} offset {} status: {} (Total: {}, Success: {}, Fail: {}, Excp: {})",
                        topic, partition, offset, status, totalRecords, successCount, failureCount, exceptionCount);
            } else {
                log.debug("[KAFKA-TRACKER] Logged offset {} for {} (Partition {}) | Status: {}", offset, topic, partition, status);
            }
            
            String sql = "INSERT INTO meter_data.kafka_offset_tracker " +
                         "(topic, partition_id, offset_id, received_time, hes, total_records, success_count, failure_count, exception_count, summary, status) " +
                         "VALUES (?, ?, ?, now(), ?, ?, ?, ?, ?, ?, ?)";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, topic);
                ps.setInt(2, partition);
                ps.setLong(3, offset);
                ps.setString(4, hes);
                ps.setInt(5, totalRecords);
                ps.setInt(6, successCount);
                ps.setInt(7, failureCount);
                ps.setInt(8, exceptionCount);
                ps.setString(9, summary);
                ps.setString(10, status);
                ps.executeUpdate();
            }
        } catch (Throwable e) {
            // Never let tracking failures crash consumer processing or throw
            log.warn("[KAFKA-TRACKER] Failed to persist offset tracking for {} (P:{} @ {}): {}", topic, partition, offset, e.getMessage());
        }
    }

    public List<Map<String, Object>> getTopicSummary(String hes, String fromDate, String toDate) {
        return kafkaTrackerDao.getTopicSummary(hes, fromDate, toDate);
    }

    public List<Map<String, Object>> getPartitionSummary(String topic, String hes, String fromDate, String toDate) {
        return kafkaTrackerDao.getPartitionSummary(topic, hes, fromDate, toDate);
    }

    public List<Map<String, Object>> getRecentOffsets(String topic, String hes, int limit, int page, String fromDate, String toDate, String status) {
        return kafkaTrackerDao.getRecentOffsets(topic, hes, limit, page, fromDate, toDate, status);
    }

    public List<Map<String, Object>> findGaps(String topic, String hes, int limit, String fromDate, String toDate) {
        return kafkaTrackerDao.findGaps(topic, hes, limit, fromDate, toDate);
    }
}
