package com.bcits.daoimpl;

import com.bcits.dao.KafkaTrackerDao;
import com.bcits.entity.KafkaTrackerEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import jakarta.persistence.Query;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import java.util.ArrayList;

@Repository
public class KafkaTrackerDaoImpl extends GenericDaoImpl<KafkaTrackerEntity> implements KafkaTrackerDao {

    private static final Logger log = LoggerFactory.getLogger(KafkaTrackerDaoImpl.class);

	@Autowired
    private DataSource dataSource;
    @Override
    @Transactional(readOnly = true)
    public List<Map<String, Object>> getTopicSummary(String hes, String fromDate, String toDate) {
        String sql = "SELECT k.topic, SUM(k.total_records) as total_messages, " +
                     "COALESCE((SELECT COUNT(*) FROM meter_data.sch_meter_trackload s " +
                     "WHERE s.apiname = k.topic " +
                     "AND (s.hes_type = CAST(:hes AS varchar) OR CAST(:hes AS varchar) IS NULL) " +
                     "AND (s.time_stamp >= TO_TIMESTAMP(CAST(:fromDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:fromDate AS text) IS NULL) " +
                     "AND (s.time_stamp <= TO_TIMESTAMP(CAST(:toDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:toDate AS text) IS NULL)), 0) as success_count, " +
                     "SUM(k.failure_count) as failure_count, " +
                     "SUM(k.exception_count) as exception_count, " +
                     "MAX(k.received_time) as last_message_time " +
                     "FROM meter_data.kafka_offset_tracker k " +
                     "WHERE (k.hes = CAST(:hes AS varchar) OR CAST(:hes AS varchar) IS NULL) " +
                     "AND (k.received_time >= TO_TIMESTAMP(CAST(:fromDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:fromDate AS text) IS NULL) " +
                     "AND (k.received_time <= TO_TIMESTAMP(CAST(:toDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:toDate AS text) IS NULL) " +
                     "GROUP BY k.topic ORDER BY total_messages DESC";
        
        Query query = entityManager.createNativeQuery(sql);
        query.setParameter("hes", (hes == null || hes.trim().isEmpty()) ? null : hes);
        query.setParameter("fromDate", (fromDate == null || fromDate.trim().isEmpty()) ? null : fromDate);
        query.setParameter("toDate", (toDate == null || toDate.trim().isEmpty()) ? null : toDate);
        
        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();
        List<Map<String, Object>> list = new ArrayList<>();
        for (Object[] row : results) {
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("topic", row[0]);
            map.put("total_messages", row[1]);
            map.put("success_count", row[2]);
            map.put("failure_count", row[3]);
            map.put("exception_count", row[4]);
            map.put("last_message_time", row[5]);
            list.add(map);
        }
        return list;
    }

    @Override
    @Transactional(readOnly = true)
    public List<Map<String, Object>> getPartitionSummary(String topic, String hes, String fromDate, String toDate) {
        String sql = "SELECT partition_id, SUM(total_records) as total_messages, " +
                     "SUM(success_count) as success_count, SUM(failure_count) as failure_count, " +
                     "SUM(exception_count) as exception_count " +
                     "FROM meter_data.kafka_offset_tracker " +
                     "WHERE topic = :topic AND (hes = CAST(:hes AS varchar) OR CAST(:hes AS varchar) IS NULL) " +
                     "AND (received_time >= TO_TIMESTAMP(CAST(:fromDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:fromDate AS text) IS NULL) " +
                     "AND (received_time <= TO_TIMESTAMP(CAST(:toDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:toDate AS text) IS NULL) " +
                     "GROUP BY partition_id ORDER BY partition_id";
        
        Query query = entityManager.createNativeQuery(sql);
        query.setParameter("topic", topic);
        query.setParameter("hes", (hes == null || hes.trim().isEmpty()) ? null : hes);
        query.setParameter("fromDate", (fromDate == null || fromDate.trim().isEmpty()) ? null : fromDate);
        query.setParameter("toDate", (toDate == null || toDate.trim().isEmpty()) ? null : toDate);
        
        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();
        List<Map<String, Object>> list = new ArrayList<>();
        for (Object[] row : results) {
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("partition_id", row[0]);
            map.put("total_messages", row[1]);
            map.put("success_count", row[2]);
            map.put("failure_count", row[3]);
            map.put("exception_count", row[4]);
            list.add(map);
        }
        return list;
    }

    @Override
    @Transactional(readOnly = true)
    public List<Map<String, Object>> getRecentOffsets(String topic, String hes, int limit, int page, String fromDate, String toDate, String status) {
        String sql = "SELECT * FROM meter_data.kafka_offset_tracker " +
                     "WHERE topic = :topic AND (hes = CAST(:hes AS varchar) OR CAST(:hes AS varchar) IS NULL) " +
                     "AND (received_time >= TO_TIMESTAMP(CAST(:fromDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:fromDate AS text) IS NULL) " +
                     "AND (received_time <= TO_TIMESTAMP(CAST(:toDate AS text), 'YYYY-MM-DD HH24:MI:SS') OR CAST(:toDate AS text) IS NULL) " +
                     "AND (status = CAST(:status AS varchar) OR CAST(:status AS varchar) IS NULL) " +
                     "ORDER BY received_time DESC";
        
        Query query = entityManager.createNativeQuery(sql, KafkaTrackerEntity.class);
        query.setParameter("topic", topic);
        query.setParameter("hes", (hes == null || hes.trim().isEmpty()) ? null : hes);
        query.setParameter("fromDate", (fromDate == null || fromDate.trim().isEmpty()) ? null : fromDate);
        query.setParameter("toDate", (toDate == null || toDate.trim().isEmpty()) ? null : toDate);
        query.setParameter("status", (status == null || status.trim().isEmpty()) ? null : status);
        
        if (limit > 0) {
            query.setMaxResults(limit);
            query.setFirstResult((page - 1) * limit);
        }
        
        @SuppressWarnings("unchecked")
        List<KafkaTrackerEntity> entities = query.getResultList();
        
        List<Map<String, Object>> results = new ArrayList<>();
        for (KafkaTrackerEntity e : entities) {
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("topic", e.getTopic());
            map.put("partitionId", e.getPartitionId());
            map.put("offsetId", e.getOffsetId());
            map.put("receivedTime", e.getReceivedTime());
            map.put("totalRecords", e.getTotalRecords());
            map.put("successCount", e.getSuccessCount());
            map.put("failureCount", e.getFailureCount());
            map.put("exceptionCount", e.getExceptionCount());
            map.put("status", e.getStatus());
            map.put("summary", e.getSummary());
            map.put("hes", e.getHes());
            results.add(map);
        }
        
        // Add gaps if any
        List<Map<String, Object>> gaps = findGapsInList(entities);
        results.addAll(gaps);
        
        // Final sort by offsetId DESC to keep chronological order
        results.sort((m1, m2) -> {
            Long o1 = ((Number) m1.get("offsetId")).longValue();
            Long o2 = ((Number) m2.get("offsetId")).longValue();
            return o2.compareTo(o1);
        });
        
        return results;
    }

    private List<Map<String, Object>> findGapsInList(List<KafkaTrackerEntity> entities) {
        List<Map<String, Object>> gaps = new ArrayList<>();
        if (entities.size() < 2) return gaps;
        
        // Group by partition
        Map<Integer, List<KafkaTrackerEntity>> partMap = new java.util.HashMap<>();
        for (KafkaTrackerEntity e : entities) {
            partMap.computeIfAbsent(e.getPartitionId(), k -> new ArrayList<>()).add(e);
        }
        
        for (Map.Entry<Integer, List<KafkaTrackerEntity>> entry : partMap.entrySet()) {
            List<KafkaTrackerEntity> list = entry.getValue();
            // Sort by offset ascending to find gaps
            list.sort((e1, e2) -> e1.getOffsetId().compareTo(e2.getOffsetId()));
            
            for (int i = 0; i < list.size() - 1; i++) {
                KafkaTrackerEntity current = list.get(i);
                KafkaTrackerEntity next = list.get(i+1);
                
                long expectedNext = current.getOffsetId() + current.getTotalRecords();
                if (next.getOffsetId() > expectedNext) {
                    // Gap detected
                    Map<String, Object> gap = new java.util.HashMap<>();
                    gap.put("topic", current.getTopic());
                    gap.put("partitionId", current.getPartitionId());
                    gap.put("offsetId", expectedNext);
                    gap.put("totalRecords", (int)(next.getOffsetId() - expectedNext));
                    gap.put("status", "GAP");
                    gap.put("summary", "Missing Offsets: " + expectedNext + " to " + (next.getOffsetId() - 1));
                    gap.put("receivedTime", null);
                    gap.put("isGap", true);
                    gaps.add(gap);
                }
            }
        }
        return gaps;
    }

    @Override
    public List<Map<String, Object>> findGaps(String topic, String hes, int limit, String fromDate, String toDate) {
        List<Map<String, Object>> recent = getRecentOffsets(topic, hes, limit, 1, fromDate, toDate, null);
        List<Map<String, Object>> gaps = new ArrayList<>();
        if (recent != null) {
            for (Map<String, Object> m : recent) {
                if ("GAP".equals(m.get("status"))) {
                    gaps.add(m);
                }
            }
        }
        return gaps;
    }

    

    @Override
    public KafkaTrackerEntity findByTopicAndPartitionAndOffset(String topic, int partition, long offset) {
        String sql = "SELECT * FROM meter_data.kafka_offset_tracker " +
                     "WHERE topic = ? AND partition_id = ? AND offset_id = ? " +
                     "ORDER BY received_time DESC LIMIT 1";
        try (java.sql.Connection conn = dataSource.getConnection();
             java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setLong(3, offset);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    KafkaTrackerEntity entity = new KafkaTrackerEntity();
                    entity.setId(rs.getLong("id"));
                    entity.setTopic(rs.getString("topic"));
                    entity.setPartitionId(rs.getInt("partition_id"));
                    entity.setOffsetId(rs.getLong("offset_id"));
                    entity.setReceivedTime(rs.getTimestamp("received_time"));
                    entity.setTotalRecords(rs.getInt("total_records"));
                    entity.setSuccessCount(rs.getInt("success_count"));
                    entity.setFailureCount(rs.getInt("failure_count"));
                    entity.setExceptionCount(rs.getInt("exception_count"));
                    entity.setStatus(rs.getString("status"));
                    entity.setSummary(rs.getString("summary"));
                    entity.setHes(rs.getString("hes"));
                    return entity;
                }
            }
        } catch (Throwable ex) {
            log.error("[KAFKA-TRACKER] Error finding offset entity ({}-{}@{}): {}", topic, partition, offset, ex.getMessage());
        }
        return null;
    }
}
