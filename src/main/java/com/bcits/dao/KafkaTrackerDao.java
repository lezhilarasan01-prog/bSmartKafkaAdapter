package com.bcits.dao;

import com.bcits.entity.KafkaTrackerEntity;
import java.util.List;
import java.util.Map;

public interface KafkaTrackerDao extends GenericDao<KafkaTrackerEntity> {
    List<Map<String, Object>> getTopicSummary(String hes, String fromDate, String toDate);
    List<Map<String, Object>> getPartitionSummary(String topic, String hes, String fromDate, String toDate);
    List<Map<String, Object>> getRecentOffsets(String topic, String hes, int limit, int page, String fromDate, String toDate, String status);
    List<Map<String, Object>> findGaps(String topic, String hes, int limit, String fromDate, String toDate);
    KafkaTrackerEntity findByTopicAndPartitionAndOffset(String topic, int partition, long offset);
}
