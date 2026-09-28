package com.bcits.controller;

import com.bcits.responseDataModel.DynamicKafkaListenerService;
import com.bcits.responseDataModel.KafkaTrackerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tracker")
public class KafkaTrackerController {

    @Autowired
    private KafkaTrackerService kafkaTrackerService;

    @Autowired
    private DynamicKafkaListenerService dynamicKafkaListenerService;

    @GetMapping("/topic-summary")
    public List<Map<String, Object>> getTopicSummary(
            @RequestParam(required = false) String hes,
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate) {
        if (fromDate == null || fromDate.trim().isEmpty() || fromDate.equalsIgnoreCase("null") || fromDate.equalsIgnoreCase("undefined") ||
            toDate == null || toDate.trim().isEmpty() || toDate.equalsIgnoreCase("null") || toDate.equalsIgnoreCase("undefined")) {
            return java.util.Collections.emptyList();
        }
        return kafkaTrackerService.getTopicSummary(hes, fromDate, toDate);
    }

    @GetMapping("/partition-summary")
    public List<Map<String, Object>> getPartitionSummary(
            @RequestParam String topic, 
            @RequestParam(required = false) String hes,
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate) {
        if (fromDate == null || fromDate.trim().isEmpty() || fromDate.equalsIgnoreCase("null") || fromDate.equalsIgnoreCase("undefined") ||
            toDate == null || toDate.trim().isEmpty() || toDate.equalsIgnoreCase("null") || toDate.equalsIgnoreCase("undefined")) {
            return java.util.Collections.emptyList();
        }
        return kafkaTrackerService.getPartitionSummary(topic, hes, fromDate, toDate);
    }

    @GetMapping("/recent-offsets")
    public List<Map<String, Object>> getRecentOffsets(
            @RequestParam String topic, 
            @RequestParam(required = false) String hes, 
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate) {
        if (fromDate == null || fromDate.trim().isEmpty() || fromDate.equalsIgnoreCase("null") || fromDate.equalsIgnoreCase("undefined") ||
            toDate == null || toDate.trim().isEmpty() || toDate.equalsIgnoreCase("null") || toDate.equalsIgnoreCase("undefined")) {
            return java.util.Collections.emptyList();
        }
        return kafkaTrackerService.getRecentOffsets(topic, hes, limit, page, fromDate, toDate, status);
    }

    @GetMapping("/gaps")
    public List<Map<String, Object>> findGaps(
            @RequestParam String topic, 
            @RequestParam(required = false) String hes, 
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate) {
        if (fromDate == null || fromDate.trim().isEmpty() || fromDate.equalsIgnoreCase("null") || fromDate.equalsIgnoreCase("undefined") ||
            toDate == null || toDate.trim().isEmpty() || toDate.equalsIgnoreCase("null") || toDate.equalsIgnoreCase("undefined")) {
            return java.util.Collections.emptyList();
        }
        return kafkaTrackerService.findGaps(topic, hes, limit, fromDate, toDate);
    }

    @PostMapping("/reprocess")
    public Map<String, Object> reprocessOffset(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        int partition = (int) body.get("partition");
        long offset = ((Number) body.get("offset")).longValue();
        
        dynamicKafkaListenerService.requestReSeek(topic, partition, offset);
        
        return java.util.Collections.singletonMap("success", true);
    }

    @PostMapping("/restart-partition")
    public Map<String, Object> restartPartition(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        int partition = ((Number) body.get("partition")).intValue();
        boolean success = dynamicKafkaListenerService.restartPartition(topic, partition);
        Map<String, Object> resp = new java.util.HashMap<>();
        resp.put("success", success);
        resp.put("message", "Partition " + topic + "-" + partition + " restart triggered successfully");
        return resp;
    }

    @PostMapping("/restart-stalled")
    public Map<String, Object> restartStalled() {
        int count = dynamicKafkaListenerService.restartAllStalledPartitions();
        Map<String, Object> resp = new java.util.HashMap<>();
        resp.put("restartedCount", count);
        resp.put("message", count + " stalled partition(s) restarted");
        return resp;
    }

    @GetMapping("/partition-health")
    public List<Map<String, Object>> getPartitionHealth() {
        return dynamicKafkaListenerService.getPartitionHealthStatus();
    }
}
