package com.bcits.entity;

import java.sql.Timestamp;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import com.bcits.utils.SchemaUtils;

@Entity
@Table(name = "kafka_offset_tracker", schema = SchemaUtils.PRIMARY_SCHEMA_NAME)
public class KafkaTrackerEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "topic")
    private String topic;

    @Column(name = "partition_id")
    private Integer partitionId;

    @Column(name = "offset_id")
    private Long offsetId;

    @Column(name = "received_time")
    private Timestamp receivedTime;

    @Column(name = "total_records")
    private Integer totalRecords;

    @Column(name = "success_count")
    private Integer successCount;

    @Column(name = "failure_count")
    private Integer failureCount = 0;

    @Column(name = "exception_count")
    private Integer exceptionCount = 0;

    @Column(name = "status")
    private String status;

    @Column(name = "summary")
    private String summary;

    @Column(name = "hes")
    private String hes;

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public Integer getPartitionId() { return partitionId; }
    public void setPartitionId(Integer partitionId) { this.partitionId = partitionId; }

    public Long getOffsetId() { return offsetId; }
    public void setOffsetId(Long offsetId) { this.offsetId = offsetId; }

    public Timestamp getReceivedTime() { return receivedTime; }
    public void setReceivedTime(Timestamp receivedTime) { this.receivedTime = receivedTime; }

    public Integer getTotalRecords() { return totalRecords; }
    public void setTotalRecords(Integer totalRecords) { this.totalRecords = totalRecords; }

    public Integer getSuccessCount() { return successCount; }
    public void setSuccessCount(Integer successCount) { this.successCount = successCount; }

    public Integer getFailureCount() { return failureCount; }
    public void setFailureCount(Integer failureCount) { this.failureCount = failureCount; }

    public Integer getExceptionCount() { return exceptionCount; }
    public void setExceptionCount(Integer exceptionCount) { this.exceptionCount = exceptionCount; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getSummary() { return summary; }
    public void setSummary(String summary) { this.summary = summary; }

    public String getHes() { return hes; }
    public void setHes(String hes) { this.hes = hes; }
}
