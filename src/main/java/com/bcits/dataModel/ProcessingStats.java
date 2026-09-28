package com.bcits.dataModel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcessingStats {
    private int totalRecords;
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger exceptionCount = new AtomicInteger(0);
    private final Map<String, Integer> typeCounts = new ConcurrentHashMap<>();

    public void incrementSuccess() { successCount.incrementAndGet(); }
    public void incrementFailure() { failureCount.incrementAndGet(); }
    public void incrementException() { exceptionCount.incrementAndGet(); }
    public void addExceptionCount(int count) { exceptionCount.addAndGet(count); }

    public void addType(String type) {
        if (type != null) {
            typeCounts.merge(type, 1, Integer::sum);
        }
    }

    public int getTotalRecords() { return totalRecords; }
    public void setTotalRecords(int totalRecords) { this.totalRecords = totalRecords; }

    public int getSuccessCount() { return successCount.get(); }
    public void setSuccessCount(int successCount) { this.successCount.set(successCount); }

    public int getFailureCount() { return failureCount.get(); }
    public int getExceptionCount() { return exceptionCount.get(); }
    public void setFailureCount(int failureCount) { this.failureCount.set(failureCount); }
    public void setExceptionCount(int exceptionCount) { this.exceptionCount.set(exceptionCount); }

    public Map<String, Integer> getTypeCounts() { return typeCounts; }

    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        typeCounts.forEach((k, v) -> {
            if (sb.length() > 0) sb.append(", ");
            sb.append(v).append(" ").append(k);
        });
        return sb.toString();
    }
}
