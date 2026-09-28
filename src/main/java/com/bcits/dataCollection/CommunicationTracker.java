package com.bcits.dataCollection;

import java.io.File;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.bcits.dao.SchduledMeterTrackDao;

@Component
public class CommunicationTracker {

    @Autowired
    private SchduledMeterTrackDao schMtrTrckDao;

    private static final Logger LOGGER = LogManager.getLogger(CommunicationTracker.class);
    private static final Logger METER_COMM_LOGGER = LogManager.getLogger("MeterCommunicationLogger");

    private static final ZoneId IST_ZONE = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");

    private static final int BATCH_SIZE = 500;
    private static final int MAX_RETRIES = 3;

    // ==========================================================
    // 🔹 Separate Maps per Stream
    // ==========================================================
    private final ConcurrentHashMap<String, MeterComm> loadSurveyMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MeterComm> dailyLoadMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MeterComm> instantMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MeterComm> billingEventMap = new ConcurrentHashMap<>();

    // ==========================================================
    // 🔹 ADD COMMUNICATION ENTRY
    // ==========================================================
    public void add(String meterNo, String type) {
        try {
            if (meterNo == null || type == null) return;

            ZonedDateTime istNow = ZonedDateTime.now(IST_ZONE);
            ConcurrentHashMap<String, MeterComm> selectedMap = getMapByType(type);

            if (selectedMap == null) return;

            selectedMap.compute(meterNo, (k, existing) -> {
                if (existing == null) {
                    return new MeterComm(istNow, type);
                } else {
                    existing.setLastCommunication(istNow);
                    return existing;
                }
            });
        } catch (Exception e) {
            logException("Exception in add meter communication entry", e);
        }
    }

    // ==========================================================
    // 🔹 TYPE GROUPING LOGIC
    // ==========================================================
    private ConcurrentHashMap<String, MeterComm> getMapByType(String type) {
        if (type.equalsIgnoreCase("Bulk Load")) {
            return loadSurveyMap;
        } else if (type.equalsIgnoreCase("Daily Load")) {
            return dailyLoadMap;
        } else if (type.equalsIgnoreCase("Instantaneous Push") ||
                   type.equalsIgnoreCase("Instantaneous Pull") || 
                   type.equalsIgnoreCase("Instantaneous")) {
            return instantMap;
        } else if (type.equalsIgnoreCase("Billing") ||
                   type.toLowerCase().contains("event")) {
            return billingEventMap;
        }
        return null;
    }

    // ==========================================================
    // 🔹 STAGGERED SCHEDULERS (No @Transactional on Schedulers)
    // ==========================================================

    // 🔹 SCHEDULER 1 → LOAD SURVEY (Runs at :05 and :50)
    @Scheduled(cron = "0 5,50 * * * ?")
    public void processLoadSurvey() {
        try {
            executeAndClear(loadSurveyMap);
        } catch (Exception e) {
            logException("Exception in processLoadSurvey scheduler", e);
        }
    }

    // 🔹 SCHEDULER 2 → DAILY LOAD (Runs at :10, :25, :40, :55)
    @Scheduled(cron = "0 10/15 * * * ?")
    public void processDailyLoad() {
        try {
            executeAndClear(dailyLoadMap);
        } catch (Exception e) {
            logException("Exception in processDailyLoad scheduler", e);
        }
    }

    // 🔹 SCHEDULER 3 → INSTANT (Runs at :02 and :32)
    @Scheduled(cron = "0 2,32 * * * ?")
    public void processInstant() {
        try {
            executeAndClear(instantMap);
        } catch (Exception e) {
            logException("Exception in processInstant scheduler", e);
        }
    }

    // 🔹 SCHEDULER 4 → BILLING + EVENTS (Runs at :15 every 2 hours)
    @Scheduled(cron = "0 15 */2 * * *")
    public void processBillingEvent() {
        try {
            executeAndClear(billingEventMap);
        } catch (Exception e) {
            logException("Exception in processBillingEvent scheduler", e);
        }
    }

    // ==========================================================
    // 🔹 COMMON EXECUTION METHOD (With Key Sorting & Batching)
    // ==========================================================
    private void executeAndClear(ConcurrentHashMap<String, MeterComm> map) {
        if (map.isEmpty()) return;

        ConcurrentHashMap<String, MeterComm> snapshot = new ConcurrentHashMap<>();
        for (String key : map.keySet()) {
            MeterComm val = map.remove(key);
            if (val != null) {
                snapshot.put(key, val);
            }
        }

        if (snapshot.isEmpty()) return;

        // 1. Sort meter numbers ascending to ensure deterministic lock ordering
        //    (Prevents deadlocks between concurrent batches)
        List<String> sortedMeters = new ArrayList<>(snapshot.keySet());
        Collections.sort(sortedMeters);

        // 2. Process in batches of BATCH_SIZE (500)
        for (int i = 0; i < sortedMeters.size(); i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, sortedMeters.size());
            List<String> batchMeters = sortedMeters.subList(i, end);

            boolean success = executeBatchWithRetry(batchMeters, snapshot);
            if (!success) {
                // Return failed meters to map so they can be retried on next scheduled cycle
                for (String meter : batchMeters) {
                    MeterComm comm = snapshot.get(meter);
                    if (comm != null) {
                        map.putIfAbsent(meter, comm);
                    }
                }
            }
        }
    }

    // ==========================================================
    // 🔹 BATCH EXECUTION WITH TRANSIENT RETRY LOGIC
    // ==========================================================
    private boolean executeBatchWithRetry(List<String> batchMeters, ConcurrentHashMap<String, MeterComm> snapshot) {
        String query = buildBatchQuery(batchMeters, snapshot);
        if (query == null) return true;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                schMtrTrckDao.bulkInsert(query);
                return true;
            } catch (Exception e) {
                if (attempt == MAX_RETRIES) {
                    logException("Failed inserting batch after " + MAX_RETRIES + " attempts", e);
                    return false;
                }
                try {
                    // Randomized sleep (50ms - 200ms) to resolve any transient lock contention
                    Thread.sleep(ThreadLocalRandom.current().nextInt(50, 200));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    // ==========================================================
    // 🔹 BATCH QUERY BUILDER (Deterministic Order + Timestamp Protection)
    // ==========================================================
    private String buildBatchQuery(List<String> sortedMeters, ConcurrentHashMap<String, MeterComm> snapshot) {
        if (sortedMeters == null || sortedMeters.isEmpty()) return null;

        StringBuilder sb = new StringBuilder(sortedMeters.size() * 120);

        sb.append("INSERT INTO meter_data.meter_communication ")
          .append("(meter_number, first_communication, last_communication, type) VALUES ");

        for (String meter : sortedMeters) {
            MeterComm comm = snapshot.get(meter);
            if (comm == null) continue;

            sb.append("('")
              .append(meter).append("', '")
              .append(comm.getFirstCommunication().format(FORMATTER)).append("', '")
              .append(comm.getLastCommunication().format(FORMATTER)).append("', '")
              .append(comm.getType()).append("'),");
        }

        if (sb.charAt(sb.length() - 1) == ',') {
            sb.setLength(sb.length() - 1);
        } else {
            return null;
        }

        // Use GREATEST to prevent an older out-of-order batch from overwriting a newer communication time
        sb.append(" ON CONFLICT (meter_number) DO UPDATE SET ")
          .append("last_communication = GREATEST(meter_data.meter_communication.last_communication, EXCLUDED.last_communication), ")
          .append("type = CASE ")
          .append("  WHEN EXCLUDED.last_communication >= meter_data.meter_communication.last_communication THEN EXCLUDED.type ")
          .append("  ELSE meter_data.meter_communication.type ")
          .append("END;");

        return sb.toString();
    }

    // ==========================================================
    // 🔹 EXCEPTION LOGGING
    // ==========================================================
    private void logException(String message, Throwable e) {
        LOGGER.error(message, e);
        METER_COMM_LOGGER.error(message, e);
        logExceptionToFile(e);
    }

    private static synchronized void logExceptionToFile(Throwable e) {
        try {
            File dir = new File("logs");
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File file = new File(dir, "meter_communication_exceptions.log");
            try (PrintWriter pw = new PrintWriter(new java.io.FileWriter(file, true))) {
                pw.println("Timestamp: " + LocalDateTime.now());
                e.printStackTrace(pw);
                pw.println("--------------------------------------------------------------------------------");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // ==========================================================
    // 🔹 INNER CLASS
    // ==========================================================
    private static class MeterComm {

        private final ZonedDateTime firstCommunication;
        private ZonedDateTime lastCommunication;
        private final String type;

        MeterComm(ZonedDateTime ts, String type) {
            this.firstCommunication = ts;
            this.lastCommunication = ts;
            this.type = type;
        }

        public ZonedDateTime getFirstCommunication() {
            return firstCommunication;
        }

        public ZonedDateTime getLastCommunication() {
            return lastCommunication;
        }

        public void setLastCommunication(ZonedDateTime lastCommunication) {
            this.lastCommunication = lastCommunication;
        }

        public String getType() {
            return type;
        }
    }
}
