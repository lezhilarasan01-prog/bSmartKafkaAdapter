package com.bcits.utils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;

import com.bcits.dao.SchduledMeterTrackDao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class GenericExceptionTracker {
	
	private static final Logger log = LoggerFactory.getLogger(GenericExceptionTracker.class);
	
	protected static EntityManager entityManager;
	private static final Pattern TS_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{1,2}:\\d{2}:\\d{2}");

	@PersistenceContext
	public void setEntityManager(EntityManager entityManager) {
		GenericExceptionTracker.entityManager = entityManager;
	}
	private static final DateTimeFormatter TS_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
			.withResolverStyle(ResolverStyle.STRICT);
	private GenericExceptionTracker() {} // prevent instantiation

	//private static boolean hasNumExcpdata=false;
	private static final ThreadLocal<Boolean> hasNumExcpdata = ThreadLocal.withInitial(() -> false);
	private static final String DLQ_TOPIC_SUFFIX = "-dlq"; 
	
	private static KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	public void setKafkaTemplate(KafkaTemplate<String, String> kafkaTemplate) {
		GenericExceptionTracker.kafkaTemplate = kafkaTemplate;
	}
	
	private static Environment env;
	
	@Autowired
	public void setEnv(Environment env) {
		GenericExceptionTracker.env = env;
	}

	//private static final Map<String, List<String>> exceptionMap = new HashMap<>();
	private static final ThreadLocal<Map<String, List<String>>> exceptionMap = ThreadLocal.withInitial(HashMap::new);
	
	private static SchduledMeterTrackDao schMtrTrckDao;

	@Autowired
	public void setSchMtrTrckDao(SchduledMeterTrackDao schMtrTrckDao) {
		GenericExceptionTracker.schMtrTrckDao = schMtrTrckDao;
	}

//	public static BigDecimal exceptionPayload(String paramName, String obc, String formattedValue, int meterSampleId,
//			String apiName, String smm, String hes) {
//		try {
//			if (formattedValue == null) {
//				hasNumExcpdata = true;
//				recordException(meterSampleId, paramName, null, "No value provided");
//				return null;
//			}
//
//			// Clean the input value
//			String extractedPart = formattedValue.contains(" ") ? formattedValue.split("\\s+")[0] : formattedValue;
//
//			extractedPart = extractedPart.replaceAll("(W|A|V|Hz|VA|varh?|min\\.?|T|VAh)", "").trim();
//
//			// Try to parse
//			BigDecimal number = null;
//			try {
//				number = new BigDecimal(extractedPart);
//			} catch (NumberFormatException e) {
//				hasNumExcpdata = true;
//				recordException(meterSampleId, paramName, extractedPart,"PARAM:" + paramName + ",REMARKS:'Invalid number format: " + e.getMessage());
//				return null;
//			}
//
//			// Check OBC value missing
//			if (obc != null && !obc.trim().isEmpty() && extractedPart.isEmpty()) {
//				hasNumExcpdata = true;
//				recordException(meterSampleId, paramName, extractedPart, "OBC value missing");
//			}
//
//			// Check limit dynamically
//			BigDecimal maxLimit = new BigDecimal("9999999999.99999");
//			if (number.compareTo(maxLimit) > 0) {
//				hasNumExcpdata = true;
//				recordException(meterSampleId, paramName, extractedPart, "Value exceeds max limit (" + maxLimit + ")");
//			}
//			return number;
//		} catch (Exception e) {
//			hasNumExcpdata = true;
//			recordException(meterSampleId, paramName, formattedValue,
//					"Unexpected error: " + e.getClass().getSimpleName() + " - " + e.getMessage());
//			return null;
//		}
//	}
//
//	private static void recordException(int meterSampleId, String paramName, String value, String remarks) {
//		String combined = "PARAM:" + paramName + ", VALUE:" + (value != null ? value : "null") + ", REMARKS:" + remarks;
//		exceptionMap.computeIfAbsent(meterSampleId, k -> new ArrayList<>()).add(combined);
//	}

	public static BigDecimal exceptionPayload(String paramName, String obc, String formattedValue, Long meterSampleId,
			String apiName, String smm, String hes) {
		
		BigDecimal maxLimit;
		try {
//			if (formattedValue == null) {
//				hasNumExcpdata = true;
//				recordException(meterSampleId, paramName, null, "No value provided");
//				return null;
//			}
//
//			String extractedPart = formattedValue.contains(" ") ? formattedValue.split("\\s+")[0] : formattedValue;
//			extractedPart = extractedPart.replaceAll("(W|A|V|Hz|VA|varh?|min\\.?|T|VAh)", "").trim();
//
//			BigDecimal number;
//			try {
//				number = new BigDecimal(extractedPart);
//			} catch (NumberFormatException e) {
//				hasNumExcpdata = true;
//				recordException(meterSampleId, paramName, extractedPart, "Invalid number format: " + e.getMessage());
//				return null;
//			}
			
		if (formattedValue == null || formattedValue.trim().isEmpty()) {
				return null;
			}

			String trimmed = formattedValue.trim();
			if (trimmed.equalsIgnoreCase("DLMS_TYPE_UNKNOWN") 
					|| trimmed.equalsIgnoreCase("null") 
					|| trimmed.equalsIgnoreCase("NaN")
					|| trimmed.equalsIgnoreCase("N/A")
					|| trimmed.equals("-")) {
				return null;
			}

			java.util.regex.Matcher matcher = Pattern.compile("[-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?").matcher(trimmed);
			if (!matcher.find()) {
				return null;
			}
			String extractedPart = matcher.group();
			if (extractedPart == null || extractedPart.isEmpty() || extractedPart.equals(".") || extractedPart.equals("-") || extractedPart.equals("+")) {
				return null;
			}

			BigDecimal number;
			try {
				number = new BigDecimal(extractedPart);
			} catch (NumberFormatException e) {
				hasNumExcpdata.set(true);
				recordException(meterSampleId, paramName, extractedPart, "Invalid number format: " + e.getMessage());
				return null;
			}
			if(obc.equalsIgnoreCase("1.0.81.7.10.255") || obc.equalsIgnoreCase("1.0.81.7.20.255") || obc.equalsIgnoreCase("1.0.81.7.21.255")) {
				 maxLimit = new BigDecimal("360.000");
			}else if(obc.equalsIgnoreCase("0.0.17.0.0.255")) {
				maxLimit  = new BigDecimal("99999.99999");
			}else {
				 maxLimit = new BigDecimal("9999999999.99999");
			}
			
			if (number.abs().compareTo(maxLimit) > 0) {
				hasNumExcpdata.set(true);
			    recordException(meterSampleId,paramName,extractedPart,"Value exceeds DB limit ±" + maxLimit);
			}

			return number;
		} catch (Exception e) {
			hasNumExcpdata.set(true);
			recordException(meterSampleId, paramName, formattedValue,
					"Unexpected error: " + e.getClass().getSimpleName() + " - " + e.getMessage());
			return null;
		}
	}
	public static Long exceptionPayloadLong(String paramName, String obc, String formattedValue, Long meterSampleId,
			String apiName, String smm, String hes) {
		BigDecimal bd = exceptionPayload(paramName, obc, formattedValue, meterSampleId, apiName, smm, hes);
		if (bd == null) {
			return null;
		}
		
		try {
			return bd.longValue();
		} catch (Exception e) {
			hasNumExcpdata.set(true);
			recordException(meterSampleId, paramName, formattedValue, "Value out of bounds for Long: " + e.getMessage());
			return null;
		}
	}
	public static Integer exceptionPayloadInteger(String paramName, String obc, String formattedValue, Long meterSampleId,
			String apiName, String smm, String hes) {
		BigDecimal bd = exceptionPayload(paramName, obc, formattedValue, meterSampleId, apiName, smm, hes);
		if (bd == null) {
			return null;
		}
		
		try {
			return bd.intValue();
		} catch (Exception e) {
			hasNumExcpdata.set(true);
			recordException(meterSampleId, paramName, formattedValue, "Value out of bounds for Integer: " + e.getMessage());
			return null;
		}
	}

//	private static void recordException(int meterSampleId,String paramName,String value,String remarks) {
//	    String key = String.valueOf(meterSampleId);
//	    String combined = "PARAM:" + paramName +", VALUE:" + (value != null ? value : "null")+", REMARKS:" + remarks;
//	    exceptionMap.computeIfAbsent(key, k -> new ArrayList<>()).add(combined);
//	}
	private static void recordException(Long meterSampleId, String paramName, String value, String remarks) {
		String key = String.valueOf(meterSampleId);
		String combined = "PARAM:" + paramName + ", VALUE:" + (value != null ? value : "null") + ", REMARKS:" + remarks;
		log.debug("[EXCEPTION-TRACKER] {}", combined);
		Map<String, List<String>> map = exceptionMap.get();
		map.computeIfAbsent(key, k -> new ArrayList<>()).add(combined);
	}

	public static boolean hasExceptions() {
		return hasNumExcpdata.get();
	}

	public static List<String> getExceptions(String msn) {
		return exceptionMap.get().getOrDefault(msn, new ArrayList<>());
	}

	public static void reset() {
		hasNumExcpdata.set(false);
		exceptionMap.get().clear();
	}
	
	public static void logPayloadException(Long meterSampleId, String paramName, String remarks) {
		hasNumExcpdata.set(true);
	    recordException(meterSampleId, paramName, null, remarks);
	}
	
	
	public static void sendExpToDlq(String message) {
		try {
			String configuredDlq = env != null ? env.getProperty("kafka.dlqTopics") : null;
			if (configuredDlq == null || configuredDlq.trim().isEmpty()) {
				configuredDlq = "BSMART_BLOCKLOAD_TOPIC_DLQ";
			}
			String dlqTopic = configuredDlq.split(",")[0].trim();
			ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, null, message);
			dlqRecord.headers().add("original-source", "manual".getBytes(StandardCharsets.UTF_8));
			if (kafkaTemplate != null) {
				kafkaTemplate.send(dlqRecord);
				log.info("Sent to DLQ ({}): {}", dlqTopic, message);
			} else {
				log.error("KafkaTemplate is null, cannot send to DLQ");
			}
		} catch (Exception e) {
			log.error("Failed to send to DLQ: {}", e.getMessage());
		}
	}
	
	public static List<?> checknamePlatedetails(String mtrno) {
	    List<?> list = null;
	    try {
	        String sql = "SELECT * FROM meter_data.name_plate WHERE meter_serial_number = '" + mtrno + "'";
	        list =entityManager.createNativeQuery(sql).getResultList();
	    } catch (Exception ex) {
	        ex.printStackTrace();
	    }
	    return list;
	}

	public static void insertAndDeleteNamePlateData(String mtrno) {
	    try {
	        String qry = "INSERT INTO meter_data.name_plate_hst(id, meter_serial_number, device_id, manufacturer_name, firmware_version, meter_type, meter_catagory, current_rating, hardware_version, year_of_manufacture, nflag, created_time, updated_time, node_id, ct_ratio, pt_ratio, server_time) "
	                + "(SELECT nextval('meter_data.name_plate_id_seq'), meter_serial_number, device_id, manufacturer_name, firmware_version, meter_type, meter_catagory, current_rating, hardware_version, year_of_manufacture, nflag, created_time, updated_time, node_id, ct_ratio, pt_ratio, server_time FROM meter_data.name_plate WHERE meter_serial_number = '" + mtrno + "')";
	        String dltqry = "DELETE FROM meter_data.name_plate WHERE meter_serial_number = '" + mtrno + "'";

	        int k = schMtrTrckDao.bulkInsert(qry);
	        int j = schMtrTrckDao.bulkInsert(dltqry);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	
	public static void logPayloadException(Long meterSampleId, String paramName, String value, String remarks) {
	    hasNumExcpdata.set(true);
	    recordException(meterSampleId, paramName, value, remarks);
	}
}
