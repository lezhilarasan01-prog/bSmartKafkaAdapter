/**
 * 
 */
package com.bcits.dataCollection;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bcits.dataModel.DeviceData;
import com.bcits.dataModel.DeviceData.PreVEEData;
import com.bcits.dataModel.RegisterValue;
import com.bcits.dataModel.RegisterValues;
import com.bcits.responseDataModel.ResponseParser;
import com.bcits.utils.GenericExceptionTracker;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author TARIK
 *
 */
public class DailyLoadProcessor implements DataProcessor{
	
	private static DailyLoadProcessor processor;
	private static final List<Map<String, Object>> exceptionList = new ArrayList<>();

	private boolean hasData;
	private boolean hasExcpData;

	private StringBuilder dailybuilder = new StringBuilder();
	private StringBuilder excpbuilder = new StringBuilder();
	private Gson gson;
	private boolean hasNumExcpdata=false;
	private BigDecimal kwh_Imp = null, kwh_Exp = null, kvah_Imp = null, kvah_Exp = null;
	private BigDecimal kvarh_q1= null,kvarh_q2= null,kvarh_q3= null ,kvarh_q4 = null;
	private final Map<Integer, List<String>> exceptionMap = new HashMap<>();

	private Timestamp read_time=null ,create_time=null;
	private static final ThreadLocal<SimpleDateFormat> format = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	private String val;
	private  List<RegisterValue> dt;
	private String dType;
	private String hes_time=null;
	
	
	public static DailyLoadProcessor getInstance() {
		if(processor==null)
			processor = new DailyLoadProcessor();
		return processor;
	}
	
	@Override
	public void init() {		
		dailybuilder.setLength(0);
		dailybuilder.append("insert into meter_data.daily_load "
				+ "(time_stamp, flag , mtrno, rtc_date_time,"
				+ "cum_active_import_energy, cum_active_export_energy, cum_apparent_import_energy, cum_apparent_export_energy ,"
				+ "cum_reactive_energy5, cum_reactive_energy6, cum_reactive_energy7, cum_reactive_energy8,server_time,hes_type) values ");
		
		excpbuilder.setLength(0);
		excpbuilder.append("insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");
	
		
	}
	
//	@Override
//	public void addData(DeviceData scMM , String hesName) {
//		try {
//			generateDailyLoadBatchData(scMM , hesName);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} //Parse Data & Generate Batch Query	
//		hasData = true;
//	}
	
	
	@SuppressWarnings("null")
	@Override
	public void addData(DeviceData scMM, String hesName) {

		try {
			if (scMM == null) {
				System.out.println("DeviceData is null");
				return;
			}
			if (scMM.getPreVEE() == null || scMM.getPreVEE().length == 0) {
				Gson gson = new Gson();
				String smmJson = gson.toJson(scMM);
				String safeSmm = smmJson.replace("'", "''");
				String remarks = "REMARKS: preVEE payload is empty";
				hasExcpData = true;
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','DailyLoad','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + remarks
						+ "','" + hesName + "'),");
				return;
			} else {
				generateDailyLoadBatchData(scMM, hesName);
				hasData = true;
			}
		} catch (Exception ex) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = ex.getMessage() == null ? "Unknown Exception" : ex.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','DailyLoad','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "','" + (hesName != null ? hesName : "") + "'),");
			ex.printStackTrace();
		}
	}
	

	private Timestamp parseTimestamp(String rawDt, Timestamp fallback) {
		if (rawDt == null || rawDt.trim().isEmpty() || "null".equalsIgnoreCase(rawDt.trim())) {
			return fallback;
		}
		String clean = rawDt.trim().replace("T", " ");
		if (clean.contains("+")) {
			clean = clean.split("\\+")[0].trim();
		}
		if (clean.endsWith("Z") || clean.endsWith("z")) {
			clean = clean.substring(0, clean.length() - 1).trim();
		}

		String[] patterns = {
			"yyyy-MM-dd HH:mm:ss",
			"yyyy-MM-dd HH:mm:ss.SSS",
			"yyyy-MM-dd HH:mm",
			"dd-MM-yyyy HH:mm:ss",
			"dd/MM/yyyy HH:mm:ss",
			"yyyy/MM/dd HH:mm:ss"
		};

		for (String p : patterns) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(p);
				sdf.setLenient(false);
				Date d = sdf.parse(clean);
				if (d != null) {
					Calendar cal = Calendar.getInstance();
					cal.setTime(d);
					int yr = cal.get(Calendar.YEAR);
					if (yr >= 1970 && yr <= 2100) {
						return new Timestamp(d.getTime());
					}
				}
			} catch (Exception ignored) {
			}
		}
		return fallback;
	}

	@Override
	public String getQuery() {
		if (dailybuilder.length() > 0 && dailybuilder.charAt(dailybuilder.length() - 1) == ',') {
			return dailybuilder.substring(0, dailybuilder.length() - 1) + " ON CONFLICT DO NOTHING";
		}
		return dailybuilder.toString();
	}
	
	@Override
	public String getExcepQuery() {
		if (excpbuilder.length() > 0 && excpbuilder.charAt(excpbuilder.length() - 1) == ',') {
			return excpbuilder.substring(0, excpbuilder.length() - 1) + " ON CONFLICT DO NOTHING";
		}
		return excpbuilder.toString();
	}
	
	@Override
	public boolean hasData() {
		return hasData;
	}
	
	@Override
	public boolean hasExcpData() {
		return hasExcpData;
	}

	@Override
	public void reset() {
		excpbuilder.setLength(0);
		dailybuilder.setLength(0);
		hasData = false;
		hasExcpData = false;
	}
	
	private RegisterValues[] rv;
	private String[] st, ct;
	private String obc;
	private PreVEEData[] preVdt;
	
	private void generateDailyLoadBatchData(DeviceData scMM, String hesName) {		

		preVdt = scMM.getPreVEE();
		String hesDt = scMM.getHesDateTime();
		Timestamp hesTimestamp = parseTimestamp(hesDt, new Timestamp(System.currentTimeMillis()));
		hes_time = "'" + hesTimestamp + "'";
		
		gson = new Gson();
		String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {}.getType());
		try {
			GenericExceptionTracker.reset();
			if (preVdt != null) {
				for (int r = 0; r < preVdt.length; r++) {
					if (preVdt[r] == null) continue;
					obc = preVdt[r].getRegisterId();
					if (obc == null) continue;
					
					if (obc.equalsIgnoreCase("0.0.1.0.0.255") || obc.equalsIgnoreCase("0.0.0.1.0.255")
							|| obc.equalsIgnoreCase("1.0.0.1.2.255") || obc.equalsIgnoreCase("0.0.99.2.0.255")) {
						read_time = parseTimestamp(preVdt[r].getEnergyQuantity(), null);
					} else if (obc.equalsIgnoreCase("1.0.1.8.0.255") || obc.equalsIgnoreCase("1.0.1.29.0.255")) {
						kwh_Imp = GenericExceptionTracker.exceptionPayload("kwh_Imp", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.2.8.0.255") || obc.equalsIgnoreCase("1.0.2.29.0.255")) {
						kwh_Exp = GenericExceptionTracker.exceptionPayload("kwh_Exp", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.9.8.0.255") || obc.equalsIgnoreCase("1.0.9.29.0.255")) {
						kvah_Imp = GenericExceptionTracker.exceptionPayload("kvah_Imp", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.10.8.0.255") || obc.equalsIgnoreCase("1.0.10.29.0.255")) {
						kvah_Exp = GenericExceptionTracker.exceptionPayload("kvah_Exp", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.5.8.0.255") || obc.equalsIgnoreCase("1.0.5.29.0.255")) {
						kvarh_q1 = GenericExceptionTracker.exceptionPayload("kvarh_q1", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.6.8.0.255") || obc.equalsIgnoreCase("1.0.6.29.0.255")) {
						kvarh_q2 = GenericExceptionTracker.exceptionPayload("kvarh_q2", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.7.8.0.255") || obc.equalsIgnoreCase("1.0.7.29.0.255")) {
						kvarh_q3 = GenericExceptionTracker.exceptionPayload("kvarh_q3", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.8.8.0.255") || obc.equalsIgnoreCase("1.0.8.29.0.255")) {
						kvarh_q4 = GenericExceptionTracker.exceptionPayload("kvarh_q4", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					}
				}
			}

			if (read_time == null && scMM.getMeterDateTime() != null && !scMM.getMeterDateTime().trim().isEmpty()) {
				read_time = parseTimestamp(scMM.getMeterDateTime(), null);
			}
			if (read_time == null && scMM.getHesDateTime() != null && !scMM.getHesDateTime().trim().isEmpty()) {
				read_time = parseTimestamp(scMM.getHesDateTime(), null);
			}
			if (read_time == null) {
				read_time = hesTimestamp;
			}

			if (GenericExceptionTracker.hasExceptions()) {
				hasExcpData = true;
				List<String> exceptions = GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
				String combinedExceptions = String.join(",", exceptions);
				String pType = ResponseParser.type != null ? ResponseParser.type : "Daily Load";
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + pType + "','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId().replace("'", "''") : "") + "','" + smm.replace("'", "''") + "','"
						+ combinedExceptions.replace("'", "''") + "','" + (hesName != null ? hesName.replace("'", "''") : "") + "'),");
				GenericExceptionTracker.sendExpToDlq(smm.replace("'", "''"));
			} else {
				String devId = scMM.getDeviceId() != null ? scMM.getDeviceId().trim().replace("'", "''") : "";
				String safeHes = hesName != null ? hesName.trim().replace("'", "''") : "";
				dailybuilder.append("(now(),'" + scMM.getSeqId() + "', '" + devId + "','" + read_time + "',"
						+ kwh_Imp + ", " + kwh_Exp + ", " + kvah_Imp + ", " + kvah_Exp + ","
						+ kvarh_q1 + "," + kvarh_q2 + "," + kvarh_q3 + "," + kvarh_q4 + "," + hes_time + ",'" + safeHes + "'),");
				hasData = true;
			}

		} catch (Exception e) {
			hasExcpData = true;
			String safeSmm = smm.replace("'", "''");
			String pType = ResponseParser.type != null ? ResponseParser.type : "Daily Load";
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + pType + "','" + (scMM.getDeviceId() != null ? scMM.getDeviceId().replace("'", "''") : "") + "','" + safeSmm + "','REMARKS:" + (e.getMessage() != null ? e.getMessage().replace("'", "''") : "Error") + "','" + (hesName != null ? hesName.replace("'", "''") : "") + "'),");
		}

		

		preVdt=null;
//		st=null;
//		ct=null;
		resetVariable();
	}
	
	public void resetVariable() {
		
		kwh_Imp = null; kwh_Exp = null; kvah_Imp = null; kvah_Exp = null;
		kvarh_q1= null;kvarh_q2= null;kvarh_q3= null ;kvarh_q4 = null;	
		read_time=null ;create_time=null;
	}	

	public String zonedDateTimeConverter(String obisDate) {
		if (obisDate == null || obisDate.trim().isEmpty()) {
			return null;
		}
		try {
			// Parse the string timestamp into a LocalDateTime object
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			LocalDateTime localDateTime = LocalDateTime.parse(obisDate.trim(), formatter);

			// Set the timezone of the LocalDateTime object to local time zone
			ZoneId localTimeZone = ZoneId.systemDefault();
			ZonedDateTime localZonedDateTime = localDateTime.atZone(localTimeZone);

			// Add time difference of 5 hours and 30 minutes
			ZonedDateTime gmt5_30ZonedDateTime = localZonedDateTime.plusHours(5).plusMinutes(30);

			// Convert to string timestamp in GMT+5:30 time zone
			return gmt5_30ZonedDateTime.format(formatter);
		} catch (Exception e) {
			return obisDate;
		}
	}

	Double getDouble(String value) {// GET DOUBLE VALUE WITH MAX 3
		// DECIMALS
		try {
			if (value == null || value.trim().isEmpty()) {
				return null;
			}
			String extracted = value.trim().split("\\s+")[0];
			double number = Math.round(Double.parseDouble(extracted) * 1000);
			if (number > Double.MAX_VALUE) {
	            System.out.println("The value exceeds the maximum size of a double.");
	            return (double) (Integer.parseInt(extracted) / 1000);
	        } else {
//	            System.out.println("The value is within the range of a double.");
	            return number / 1000;
	        }	
		} catch (Exception e) {
			return null;
		}
	}
	
	
//	public static BigDecimal exceptionPayload11(String obc,String formattedValue,int meterSampleId,String apiName,StringBuilder excpBuilder,String smm,String hes) {
//	    try {
//	        String extractedPart = formattedValue.contains(" ") ? formattedValue.split("\\s+")[0] : formattedValue;
//	        extractedPart = extractedPart.replace("W", "").replace("A", "") .replace("V", "").replace("Hz", "").replace("VA", "")
//	                .replace("var", "").replace("min.", "").replace("T", " ").replace("varh", "").replace("VAh", "").replace(" min.", "");
//	        BigDecimal extractedValue = new BigDecimal(extractedPart);
//	        BigDecimal maxAllowed = new BigDecimal("9999999999.99999");
//
//	        if (extractedValue.abs().compareTo(maxAllowed) > 0) {
//	            hasNumExcpdata = true;
//	            System.out.println("hasNumExcpdata " + hasNumExcpdata);
//	            String existingData = excpBuilder.length() > 0 ? excpBuilder.toString() : "";
//	            if (!existingData.isEmpty()) {
//	                excpBuilder.setLength(0); // clear old content
//	                excpBuilder.append(existingData).append(" | "); // separator
//	            }
//	            excpBuilder.append(obc).append(":").append(extractedValue);
//	            excpBuilder.append("(now(),'" + meterSampleId + "','" + apiName + "','" + smm + "','"+obc+"','"+extractedValue+"','"+hes+"'),");
//	        }
//	        System.out.println("extractedValue "+extractedValue);
//	        return extractedValue;
//	    } catch (NumberFormatException e) {
//	        System.err.println("Error parsing value for OBC: " + obc + " -> " + e.getMessage());
//	        return null;
//	    }
//	}


	
//	public static BigDecimal exceptionPayload(String paramName, String obc, String formattedValue, int meterSampleId,
//			String apiName, String smm, String hes) {
//		try {
//			if (formattedValue == null) {
//				recordException(meterSampleId, paramName, null, "No value provided");
//				return null;
//			}
//
//
//			String extractedPart = formattedValue.contains(" ") ? formattedValue.split("\\s+")[0] : formattedValue;
//
//			extractedPart = extractedPart.replaceAll("(W|A|V|Hz|VA|varh?|min\\.?|T|VAh)", "").trim();
//
//			String remarks = null;
//
//			if (!isNumeric(extractedPart)) {
//				remarks = "Non-numeric value in response";
//			} else if (obc != null && !obc.trim().isEmpty() && extractedPart.isEmpty()) {
//				remarks = "OBC has no value";
//			} else if (valueExceedsLimit(extractedPart)) {
//				remarks = "Value exceeds allowed limit";
//			}
//
//			if (remarks != null) {
//				hasNumExcpdata = true;
//				recordException(meterSampleId, paramName, extractedPart, remarks);
//			}
//
//			return new BigDecimal(extractedPart);
//
//		} catch (Exception e) {
//			hasNumExcpdata = true;
//			recordException(meterSampleId, paramName, formattedValue, "Parse error: " + e.getMessage());
//			return null;
//		}
//	}
//
//	private static boolean isNumeric(String str) {
//		if (str == null || str.trim().isEmpty())
//			return false;
//		try {
//			new BigDecimal(str);
//			return true;
//		} catch (NumberFormatException e) {
//			return false;
//		}
//	}
//
//	private static boolean valueExceedsLimit(String value) {
//		try {
//			return new BigDecimal(value).compareTo(new BigDecimal("9999999999")) > 0;
//		} catch (NumberFormatException e) {
//			return false;
//		}
//	}
//
//	private static void recordException(int meterSampleId, String paramName, String value, String remarks) {
//		String combined = "PARAM:" + paramName + ", VALUE:" + (value != null ? value : "null") + ", REMARKS:" + remarks;
//		exceptionMap.computeIfAbsent(meterSampleId, k -> new ArrayList<>()).add(combined);
//	}

	
	
	


}
