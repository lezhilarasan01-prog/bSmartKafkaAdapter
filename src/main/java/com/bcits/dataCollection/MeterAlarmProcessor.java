/**
 * 
 */
package com.bcits.dataCollection;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bcits.dataModel.DeviceData;
import com.bcits.dataModel.DeviceData.PreVEEData;
import com.bcits.responseDataModel.ResponseParser;
import com.bcits.utils.GenericExceptionTracker;
import com.bcits.dataModel.PreVEE;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.text.ParseException;

/**
 * @author Tarik
 *
 */
public class MeterAlarmProcessor implements DataProcessor{
	
	private static MeterAlarmProcessor processor;
	private boolean hasData;
	private boolean hasExcpData;
	private Gson gson;
	private StringBuilder builder = new StringBuilder();
	private StringBuilder excpbuilder = new StringBuilder();
	private String meterNumber=null,sequenceNumber=null,alarmActive=null,serverTime=null,hesType=null;
	private Long meterAlarmId = null;
	private  PreVEEData[] preVdt;
	private Timestamp alarmTime=null,event_time = null;
	private char[] alarms;
	private ArrayList<String> alarmList = new ArrayList<String>();

	public static Timestamp parseTimestamp(String rawDt, Timestamp fallback) {
		if (rawDt == null || rawDt.trim().isEmpty() || "null".equalsIgnoreCase(rawDt.trim()) || "undefined".equalsIgnoreCase(rawDt.trim())) {
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
				"yyyy-MM-dd HH:mm:ss.SSS",
				"yyyy-MM-dd HH:mm:ss",
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
				java.util.Calendar cal = java.util.Calendar.getInstance();
				cal.setTime(d);
				int year = cal.get(java.util.Calendar.YEAR);
				if (year >= 1970 && year <= 2100) {
					return new Timestamp(d.getTime());
				}
			} catch (Exception ignored) {
			}
		}
		return fallback;
	}
	public static MeterAlarmProcessor getInstance() {
		if(processor==null)
			processor = new MeterAlarmProcessor();
		return processor;
	}

	
	
	@Override
	public void init() {
		builder.setLength(0);
		builder.append("insert into meter_data.meter_alarm "
				+ "(meter_number, meter_alarm_id,  alarm_time, sequence_number, alarm_active, time_stamp, server_time,hes_type) values ");	
		excpbuilder.setLength(0);
		excpbuilder.append("insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");

	}


	@Override
	public String getQuery() {
		return builder.toString().substring(0, builder.length() - 1)+" ON CONFLICT DO NOTHING";
	}

	@Override
	public boolean hasData() {
		return hasData;
	}

	@Override
	public void reset() {
		builder.setLength(0);
		hasData=false;
	}

	
	public void resetVariable() {
		meterNumber=null;meterAlarmId=null;sequenceNumber=null;alarmActive=null;alarmTime=null;event_time = null;
	}
	
	@Override
	public void addData(DeviceData scMM , String hesName) {
		try {
			generateMeterAlarmBatchData(scMM , hesName);
		} catch (ParseException e) {
			e.printStackTrace();
		} //Parse Data & Generate Batch Query	
		hasData = true;
	}	

//	@Override
//	public void addData(DeviceData scMM, String hesName) {
//
//		try {
//			
//			System.out.println("scMM "+scMM.getPreVEE());
//			GenericExceptionTracker.reset();
//			if (scMM.getPreVEE() == null) {
//				throw new NullPointerException("preVEE payload is empty");
//			}
//			if (scMM.getPreVEE().length<=0) {
//				throw new IllegalArgumentException("RegisterValues payload is empty");
//			}
//			generateMeterAlarmBatchData(scMM, hesName);
//			hasData = true;
//
//		} catch (ParseException e) {
//			hasExcpData = true;
//			Gson gson = new GsonBuilder().serializeNulls().create();
//			String safeSmm = gson.toJson(scMM).replace("'", "''");
//			String safeMsg = e.toString().replace("'", "''");
//			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Meter Alarm','"
//					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "','" + hesName + "'),");
//			e.printStackTrace();
//		} catch (Exception ex) {
//			hasExcpData = true;
//			Gson gson = new GsonBuilder().serializeNulls().create();
//			String safeSmm = gson.toJson(scMM).replace("'", "''");
//			String safeMsg = ex.toString().replace("'", "''");
//			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Meter Alarm','"
//					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "','" + hesName + "'),");
//			ex.printStackTrace();
//		}
//	}
	
	private void generateMeterAlarmBatchData(DeviceData scMM , String hesName) throws ParseException {
	    try {
	        meterNumber = scMM.getDeviceId();
	        ArrayList<String> alrmDesc = new ArrayList<>();
	        if (scMM.getHesId().equalsIgnoreCase("JIO")) {
	        	serverTime = String.valueOf(scMM.getHesDateTime());
	        	String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {}.getType());
	            preVdt = scMM.getPreVEE();

	            // Event ESW log (profile code check)
	            if (scMM.getProfileCode().equalsIgnoreCase("0.0.94.91.26.255")) {

	                try {
	                    for (int r = 0; r < preVdt.length; r++) {
	                        String obc = preVdt[r].getRegisterId();
	                        String atr = preVdt[r].getAttribute();

	                        if (obc.equalsIgnoreCase("0.0.94.91.18.255")) { 
	                            // EventCode -> Parse alarm bitstring
	                            alrmDesc = parseAlarm(preVdt[r].getEnergyQuantity());

	                        } else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { 
	                            // Event time
	                            String val = preVdt[r].getEnergyQuantity();
	                            if (val != null && !val.trim().isEmpty()) {
	                                event_time = parseTimestamp(val.trim(), null);
	                                if (event_time == null) {
	                                    GenericExceptionTracker.logPayloadException(scMM.getSeqId(), "event_time", "Invalid timestamp format: " + val);
	                                }
	                            } else {
	                            	GenericExceptionTracker.logPayloadException(scMM.getSeqId(), "event_time", "Event time missing or empty");
	                            }

	                            
	                        }
	                    }
	                } catch (Exception e) {
	                    e.printStackTrace();
	                }

	                // Iterate alarms if available
	                for (String almd : alrmDesc) {
	                    if (event_time == null) {
            		        hasExcpData = true;
            		        List<String> exceptions = GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
            		        String combinedExceptions = String.join(",", exceptions);
            		        if (combinedExceptions.isEmpty()) {
            		        	combinedExceptions = "Event time is null";
            		        }
            		        excpbuilder.append("(now(),'" + scMM.getSeqId() + "','"+ResponseParser.type+"','" + (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + smm.replace("'", "''") + "','"+ combinedExceptions.replace("'", "''") + "','"+ hesName + "'),"
            		        );
	            		    
	                    } else {
//	                        builder.append("( now(),'" + scMM.getDeviceId() + "', '" + scMM.getSeqId() + "', '"
//	                                + scMM.getSequenceNumber() + "','" + event_time + "','" + almd + "','"+serverTime+"'),");
	                    }
	                }
	            }
	        }else if(scMM.getHesId().equalsIgnoreCase("BCITS") || scMM.getHesId().equalsIgnoreCase("KIMBAL-7")) {
	        	 meterAlarmId = scMM.getSeqId(); // assuming seqId is your counterId
	 	        alarmTime = parseAlarmTimestamp(scMM.getAlarmTime());
	 	        sequenceNumber = String.valueOf(scMM.getSeqId());
	 	        serverTime = String.valueOf(scMM.getHesDateTime());
	 	        String desc = scMM.getAlarmDescription();
	 	        if (desc != null && !desc.trim().isEmpty()) {
	 	        	String[] alarmArray = desc.split("/");
	 	        	StringBuilder snBuilder = new StringBuilder();
	 	        	for (String sa : alarmArray) {
	 	        		if (sa != null && !sa.trim().isEmpty()) {
	 	        			snBuilder.append(sa.trim()).append(",");
	 	        		}
	 	        	}
	 	        	alarmActive = snBuilder.length() > 0 ? snBuilder.substring(0, snBuilder.length() - 1) : "";
	 	        } else {
	 	        	alarmActive = "";
	 	        }
	 	        
	 	       builder.append("('" + meterNumber + "'," + meterAlarmId + ",'" + alarmTime + "','" 
	                      + sequenceNumber + "', '" + alarmActive + "',now(),'" + serverTime + "','"+hesName+"'),");
	        	
			} 
	        else if (scMM.getHesId().equalsIgnoreCase("cyanconnode")) {

	            meterAlarmId = scMM.getSeqId();
	            alarmTime = parseAlarmTimestamp(scMM.getAlarmTime());
	            sequenceNumber = String.valueOf(scMM.getSeqId());
	            serverTime = String.valueOf(scMM.getHesDateTime());
	            String desc = scMM.getAlarmDescription();
	            String[] alarmArray = desc != null ? desc.split(",") : new String[0];

	            for (String sa : alarmArray) {
	                if (sa != null && !sa.trim().isEmpty()) {

	                    String value = sa.trim();

	                    // ✅ Case 1: Numeric index
	                    if (value.matches("\\d+")) {

	                        int index = Integer.parseInt(value);

	                        char[] bitArray = new char[128];
	                        Arrays.fill(bitArray, '0');

	                        if (index >= 0 && index < bitArray.length) {
	                            bitArray[index] = '1';
	                        }

	                        String bitString = new String(bitArray);
	                        ArrayList<String> alarms = parseAlarm(bitString);

	                        for (String alarm : alarms) {
	                            builder.append("('" + meterNumber + "'," + meterAlarmId + ",'" + alarmTime + "','" 
	                                    + sequenceNumber + "', '" + alarm + "',now(),'" + serverTime + "','" + hesName + "'),");
	                        }

	                    } else {
	                        if (value.contains("R_PHASE_VOLTAGE_MISSING")) {
	                            value = "R Phase - Voltage missing";
	                        } else if (value.contains("Y_PHASE_VOLTAGE_MISSING")) {
	                            value = "Y Phase - Voltage missing";
	                        } else if (value.contains("B_PHASE_VOLTAGE_MISSING")) {
	                            value = "B Phase - Voltage missing";
	                        } else if (value.contains("METER_OVER_VOLTAGE")) {
	                            value = "Over Voltage";
	                        } else if (value.contains("METER_UNDER_VOLTAGE")) {
	                            value = "Low Voltage";
	                        } else if (value.contains("METER_VOLT_UNBALANCE")) {
	                            value = "Voltage unbalance";
	                        } else if (value.contains("R_PHASE_CURRENT_REVERSE")) {
	                            value = "R Phase - Current reverse";
	                        } else if (value.contains("Y_PHASE_CURRENT_REVERSE")) {
	                            value = "Y Phase - Current reverse";
	                        } else if (value.contains("B_PHASE_CURRENT_REVERSE")) {
	                            value = "B Phase - Current reverse";
	                        } else if (value.contains("METER_CURRENT_UNBALANCE")) {
	                            value = "Current unbalance";
	                        } else if (value.contains("METER_CURRENT_BYPASS")) {
	                            value = "Current bypass/short";
	                        } else if (value.contains("METER_OVER_CURRENT")) {
	                            value = "Over Current in any phase";
	                        } else if (value.contains("METER_LOW_PF")) {
	                            value = "Very low PF";
	                        } else if (value.contains("METER_EARTH_LEAKAGE")) {
	                            value = "Earth Loading";
	                        } else if (value.contains("METER_MAGNETIC_INFLUENCE")) {
	                            value = "Magnet influence";
	                        } else if (value.contains("METER_NEUTRAL_DISTURBANCE")) {
	                            value = "Neutral disturbance";
	                        } else if (value.contains("METER_COVER_OPEN")) {
	                            value = "Meter cover open";
	                        } else if (value.contains("METER_CONNECT_DISCONNECT")) {
	                            value = "Meter load connect/disconnect";
	                        } else if (value.contains("METER_LAST_GASP")) {
	                            value = "Last Gasp";
	                        } else if (value.contains("METER_FIRST_BREATH")) {
	                            value = "First Breath";
	                        } else if (value.contains("METER_BILLING_INCREMENT")) {
	                            value = "Billing increment";
	                        }
	                        builder.append("('" + meterNumber + "'," + meterAlarmId + ",'" + alarmTime + "','" 
	                                + sequenceNumber + "', '" + value + "',now(),'" + serverTime + "','" + hesName + "'),");
	                    }
	                }
	            }
	        }

	    } catch (Exception e) {
	        e.printStackTrace();
	        hasExcpData=true;
			String safeMsg = e.getMessage() == null ? "Unknown Exception" : e.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Meter Alarm','" + (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','','"+ safeMsg + "','"+ hesName + "'),");
	    } finally {
	        resetVariable();
	    }
	}
	
	
	

	@Override
	public boolean hasExcpData() {
		return hasExcpData;
	}

	@Override
	public String getExcepQuery() {
		return excpbuilder.toString().substring(0, excpbuilder.length() - 1)+" ON CONFLICT DO NOTHING";
	}


	public static Timestamp parseAlarmTimestamp(String rawVal) {
		if (rawVal == null || rawVal.trim().isEmpty() || "null".equalsIgnoreCase(rawVal.trim())) {
			return null;
		}
		try {
			String clean = rawVal.trim().replace("T", " ");
			if (clean.contains("+")) {
				clean = clean.split("\\+")[0].trim();
			}
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return new Timestamp(sdf.parse(clean).getTime());
		} catch (Exception ex1) {
			try {
				OffsetDateTime odt = OffsetDateTime.parse(rawVal.trim());
				return Timestamp.valueOf(odt.toLocalDateTime());
			} catch (Exception ex2) {
				return null;
			}
		}
	}

	private ArrayList<String> parseAlarm(String eventBitString) {
		alarmList.clear();// clearing alarms
		if (eventBitString == null || eventBitString.trim().isEmpty()) {
			return alarmList;
		}
		alarms = eventBitString.toCharArray();
		for (int index = 0; index < alarms.length; index++) {
			if (alarms[index] == '1') {
				String alarm = null;
				switch (index) {

				// Three phase meter
				case 0:
					alarm = "R Phase - Voltage missing";
					break;
				case 1:
					alarm = "Y Phase - Voltage missing";
					break;
				case 2:
					alarm = "B Phase - Voltage missing";
					break;
				case 3:
					alarm = "Over Voltage";
					break;
				case 4:
					alarm = "Low Voltage";
					break;
				case 5:
					alarm = "Voltage unbalance";
					break;
				case 6:
					alarm = "R Phase - Current reverse";
					break;
				case 7:
					alarm = "Y Phase - Current reverse";
					break;
				case 8:
					alarm = "B Phase - Current reverse";
					break;
				case 9:
					alarm = "Current unbalance";
					break;
				case 10:
					alarm = "Current bypass/short";
					break;
				case 11:
					alarm = "Over Current in any phase";
					break;
				case 12:
					alarm = "Very low PF";
					break;

				// Single phase meter
				case 51:
					alarm = "Earth Loading";
					break;

				// Common to Three and Single Phase
				case 81:
					alarm = "Influence of permanent magnet or ac/dc electromagnet";
					break;
				case 82:
					alarm = "Neutral disturbance- HF,dc or alternate method";
					break;
				case 83:
					alarm = "Meter cover open";
					break;
				case 84:
					alarm = "Meter load disconnected/Meter load connected";
					break;
				case 85:
					alarm = "Last Gasp";
					break;
				case 86:
					alarm = "First Breath";
					break;
				case 87:
					alarm = "Increment in Billing counter (Manual/MRI reset)";
					break;
				case 127:
					alarm = "DI-1 CB Open (Planned Shutdown)– Occurrence";
					break;
				case 126:
					alarm = "DI-2 CB Closed– Occurrence";
					break;
				case 125:
					alarm = "DI-3 CB Open (Protection Trip)– Occurrence";
					break;
				case 124:
					alarm = "Digital Input 4- Occurrence";
					break;

				default:
					break;
				}
				if (alarm != null)
					alarmList.add(alarm);
			}
		}
		return alarmList;
	}

	
}
