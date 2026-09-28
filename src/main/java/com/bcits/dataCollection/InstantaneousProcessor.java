package com.bcits.dataCollection;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.bcits.dao.SchduledMeterTrackDao;
import com.bcits.dataModel.DeviceData;
import com.bcits.dataModel.DeviceData.PreVEEData;
import com.bcits.dataModel.RegisterValue;
import com.bcits.dataModel.RegisterValues;
import com.bcits.responseDataModel.ResponseParser;
import com.bcits.utils.GenericExceptionTracker;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author Remith
 *
 */
public class InstantaneousProcessor implements DataProcessor{

	
	@Autowired
	private SchduledMeterTrackDao schMtrTrckDao;
	private static InstantaneousProcessor processor;
	private boolean hasData;
	private boolean hasExcpData;
	private final Map<Integer, List<String>> exceptionMap = new HashMap<>();
	private StringBuilder builder = new StringBuilder();
	private StringBuilder excpbuilder = new StringBuilder();
	private Gson gson;
	private boolean hasNumExcpdata=false;
	private BigDecimal i_r = null, i_y = null, i_b = null, v_r = null, v_y = null, v_b = null, pf_r = null, pf_y = null, pf_b = null, threePhasePf=null,frequency=null;
	private BigDecimal v_ry_angle = null,  v_rb_angle = null, v_yb_angle = null;
	private BigDecimal kwh = null,  kvah = null, kvarh_lag = null, kvarh_lead = null,kva = null ,kwh_exp = null, kvah_exp = null, load_limit_value= null/* ,kw=0, kvar=0,kvarh=0 */;
	private BigDecimal signedActivePower=null,signedReactivePower=null, mdKw = null, mdKva = null;
	private BigDecimal reactiveImpActiveExp=null ,reactiveExpActiveExp = null;
	//private Integer powerOffCount=null, powerOffDuration=null, tamperCount=null,billingCount=null,programmingCount=null;
	private Integer billingCount=null,programmingCount=null,totalPowerOnDuration=null;
	private Long powerOffCount=null, powerOffDuration=null, tamperCount=null;
	private Timestamp read_time=null, billingDate=null,server_time=null;
	private String dateMdKw=null,dateMdKva=null,powerVoltage=null,phaseCurrent=null,neutralCurrent=null ,load_switch_status=null , phase_sequence = null;
	private String obc;
	private String val;
	private  List<RegisterValue> dt;
	private String dType;
	private PreVEEData[] preVdt;

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
				// Must be a valid 4-digit year between 1970 and 2100 to prevent PostgreSQL timestamp out of range
				if (year >= 1970 && year <= 2100) {
					return new Timestamp(d.getTime());
				}
			} catch (Exception ignored) {
			}
		}
		return fallback;
	}

	private static final String TPD_EXCEPTION_LOG_PATH =
	        getJarDirectory() + File.separator + "logs" + File.separator + "totalPowerOnDuration_exceptions.log";
	private static final Object TPD_LOG_LOCK = new Object();
	private static String getJarDirectory() {
	    try {
	        String jarPath = InstantaneousProcessor.class.getProtectionDomain()
	                .getCodeSource().getLocation().toURI().getPath();
	        File jarFile = new File(jarPath);
	        return jarFile.getParentFile().getAbsolutePath();
	    } catch (Exception e) {
	        return "."; // fallback to current working directory
	    }
	}
	private void logTotalPowerOnDurationException(long l, String meterNo, String rawValue, String remarks) {
	    synchronized (TPD_LOG_LOCK) {
	        try {
	            File logFile = new File(TPD_EXCEPTION_LOG_PATH);
	            File parentDir = logFile.getParentFile();
	            if (parentDir != null && !parentDir.exists()) {
	                parentDir.mkdirs();
	            }
	            try (FileWriter fw = new FileWriter(logFile, true);
	                 PrintWriter pw = new PrintWriter(fw)) {
	                String logLine = String.format(
	                    "%s | seq_id=%s | meterno=%s | totalPowerOnDuration=%s | remarks=%s",
	                    new Timestamp(System.currentTimeMillis()),
	                    l,
	                    meterNo,
	                    rawValue,
	                    remarks
	                );
	                pw.println(logLine);
	            }
	        } catch (IOException ioe) {
	            // last-resort fallback so a logging failure never breaks the main flow
	            ioe.printStackTrace();
	        }
	    }
	}
	public static InstantaneousProcessor getInstance() {
		if(processor==null)
			processor = new InstantaneousProcessor();
		return processor;
	}
	
	@Override
	public void init() {
		builder.setLength(0);

		builder.append("insert into meter_data.amiinstantaneous "
				+ "(time_stamp, iflag, meter_number, rdate, i_r, i_y, i_b, v_r, v_y, v_b, pf_r, pf_y, pf_b, pf_threephase, frequency,"
				+ "kva, signed_active_power, signed_reactive_power, power_off_count, power_off_duration, tamper_count, billing_count, programming_count,billing_date,"
				+ "kwh, kwh_exp, kvarh_lag,reactive_imp_active_exp,reactive_exp_active_exp, kvarh_lead, kvah,kvah_exp,md_kw,date_md_kw, "
				+ "md_kva,date_md_kva,power_voltage,phase_current,neutral_current,total_power_on_duration,power_kw,server_time,load_switch_status,load_limit_value,v_ry_angle,v_rb_angle,v_yb_angle,phase_sequence,hes_type) values ");
		excpbuilder.setLength(0);
		excpbuilder.append(
				"insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");
	}
	
//	@Override
//	public void addData(DeviceData scMM, String hesName) {
//		try {
//			generateInstantBatchData(scMM , hesName);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} //Parse Data & Generate Batch Query	
//		hasData = true;
//	}
	
	@SuppressWarnings("null")
	@Override
	public void addData(DeviceData scMM, String hesName) {

		try {
			GenericExceptionTracker.reset();
			if (scMM.getPreVEE() == null || scMM.getPreVEE().length == 0) {
				Gson gson = new Gson();
				String smmJson = gson.toJson(scMM);
				String safeSmm = smmJson.replace("'", "''");
				String remarks = "REMARKS: preVEE payload is empty";
				hasExcpData = true;
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Instantaneous','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + remarks + "','" + hesName
						+ "'),");
				return;
			} 
				generateInstantBatchData(scMM, hesName);
				hasData = true;
			
		} catch (ParseException e) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = e.getMessage() == null ? "Parse Exception" : e.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId()  + "','Instantaneous','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "','" + hesName + "'),");
			e.printStackTrace();
		} catch (Exception ex) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = ex.getMessage() == null ? "Unknown Exception" : ex.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Instantaneous','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "','" + hesName + "'),");
			ex.printStackTrace();
		}
	}

	@Override
	public String getQuery() {
		return builder.toString().substring(0, builder.length() - 1)+" ON CONFLICT DO NOTHING";
	}

	@Override
	public String getExcepQuery() {
		return excpbuilder.toString().substring(0, excpbuilder.length() - 1)+" ON CONFLICT DO NOTHING";
	}
	
	//private RegisterValues[] rv;
	
	public void generateInstantBatchData(DeviceData scMM, String hesName) throws ParseException {		
		resetVariable();
		preVdt = scMM.getPreVEE();
		gson = new Gson();
		String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {}.getType());
		try {
			//hasNumExcpdata=false;
			GenericExceptionTracker.reset();
			for (int r = 0; r < preVdt.length; r++) {

				String obc = preVdt[r].getRegisterId();;
				String atr = preVdt[r].getAttribute();
				if (obc.equalsIgnoreCase("0.0.1.0.0.255")) {
					read_time = parseTimestamp(preVdt[r].getEnergyQuantity(), null);
				}else if (obc.equalsIgnoreCase("1.0.31.7.0.255")) {
					i_r = GenericExceptionTracker.exceptionPayload("i_r",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) {
					i_y = GenericExceptionTracker.exceptionPayload("i_y",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) {
					i_b = GenericExceptionTracker.exceptionPayload("i_b",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) {
					v_r = GenericExceptionTracker.exceptionPayload("v_r",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) {
					v_y = GenericExceptionTracker.exceptionPayload("v_y",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) {
					v_b = GenericExceptionTracker.exceptionPayload("v_b",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) {
					pf_r = GenericExceptionTracker.exceptionPayload("pf_r",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) {
					pf_y = GenericExceptionTracker.exceptionPayload("pf_y",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) {
					pf_b = GenericExceptionTracker.exceptionPayload("pf_b",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) {
					threePhasePf = GenericExceptionTracker.exceptionPayload("threePhasePf",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.14.7.0.255")) {
					frequency = GenericExceptionTracker.exceptionPayload("frequency",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.7.0.255")) {
					kva = GenericExceptionTracker.exceptionPayload("kva",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.7.0.255")) {
					signedActivePower = GenericExceptionTracker.exceptionPayload("signedActivePower",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.3.7.0.255")) {
					signedReactivePower = GenericExceptionTracker.exceptionPayload("signedReactivePower",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("0.0.96.7.0.255")) {
					//powerOffCount = Integer.parseInt(preVdt[r].getEnergyQuantity());
					powerOffCount = GenericExceptionTracker.exceptionPayloadLong("powerOffCount", obc,  preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("0.0.94.91.8.255")) {
					//powerOffDuration = Integer.parseInt(preVdt[r].getEnergyQuantity().replace("min.", "").trim());
					powerOffDuration = GenericExceptionTracker.exceptionPayloadLong("powerOffDuration", obc,  preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("0.0.94.91.0.255")) {
					//	tamperCount = Integer.parseInt(preVdt[r].getEnergyQuantity());
						tamperCount = GenericExceptionTracker.exceptionPayloadLong("tamperCount", obc,  preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("0.0.0.1.0.255")) {
					//	billingCount = Integer.parseInt(preVdt[r].getEnergyQuantity());
						billingCount = GenericExceptionTracker.exceptionPayloadInteger("billingCount", obc,  preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("0.0.96.2.0.255")) {
						//programmingCount = Integer.parseInt(preVdt[r].getEnergyQuantity());
						programmingCount = GenericExceptionTracker.exceptionPayloadInteger("programmingCount", obc,  preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("0.0.0.1.2.255")) {
					billingDate = parseTimestamp(preVdt[r].getEnergyQuantity(), null);
				} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) {
					kwh = GenericExceptionTracker.exceptionPayload("kwh",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) {
					kwh_exp = GenericExceptionTracker.exceptionPayload("kwh_exp",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.0.255")) {
					kvarh_lag = GenericExceptionTracker.exceptionPayload("kvarh_lag",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.0.255")) {
					reactiveImpActiveExp = GenericExceptionTracker.exceptionPayload("reactiveImpActiveExp",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.0.255")) {
					reactiveExpActiveExp = GenericExceptionTracker.exceptionPayload("reactiveExpActiveExp",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.0.255")) {
					kvarh_lead = GenericExceptionTracker.exceptionPayload("kvarh_lead",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.0.255")) {
					kvah = GenericExceptionTracker.exceptionPayload("kvah",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.0.255")) {
					kvah_exp = GenericExceptionTracker.exceptionPayload("kvah_exp",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.6.0.255") && atr.equalsIgnoreCase("2")) {
					mdKw = GenericExceptionTracker.exceptionPayload("mdKw",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.6.0.255") && atr.equalsIgnoreCase("5")) {
					dateMdKw = preVdt[r].getEnergyQuantity();
				} else if (obc.equalsIgnoreCase("1.0.9.6.0.255") && atr.equalsIgnoreCase("2")) {
					mdKva = GenericExceptionTracker.exceptionPayload("mdKva",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				}
				else if (obc.equalsIgnoreCase("1.0.9.6.0.255") && atr.equalsIgnoreCase("5")) {
					dateMdKva = preVdt[r].getEnergyQuantity();
				} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) {
					powerVoltage = preVdt[r].getEnergyQuantity();
				} else if (obc.equalsIgnoreCase("1.0.11.7.0.255")) {
					phaseCurrent = preVdt[r].getEnergyQuantity();
				} else if (obc.equalsIgnoreCase("1.0.91.7.0.255")) {
					neutralCurrent = preVdt[r].getEnergyQuantity();
				} 
				
//				else if (obc.equalsIgnoreCase("0.0.94.91.14.255")) {
//					//String[] s10 = preVdt[r].getEnergyQuantity().replace(" min.", "").split("\\.");
//						//totalPowerOnDuration = getInteger(s10[0]);
//						totalPowerOnDuration = GenericExceptionTracker.exceptionPayloadInteger("totalPowerOnDuration", obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm,hesName);
//				}
				
				else if (obc.equalsIgnoreCase("0.0.94.91.14.255")) {

					LocalDate cutOffDate = LocalDate.of(2026, 9, 6);
					if (LocalDate.now().isBefore(cutOffDate)) {
						String formattedValue = val;

						if (formattedValue != null && !formattedValue.trim().isEmpty()) {

							if (formattedValue.contains(".")) {
								logTotalPowerOnDurationException(scMM.getSeqId(), scMM.getDeviceId(), val,
										"Decimal Value was: " + formattedValue);
								formattedValue = formattedValue.substring(0, formattedValue.indexOf("."));
							}

							if (formattedValue != null && !formattedValue.trim().isEmpty()) {
								try {
									totalPowerOnDuration = Integer.parseInt(formattedValue.trim());
								} catch (NumberFormatException e) {
									// fallback to exception tracker if value is not a valid integer even after
									// truncation
									totalPowerOnDuration = GenericExceptionTracker.exceptionPayloadInteger(
											"totalPowerOnDuration", obc, val, scMM.getSeqId(), ResponseParser.type,
											smm.toString(), hesName);
									logTotalPowerOnDurationException(scMM.getSeqId(), scMM.getDeviceId(), val,
											"NumberFormatException: " + e.getMessage());
								}
							} else {
								totalPowerOnDuration = GenericExceptionTracker.exceptionPayloadInteger(
										"totalPowerOnDuration", obc, val, scMM.getSeqId(), ResponseParser.type,
										smm.toString(), hesName);
								logTotalPowerOnDurationException(scMM.getSeqId(), scMM.getDeviceId(), val,
										"Null or empty value for totalPowerOnDuration after truncation");
							}
						} else {
							totalPowerOnDuration = GenericExceptionTracker.exceptionPayloadInteger(
									"totalPowerOnDuration", obc, val, scMM.getSeqId(), ResponseParser.type,
									smm.toString(), hesName);
							logTotalPowerOnDurationException(scMM.getSeqId(), scMM.getDeviceId(), val,
									"Null or empty value for totalPowerOnDuration");
						}
					} else {
						totalPowerOnDuration = GenericExceptionTracker.exceptionPayloadInteger("totalPowerOnDuration",
								obc, val, scMM.getSeqId(), ResponseParser.type, smm.toString(), hesName);
						logTotalPowerOnDurationException(scMM.getSeqId(), scMM.getDeviceId(), val,
								"Post-cutover date, routed to GenericExceptionTracker");
					}

				}else if (obc.equalsIgnoreCase("0.0.96.3.10.255")) {
					load_switch_status = preVdt[r].getEnergyQuantity();
				} else if (obc.equalsIgnoreCase("0.0.17.0.0.255")) {
					load_limit_value = GenericExceptionTracker.exceptionPayload("load_limit_value",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				}
				else if (obc.equalsIgnoreCase("1.0.81.7.10.255")) {
					v_ry_angle = GenericExceptionTracker.exceptionPayload("v_ry_angle",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.81.7.20.255")) {
					v_rb_angle = GenericExceptionTracker.exceptionPayload("v_rb_angle",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.81.7.21.255")) {
					v_yb_angle = GenericExceptionTracker.exceptionPayload("v_yb_angle",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.128.7.0.255")) {
					phase_sequence = preVdt[r].getEnergyQuantity();
				}
				
			

			}
			server_time = parseTimestamp(scMM.getHesDateTime(), new Timestamp(System.currentTimeMillis()));

			
			String billingDateString = (billingDate == null) ? null : "'" + billingDate + "'";
			String deviceIdStr = (scMM.getDeviceId() == null) ? null : "'" + scMM.getDeviceId().trim().replace("'", "''") + "'";
			String readTimeStr = (read_time == null) ? "'" + server_time + "'" : "'" + read_time + "'";
			
			Timestamp parsedKwTs = parseTimestamp(dateMdKw, null);
			String dateMdKwStr = (parsedKwTs != null) ? "'" + parsedKwTs + "'" : ((dateMdKw != null && !dateMdKw.trim().isEmpty()) ? "'" + dateMdKw.trim().replace("'", "''") + "'" : null);
			
			Timestamp parsedKvaTs = parseTimestamp(dateMdKva, null);
			String dateMdKvaStr = (parsedKvaTs != null) ? "'" + parsedKvaTs + "'" : ((dateMdKva != null && !dateMdKva.trim().isEmpty()) ? "'" + dateMdKva.trim().replace("'", "''") + "'" : null);

			String powerVoltageStr = (powerVoltage == null) ? null : "'" + powerVoltage.replace("V", "").trim().replace("'", "''") + "'";
			String phaseCurrentStr = (phaseCurrent == null) ? null : "'" + phaseCurrent.replace("A", "").trim().replace("'", "''") + "'";
			String neutralCurrentStr = (neutralCurrent == null) ? null : "'" + neutralCurrent.replace("A", "").trim().replace("'", "''") + "'";
			String serverTimeStr = (server_time == null) ? "now()" : "'" + server_time + "'";
			String loadSwitchStatusStr = (load_switch_status == null) ? null : "'" + load_switch_status.trim().replace("'", "''") + "'";
			String phaseSequenceStr = (phase_sequence == null) ? null : "'" + phase_sequence.trim().replace("'", "''") + "'";

			if(GenericExceptionTracker.hasExceptions())  {
				hasExcpData=true;
				List<String> exceptions = GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
				String combinedExceptions = String.join(",", exceptions);
		        excpbuilder.append("(now(),'" +scMM.getSeqId()  + "','Instantaneous','" + (scMM.getDeviceId()  != null ? scMM.getDeviceId()  : "") + "','" + smm.toString() + "','"+ combinedExceptions + "','"+ hesName + "'),");
		    }else {
		    	builder.append("( now(),'"+ scMM.getSeqId() +"', " + deviceIdStr + "," + readTimeStr + ",  " + i_r + ", " + i_y +", "+ i_b +", " + v_r + ", " + v_y + ", " + v_b + ", " + pf_r + ", " + pf_y + ", " + pf_b + ", " + threePhasePf + ", " + frequency + ", "
						+ kva + ", " + signedActivePower + ", " + signedReactivePower + ", " + powerOffCount + "," + powerOffDuration + "," + tamperCount + "," + billingCount + "," + programmingCount + "," + billingDateString + ","
						+ kwh + ", " + kwh_exp + ", " + kvarh_lag + ", " + reactiveImpActiveExp + ", " + reactiveExpActiveExp + ", " + kvarh_lead + ", " + kvah + ", " + kvah_exp + ", " + mdKw + "," + dateMdKwStr + ", "
						+ mdKva + ", " + dateMdKvaStr + "," + powerVoltageStr + "," + phaseCurrentStr + "," + neutralCurrentStr + "," + totalPowerOnDuration + ", " + signedActivePower + "," + serverTimeStr + "," + loadSwitchStatusStr + "," + load_limit_value + "," + v_ry_angle + "," + v_rb_angle + "," + v_yb_angle + "," + phaseSequenceStr + ",'"+hesName+"'),");	
			
		    }
		}  catch (Exception e) {
		    hasExcpData = true;
		    String safeSmm = smm.replace("'", "''"); // escape single quotes
		    excpbuilder.append("(now(),'" + scMM.getSeqId() + "','"+ResponseParser.type+"','" + (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm +"','REMARKS:" + e.getMessage() + "','"+ hesName + "'),");
		    e.printStackTrace();
		}
		resetVariable();
	}

//	public void generateCrystalInstantBatchData(SchCrystalMeterDataMain scMM,String apiName, String flag) throws ParseException {		
//		
//		rv = scMM.getRegisterValues();
//		gson = new Gson();
//		String smm = gson.toJson(scMM, new TypeToken<SchCrystalMeterDataMain>() {}.getType());
//		try {
//			for (int r = 0; r < rv.length; r++) {
//
//				String obc = rv[r].getFormattedRegisterObisCode();
//				String atr = rv[r].getAttributeId();
//				if (obc.equalsIgnoreCase("0.0.1.0.0.255")) {
//					String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//					//System.out.println("Before Conversion "+new Timestamp(format.parse(s1[0]).getTime()));
//					read_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//					//System.out.println("After Conversion "+read_time);
//				}
//				if (obc.equalsIgnoreCase("1.0.31.7.0.255")) {
//					i_r = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//				} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) {
//					i_y = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//				} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) {
//					i_b = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//				} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) {
//					v_r = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//				} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) {
//					v_y = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//				} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) {
//					v_b = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//				} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) {
//					pf_r = Double.parseDouble(rv[r].getFormattedValue());
//				} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) {
//					pf_y = Double.parseDouble(rv[r].getFormattedValue());
//				} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) {
//					pf_b = Double.parseDouble(rv[r].getFormattedValue());
//				} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) {
//					threePhasePf = Double.parseDouble(rv[r].getFormattedValue());
//				} else if (obc.equalsIgnoreCase("1.0.14.7.0.255")) {
//					frequency = Double.parseDouble(rv[r].getFormattedValue().replace("Hz", ""));
//				} else if (obc.equalsIgnoreCase("1.0.9.7.0.255")) {
////					if (rv[r].getFormattedValue().replace("VA", "").contains("DLMS_TYPE_UNKNOWN")) {
////						kva = null;
////					} else {
//						kva = Double.parseDouble(rv[r].getFormattedValue().replace("VA", ""));
////					}
//				} else if (obc.equalsIgnoreCase("1.0.1.7.0.255")) {
//					signedActivePower = Double.parseDouble(rv[r].getFormattedValue().replace("W", ""));
//				} else if (obc.equalsIgnoreCase("1.0.3.7.0.255")) {
//					signedReactivePower = Double.parseDouble(rv[r].getFormattedValue().replace("var", ""));
//				} else if (obc.equalsIgnoreCase("0.0.96.7.0.255")) {
//					powerOffCount = Integer.parseInt(rv[r].getFormattedValue());
//				} else if (obc.equalsIgnoreCase("0.0.94.91.8.255")) {
//					powerOffDuration = Integer.parseInt(rv[r].getFormattedValue().replace("min.", "").trim());
//				} else if (obc.equalsIgnoreCase("0.0.94.91.0.255")) {
////					if (rv[r].getFormattedValue().contains("DLMS_TYPE_UNKNOWN")
////							|| rv[r].getFormattedValue().contains("+05:30")) {
////						tamperCount = 0;
////					} else {
//						tamperCount = Integer.parseInt(rv[r].getFormattedValue());
////					}
//
//				} else if (obc.equalsIgnoreCase("0.0.0.1.0.255")) {
////					if (rv[r].getFormattedValue().contains("DLMS_TYPE_UNKNOWN")) {
////						billingCount = 0;
////					} else {
//						billingCount = Integer.parseInt(rv[r].getFormattedValue());
////					}
////				billingCount=Integer.parseInt(rv[r].getFormattedValue());
//				} else if (obc.equalsIgnoreCase("0.0.96.2.0.255")) {
////					if (rv[r].getFormattedValue().contains("DLMS_TYPE_UNKNOWN")
////							|| rv[r].getFormattedValue().contains("+05:30")) {
////						programmingCount = 0;
////					} else {
//						programmingCount = Integer.parseInt(rv[r].getFormattedValue());
////					}
//					// programmingCount=Integer.parseInt(rv[r].getFormattedValue());
//				} else if (obc.equalsIgnoreCase("0.0.0.1.2.255")) {
//					if (rv[r].getFormattedValue() != null) {
//						String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//						if (s1[0].equalsIgnoreCase("")) {
//
//						} else {
//							//billingDate = new Timestamp(format.parse(s1[0]).getTime());
//							billingDate = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//					}
//					
//				} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) {
////				kwh=Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
////					if (rv[r].getFormattedValue().replace("Wh", "").contains("DLMS_TYPE_UNKNOWN")) {
////						kwh = null;
////					} else {
//						kwh = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
////					}
//
//				} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) {
////					if (rv[r].getFormattedValue().replace("Wh", "").contains("DLMS_TYPE_UNKNOWN")) {
////						kwh_exp = null;
//						// }else if(rv[r].getFormattedValue().replace("Wh", "").contains("true")) {
//						// kwh_exp=null;
////					} else {
//						kwh_exp = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
////					}
//					// kwh_exp=Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//				} else if (obc.equalsIgnoreCase("1.0.5.8.0.255")) {
//					kvarh_lag = Double.parseDouble(rv[r].getFormattedValue().replace("varh", ""));
//				} else if (obc.equalsIgnoreCase("1.0.6.8.0.255")) {
//					reactiveImpActiveExp = Double.parseDouble(rv[r].getFormattedValue().replace("varh", ""));
//				} else if (obc.equalsIgnoreCase("1.0.7.8.0.255")) {
//					reactiveExpActiveExp = Double.parseDouble(rv[r].getFormattedValue().replace("varh", ""));
//				} else if (obc.equalsIgnoreCase("1.0.8.8.0.255")) {
//					kvarh_lead = Double.parseDouble(rv[r].getFormattedValue().replace("varh", ""));
//				} else if (obc.equalsIgnoreCase("1.0.9.8.0.255")) {
////				kvah=Double.parseDouble(rv[r].getFormattedValue().replace("VAh", ""));
////					if (rv[r].getFormattedValue().replace("VAh", "").contains("DLMS_TYPE_UNKNOWN")) {
////						kvah = null;
////					} else {
//						kvah = Double.parseDouble(rv[r].getFormattedValue().replace("VAh", ""));
////					}
//
//				} else if (obc.equalsIgnoreCase("1.0.10.8.0.255")) {
////					if (rv[r].getFormattedValue().replace("VAh", "").contains("DLMS_TYPE_UNKNOWN")) {
////						kvah_exp = null;
////					} else {
//						kvah_exp = Double.parseDouble(rv[r].getFormattedValue().replace("VAh", ""));
////					}
////				kvah_exp=Double.parseDouble(rv[r].getFormattedValue().replace("VAh", ""));
//				} else if (obc.equalsIgnoreCase("1.0.1.6.0.255") && atr.equalsIgnoreCase("2")) {
//					if (rv[r].getFormattedValue() != null) {
//						mdKw = Double.parseDouble(rv[r].getFormattedValue().replace("W", ""));
//					}
//					
//				} else if (obc.equalsIgnoreCase("1.0.1.6.0.255") && atr.equalsIgnoreCase("5")) {
//					if (rv[r].getFormattedValue() != null) {
//						dateMdKw = (rv[r].getFormattedValue()==null)? null : "'" + zonedDateTimeConverter(rv[r].getFormattedValue()) + "'";
//					}
//					
//				} else if (obc.equalsIgnoreCase("1.0.9.6.0.255") && atr.equalsIgnoreCase("2")) {
////					if (rv[r].getFormattedValue().replace("VA", "").contains("DLMS_TYPE_UNKNOWN")) {
////						mdKva = null;
////					} else {
////						mdKva = Double.parseDouble(rv[r].getFormattedValue().replace("VA", ""));
////					}
//					if (rv[r].getFormattedValue() != null) {
//						mdKva = Double.parseDouble(rv[r].getFormattedValue().replace("VA", ""));
//					}
//						
//				}
//
//				else if (obc.equalsIgnoreCase("1.0.9.6.0.255") && atr.equalsIgnoreCase("5")) {
//					if (rv[r].getFormattedValue() != null) {
//						dateMdKva = (rv[r].getFormattedValue()==null)? null : "'" + zonedDateTimeConverter(rv[r].getFormattedValue()) + "'";
//						
//					}
//					
//				} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) {
//					powerVoltage = rv[r].getFormattedValue().replace("V", "");
//				} else if (obc.equalsIgnoreCase("1.0.11.7.0.255")) {
//					phaseCurrent = rv[r].getFormattedValue().replace("A", "");
//				} else if (obc.equalsIgnoreCase("1.0.91.7.0.255")) {
//					neutralCurrent = rv[r].getFormattedValue().replace("A", "");
//				} else if (obc.equalsIgnoreCase("0.0.94.91.14.255")) {
//					String[] s10 = rv[r].getFormattedValue().replace(" min.", "").split("\\.");
////					if (s10[0].contains("DLMS_TYPE_UNKNOWN")) {
////						totalPowerOnDuration = null;
////					} else {
//						// System.out.println("Parsing_ -- "+Integer.valueOf(s10[0]));
//						totalPowerOnDuration = getInteger(s10[0]);
//
////					}
//				}else if (obc.equalsIgnoreCase("0.0.96.3.10.255")) {
//					load_switch_status = (rv[r].getFormattedValue()==null)? null : "'" + rv[r].getFormattedValue() + "'" ;
//				} else if (obc.equalsIgnoreCase("0.0.17.0.0.255")) {
//					load_limit_value = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//				} 
//				
//				else if (obc.equalsIgnoreCase("1.0.81.7.10.255")) {
//					v_ry_angle = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//				} else if (obc.equalsIgnoreCase("1.0.81.7.20.255")) {
//					v_rb_angle = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//				} else if (obc.equalsIgnoreCase("1.0.81.7.21.255")) {
//					v_yb_angle = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//				} else if (obc.equalsIgnoreCase("1.0.128.7.0.255")) {
//					phase_sequence = (rv[r].getFormattedValue()==null)? null : "'" + rv[r].getFormattedValue() + "'" ;
//				}
//
//			}
//			try {
//				server_time = new Timestamp(format.parse(zonedDateTimeConverter(scMM.getServerTime())).getTime());
//			} catch (Exception e) {
//				e.printStackTrace();
//
//			}
//
//			String billingDateString = (billingDate==null)? null : "'" + billingDate + "'";
//			
//			
//			if(flag.equalsIgnoreCase("SCH")) {
// 			//FOR Schedule data parsing
//				builder.append("( now(),'"+ scMM.getMeterSampleId() +"', '" +  scMM.getDeviceId() + "','" + read_time + "',  " + i_r + ", " + i_y +", "+ i_b +", " + v_r + ", " + v_y + ", " + v_b + ", " + pf_r + ", " + pf_y + ", " + pf_b + ", " + threePhasePf + ", " + frequency + ", "
//						+ kva + ", " + signedActivePower + ", " + signedReactivePower + ", " + powerOffCount + "," + powerOffDuration + "," + tamperCount + "," + billingCount + "," + programmingCount + "," + billingDateString + ","
//						+ kwh + ", " + kwh_exp + ", " + kvarh_lag + ", " + reactiveImpActiveExp + ", " + reactiveExpActiveExp + ", " + kvarh_lead + ", " + kvah + ", " + kvah_exp + ", " + mdKw + "," + dateMdKw + ", "
//						+ mdKva + ", " + dateMdKva + "," + powerVoltage + "," + phaseCurrent + "," + neutralCurrent + "," + totalPowerOnDuration + ", " + signedActivePower + ",'" + server_time + "'," + load_switch_status + "," + load_limit_value + "," + v_ry_angle + "," + v_rb_angle + "," + v_yb_angle + "," + phase_sequence + "),");	
//			}else if(flag.equalsIgnoreCase("OD")) {
////			// FOR OD data parsing
//			builder.append("( now(),'OD', '" +  scMM.getDeviceId() + "','" + read_time + "',  " + i_r + ", " + i_y +", "+ i_b +", " + v_r + ", " + v_y + ", " + v_b + ", " + pf_r + ", " + pf_y + ", " + pf_b + ", " + threePhasePf + ", " + frequency + ", "
//					+ kva + ", " + signedActivePower + ", " + signedReactivePower + ", " + powerOffCount + "," + powerOffDuration + "," + tamperCount + "," + billingCount + "," + programmingCount + "," + billingDateString + ","
//					+ kwh + ", " + kwh_exp + ", " + kvarh_lag + ", " + reactiveImpActiveExp + ", " + reactiveExpActiveExp + ", " + kvarh_lead + ", " + kvah + ", " + kvah_exp + ", " + mdKw + "," + dateMdKw + ", "
//					+ mdKva + ", " + dateMdKva + "," + powerVoltage + "," + phaseCurrent + "," + neutralCurrent + "," + totalPowerOnDuration + ", " + signedActivePower + ",'" + server_time + "'," + load_switch_status + "," + load_limit_value + "," + v_ry_angle + "," + v_rb_angle + "," + v_yb_angle + "," + phase_sequence + "),");	
//
//			}
//		} catch (Exception e) {
//			hasExcpData=true;
//			excpbuilder.append("(now(),'" + scMM.getMeterSampleId() + "','" + apiName  + "','" + smm.toString() + "'),");
//			 e.printStackTrace();
//		}
//		
//		rv=null;
//		resetVariable();
//	}
	
	
	
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
		builder.setLength(0);
		hasData=false;
		hasExcpData=false;
	}
	
	public void resetVariable() {
		
		i_r = null; i_y = null; i_b = null; v_r = null; v_y = null; v_b = null; pf_r = null; pf_y = null; pf_b = null; threePhasePf=null;frequency=null;
		kwh = null; kwh_exp = null; kvah = null; kvah_exp = null; kvarh_lag = null; kvarh_lead = null; kva = null;
		signedActivePower=null;signedReactivePower=null; mdKw = null; mdKva = null;
		reactiveImpActiveExp=null ;reactiveExpActiveExp = null;
		powerOffCount=null; powerOffDuration=null; tamperCount=null;billingCount=null;programmingCount=null;
		read_time=null; billingDate=null;
		dateMdKw=null; dateMdKva=null;powerVoltage=null;phaseCurrent=null;neutralCurrent=null;
		totalPowerOnDuration=null;server_time=null;load_switch_status=null;load_limit_value=null;v_ry_angle=null;v_rb_angle=null;v_yb_angle=null;phase_sequence=null;
	}

	public String bParseDate(String obisDate) {

//		String[] strr = obisDate.replace("+", ",").split(",");

		SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MMM/yyyy hh:mm:ss a");
		Date date = null;
		String validDate = null;
		try {
			date = inputFormat.parse(obisDate);
			validDate = outputFormat.format(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} // hardcode here and check
		

		return validDate;

	}
	
	Integer getInteger(String value) {// GET DOUBLE VALUE WITH MAX 3
		// DECIMALS
		try {
			if (value == null || value.trim().isEmpty()) {
				return null;
			}
			return Integer.parseInt(value.trim().split("\\s+")[0]);
		} catch (Exception e) {
			return null;
		}
	}
	
	
	public String zonedDateTimeConverter(String obisDate) {

        // Parse the string timestamp into a LocalDateTime object
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(obisDate, formatter);

        // Set the timezone of the LocalDateTime object to local time zone
        ZoneId localTimeZone = ZoneId.systemDefault();
        ZonedDateTime localZonedDateTime = localDateTime.atZone(localTimeZone);

        // Add time difference of 5 hours and 30 minutes
        ZonedDateTime gmt5_30ZonedDateTime = localZonedDateTime.plusHours(5).plusMinutes(30);

        // Convert to string timestamp in GMT+5:30 time zone
        String timestampGMT5_30 = gmt5_30ZonedDateTime.format(formatter);
		return timestampGMT5_30;


	}
}
