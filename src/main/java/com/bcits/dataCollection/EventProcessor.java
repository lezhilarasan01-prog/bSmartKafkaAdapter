/**
 * 
 */
package com.bcits.dataCollection;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bcits.dataModel.DeviceData;
import com.bcits.dataModel.DeviceData.PreVEEData;
import com.bcits.dataModel.RegisterValue;
import com.bcits.dataModel.RegisterValues;
import com.bcits.responseDataModel.ResponseParser;
import com.bcits.logger.BsmartLogWriter;
import com.bcits.logger.ModuleName;
import com.bcits.utils.GenericExceptionTracker;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author TARIK
 *
 */
public class EventProcessor implements DataProcessor {

	private static EventProcessor processor;
	private boolean hasData;
	private boolean hasExcpData;
	private boolean hasNumExcpdata=false;
	private StringBuilder builder = new StringBuilder();
	private StringBuilder excpbuilder = new StringBuilder();
	private Gson gson;
	private final Map<Integer, List<String>> exceptionMap = new HashMap<>();
	private Timestamp event_time = null, create_time = null/* , sample_time=null, */;
	private String eventCode = null,cumulativeTamperCount = null;
	private BigDecimal i_r = null, i_y = null, i_b = null, v_r = null, v_y = null, v_b = null, pf_r = null, pf_y = null,
			pf_b = null;
	private BigDecimal kwh = null, kwhExp = null;

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

	private RegisterValues[] rv;
	private String[] st, ct;
	private char[] alarms;
	private String obc, atr;
	private String val;
	private List<RegisterValue> dt;
	private String dType;
	private String type;
	private PreVEEData[] preVdt;

	private ArrayList<String> alarmList = new ArrayList<String>();
	
	//Added by Ain on 20/10/2023
//		public static final String schedulerUrl = ResourceBundle.getBundle("hes").getString("scheduler.url");


	public static EventProcessor getInstance() {
		if (processor == null)
			processor = new EventProcessor();
		return processor;
	}

	@Override
	public void init() {
		builder.setLength(0);
		builder.append("insert into meter_data.events  "
				+ "(time_stamp, flag, event_code, meter_number, event_time, i_r, i_y, i_b, v_r, v_y, v_b, pf_r, pf_y, pf_b,"
				+ "kwh, kwh_exp,server_time,cumulative_tamper_count,hes_type) values ");

//		builder.append("insert into meter_data.meter_alarm "
//				+ "(time_stamp, meter_number, meter_alarm_id, sequence_number, alarm_time, alarm_active, server_time) values ");

		excpbuilder.setLength(0);
		excpbuilder.append("insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");

	}

//	@Override
//	public void addData(DeviceData scMM, String hesName) {
//		try {
//			generateEventBatchData(scMM , hesName);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} // Parse Data & Generate Batch Query
//		hasData = true;
//	}

	@Override
	public void addData(DeviceData scMM, String hesName) {

		try {
			GenericExceptionTracker.reset();
			if (scMM.getPreVEE() == null || scMM.getPreVEE().length == 0) {
				hasExcpData = true;
				Gson gson = new Gson();
				String smmJson = gson.toJson(scMM);
				String safeSmm = smmJson.replace("'", "''");
				String remarks = "REMARKS: preVEE payload is empty";
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Events','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + remarks
						+ "','" + hesName + "'),");
				return;
			}
			generateEventBatchData(scMM, hesName);
			hasData = true;

		} catch (ParseException e) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = e.getMessage() == null ? "Parse Exception" : e.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Events','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg
					+ "',hes),");
			e.printStackTrace();
		} catch (Exception ex) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = ex.getMessage() == null ? "Unknown Exception" : ex.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Events','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg
					+ "',hes),");
			ex.printStackTrace();
		}
	}
	

	@Override
	public String getQuery() {
		return builder.toString().substring(0, builder.length() - 1) + " ON CONFLICT DO NOTHING";
	}

	@Override
	public String getExcepQuery() {
		return excpbuilder.toString().substring(0, excpbuilder.length() - 1) + " ON CONFLICT DO NOTHING";
	}

	private void generateEventBatchData(DeviceData scMM, String hesName) throws ParseException {

		preVdt = scMM.getPreVEE();
		/*
		 * st = scMM.getSampleTime().replace("T", " ").split("\\+"); sample_time=(new
		 * Timestamp(format.parse(st[0]).getTime()));
		 */
		create_time = parseTimestamp(scMM.getHesDateTime(), new Timestamp(System.currentTimeMillis()));
		gson = new Gson();
		String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {
		}.getType());
		try {
			// hasNumExcpdata=false;
			GenericExceptionTracker.reset();
			if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.0.255")) {// voltage event log

				try {
					for (int r = 0; r < preVdt.length; r++) {
						obc = preVdt[r].getRegisterId();
						atr = preVdt[r].getAttribute();

						if (obc.equalsIgnoreCase("0.0.96.11.0.255")) { // eventcode
							// eventCode = preVdt[r].getEnergyQuantity();
//							eventCode = (preVdt[r].getEnergyQuantity() == null) ? null
//									: "" + preVdt[r].getEnergyQuantity() + "";
							eventCode = validateAndGetEventCode(preVdt[r].getEnergyQuantity(), scMM.getSeqId());
						}
						if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
							event_time = parseEventTime(preVdt[r].getEnergyQuantity());
						} else if (obc.equalsIgnoreCase("1.0.31.7.0.255")) { // i_r
							// i_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_r = GenericExceptionTracker.exceptionPayload("i_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) { // i_y
							// i_y = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_y = GenericExceptionTracker.exceptionPayload("i_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) { // i_b
							// i_b = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_b = GenericExceptionTracker.exceptionPayload("i_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) { // v_r
							// v_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_r = GenericExceptionTracker.exceptionPayload("v_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) { // v_y
							// v_y = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_y = GenericExceptionTracker.exceptionPayload("v_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) { // v_b
							// v_b = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_b = GenericExceptionTracker.exceptionPayload("v_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) { // pf_r
							// pf_r = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_r = GenericExceptionTracker.exceptionPayload("pf_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) { // pf_y
							// pf_y = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_y = GenericExceptionTracker.exceptionPayload("pf_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) { // pf_b
							// pf_b = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_b = GenericExceptionTracker.exceptionPayload("pf_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
							// kwh = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("Wh", ""));
							kwh = GenericExceptionTracker.exceptionPayload("kwh", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) { // kwh_exp
							// kwhExp = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("Wh", ""));
							kwhExp = GenericExceptionTracker.exceptionPayload("kwhExp", obc,
									preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) { // v_r
							// v_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_r = GenericExceptionTracker.exceptionPayload("v_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.94.91.14.255")) { // i_r
							// i_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_r = GenericExceptionTracker.exceptionPayload("i_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) { // pf_r
							// pf_r = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_r = GenericExceptionTracker.exceptionPayload("pf_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						}

//						else if (obc.equalsIgnoreCase("0.0.94.91.0.255")) { // cumulativeTamperCount
//							//cumulativeTamperCount = rv[r].getFormattedValue();
//							cumulativeTamperCount = (rv[r].getFormattedValue()==null)? null : "'" + rv[r].getFormattedValue() + "'";
//						}

						else if ("0.0.94.91.0.255".equalsIgnoreCase(obc)) {
							String value = preVdt[r] != null ? preVdt[r].getEnergyQuantity() : null;
							cumulativeTamperCount = (value != null) ? "'" + value + "'" : null;
						}

//						else if (obc.equalsIgnoreCase("0.0.94.91.0.255")) { // cumulativeTamperCount
//							//cumulativeTamperCount = rv[r].getFormattedValue();
//							cumulativeTamperCount = (rv[r].getFormattedValue()==null)? null : "'" + rv[r].getFormattedValue() + "'";
//						}

						else if ("0.0.94.91.0.255".equalsIgnoreCase(obc)) {
							String value = preVdt[r] != null ? preVdt[r].getEnergyQuantity() : null;
							cumulativeTamperCount = (value != null) ? "'" + value + "'" : null;
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.1.255")) {// current event log

				try {
					for (int r = 0; r < preVdt.length; r++) {
						obc = preVdt[r].getRegisterId();
						atr = preVdt[r].getAttribute();

						if (obc.equalsIgnoreCase("0.0.96.11.1.255")) { // eventCode
							// eventCode = preVdt[r].getEnergyQuantity();
//							eventCode = (preVdt[r].getEnergyQuantity() == null) ? null
//									: "" + preVdt[r].getEnergyQuantity() + "";
							eventCode = validateAndGetEventCode(preVdt[r].getEnergyQuantity(), scMM.getSeqId());

						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
							event_time = parseEventTime(preVdt[r].getEnergyQuantity());
						} else if (obc.equalsIgnoreCase("1.0.31.7.0.255")) { // i_r
							// i_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_r = GenericExceptionTracker.exceptionPayload("i_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) { // i_y
							// i_y = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_y = GenericExceptionTracker.exceptionPayload("i_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) { // i_b
							// i_b = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_b = GenericExceptionTracker.exceptionPayload("i_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) { // v_r
							// v_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_r = GenericExceptionTracker.exceptionPayload("v_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) { // v_y
							// v_y = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_y = GenericExceptionTracker.exceptionPayload("v_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) { // v_b
							// v_b = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_b = GenericExceptionTracker.exceptionPayload("v_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) { // pf_r
							// pf_r = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_r = GenericExceptionTracker.exceptionPayload("pf_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) { // pf_y
							// pf_y = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_y = GenericExceptionTracker.exceptionPayload("pf_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) { // pf_b
							// pf_b = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_b = GenericExceptionTracker.exceptionPayload("pf_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
							// kwh = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("Wh", ""));
							kwh = GenericExceptionTracker.exceptionPayload("kwh", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) { // kwh_exp
							// kwhExp = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("Wh", ""));
							kwhExp = GenericExceptionTracker.exceptionPayload("kwhExp", obc,
									preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) { // v_r
//							v_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_r = GenericExceptionTracker.exceptionPayload("v_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.94.91.14.255")) { // i_r
							// i_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							kwh = GenericExceptionTracker.exceptionPayload("kwh", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) { // pf_r
							// pf_r = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_r = GenericExceptionTracker.exceptionPayload("pf_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						}

//						else if (obc.equalsIgnoreCase("0.0.94.91.0.255")) { // cumulativeTamperCount
//							//cumulativeTamperCount = rv[r].getFormattedValue();
//							cumulativeTamperCount = (rv[r].getFormattedValue()==null)? null : "'" + rv[r].getFormattedValue() + "'";
//						}

						else if ("0.0.94.91.0.255".equalsIgnoreCase(obc)) {
							String value = preVdt[r] != null ? preVdt[r].getEnergyQuantity() : null;
							cumulativeTamperCount = (value != null) ? "'" + value + "'" : null;
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.2.255")) {// power event log

				try {
					for (int r = 0; r < preVdt.length; r++) {
						obc = preVdt[r].getRegisterId();
						atr = preVdt[r].getAttribute();

						if (obc.equalsIgnoreCase("0.0.96.11.2.255")) { // eventCode
							// eventCode = preVdt[r].getEnergyQuantity();
//							eventCode = (preVdt[r].getEnergyQuantity() == null) ? null
//									: "" + preVdt[r].getEnergyQuantity() + "";
							eventCode = validateAndGetEventCode(preVdt[r].getEnergyQuantity(), scMM.getSeqId());
						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
							event_time = parseEventTime(preVdt[r].getEnergyQuantity());
						}

					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.3.255")) {// Transaction Event Log

				try {
					for (int r = 0; r < preVdt.length; r++) {

						obc = preVdt[r].getRegisterId();
						atr = preVdt[r].getAttribute();

						if (obc.equalsIgnoreCase("0.0.96.11.3.255")) { // eventCode
//							eventCode = preVdt[r].getEnergyQuantity();
//							eventCode = (preVdt[r].getEnergyQuantity() == null) ? null
//									: "" + preVdt[r].getEnergyQuantity() + "";
							eventCode = validateAndGetEventCode(preVdt[r].getEnergyQuantity(), scMM.getSeqId());
						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
							event_time = parseEventTime(preVdt[r].getEnergyQuantity());
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.4.255")) {// other event log

				try {
					for (int r = 0; r < preVdt.length; r++) {
						obc = preVdt[r].getRegisterId();
						atr = preVdt[r].getAttribute();

						if (obc.equalsIgnoreCase("0.0.96.11.4.255")) { // eventCode
							// eventCode = preVdt[r].getEnergyQuantity();
//							eventCode = (preVdt[r].getEnergyQuantity() == null) ? null
//									: "" + preVdt[r].getEnergyQuantity() + "";
							eventCode = validateAndGetEventCode(preVdt[r].getEnergyQuantity(), scMM.getSeqId());
						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
							event_time = parseEventTime(preVdt[r].getEnergyQuantity());
						} else if (obc.equalsIgnoreCase("1.0.31.7.0.255")) { // i_r
							// i_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_r = GenericExceptionTracker.exceptionPayload("i_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) { // i_y
							// i_y = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_y = GenericExceptionTracker.exceptionPayload("i_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) { // i_b
							// i_b = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_b = GenericExceptionTracker.exceptionPayload("i_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) { // v_r
							// v_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_r = GenericExceptionTracker.exceptionPayload("v_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) { // v_y
							// v_y = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_y = GenericExceptionTracker.exceptionPayload("v_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) { // v_b
							// v_b = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_b = GenericExceptionTracker.exceptionPayload("v_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) { // pf_r
							// pf_r = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_r = GenericExceptionTracker.exceptionPayload("pf_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) { // pf_y
							// pf_y = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_y = GenericExceptionTracker.exceptionPayload("pf_y", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) { // pf_b
							// pf_b = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_b = GenericExceptionTracker.exceptionPayload("pf_b", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
							// kwh = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("Wh", ""));
							kwh = GenericExceptionTracker.exceptionPayload("kwh", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) { // kwh_exp
							// kwhExp = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("Wh", ""));
							kwhExp = GenericExceptionTracker.exceptionPayload("kwhExp", obc,
									preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) { // v_r
							// v_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("V", ""));
							v_r = GenericExceptionTracker.exceptionPayload("v_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.94.91.14.255")) { // i_r
							// i_r = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("A", ""));
							i_r = GenericExceptionTracker.exceptionPayload("i_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) { // pf_r
							// pf_r = Double.parseDouble(preVdt[r].getEnergyQuantity());
							pf_r = GenericExceptionTracker.exceptionPayload("pf_r", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
							// kwh = Double.parseDouble(preVdt[r].getEnergyQuantity().replace("Wh", ""));
							kwh = GenericExceptionTracker.exceptionPayload("kwh", obc, preVdt[r].getEnergyQuantity(),
									scMM.getSeqId(), ResponseParser.type, smm, hesName);
						}

//						else if (obc.equalsIgnoreCase("0.0.94.91.0.255")) { // cumulativeTamperCount
//							//cumulativeTamperCount = rv[r].getFormattedValue();
//							cumulativeTamperCount = (rv[r].getFormattedValue()==null)? null : "'" + rv[r].getFormattedValue() + "'";
//						}

						else if ("0.0.94.91.0.255".equalsIgnoreCase(obc)) {
							String value = preVdt[r] != null ? preVdt[r].getEnergyQuantity() : null;
							cumulativeTamperCount = (value != null) ? "'" + value + "'" : null;
						}

					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.5.255")) { // Non Roll Over Event
																					// Log

				try {
					for (int r = 0; r < preVdt.length; r++) {

						obc = preVdt[r].getRegisterId();
						atr = preVdt[r].getAttribute();

						if (obc.equalsIgnoreCase("0.0.96.11.5.255")) { // eventCode
//							eventCode = preVdt[r].getEnergyQuantity();
//							eventCode = (preVdt[r].getEnergyQuantity() == null) ? null
//									: "" + preVdt[r].getEnergyQuantity() + "";

							eventCode = validateAndGetEventCode(preVdt[r].getEnergyQuantity(), scMM.getSeqId());
						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
							event_time = parseEventTime(preVdt[r].getEnergyQuantity());
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.6.255")) {// controled event log

				try {
					for (int r = 0; r < preVdt.length; r++) {

						obc = preVdt[r].getRegisterId();
						atr = preVdt[r].getAttribute();

						if (obc.equalsIgnoreCase("0.0.96.11.6.255")) { // eventCode
							// eventCode = preVdt[r].getEnergyQuantity();
//							eventCode = (preVdt[r].getEnergyQuantity() == null) ? null
//									: "" + preVdt[r].getEnergyQuantity() + "";
							eventCode = validateAndGetEventCode(preVdt[r].getEnergyQuantity(), scMM.getSeqId());
						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
							event_time = parseEventTime(preVdt[r].getEnergyQuantity());
						}

					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			if (GenericExceptionTracker.hasExceptions()) {
				hasExcpData = true;
				List<String> exceptions = GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
				String combinedExceptions = String.join(",", exceptions);
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + ResponseParser.type + "','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + smm.toString() + "','"
						+ combinedExceptions + "','" + hesName + "'),");

				GenericExceptionTracker.sendExpToDlq(smm.replace("'", "''").toString());
			}
			if (GenericExceptionTracker.hasExceptions()) {
				hasExcpData = true;
				List<String> exceptions = GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
				String combinedExceptions = String.join(",", exceptions);
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + ResponseParser.type + "','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + smm.toString() + "','"
						+ combinedExceptions + "','" + hesName + "'),");

			} else {

				builder.append("(now(),'" + scMM.getSeqId() + "', '" + eventCode + "', '" + scMM.getDeviceId() + "','"
						+ event_time + "'," + i_r + ", " + i_y + ", " + i_b + ", " + v_r + ", " + v_y + ", " + v_b
						+ ", " + pf_r + ", " + pf_y + ", " + pf_b + "," + kwh + ", " + kwhExp + ",'" + create_time
						+ "'," + cumulativeTamperCount + ",'" + hesName + "'),");
			}

		} catch (Exception e) {
			hasExcpData = true;
			String safeSmm = smm.replace("'", "''"); // escape single quotes
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + ResponseParser.type + "','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','REMARKS:"
					+ e.getMessage() + "','" + hesName + "'),");
			e.printStackTrace();
		}

		rv = null;
		resetVariable();
	}

//	private void generateCrystalEventBatchData(SchCrystalMeterDataMain scMM, String apiName,String flag) throws ParseException {
//		ArrayList<String> alrmDesc = null;
//		rv = scMM.getRegisterValues();
//		/*
//		 * st = scMM.getSampleTime().replace("T", " ").split("\\+"); sample_time=(new
//		 * Timestamp(format.parse(st[0]).getTime()));
//		 */
//		try {
//			create_time = new Timestamp(format.parse(zonedDateTimeConverter(scMM.getServerTime())).getTime());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		gson = new Gson();
//		String smm = gson.toJson(scMM, new TypeToken<SchCrystalMeterDataMain>() {
//		}.getType());
//		try {
//			if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.99.98.0.255")) {// voltage event log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//						obc = rv[r].getFormattedRegisterObisCode();
//						atr = rv[r].getAttributeId();
//
//						if (obc.equalsIgnoreCase("0.0.96.11.0.255")) { // eventcode
//							eventCode = rv[r].getFormattedValue();
//						}
//						if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//						if (obc.equalsIgnoreCase("1.0.31.7.0.255")) { // i_r
//							i_r = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) { // i_y
//							i_y = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) { // i_b
//							i_b = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) { // v_r
//							v_r = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) { // v_y
//							v_y = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) { // v_b
//							v_b = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) { // pf_r
//							pf_r = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) { // pf_y
//							pf_y = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) { // pf_b
//							pf_b = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
//							kwh = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//						} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) { // kwh_exp
//							kwhExp = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//						} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) { // v_r
//							v_r = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.94.91.14.255")) { // i_r
//							i_r = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) { // pf_r
//							pf_r = Double.parseDouble(rv[r].getFormattedValue());
//						}
//					}
//
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			} else if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.99.98.1.255")) {// current event log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//						obc = rv[r].getFormattedRegisterObisCode();
//						String atr = rv[r].getAttributeId();
//
//						if (obc.equalsIgnoreCase("0.0.96.11.1.255")) { // eventCode
//							eventCode = rv[r].getFormattedValue();
//						}
//						if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//						if (obc.equalsIgnoreCase("1.0.31.7.0.255")) { // i_r
//							i_r = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) { // i_y
//							i_y = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) { // i_b
//							i_b = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) { // v_r
//							v_r = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) { // v_y
//							v_y = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) { // v_b
//							v_b = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) { // pf_r
//							pf_r = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) { // pf_y
//							pf_y = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) { // pf_b
//							pf_b = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
//							kwh = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//						} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) { // kwh_exp
//							kwhExp = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//						} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) { // v_r
//							v_r = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.94.91.14.255")) { // i_r
//							i_r = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) { // pf_r
//							pf_r = Double.parseDouble(rv[r].getFormattedValue());
//						}
//					}
//
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			} else if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.99.98.2.255")) {// power event log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//						obc = rv[r].getFormattedRegisterObisCode();
//						String atr = rv[r].getAttributeId();
//
//						if (obc.equalsIgnoreCase("0.0.96.11.2.255")) { // eventCode
//							eventCode = rv[r].getFormattedValue();
//
//						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//
//					}
//
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			} else if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.99.98.3.255")) {// Transaction Event Log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//
//						obc = rv[r].getFormattedRegisterObisCode();
//						atr = rv[r].getAttributeId();
//
//						if (obc.equalsIgnoreCase("0.0.96.11.3.255")) { // eventCode
//							eventCode = rv[r].getFormattedValue();
//						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//
//					}
//
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			} else if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.99.98.4.255")) {// other event log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//						obc = rv[r].getFormattedRegisterObisCode();
//						atr = rv[r].getAttributeId();
//
//						if (obc.equalsIgnoreCase("0.0.96.11.4.255")) { // eventCode
//							eventCode = rv[r].getFormattedValue();
//						}
//						if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//						if (obc.equalsIgnoreCase("1.0.31.7.0.255")) { // i_r
//							i_r = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.51.7.0.255")) { // i_y
//							i_y = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.71.7.0.255")) { // i_b
//							i_b = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.32.7.0.255")) { // v_r
//							v_r = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.52.7.0.255")) { // v_y
//							v_y = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.72.7.0.255")) { // v_b
//							v_b = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.33.7.0.255")) { // pf_r
//							pf_r = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.53.7.0.255")) { // pf_y
//							pf_y = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.73.7.0.255")) { // pf_b
//							pf_b = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
//							kwh = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//						} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) { // kwh_exp
//							kwhExp = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//						} else if (obc.equalsIgnoreCase("1.0.12.7.0.255")) { // v_r
//							v_r = Double.parseDouble(rv[r].getFormattedValue().replace("V", ""));
//						} else if (obc.equalsIgnoreCase("1.0.94.91.14.255")) { // i_r
//							i_r = Double.parseDouble(rv[r].getFormattedValue().replace("A", ""));
//						} else if (obc.equalsIgnoreCase("1.0.13.7.0.255")) { // pf_r
//							pf_r = Double.parseDouble(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) { // kwh
//							kwh = Double.parseDouble(rv[r].getFormattedValue().replace("Wh", ""));
//						}
//
//					}
//
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			} else if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.99.98.5.255")) { // Non Roll Over Event
//																									// Log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//
//						obc = rv[r].getFormattedRegisterObisCode();
//						atr = rv[r].getAttributeId();
//
//						if (obc.equalsIgnoreCase("0.0.96.11.5.255")) { // eventCode
//							eventCode = rv[r].getFormattedValue();
//						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//
//					}
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//
//			} else if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.99.98.6.255")) {// controled event log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//
//						obc = rv[r].getFormattedRegisterObisCode();
//						atr = rv[r].getAttributeId();
//
//						if (obc.equalsIgnoreCase("0.0.96.11.6.255")) { // eventCode
//							eventCode = rv[r].getFormattedValue();
//						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//
//					}
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//
//			} else if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.94.91.26.255")) {// Event ESW log
//
//				try {
//					for (int r = 0; r < rv.length; r++) {
//
//						obc = rv[r].getFormattedRegisterObisCode();
//						if (obc.equalsIgnoreCase("0.0.94.91.18.255")) { // eventCode 0.0.94.91.26.255
//							alrmDesc = parseAlarm(rv[r].getFormattedValue());
//						} else if (obc.equalsIgnoreCase("0.0.1.0.0.255")) { // eventtime
//							String[] s1 = rv[r].getFormattedValue().replace("T", " ").split("\\+");
//							if (s1[0].contains("*")) {
//								s1[0] = s1[0].replaceAll("\\*", "00");
//							}
//							event_time = new Timestamp(format.parse(zonedDateTimeConverter(s1[0])).getTime());
//						}
//
//					}
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//
//			}
//			if (scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.94.91.26.255")) {
//				// For Each Loop for iterating ArrayList
//				for (String almd : alrmDesc)
//					builder.append("( now(),'" + scMM.getDeviceId() + "', '" + scMM.getMeterSampleId() + "', '"
//							+ scMM.getSequenceNumber() + "','" + event_time + "','" + almd + "','" + create_time
//							+ "'),");
//
//			} else {
//	 			//FOR Schedule data parsing
//				if(flag.equalsIgnoreCase("SCH")) {
//		 			//FOR Schedule data parsing
//					builder.append("( now(),'"+ scMM.getMeterSampleId() +"', '" + eventCode + "', '" + scMM.getDeviceId() + "','" + event_time + "',"
//							+ i_r + ", " + i_y + ", " + i_b + ", " + v_r + ", " + v_y + ", " + v_b + ", " + pf_r + ", "
//							+ pf_y + ", " + pf_b + "," + kwh + ", " + kwhExp + ",'" + create_time + "'),");
//				}else if(flag.equalsIgnoreCase("OD")) {
////					 FOR OD data parsing
//					builder.append("( now(),'OD', '" + eventCode + "', '" + scMM.getDeviceId() + "','" + event_time + "',"
//							+ i_r + ", " + i_y + ", " + i_b + ", " + v_r + ", " + v_y + ", " + v_b + ", " + pf_r + ", "
//							+ pf_y + ", " + pf_b + "," + kwh + ", " + kwhExp + ",'" + create_time + "'),");
//				}
//			
//			}
//
//		} catch (Exception e) {
//			hasExcpData = true;
//			excpbuilder.append("(now(),'" + scMM.getMeterSampleId() + "','" + apiName + "','" + smm.toString() + "'),");
//			e.printStackTrace();
//		}
//
//		//Changes made by Ain on 24/10/2023
////		 pushEventsDataTrigger thread = new pushEventsDataTrigger(eventCode, event_time, scMM.getDeviceId());
////		 thread.start();
//		
//		if (!scMM.getFormattedProfileObisCode().equalsIgnoreCase("0.0.94.91.26.255")) {//Should Avoid/validate Alarm/ESW Data cases bcz event code is null
//			if((eventCode.equalsIgnoreCase("69")|| eventCode.equalsIgnoreCase("70")|| eventCode.equalsIgnoreCase("101")|| eventCode.equalsIgnoreCase("102")) && (scMM.getDeviceId().equalsIgnoreCase("SA1017703")||scMM.getDeviceId().equalsIgnoreCase("SA1017704") || scMM.getDeviceId().equalsIgnoreCase("SA1017705"))) {
////				sendTamperEventsDataTrigger thread = new sendTamperEventsDataTrigger(eventCode, event_time, scMM.getDeviceId());
////				 thread.start();
//			}
//		}
//		
//		rv = null;
//		resetVariable();
//	}

	private ArrayList<String> parseAlarm(String eventBitString) {
		alarmList.clear();// clearing alarms
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
					alarm = "Last Gasp- Occurrence";
					break;
				case 86:
					alarm = "First Breath- Restoration";
					break;
				case 87:
					alarm = "Increment in Billing counter (Manual/MRI reset)";
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
		hasData = false;
		hasExcpData = false;
	}

	public void resetVariable() {

		i_r = null;
		i_y = null;
		i_b = null;
		v_r = null;
		v_y = null;
		v_b = null;
		pf_r = null;
		pf_y = null;
		pf_b = null;
		kwh = null;
		kwhExp = null;
		event_time = null;
		eventCode = null;
		create_time = null;
	}

	private String validateAndGetEventCode(String val, Long counterId) {
		if (val == null) {
			BsmartLogWriter.error(ModuleName.HESINTEGRATION, this, "Event code validation failed: Event code is null for counterId: " + counterId);
			GenericExceptionTracker.logPayloadException(counterId, "eventCode", null, "Event code is null");
			return null;
		}
		String trimmed = val.trim();
		if (trimmed.isEmpty()) {
			BsmartLogWriter.error(ModuleName.HESINTEGRATION, this, "Event code validation failed: Event code is empty for counterId: " + counterId);
			GenericExceptionTracker.logPayloadException(counterId, "eventCode", val, "Event code is empty");
			return null;
		}
		// Handle units, spaces and unknown types dynamically
		String extractedPart = trimmed.contains(" ") ? trimmed.split("\\s+")[0] : trimmed;
		if (extractedPart.equalsIgnoreCase("DLMS_TYPE_UNKNOWN")) {
			BsmartLogWriter.error(ModuleName.HESINTEGRATION, this, "Event code validation failed: Event code is DLMS_TYPE_UNKNOWN for counterId: " + counterId);
			GenericExceptionTracker.logPayloadException(counterId, "eventCode", val, "Event code is DLMS_TYPE_UNKNOWN");
			return null;
		}
		extractedPart = extractedPart.replaceAll("(W|A|V|Hz|VA|varh?|min\\.?|T|VAh)", "").trim();
		if (extractedPart.isEmpty()) {
			return null;
		}
		try {
			BigDecimal bd = new BigDecimal(extractedPart);
			// Check if it represents a whole number
			if (bd.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) == 0) {
				return bd.toBigInteger().toString();
			} else {
				BsmartLogWriter.error(ModuleName.HESINTEGRATION, this, "Event code validation failed: Event code is decimal (" + val + ") for counterId: " + counterId);
				GenericExceptionTracker.logPayloadException(counterId, "eventCode", val, "Event code is decimal: " + val);
				return null;
			}
		} catch (NumberFormatException e) {
			BsmartLogWriter.error(ModuleName.HESINTEGRATION, this, "Event code validation failed: Event code contains invalid characters (" + val + ") for counterId: " + counterId);
			BsmartLogWriter.error(ModuleName.HESINTEGRATION, this, e);
			GenericExceptionTracker.logPayloadException(counterId, "eventCode", val, "Event code contains invalid characters: " + val);
			return null;
		}
	}

	public static Timestamp parseEventTime(String rawVal) {
		if (rawVal == null || rawVal.trim().isEmpty() || "null".equalsIgnoreCase(rawVal.trim())) {
			return null;
		}
		try {
			String clean = rawVal.trim().replace("T", " ");
			if (clean.contains("*")) {
				clean = clean.replaceAll("\\*", "00");
			}
			if (clean.contains("+")) {
				clean = clean.split("\\+")[0].trim();
			}
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return new Timestamp(sdf.parse(clean).getTime());
		} catch (Exception e1) {
			try {
				OffsetDateTime odt = OffsetDateTime.parse(rawVal.trim());
				return Timestamp.valueOf(odt.toLocalDateTime());
			} catch (Exception e2) {
				return null;
			}
		}
	}
	
}
