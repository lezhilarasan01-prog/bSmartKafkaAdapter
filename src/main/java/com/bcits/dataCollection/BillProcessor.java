/**
 * 
 */
package com.bcits.dataCollection;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
import com.bcits.utils.GenericExceptionTracker;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author TARIK
 *
 */
public class BillProcessor implements DataProcessor {
	
	private static BillProcessor processor;
	private boolean hasData;
	private boolean hasExcpData;
	private boolean hasNumExcpdata=false;
	private StringBuilder builder = new StringBuilder();
	private StringBuilder excpbuilder = new StringBuilder();
	private Gson gson;
	private final Map<Integer, List<String>> exceptionMap = new HashMap<>();
	private BigDecimal kwh = null,  kvah = null;
	private BigDecimal kva = null,  sys_PF_Billing = null, 
			kwh_TZ1= null, kwh_TZ2= null, kwh_TZ3= null, kwh_TZ4= null, kwh_TZ5= null, kwh_TZ6= null, kwh_TZ7= null, kwh_TZ8= null;
	private BigDecimal kvah_TZ1= null,kvah_TZ2= null,kvah_TZ3= null,kvah_TZ4= null,kvah_TZ5= null,kvah_TZ6= null,kvah_TZ7= null,kvah_TZ8= null;	
	private BigDecimal demand_kw = null;
	
	private BigDecimal  kw_TZ1 = null, kw_TZ2 = null, kw_TZ3 = null, kw_TZ4 = null, kw_TZ5 = null, kw_TZ6 = null, kw_TZ7 = null, kw_TZ8 = null, 
			kva_TZ1 = null, kva_TZ2 = null, kva_TZ3 = null, kva_TZ4 = null, kva_TZ5 = null, kva_TZ6 = null, kva_TZ7 = null, kva_TZ8 = null;
	
	private String dateKwTZ1 = null, dateKwTZ2 = null, dateKwTZ3 = null, dateKwTZ4 = null, dateKwTZ5 = null, dateKwTZ6 = null,
			dateKwTZ7 = null, dateKwTZ8 = null;
	
	private String dateKvaTZ1 = null, dateKvaTZ2 = null, dateKvaTZ3 = null, dateKvaTZ4 = null, dateKvaTZ5 = null, dateKvaTZ6 = null,
			dateKvaTZ7 = null, dateKvaTZ8 = null;
	
	private String occDateKw = null, dateKva = null;
	private String bill_power_on_duration=null,bill_kwh_export=null,bill_kvah_export=null,reactive_imp_active_imp=null,reactive_imp_active_exp=null,reactive_exp_active_exp=null,reactive_exp_active_imp=null;

	private BigDecimal fundamental_kwh=null,
			kVArh_Q1_T1=null,kVArh_Q1_T2=null,kVArh_Q1_T3=null,kVArh_Q1_T4=null,kVArh_Q1_T5=null,kVArh_Q1_T6=null,kVArh_Q1_T7=null,kVArh_Q1_T8=null,
			kVArh_Q4_T1=null,kVArh_Q4_T2=null,kVArh_Q4_T3=null,kVArh_Q4_T4=null,kVArh_Q4_T5=null,kVArh_Q4_T6=null,kVArh_Q4_T7=null,kVArh_Q4_T8=null,
			sys_pf_exp_billing=null,
			kwh_exp_tz1=null,kwh_exp_tz2=null,kwh_exp_tz3=null,kwh_exp_tz4=null,kwh_exp_tz5=null,kwh_exp_tz6=null,kwh_exp_tz7=null,kwh_exp_tz8=null,
			kvah_exp_tz1=null,kvah_exp_tz2=null,kvah_exp_tz3=null,kvah_exp_tz4=null,kvah_exp_tz5=null,kvah_exp_tz6=null,kvah_exp_tz7=null,kvah_exp_tz8=null,
			MD_kw_exp=null,MD_kva_exp=null;
	private String MD_date_kw_exp=null,MD_date_kva_exp=null;
	private BigDecimal sys_pf_tz1=null,sys_pf_tz2=null,sys_pf_tz3=null,sys_pf_tz4=null,sys_pf_tz5=null,sys_pf_tz6=null,sys_pf_tz7=null,sys_pf_tz8=null,
			sys_pf_exp_tz1=null,sys_pf_exp_tz2=null,sys_pf_exp_tz3=null,sys_pf_exp_tz4=null,sys_pf_exp_tz5=null,sys_pf_exp_tz6=null,sys_pf_exp_tz7=null,sys_pf_exp_tz8=null,
			kvarh_q2_t1=null,kvarh_q2_t2=null,kvarh_q2_t3=null,kvarh_q2_t4=null,kvarh_q2_t5=null,kvarh_q2_t6=null,kvarh_q2_t7=null,kvarh_q2_t8=null,
			kvarh_q3_t1=null,kvarh_q3_t2=null,kvarh_q3_t3=null,kvarh_q3_t4=null,kvarh_q3_t5=null,kvarh_q3_t6=null,kvarh_q3_t7=null,kvarh_q3_t8=null,
			MD_kw_exp_tz1=null,MD_kw_exp_tz2=null,MD_kw_exp_tz3=null,MD_kw_exp_tz4=null,MD_kw_exp_tz5=null,MD_kw_exp_tz6=null,MD_kw_exp_tz7=null,MD_kw_exp_tz8=null;
	private String MD_date_kw_exp_tz1=null,MD_date_kw_exp_tz2=null,MD_date_kw_exp_tz3=null,MD_date_kw_exp_tz4=null,MD_date_kw_exp_tz5=null,MD_date_kw_exp_tz6=null,MD_date_kw_exp_tz7=null,MD_date_kw_exp_tz8=null;
	private BigDecimal MD_kva_exp_tz1=null,MD_kva_exp_tz2=null,MD_kva_exp_tz3=null,MD_kva_exp_tz4=null,MD_kva_exp_tz5=null,MD_kva_exp_tz6=null,MD_kva_exp_tz7=null,MD_kva_exp_tz8=null;
	private String MD_date_kva_exp_tz1=null,MD_date_kva_exp_tz2=null,MD_date_kva_exp_tz3=null,MD_date_kva_exp_tz4=null,MD_date_kva_exp_tz5=null,MD_date_kva_exp_tz6=null,MD_date_kva_exp_tz7=null,MD_date_kva_exp_tz8=null;

	private Timestamp read_time=null, billing_date=null,create_time=null;

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

	private String val;
	private  List<RegisterValue> dt;
	private String dType;
	private  PreVEEData[] preVdt;
	
	public static BillProcessor getInstance() {
		if(processor==null)
			processor = new BillProcessor();
		return processor;
	}

	@Override
	public void init() {
		builder.setLength(0);
		
		///// All Other Project 
		builder.append("insert into meter_data.bill_history (flag, time_stamp, meter_number, billing_date, sys_pf_billing,"
				+ " kwh, kwh_tz1 , kwh_tz2, kwh_tz3, kwh_tz4, kwh_tz5, kwh_tz6, kwh_tz7, kwh_tz8,"
				+ " kvah, kvah_tz1, kvah_tz2, kvah_tz3, kvah_tz4, kvah_tz5, kvah_tz6, kvah_tz7, kvah_tz8, occ_date_kw, demand_kw, "
				+ " kw_tz1, kw_tz2, kw_tz3, kw_tz4, kw_tz5, kw_tz6, kw_tz7, kw_tz8, "
				+ " date_kw_tz1, date_kw_tz2, date_kw_tz3, date_kw_tz4, date_kw_tz5, date_kw_tz6, date_kw_tz7, date_kw_tz8,"
				+ " kva, date_kva, kva_tz1, kva_tz2, kva_tz3, kva_tz4, kva_tz5, kva_tz6, kva_tz7, kva_tz8,"
				+ " date_kva_tz1, date_kva_tz2, date_kva_tz3, date_kva_tz4, date_kva_tz5, date_kva_tz6, date_kva_tz7, date_kva_tz8,"
				+ " bill_power_on_duration, bill_kwh_export, bill_kvah_export, reactive_imp_active_imp, reactive_imp_active_exp , reactive_exp_active_exp, reactive_exp_active_imp,server_time,"
				+ "fundamental_kwh,kvarh_q1_t1,kvarh_q1_t2,kvarh_q1_t3,kvarh_q1_t4,kvarh_q1_t5,kvarh_q1_t6,kvarh_q1_t7,kvarh_q1_t8,kvarh_q4_t1,kvarh_q4_t2,kvarh_q4_t3,kvarh_q4_t4,kvarh_q4_t5,kvarh_q4_t6,kvarh_q4_t7,kvarh_q4_t8,"
				+ "sys_pf_exp_billing,kwh_exp_tz1,kwh_exp_tz2,kwh_exp_tz3,kwh_exp_tz4,kwh_exp_tz5,kwh_exp_tz6,kwh_exp_tz7,kwh_exp_tz8,kvah_exp_tz1,kvah_exp_tz2,kvah_exp_tz3,kvah_exp_tz4,kvah_exp_tz5,kvah_exp_tz6,kvah_exp_tz7,kvah_exp_tz8,"
				+ "md_kw_exp,md_date_kw_exp,md_kva_exp,md_date_kva_exp,sys_pf_tz1,sys_pf_tz2,sys_pf_tz3,sys_pf_tz4,sys_pf_tz5,sys_pf_tz6,sys_pf_tz7,sys_pf_tz8,"
				+ "sys_pf_exp_tz1,sys_pf_exp_tz2,sys_pf_exp_tz3,sys_pf_exp_tz4,sys_pf_exp_tz5,sys_pf_exp_tz6,sys_pf_exp_tz7,sys_pf_exp_tz8,"
				+ "kvarh_q3_t1,kvarh_q3_t2,kvarh_q3_t3,kvarh_q3_t4,kvarh_q3_t5,kvarh_q3_t6,kvarh_q3_t7,kvarh_q3_t8,"
				+ "kvarh_q2_t1,kvarh_q2_t2,kvarh_q2_t3,kvarh_q2_t4,kvarh_q2_t5,kvarh_q2_t6,kvarh_q2_t7,kvarh_q2_t8,"
				+ "md_kw_exp_tz1,md_date_kw_exp_tz1,md_kw_exp_tz2,md_date_kw_exp_tz2,md_kw_exp_tz3,md_date_kw_exp_tz3,md_kw_exp_tz4,md_date_kw_exp_tz4,md_kw_exp_tz5,md_date_kw_exp_tz5,md_kw_exp_tz6,md_date_kw_exp_tz6,md_kw_exp_tz7,md_date_kw_exp_tz7,md_kw_exp_tz8,md_date_kw_exp_tz8,"
				+ "md_kva_exp_tz1,md_date_kva_exp_tz1,md_kva_exp_tz2,md_date_kva_exp_tz2,md_kva_exp_tz3,md_date_kva_exp_tz3,md_kva_exp_tz4,md_date_kva_exp_tz4,md_kva_exp_tz5,md_date_kva_exp_tz5,md_kva_exp_tz6,md_date_kva_exp_tz6,md_kva_exp_tz7,md_date_kva_exp_tz7,md_kva_exp_tz8,md_date_kva_exp_tz8,hes_type) values ");

		excpbuilder.setLength(0);
		excpbuilder.append("insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");
	}

//	@Override
//	public void addData(DeviceData scMM, String hesName) {
//		try {
//			generateBillBatchData(scMM ,hesName);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} //Parse Data & Generate Batch Query	
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
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Billing','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + remarks
						+ "','" + hesName + "'),");
				return;
			}
				generateBillBatchData(scMM, hesName);
				hasData = true;
			
		}  catch (ParseException e) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = e.getMessage() == null ? "Parse Exception" : e.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Billing','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "','"+hesName+"'),");
			e.printStackTrace();
		} catch (Exception ex) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = ex.getMessage() == null ? "Unknown Exception" : ex.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Billing','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "','"+hesName+"'),");
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
	
	private RegisterValues[] rv;
	private String[] st,ct;
	private String obc,atr;
	
//	private void generateBillBatchData(DeviceData scMM, String hesName) throws ParseException {		
//
//		preVdt = scMM.getPreVEE();
//		ct = scMM.getHesDateTime().replace("T", " ").split("\\+");
//		gson = new Gson();
//		String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {}.getType());
//		
//		create_time=new Timestamp(format.parse(ct[0]).getTime());
//		
//		try {
//			hasNumExcpdata=false;
//			for (int r = 0; r < preVdt.length; r++) {
//				obc = preVdt[r].getRegisterId();
//				atr = preVdt[r].getAttribute();
//
//				if (obc.equalsIgnoreCase("0.0.0.1.2.255")) {
//					String[] s1 = preVdt[r].getEnergyQuantity().replace("T", " ").split("\\+");
////				read_time= new Timestamp(format.parse(s1[0]).getTime());
//					billing_date = new Timestamp(format.parse(s1[0]).getTime());
//
//				} else if (obc.equalsIgnoreCase("1.0.13.0.0.255")) {
//				//	sys_PF_Billing = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					sys_PF_Billing = GenericExceptionTracker.exceptionPayload("sys_PF_Billing",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) {
//					//kwh = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh = GenericExceptionTracker.exceptionPayload("kwh",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.1.255")) {
//					//kwh_TZ1 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ1 = GenericExceptionTracker.exceptionPayload("kwh_TZ1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.2.255")) {
//					//kwh_TZ2 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ2 = GenericExceptionTracker.exceptionPayload("kwh_TZ2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.3.255")) {
//					//kwh_TZ3 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ3 = GenericExceptionTracker.exceptionPayload("kwh_TZ3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.4.255")) {
//					//kwh_TZ4 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ4 = GenericExceptionTracker.exceptionPayload("kwh_TZ4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.5.255")) {
//					//kwh_TZ5 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ5 = GenericExceptionTracker.exceptionPayload("kwh_TZ5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.6.255")) {
//					//kwh_TZ6 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ6 = GenericExceptionTracker.exceptionPayload("kwh_TZ6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.7.255")) {
//					//kwh_TZ7 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ7 = GenericExceptionTracker.exceptionPayload("kwh_TZ7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.1.8.8.255")) {
////				if(preVdt[r].getEnergyQuantity().split("\\s+")[0].contains("DLMS_TYPE_UNKNOWN")) {
////					kwh_TZ8=null;
////				}else {
//					//kwh_TZ8 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_TZ8 = GenericExceptionTracker.exceptionPayload("kwh_TZ8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
////				}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.8.0.255")) {
////				if(preVdt[r].getEnergyQuantity().split("\\s+")[0].contains("DLMS_TYPE_UNKNOWN")) {
////					kvah=null;
////				}else {
//					//kvah = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah = GenericExceptionTracker.exceptionPayload("kvah",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
////				}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.8.1.255")) {
//					//kvah_TZ1 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ1 = GenericExceptionTracker.exceptionPayload("kvah_TZ1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.9.8.2.255")) {
//					//kvah_TZ2 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ2 = GenericExceptionTracker.exceptionPayload("kvah_TZ2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.9.8.3.255")) {
//					//kvah_TZ3 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ3 = GenericExceptionTracker.exceptionPayload("kvah_TZ3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.9.8.4.255")) {
//					//kvah_TZ4 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ4 = GenericExceptionTracker.exceptionPayload("kvah_TZ4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.9.8.5.255")) {
//					//kvah_TZ5 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ5 = GenericExceptionTracker.exceptionPayload("kvah_TZ5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.9.8.6.255")) {
//					//kvah_TZ6 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ6 = GenericExceptionTracker.exceptionPayload("kvah_TZ6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.9.8.7.255")) {
//					//kvah_TZ7 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ7 = GenericExceptionTracker.exceptionPayload("kvah_TZ7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				} else if (obc.equalsIgnoreCase("1.0.9.8.8.255")) {
//					//kvah_TZ8 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_TZ8 = GenericExceptionTracker.exceptionPayload("kvah_TZ8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				// From Here
//				else if (obc.equalsIgnoreCase("1.0.1.6.0.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						occDateKw = null;
//					} else if (str.contains("+")) {
//						occDateKw = "'" + str + "'";
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						//demand_kw = Double.parseDouble(str.split("\\s+")[0]);
//						demand_kw = GenericExceptionTracker.exceptionPayload("demand_kw",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.1.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKwTZ1 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ1 = "'" + str + "'";
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						try {
//							//kw_TZ1 = Double.parseDouble(str.split("\\s+")[0]);
//							kw_TZ1 = GenericExceptionTracker.exceptionPayload("kw_TZ1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.2.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKwTZ2 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ2 = "'" + str + "'";
//
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						try {
//							//kw_TZ2 = Double.parseDouble(str.split("\\s+")[0]);
//							kw_TZ2 = GenericExceptionTracker.exceptionPayload("kw_TZ2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.3.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKwTZ3 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ3 = "'" + str + "'";
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						//kw_TZ3 = Double.parseDouble(str.split("\\s+")[0]);
//						kw_TZ3 = GenericExceptionTracker.exceptionPayload("kw_TZ3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.4.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKwTZ4 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ4 = "'" + str + "'";
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						//kw_TZ4 = Double.parseDouble(str.split("\\s+")[0]);
//						kw_TZ4 = GenericExceptionTracker.exceptionPayload("kw_TZ4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.5.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						dateKwTZ5 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ5 = "'" + str + "'";
//
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						//kw_TZ5 = Double.parseDouble(str.split("\\s+")[0]);
//						kw_TZ5 = GenericExceptionTracker.exceptionPayload("kw_TZ5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.6.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKwTZ6 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ6 = "'" + str + "'";
//
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//					//	kw_TZ6 = Double.parseDouble(str.split("\\s+")[0]);
//						kw_TZ6 = GenericExceptionTracker.exceptionPayload("kw_TZ6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.7.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKwTZ7 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ7 = "'" + str + "'";
//
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						//kw_TZ7 = Double.parseDouble(str.split("\\s+")[0]);
//						kw_TZ7 = GenericExceptionTracker.exceptionPayload("kw_TZ7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.1.6.8.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						dateKwTZ8 = null;
//					} else if (str.contains("+")) {
//						dateKwTZ8 = "'" + str + "'";
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						//kw_TZ8 = Double.parseDouble(str.split("\\s+")[0]);
//						kw_TZ8 = GenericExceptionTracker.exceptionPayload("kw_TZ8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				}
//				// From 2nd here
//
//				else if (obc.equalsIgnoreCase("1.0.9.6.0.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKva = null;
//					} else if (str.contains("+")) {
//						dateKva = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//						//kva = Double.parseDouble(str.split("\\s+")[0]);
//						kva = GenericExceptionTracker.exceptionPayload("kva",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.1.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						dateKvaTZ1 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ1 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//	kva_TZ1 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ1 = GenericExceptionTracker.exceptionPayload("kva_TZ1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.2.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKvaTZ2 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ2 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//	kva_TZ2 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ2 = GenericExceptionTracker.exceptionPayload("kva_TZ2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.3.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKvaTZ3 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ3 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//	kva_TZ3 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ3 = GenericExceptionTracker.exceptionPayload("kva_TZ3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.4.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKvaTZ4 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ4 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//						//kva_TZ4 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ4 = GenericExceptionTracker.exceptionPayload("kva_TZ4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.5.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKvaTZ5 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ5 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//						//kva_TZ5 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ5 = GenericExceptionTracker.exceptionPayload("kva_TZ5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.6.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						dateKvaTZ6 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ6 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//						//kva_TZ6 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ6 = GenericExceptionTracker.exceptionPayload("kva_TZ6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.7.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						dateKvaTZ7 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ7 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//	kva_TZ7 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ7 = GenericExceptionTracker.exceptionPayload("kva_TZ7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("1.0.9.6.8.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						dateKvaTZ8 = null;
//					} else if (str.contains("+")) {
//						dateKvaTZ8 = "'" + str + "'";
//					} else if (str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//						//kva_TZ8 = Double.parseDouble(str.split("\\s+")[0]);
//						kva_TZ8 = GenericExceptionTracker.exceptionPayload("kva_TZ8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} else if (obc.equalsIgnoreCase("0.0.94.91.13.255")) {
//					bill_power_on_duration = preVdt[r].getEnergyQuantity().split("\\s+")[0];
//				} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) {
//					bill_kwh_export = preVdt[r].getEnergyQuantity().split("\\s+")[0];
//				} else if (obc.equalsIgnoreCase("1.0.10.8.0.255")) {
//					bill_kvah_export = preVdt[r].getEnergyQuantity().split("\\s+")[0];
//				} else if (obc.equalsIgnoreCase("1.0.5.8.0.255")) {
//					reactive_imp_active_imp = preVdt[r].getEnergyQuantity().split("\\s+")[0];
//				} else if (obc.equalsIgnoreCase("1.0.6.8.0.255")) {
//					reactive_imp_active_exp = preVdt[r].getEnergyQuantity().split("\\s+")[0];
//				} else if (obc.equalsIgnoreCase("1.0.7.8.0.255")) {
//					reactive_exp_active_exp = preVdt[r].getEnergyQuantity().split("\\s+")[0];
//				} else if (obc.equalsIgnoreCase("1.0.8.8.0.255")) {
//					reactive_exp_active_imp = preVdt[r].getEnergyQuantity().split("\\s+")[0];
//				}
//				
//				
//				///New 102 Clm Addintion
//				
//				else if (obc.equalsIgnoreCase("1.0.129.8.0.255")) {
//				//	fundamental_kwh=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					fundamental_kwh=GenericExceptionTracker.exceptionPayload("fundamental_kwh",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				////kVAr_Q1
//				else if (obc.equalsIgnoreCase("1.0.5.8.1.255")) {
//					//kVArh_Q1_T1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T1=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.5.8.2.255")) {
//					//kVArh_Q1_T2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T2=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.5.8.3.255")) {
//					//kVArh_Q1_T3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T3=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.5.8.4.255")) {
//					//kVArh_Q1_T4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T4=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.5.8.5.255")) {
//					//kVArh_Q1_T5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T5=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.5.8.6.255")) {
//					//kVArh_Q1_T6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T6=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.5.8.7.255")) {
//					//kVArh_Q1_T7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T7=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.5.8.8.255")) {
//				//	kVArh_Q1_T8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q1_T8=GenericExceptionTracker.exceptionPayload("kVArh_Q1_T8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//				////kVAr_Q4
//				else if (obc.equalsIgnoreCase("1.0.8.8.1.255")) {
//					//kVArh_Q4_T1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T1=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.8.8.2.255")) {
//					//kVArh_Q4_T2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T2=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.8.8.3.255")) {
//					//kVArh_Q4_T3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T3=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.8.8.4.255")) {
//					//kVArh_Q4_T4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T4=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.8.8.5.255")) {
//					//kVArh_Q4_T5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T5=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.8.8.6.255")) {
//					//kVArh_Q4_T6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T6=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.8.8.7.255")) {
//					//kVArh_Q4_T7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T7=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.8.8.8.255")) {
//					//kVArh_Q4_T8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kVArh_Q4_T8=GenericExceptionTracker.exceptionPayload("kVArh_Q4_T8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.84.0.0.255")) {
//					//sys_pf_exp_billing=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					sys_pf_exp_billing=GenericExceptionTracker.exceptionPayload("sys_pf_exp_billing",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//				////kwh_exp
//				else if (obc.equalsIgnoreCase("1.0.2.8.1.255")) {
//					//kwh_exp_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz1=GenericExceptionTracker.exceptionPayload("kwh_exp_tz1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.2.8.2.255")) {
//				//	kwh_exp_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz2=GenericExceptionTracker.exceptionPayload("kwh_exp_tz2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.2.8.3.255")) {
//				//	kwh_exp_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz3=GenericExceptionTracker.exceptionPayload("kwh_exp_tz3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.2.8.4.255")) {
//					//kwh_exp_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz4=GenericExceptionTracker.exceptionPayload("kwh_exp_tz4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.2.8.5.255")) {
//				//	kwh_exp_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz5=GenericExceptionTracker.exceptionPayload("kwh_exp_tz5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.2.8.6.255")) {
//				//	kwh_exp_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz6=GenericExceptionTracker.exceptionPayload("kwh_exp_tz6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.2.8.7.255")) {
//				//	kwh_exp_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz7=GenericExceptionTracker.exceptionPayload("kwh_exp_tz7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.2.8.8.255")) {
//				//	kwh_exp_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kwh_exp_tz8=GenericExceptionTracker.exceptionPayload("kwh_exp_tz8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//				////kvah_exp
//				else if (obc.equalsIgnoreCase("1.0.10.8.1.255")) {
//					//kvah_exp_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz1=GenericExceptionTracker.exceptionPayload("kvah_exp_tz1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.10.8.2.255")) {
//					//kvah_exp_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz2=GenericExceptionTracker.exceptionPayload("kvah_exp_tz2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.10.8.3.255")) {
//				//	kvah_exp_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz3=GenericExceptionTracker.exceptionPayload("kvah_exp_tz3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.10.8.4.255")) {
//				//	kvah_exp_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz4=GenericExceptionTracker.exceptionPayload("kvah_exp_tz4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.10.8.5.255")) {
//					//kvah_exp_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz5=GenericExceptionTracker.exceptionPayload("kvah_exp_tz5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.10.8.6.255")) {
//					//kvah_exp_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz6=GenericExceptionTracker.exceptionPayload("kvah_exp_tz6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.10.8.7.255")) {
//					//kvah_exp_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz7=GenericExceptionTracker.exceptionPayload("kvah_exp_tz7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}else if (obc.equalsIgnoreCase("1.0.10.8.8.255")) {
//				//	kvah_exp_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//					kvah_exp_tz8=GenericExceptionTracker.exceptionPayload("kvah_exp_tz8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//				////MD_kw_exp & MD_kva_exp
//				else if (obc.equalsIgnoreCase("1.0.2.6.0.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						MD_date_kw_exp=null;
//					} else if (str.contains("+")) {
//						MD_date_kw_exp="'"+(str)+"'";
//					} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//						//MD_kw_exp=Double.parseDouble(str.split("\\s+")[0]);
//						MD_kw_exp=GenericExceptionTracker.exceptionPayload("MD_kw_exp",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//					
//				}
//				else if (obc.equalsIgnoreCase("1.0.10.6.0.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						MD_date_kva_exp=null;
//					} else if (str.contains("+")) {
//						MD_date_kva_exp="'"+(str)+"'";
//					} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//						//MD_kva_exp=Double.parseDouble(str.split("\\s+")[0]);
//						MD_kva_exp=GenericExceptionTracker.exceptionPayload("MD_kva_exp",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//					
//				}
//				
//			////sys_pf
//			else if (obc.equalsIgnoreCase("1.0.13.0.1.255")) {
//				//sys_pf_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz1=GenericExceptionTracker.exceptionPayload("sys_pf_tz1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.13.0.2.255")) {
//				//sys_pf_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz2=GenericExceptionTracker.exceptionPayload("sys_pf_tz2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.13.0.3.255")) {
//				//sys_pf_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz3=GenericExceptionTracker.exceptionPayload("sys_pf_tz3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.13.0.4.255")) {
//				//sys_pf_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz4=GenericExceptionTracker.exceptionPayload("sys_pf_tz4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.13.0.5.255")) {
//				//sys_pf_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz5=GenericExceptionTracker.exceptionPayload("sys_pf_tz5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.13.0.6.255")) {
//				//sys_pf_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz6=GenericExceptionTracker.exceptionPayload("sys_pf_tz6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.13.0.7.255")) {
//				//sys_pf_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz7=GenericExceptionTracker.exceptionPayload("sys_pf_tz7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.13.0.8.255")) {
//				//sys_pf_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_tz8=GenericExceptionTracker.exceptionPayload("sys_pf_tz8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}
//				
//			////sys_pf_exp
//			else if (obc.equalsIgnoreCase("1.0.84.0.1.255")) {
//				//sys_pf_exp_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz1=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.84.0.2.255")) {
//				//sys_pf_exp_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz2=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.84.0.3.255")) {
//				//sys_pf_exp_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz3=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.84.0.4.255")) {
//				//sys_pf_exp_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz4=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.84.0.5.255")) {
//				//sys_pf_exp_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz5=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.84.0.6.255")) {
//				//sys_pf_exp_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz6=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.84.0.7.255")) {
//				//sys_pf_exp_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz7=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.84.0.8.255")) {
//			//	sys_pf_exp_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				sys_pf_exp_tz8=GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}
//				
//			
//			////kvarh_lead_exp
//			else if (obc.equalsIgnoreCase("1.0.6.8.1.255")) {
//				//kvarh_q2_t1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t1=GenericExceptionTracker.exceptionPayload("kvarh_q2_t1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.6.8.2.255")) {
//				//kvarh_q2_t2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t2=GenericExceptionTracker.exceptionPayload("kvarh_q2_t2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.6.8.3.255")) {
//				//kvarh_q2_t3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t3=GenericExceptionTracker.exceptionPayload("kvarh_q2_t3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.6.8.4.255")) {
//				//kvarh_q2_t4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t4=GenericExceptionTracker.exceptionPayload("kvarh_q2_t4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.6.8.5.255")) {
//				//kvarh_q2_t5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t5=GenericExceptionTracker.exceptionPayload("kvarh_q2_t5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.6.8.6.255")) {
//			//	kvarh_q2_t6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t6=GenericExceptionTracker.exceptionPayload("kvarh_q2_t6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.6.8.7.255")) {
//				//kvarh_q2_t7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t7=GenericExceptionTracker.exceptionPayload("kvarh_q2_t7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.6.8.8.255")) {
//			//	kvarh_q2_t8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q2_t8=GenericExceptionTracker.exceptionPayload("kvarh_q2_t8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}
//				
//			////kvarh_lag_exp
//			else if (obc.equalsIgnoreCase("1.0.7.8.1.255")) {
//				//kvarh_q3_t1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t1=GenericExceptionTracker.exceptionPayload("kvarh_q3_t1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.7.8.2.255")) {
//				//kvarh_q3_t2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t2=GenericExceptionTracker.exceptionPayload("kvarh_q3_t2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.7.8.3.255")) {
//				//kvarh_q3_t3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t3=GenericExceptionTracker.exceptionPayload("kvarh_q3_t3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.7.8.4.255")) {
//				//kvarh_q3_t4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t4=GenericExceptionTracker.exceptionPayload("kvarh_q3_t4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.7.8.5.255")) {
//				//kvarh_q3_t5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t5=GenericExceptionTracker.exceptionPayload("kvarh_q3_t5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.7.8.6.255")) {
//				//kvarh_q3_t6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t6=GenericExceptionTracker.exceptionPayload("kvarh_q3_t6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.7.8.7.255")) {
//				//kvarh_q3_t7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t7=GenericExceptionTracker.exceptionPayload("kvarh_q3_t7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}else if (obc.equalsIgnoreCase("1.0.7.8.8.255")) {
//			//	kvarh_q3_t8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
//				kvarh_q3_t8=GenericExceptionTracker.exceptionPayload("kvarh_q3_t8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//			}
//				
//			////MD_kw_exp
//			else if (obc.equalsIgnoreCase("1.0.2.6.1.255")) {
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz1=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz1="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//					//MD_kw_exp_tz1=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz1=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//			}
//			else if (obc.equalsIgnoreCase("1.0.2.6.2.255")) {
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz2=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz2="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//					//MD_kw_exp_tz2=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz2=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//			}
//			else if (obc.equalsIgnoreCase("1.0.2.6.3.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz3=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz3="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//					//MD_kw_exp_tz3=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz3=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.2.6.4.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz4=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz4="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//					//MD_kw_exp_tz4=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz4=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.2.6.5.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz5=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz5="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//				//	MD_kw_exp_tz5=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz5=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.2.6.6.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz6=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz6="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//					//MD_kw_exp_tz6=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz6=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.2.6.7.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz7=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz7="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//					//MD_kw_exp_tz7=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz7=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.2.6.8.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kw_exp_tz8=null;
//				} else if (str.contains("+")) {
//					MD_date_kw_exp_tz8="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//					//MD_kw_exp_tz8=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kw_exp_tz8=GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			////MD_kva_exp
//			else if (obc.equalsIgnoreCase("1.0.10.6.1.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz1=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz1="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//MD_kva_exp_tz1=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz1=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz1",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.10.6.2.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz2=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz2="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//MD_kva_exp_tz2=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz2=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz2",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.10.6.3.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz3=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz3="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//				//	MD_kva_exp_tz3=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz3=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz3",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.10.6.4.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz4=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz4="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//MD_kva_exp_tz4=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz4=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz4",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.10.6.5.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz5=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz5="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//MD_kva_exp_tz5=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz5=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz5",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.10.6.6.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz6=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz6="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//MD_kva_exp_tz6=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz6=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz6",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.10.6.7.255")) {
//				
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz7=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz7="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//MD_kva_exp_tz7=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz7=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz7",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}
//			else if (obc.equalsIgnoreCase("1.0.10.6.8.255")) {
//				String str = preVdt[r].getEnergyQuantity();
//				if (str.equals("")) {
//					MD_date_kva_exp_tz8=null;
//				} else if (str.contains("+")) {
//					MD_date_kva_exp_tz8="'"+(str)+"'";
//				} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("VA")) {
//					//MD_kva_exp_tz8=Double.parseDouble(str.split("\\s+")[0]);
//					MD_kva_exp_tz8=GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz8",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//				}
//				
//			}	
//
//			}	
//			if(hasNumExcpdata==false) {
//			////// Other Project(MCL)
//			builder.append("('" + scMM.getSeqId() + "', now(),'" + scMM.getDeviceId() + "','" + billing_date + "','" + sys_PF_Billing + "',"
//					+ " " + kwh + "," + kwh_TZ1 + "," + kwh_TZ2 + ", " + kwh_TZ3 + ", " + kwh_TZ4 + ", " + kwh_TZ5 + ", " + kwh_TZ6 + ", " + kwh_TZ7 + ", " + kwh_TZ8 + ","
//					+ " " + kvah + "," + kvah_TZ1 + "," + kvah_TZ2 + "," + kvah_TZ3 + ", " + kvah_TZ4 + ", " + kvah_TZ5 + ", " + kvah_TZ6 + ", " + kvah_TZ7 + ", " + kvah_TZ8 + ", " + occDateKw + ", " + demand_kw + ","
//					+ " " + kw_TZ1 + "," + kw_TZ2 + ", " + kw_TZ3 + ", " + kw_TZ4 + ", " + kw_TZ5 + ", " + kw_TZ6 + ", " + kw_TZ7 + ", " + kw_TZ8 + ","
//					+ " " + dateKwTZ1 + "," + dateKwTZ2 + ", " + dateKwTZ3 + ", " + dateKwTZ4 + ", " + dateKwTZ5 + ", " + dateKwTZ6 + ", " + dateKwTZ7 + ", " + dateKwTZ8 + ","
//					+ " " + kva + "," + dateKva + ", " + kva_TZ1 + ", " + kva_TZ2 + ", " + kva_TZ3 + ", " + kva_TZ4 + ", " + kva_TZ5 + ", " + kva_TZ6 + "," + kva_TZ7 + "," + kva_TZ8 + ","
//					+ " " + dateKvaTZ1 + "," + dateKvaTZ2 + ", " + dateKvaTZ3 + ", " + dateKvaTZ4 + ", " + dateKvaTZ5 + ", " + dateKvaTZ6 + ", " + dateKvaTZ7 + ", " + dateKvaTZ8 + ","	
//					+ " '" + bill_power_on_duration + "','" + bill_kwh_export + "', '" + bill_kvah_export + "', " + reactive_imp_active_imp + ", " + reactive_imp_active_exp + ", " + reactive_exp_active_exp + ", " + reactive_exp_active_imp + ",'"+create_time+"',"
//					+ " " + fundamental_kwh + "," + kVArh_Q1_T1 + ", " + kVArh_Q1_T2 + ", " + kVArh_Q1_T3 + ", " + kVArh_Q1_T4 + ", " + kVArh_Q1_T5 + ", " + kVArh_Q1_T6 + ", " + kVArh_Q1_T7 + ", " + kVArh_Q1_T8 + ","
//					+ " " + kVArh_Q4_T1 + "," + kVArh_Q4_T2 + ", " + kVArh_Q4_T3 + ", " + kVArh_Q4_T4 + ", " + kVArh_Q4_T5 + ", " + kVArh_Q4_T6 + ", " + kVArh_Q4_T7 + ", " + kVArh_Q4_T8 + ", " + sys_pf_exp_billing + ","
//					+ " " + kwh_exp_tz1 + "," + kwh_exp_tz2 + ", " + kwh_exp_tz3 + ", " + kwh_exp_tz4 + ", " + kwh_exp_tz5 + ", " + kwh_exp_tz6 + ", " + kwh_exp_tz7 + ", " + kwh_exp_tz8 + ","
//					+ " " + kvah_exp_tz1 + "," + kvah_exp_tz2 + ", " + kvah_exp_tz3 + ", " + kvah_exp_tz4 + ", " + kvah_exp_tz5 + ", " + kvah_exp_tz6 + ", " + kvah_exp_tz7 + ", " + kvah_exp_tz8 + ","
//					+ " " + MD_kw_exp + "," + MD_date_kw_exp + ", " 
//					+ " " + MD_kva_exp + "," + MD_date_kva_exp + ", " 			
//					+ " " + sys_pf_tz1 + "," + sys_pf_tz2 + ", " + sys_pf_tz3 + ", " + sys_pf_tz4 + ", " + sys_pf_tz5 + ", " + sys_pf_tz6 + ", " + sys_pf_tz7 + ", " + sys_pf_tz8 + ","
//					+ " " + sys_pf_exp_tz1 + "," + sys_pf_exp_tz2 + ", " + sys_pf_exp_tz3 + ", " + sys_pf_exp_tz4 + ", " + sys_pf_exp_tz5 + ", " + sys_pf_exp_tz6 + ", " + sys_pf_exp_tz7 + ", " + sys_pf_exp_tz8 + ","
//					+ " " + kvarh_q3_t1 + "," + kvarh_q3_t2 + ", " + kvarh_q3_t3 + ", " + kvarh_q3_t4 + ", " + kvarh_q3_t5 + ", " + kvarh_q3_t6 + ", " + kvarh_q3_t7 + ", " + kvarh_q3_t8 + ","
//					+ " " + kvarh_q2_t1 + "," + kvarh_q2_t2 + ", " + kvarh_q2_t3 + ", " + kvarh_q2_t4 + ", " + kvarh_q2_t5 + ", " + kvarh_q2_t6 + ", " + kvarh_q2_t7 + ", " + kvarh_q2_t8 + ","
//					+ " " + MD_kw_exp_tz1 + "," + MD_date_kw_exp_tz1 + ", " + MD_kw_exp_tz2 + ", " + MD_date_kw_exp_tz2 + ", " + MD_kw_exp_tz3 + ", " + MD_date_kw_exp_tz3 + ", " + MD_kw_exp_tz4 + ", " + MD_date_kw_exp_tz4 + ","
//					+ " " + MD_kw_exp_tz5 + "," + MD_date_kw_exp_tz5 + ", " + MD_kw_exp_tz6 + ", " + MD_date_kw_exp_tz6 + ", " + MD_kw_exp_tz7 + ", " + MD_date_kw_exp_tz7 + ", " + MD_kw_exp_tz8 + ", " + MD_date_kw_exp_tz8 + ","
//					+ " " + MD_kva_exp_tz1 + "," + MD_date_kva_exp_tz1 + ", " + MD_kva_exp_tz2 + ", " + MD_date_kva_exp_tz2 + ", " + MD_kva_exp_tz3 + ", " + MD_date_kva_exp_tz3 + ", " + MD_kva_exp_tz4 + ", " + MD_date_kva_exp_tz4 + ","
//					+ " " + MD_kva_exp_tz5 + "," + MD_date_kva_exp_tz5 + ", " + MD_kva_exp_tz6 + ", " + MD_date_kva_exp_tz6 + ", " + MD_kva_exp_tz7 + ", " + MD_date_kva_exp_tz7 + ", " + MD_kva_exp_tz8 + ", " + MD_date_kva_exp_tz8 + "),");
//			}else {
//		        hasExcpData = true;
//		        List<String> exceptions = exceptionMap.getOrDefault(scMM.getSeqId(), new ArrayList<>());
//		        String combinedExceptions = String.join(",", exceptions);
//		        excpbuilder.append("(now(),'" + scMM.getSeqId() + "','"+ResponseParser.type+"','" + smm.replace("'", "''") + "','"+ combinedExceptions + "','"+ hesName + "'),"
//		        );
//		    }
//		} catch (Exception e) {
//		    hasExcpData = true;
//		    String safeSmm = smm.replace("'", "''"); // escape single quotes
//		  //  String safeMessage = (e.getMessage() != null ? e.getMessage().replace("'", "''") : "Unknown error");
//		    excpbuilder.append("(now(),'" + scMM.getSeqId() + "','"+ResponseParser.type+"','" + safeSmm +"','REMARKS:" + e.getMessage() + "','"+ hesName + "'),");
//		    e.printStackTrace();
//		}
//		
//		
//		rv=null;
////		st=null;
////		ct=null;
//		atr=null;
//		resetVariable();
//		
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
	
	public String parseDate(String obisDate) {
		if (obisDate == null || obisDate.trim().isEmpty()) {
			return null;
		}
		String[] strr = obisDate.replace("+", ",").split(",");
		if (strr.length == 0 || strr[0].trim().isEmpty()) {
			return null;
		}
		try {
			SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			SimpleDateFormat outFmt = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss a");
			Date date = inFmt.parse(strr[0].trim());
			return (date != null) ? outFmt.format(date) : null;
		} catch (Exception e) {
			return null;
		}
	}
	
	public BigDecimal parseData(String obisDate) {
		if (obisDate == null || obisDate.trim().isEmpty()) {
			return null;
		}
		String[] strr = obisDate.replace("+", ",").split(",");
		if (strr.length == 0 || strr[0].trim().isEmpty()) {
			return null;
		}
		BigDecimal data = null;
		try {
			data = new BigDecimal(strr[0].trim());
		} catch (Exception e) {
			return null;
		}
		return data;
	}

	public static String parseOffsetDate(String rawVal) {
		if (rawVal == null || rawVal.trim().isEmpty() || "null".equalsIgnoreCase(rawVal.trim())) {
			return null;
		}
		String clean = rawVal.trim();
		try {
			OffsetDateTime odt = OffsetDateTime.parse(clean);
			return "'" + Timestamp.valueOf(odt.toLocalDateTime()) + "'";
		} catch (Exception ex) {
			try {
				clean = clean.replace("T", " ");
				if (clean.contains("+")) {
					clean = clean.split("\\+")[0].trim();
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				return "'" + new Timestamp(sdf.parse(clean).getTime()) + "'";
			} catch (Exception ignored) {
				return null;
			}
		}
	}
	
	public void resetVariable() {
		kwh = null;  kvah = null;
		kva = null;  sys_PF_Billing = null; 
		kwh_TZ1= null; kwh_TZ2= null; kwh_TZ3= null; kwh_TZ4= null; kwh_TZ5= null; kwh_TZ6= null; kwh_TZ7= null; kwh_TZ8= null;
		kvah_TZ1= null;kvah_TZ2= null;kvah_TZ3= null;kvah_TZ4= null;kvah_TZ5= null;kvah_TZ6= null;kvah_TZ7= null;kvah_TZ8= null;	
		demand_kw = null;
		
		kw_TZ1 = null; kw_TZ2= null; kw_TZ3= null; kw_TZ4= null; kw_TZ5= null; kw_TZ6= null; kw_TZ7= null; kw_TZ8= null; 
		kva_TZ1= null; kva_TZ2= null; kva_TZ3= null; kva_TZ4= null; kva_TZ5= null; kva_TZ6= null; kva_TZ7= null; kva_TZ8= null;
		
		dateKwTZ1 = null; dateKwTZ2 = null; dateKwTZ3 = null; dateKwTZ4 = null; dateKwTZ5 = null; dateKwTZ6 = null;
		dateKwTZ7 = null; dateKwTZ8 = null;
		
		dateKvaTZ1 = null; dateKvaTZ2 = null; dateKvaTZ3 = null; dateKvaTZ4 = null; dateKvaTZ5 = null; dateKvaTZ6 = null;
		dateKvaTZ7 = null; dateKvaTZ8 = null;
		
		occDateKw = null; dateKva = null;
		bill_power_on_duration=null;bill_kwh_export=null;bill_kvah_export=null;reactive_imp_active_imp=null;reactive_imp_active_exp=null;reactive_exp_active_exp=null;reactive_exp_active_imp=null;

		read_time=null; billing_date=null;create_time=null;

		fundamental_kwh=null;
		kVArh_Q1_T1=null;kVArh_Q1_T2=null;kVArh_Q1_T3=null;kVArh_Q1_T4=null;kVArh_Q1_T5=null;kVArh_Q1_T6=null;kVArh_Q1_T7=null;kVArh_Q1_T8=null;
		kVArh_Q4_T1=null;kVArh_Q4_T2=null;kVArh_Q4_T3=null;kVArh_Q4_T4=null;kVArh_Q4_T5=null;kVArh_Q4_T6=null;kVArh_Q4_T7=null;kVArh_Q4_T8=null;
		sys_pf_exp_billing=null;
		kwh_exp_tz1=null;kwh_exp_tz2=null;kwh_exp_tz3=null;kwh_exp_tz4=null;kwh_exp_tz5=null;kwh_exp_tz6=null;kwh_exp_tz7=null;kwh_exp_tz8=null;
		kvah_exp_tz1=null;kvah_exp_tz2=null;kvah_exp_tz3=null;kvah_exp_tz4=null;kvah_exp_tz5=null;kvah_exp_tz6=null;kvah_exp_tz7=null;kvah_exp_tz8=null;
		MD_kw_exp=null;MD_kva_exp=null;
		MD_date_kw_exp=null;MD_date_kva_exp=null;
		sys_pf_tz1=null;sys_pf_tz2=null;sys_pf_tz3=null;sys_pf_tz4=null;sys_pf_tz5=null;sys_pf_tz6=null;sys_pf_tz7=null;sys_pf_tz8=null;
		sys_pf_exp_tz1=null;sys_pf_exp_tz2=null;sys_pf_exp_tz3=null;sys_pf_exp_tz4=null;sys_pf_exp_tz5=null;sys_pf_exp_tz6=null;sys_pf_exp_tz7=null;sys_pf_exp_tz8=null;
		kvarh_q2_t1=null;kvarh_q2_t2=null;kvarh_q2_t3=null;kvarh_q2_t4=null;kvarh_q2_t5=null;kvarh_q2_t6=null;kvarh_q2_t7=null;kvarh_q2_t8=null;
		kvarh_q3_t1=null;kvarh_q3_t2=null;kvarh_q3_t3=null;kvarh_q3_t4=null;kvarh_q3_t5=null;kvarh_q3_t6=null;kvarh_q3_t7=null;kvarh_q3_t8=null;
		MD_kw_exp_tz1=null;MD_kw_exp_tz2=null;MD_kw_exp_tz3=null;MD_kw_exp_tz4=null;MD_kw_exp_tz5=null;MD_kw_exp_tz6=null;MD_kw_exp_tz7=null;MD_kw_exp_tz8=null;
		MD_date_kw_exp_tz1=null;MD_date_kw_exp_tz2=null;MD_date_kw_exp_tz3=null;MD_date_kw_exp_tz4=null;MD_date_kw_exp_tz5=null;MD_date_kw_exp_tz6=null;MD_date_kw_exp_tz7=null;MD_date_kw_exp_tz8=null;
		MD_kva_exp_tz1=null;MD_kva_exp_tz2=null;MD_kva_exp_tz3=null;MD_kva_exp_tz4=null;MD_kva_exp_tz5=null;MD_kva_exp_tz6=null;MD_kva_exp_tz7=null;MD_kva_exp_tz8=null;
		MD_date_kva_exp_tz1=null;MD_date_kva_exp_tz2=null;MD_date_kva_exp_tz3=null;MD_date_kva_exp_tz4=null;MD_date_kva_exp_tz5=null;MD_date_kva_exp_tz6=null;MD_date_kva_exp_tz7=null;MD_date_kva_exp_tz8=null;
		
		
	}

	public String bParseDate(String obisDate) {

		String[] strr = obisDate.replace("+", ",").split(",");

		SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss a");
		Date date = null;
		try {
			date = inputFormat.parse(strr[0]);
		} catch (ParseException e) {
		
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // hardcode here and check
		
		String validDate = outputFormat.format(date);

		return validDate;

	}
	
	public String bParseDate1(String obisDate) {

		String[] strr = obisDate.replace("+", ",").split(",");

		SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss a");
		Date date = null;
		try {
			date = inputFormat.parse(strr[0]);
		} catch (ParseException e) {
		
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // hardcode here and check
		
		String validDate = outputFormat.format(date);

		return validDate;

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
	
	
	
	private void generateBillBatchData(DeviceData scMM, String hesName) throws ParseException {
		preVdt = scMM.getPreVEE();
		create_time = parseTimestamp(scMM.getHesDateTime(), new Timestamp(System.currentTimeMillis()));
		gson = new Gson();
		String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {}.getType());
		try {
			//hasNumExcpdata=false;
			GenericExceptionTracker.reset();
			for (int r = 0; r < preVdt.length; r++) {
				obc = preVdt[r].getRegisterId();
				atr = preVdt[r].getAttribute();

				if (obc.equalsIgnoreCase("0.0.0.1.2.255")) {
					billing_date = parseTimestamp(preVdt[r].getEnergyQuantity(), null);
				} 
				else if (obc.equalsIgnoreCase("1.0.13.0.0.255")) {
					// sys_PF_Billing =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_PF_Billing = GenericExceptionTracker.exceptionPayload("sys_PF_Billing", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.0.255")) {
					// kwh = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh = GenericExceptionTracker.exceptionPayload("kwh", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.1.255")) {
					// kwh_TZ1 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ1 = GenericExceptionTracker.exceptionPayload("kwh_TZ1", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.2.255")) {
					// kwh_TZ2 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ2 = GenericExceptionTracker.exceptionPayload("kwh_TZ2", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.3.255")) {
					// kwh_TZ3 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ3 = GenericExceptionTracker.exceptionPayload("kwh_TZ3", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.4.255")) {
					// kwh_TZ4 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ4 = GenericExceptionTracker.exceptionPayload("kwh_TZ4", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.5.255")) {
					// kwh_TZ5 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ5 = GenericExceptionTracker.exceptionPayload("kwh_TZ5", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.6.255")) {
					// kwh_TZ6 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ6 = GenericExceptionTracker.exceptionPayload("kwh_TZ6", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.7.255")) {
					// kwh_TZ7 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ7 = GenericExceptionTracker.exceptionPayload("kwh_TZ7", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.1.8.8.255")) {
//				if(preVdt[r].getEnergyQuantity().split("\\s+")[0].contains("DLMS_TYPE_UNKNOWN")) {
//					kwh_TZ8=null;
//				}else {
					// kwh_TZ8 = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_TZ8 = GenericExceptionTracker.exceptionPayload("kwh_TZ8", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
//				}

				} else if (obc.equalsIgnoreCase("1.0.9.8.0.255")) {
//				if(preVdt[r].getEnergyQuantity().split("\\s+")[0].contains("DLMS_TYPE_UNKNOWN")) {
//					kvah=null;
//				}else {
					// kvah = Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah = GenericExceptionTracker.exceptionPayload("kvah", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
//				}

				} else if (obc.equalsIgnoreCase("1.0.9.8.1.255")) {
					// kvah_TZ1 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ1 = GenericExceptionTracker.exceptionPayload("kvah_TZ1", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.2.255")) {
					// kvah_TZ2 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ2 = GenericExceptionTracker.exceptionPayload("kvah_TZ2", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.3.255")) {
					// kvah_TZ3 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ3 = GenericExceptionTracker.exceptionPayload("kvah_TZ3", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.4.255")) {
					// kvah_TZ4 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ4 = GenericExceptionTracker.exceptionPayload("kvah_TZ4", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.5.255")) {
					// kvah_TZ5 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ5 = GenericExceptionTracker.exceptionPayload("kvah_TZ5", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.6.255")) {
					// kvah_TZ6 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ6 = GenericExceptionTracker.exceptionPayload("kvah_TZ6", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.7.255")) {
					// kvah_TZ7 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ7 = GenericExceptionTracker.exceptionPayload("kvah_TZ7", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.9.8.8.255")) {
					// kvah_TZ8 =
					// Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_TZ8 = GenericExceptionTracker.exceptionPayload("kvah_TZ8", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}
				// From Here
//				else if (obc.equalsIgnoreCase("1.0.1.6.0.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//
//					if (str.equals("")) {
//						occDateKw = null;
//					} else if (str.contains("+")) {
//						occDateKw = "'" + str + "'";
//					} else if (str.substring(str.length() - 1, str.length()).equalsIgnoreCase("W")) {
//						//demand_kw = Double.parseDouble(str.split("\\s+")[0]);
//						demand_kw = GenericExceptionTracker.exceptionPayload("demand_kw",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//
//				} 

				else if (obc.equalsIgnoreCase("1.0.1.6.0.255") && atr.equalsIgnoreCase("2")) {
					demand_kw = GenericExceptionTracker.exceptionPayload("demand_kw", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}
				
				else if (obc.equalsIgnoreCase("1.0.1.6.0.255") && atr.equalsIgnoreCase("5")) {
					occDateKw = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.1.6.1.255") && atr.equalsIgnoreCase("2")) {
					kw_TZ1 = GenericExceptionTracker.exceptionPayload("kw_TZ1", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				else if (obc.equalsIgnoreCase("1.0.1.6.1.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ1 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.1.6.2.255") && atr.equalsIgnoreCase("2")) {
					kw_TZ2 = GenericExceptionTracker.exceptionPayload("kw_TZ2", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} 
				
				else if (obc.equalsIgnoreCase("1.0.1.6.2.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ2 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				
				else if (obc.equalsIgnoreCase("1.0.1.6.3.255") && atr.equalsIgnoreCase("2")) {
					kw_TZ3 = GenericExceptionTracker.exceptionPayload("kw_TZ3", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}
				
				else if (obc.equalsIgnoreCase("1.0.1.6.3.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ3 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				
				else if (obc.equalsIgnoreCase("1.0.1.6.4.255") && atr.equalsIgnoreCase("2")) {
					kw_TZ4 = GenericExceptionTracker.exceptionPayload("kw_TZ4", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.1.6.4.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ4 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				
				else if (obc.equalsIgnoreCase("1.0.1.6.5.2555") && atr.equalsIgnoreCase("2")) {
					kw_TZ5 = GenericExceptionTracker.exceptionPayload("kw_TZ5", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.1.6.5.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ5 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				
				else if (obc.equalsIgnoreCase("1.0.1.6.6.255") && atr.equalsIgnoreCase("2")) {
					kw_TZ6 = GenericExceptionTracker.exceptionPayload("kw_TZ6", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.1.6.6.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ6 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				
				else if (obc.equalsIgnoreCase("1.0.1.6.7.255") && atr.equalsIgnoreCase("2")) {
					kw_TZ7 = GenericExceptionTracker.exceptionPayload("kw_TZ7", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.1.6.7.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ7 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.1.6.8.255") && atr.equalsIgnoreCase("2")) {
					kw_TZ8 = GenericExceptionTracker.exceptionPayload("kw_TZ8", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} 
				
				else if (obc.equalsIgnoreCase("1.0.1.6.8.255") && atr.equalsIgnoreCase("5")) {
					dateKwTZ8 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				
				else if (obc.equalsIgnoreCase("1.0.9.6.0.255") && atr.equalsIgnoreCase("2")) {
					kva = GenericExceptionTracker.exceptionPayload("kva", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.9.6.0.255") && atr.equalsIgnoreCase("5")) {
					dateKva = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.1.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ1 = GenericExceptionTracker.exceptionPayload("kva_TZ1", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.9.6.1.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ1 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.2.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ2 = GenericExceptionTracker.exceptionPayload("kva_TZ2", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.2.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ2 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.3.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ3 = GenericExceptionTracker.exceptionPayload("kva_TZ3", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.3.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ3 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.4.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ4 = GenericExceptionTracker.exceptionPayload("kva_TZ4", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.9.6.4.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ4 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.5.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ5 = GenericExceptionTracker.exceptionPayload("kva_TZ5", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.9.6.5.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ5 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.6.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ6 = GenericExceptionTracker.exceptionPayload("kva_TZ6", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.9.6.6.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ6 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.7.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ7 = GenericExceptionTracker.exceptionPayload("kva_TZ7", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.9.6.7.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ7 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.9.6.8.255") && atr.equalsIgnoreCase("2")) {
					kva_TZ8 = GenericExceptionTracker.exceptionPayload("kva_TZ8", obc, preVdt[r].getEnergyQuantity(),
							scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.9.6.8.255") && atr.equalsIgnoreCase("5")) {
					dateKvaTZ8 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
			

				else if (obc.equalsIgnoreCase("0.0.94.91.13.255")) {
					String eq = preVdt[r].getEnergyQuantity();
					bill_power_on_duration = (eq != null && !eq.trim().isEmpty()) ? eq.trim().split("\\s+")[0] : null;
				} else if (obc.equalsIgnoreCase("1.0.2.8.0.255")) {
					String eq = preVdt[r].getEnergyQuantity();
					bill_kwh_export = (eq != null && !eq.trim().isEmpty()) ? eq.trim().split("\\s+")[0] : null;
				} else if (obc.equalsIgnoreCase("1.0.10.8.0.255")) {
					String eq = preVdt[r].getEnergyQuantity();
					bill_kvah_export = (eq != null && !eq.trim().isEmpty()) ? eq.trim().split("\\s+")[0] : null;
				} else if (obc.equalsIgnoreCase("1.0.5.8.0.255")) {
					String eq = preVdt[r].getEnergyQuantity();
					reactive_imp_active_imp = (eq != null && !eq.trim().isEmpty()) ? eq.trim().split("\\s+")[0] : null;
				} else if (obc.equalsIgnoreCase("1.0.6.8.0.255")) {
					String eq = preVdt[r].getEnergyQuantity();
					reactive_imp_active_exp = (eq != null && !eq.trim().isEmpty()) ? eq.trim().split("\\s+")[0] : null;
				} else if (obc.equalsIgnoreCase("1.0.7.8.0.255")) {
					String eq = preVdt[r].getEnergyQuantity();
					reactive_exp_active_exp = (eq != null && !eq.trim().isEmpty()) ? eq.trim().split("\\s+")[0] : null;
				} else if (obc.equalsIgnoreCase("1.0.8.8.0.255")) {
					String eq = preVdt[r].getEnergyQuantity();
					reactive_exp_active_imp = (eq != null && !eq.trim().isEmpty()) ? eq.trim().split("\\s+")[0] : null;
				}
				/// New 102 Clm Addintion

				else if (obc.equalsIgnoreCase("1.0.129.8.0.255")) {
					// fundamental_kwh=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					fundamental_kwh = GenericExceptionTracker.exceptionPayload("fundamental_kwh", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}
				//// kVAr_Q1
				else if (obc.equalsIgnoreCase("1.0.5.8.1.255")) {
					// kVArh_Q1_T1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T1 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.2.255")) {
					// kVArh_Q1_T2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T2 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.3.255")) {
					// kVArh_Q1_T3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T3 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.4.255")) {
					// kVArh_Q1_T4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T4 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.5.255")) {
					// kVArh_Q1_T5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T5 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.6.255")) {
					// kVArh_Q1_T6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T6 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.7.255")) {
					// kVArh_Q1_T7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T7 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.5.8.8.255")) {
					// kVArh_Q1_T8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q1_T8 = GenericExceptionTracker.exceptionPayload("kVArh_Q1_T8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				//// kVAr_Q4
				else if (obc.equalsIgnoreCase("1.0.8.8.1.255")) {
					// kVArh_Q4_T1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T1 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.2.255")) {
					// kVArh_Q4_T2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T2 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.3.255")) {
					// kVArh_Q4_T3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T3 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.4.255")) {
					// kVArh_Q4_T4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T4 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.5.255")) {
					// kVArh_Q4_T5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T5 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.6.255")) {
					// kVArh_Q4_T6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T6 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.7.255")) {
					// kVArh_Q4_T7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T7 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.8.8.8.255")) {
					// kVArh_Q4_T8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kVArh_Q4_T8 = GenericExceptionTracker.exceptionPayload("kVArh_Q4_T8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.0.255")) {
					// sys_pf_exp_billing=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_billing = GenericExceptionTracker.exceptionPayload("sys_pf_exp_billing", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				//// kwh_exp
				else if (obc.equalsIgnoreCase("1.0.2.8.1.255")) {
					// kwh_exp_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz1 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.2.255")) {
					// kwh_exp_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz2 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.3.255")) {
					// kwh_exp_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz3 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.4.255")) {
					// kwh_exp_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz4 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.5.255")) {
					// kwh_exp_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz5 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.6.255")) {
					// kwh_exp_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz6 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.7.255")) {
					// kwh_exp_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz7 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.2.8.8.255")) {
					// kwh_exp_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kwh_exp_tz8 = GenericExceptionTracker.exceptionPayload("kwh_exp_tz8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				//// kvah_exp
				else if (obc.equalsIgnoreCase("1.0.10.8.1.255")) {
					// kvah_exp_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz1 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.2.255")) {
					// kvah_exp_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz2 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.3.255")) {
					// kvah_exp_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz3 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.4.255")) {
					// kvah_exp_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz4 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.5.255")) {
					// kvah_exp_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz5 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.6.255")) {
					// kvah_exp_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz6 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.7.255")) {
					// kvah_exp_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz7 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.8.8.255")) {
					// kvah_exp_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvah_exp_tz8 = GenericExceptionTracker.exceptionPayload("kvah_exp_tz8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				//// MD_kw_exp & MD_kva_exp
//				else if (obc.equalsIgnoreCase("1.0.2.6.0.255")) {
//					String str = preVdt[r].getEnergyQuantity();
//					if (str.equals("")) {
//						MD_date_kw_exp=null;
//					} else if (str.contains("+")) {
//						MD_date_kw_exp="'"+(str)+"'";
//					} else if(str.substring(str.length() - 2, str.length()).equalsIgnoreCase("Wh")) {
//						//MD_kw_exp=Double.parseDouble(str.split("\\s+")[0]);
//						MD_kw_exp=GenericExceptionTracker.exceptionPayload("MD_kw_exp",obc, preVdt[r].getEnergyQuantity(), scMM.getSeqId(),ResponseParser.type, smm, hesName);
//					}
//					
//				}

				else if (obc.equalsIgnoreCase("1.0.2.6.0.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp = GenericExceptionTracker.exceptionPayload("MD_kw_exp", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} 
				
				else if (obc.equalsIgnoreCase("1.0.2.6.0.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.10.6.0.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp = GenericExceptionTracker.exceptionPayload("MD_kva_exp", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.10.6.0.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				//// sys_pf
				else if (obc.equalsIgnoreCase("1.0.13.0.1.255")) {
					// sys_pf_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz1 = GenericExceptionTracker.exceptionPayload("sys_pf_tz1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.0.2.255")) {
					// sys_pf_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz2 = GenericExceptionTracker.exceptionPayload("sys_pf_tz2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.0.3.255")) {
					// sys_pf_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz3 = GenericExceptionTracker.exceptionPayload("sys_pf_tz3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.0.4.255")) {
					// sys_pf_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz4 = GenericExceptionTracker.exceptionPayload("sys_pf_tz4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.0.5.255")) {
					// sys_pf_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz5 = GenericExceptionTracker.exceptionPayload("sys_pf_tz5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.0.6.255")) {
					// sys_pf_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz6 = GenericExceptionTracker.exceptionPayload("sys_pf_tz6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.0.7.255")) {
					// sys_pf_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz7 = GenericExceptionTracker.exceptionPayload("sys_pf_tz7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.13.0.8.255")) {
					// sys_pf_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_tz8 = GenericExceptionTracker.exceptionPayload("sys_pf_tz8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				//// sys_pf_exp
				else if (obc.equalsIgnoreCase("1.0.84.0.1.255")) {
					// sys_pf_exp_tz1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz1 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.2.255")) {
					// sys_pf_exp_tz2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz2 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.3.255")) {
					// sys_pf_exp_tz3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz3 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.4.255")) {
					// sys_pf_exp_tz4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz4 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.5.255")) {
					// sys_pf_exp_tz5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz5 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.6.255")) {
					// sys_pf_exp_tz6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz6 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.7.255")) {
					// sys_pf_exp_tz7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz7 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.84.0.8.255")) {
					// sys_pf_exp_tz8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					sys_pf_exp_tz8 = GenericExceptionTracker.exceptionPayload("sys_pf_exp_tz8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				//// kvarh_lead_exp
				else if (obc.equalsIgnoreCase("1.0.6.8.1.255")) {
					// kvarh_q2_t1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t1 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.2.255")) {
					// kvarh_q2_t2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t2 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.3.255")) {
					// kvarh_q2_t3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t3 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.4.255")) {
					// kvarh_q2_t4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t4 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.5.255")) {
					// kvarh_q2_t5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t5 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.6.255")) {
					// kvarh_q2_t6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t6 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.7.255")) {
					// kvarh_q2_t7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t7 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.6.8.8.255")) {
					// kvarh_q2_t8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q2_t8 = GenericExceptionTracker.exceptionPayload("kvarh_q2_t8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}

				//// kvarh_lag_exp
				else if (obc.equalsIgnoreCase("1.0.7.8.1.255")) {
					// kvarh_q3_t1=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t1 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.2.255")) {
					// kvarh_q3_t2=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t2 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.3.255")) {
					// kvarh_q3_t3=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t3 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.4.255")) {
					// kvarh_q3_t4=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t4 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.5.255")) {
					// kvarh_q3_t5=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t5 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.6.255")) {
					// kvarh_q3_t6=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t6 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.7.255")) {
					// kvarh_q3_t7=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t7 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				} else if (obc.equalsIgnoreCase("1.0.7.8.8.255")) {
					// kvarh_q3_t8=Double.parseDouble(preVdt[r].getEnergyQuantity().split("\\s+")[0]);
					kvarh_q3_t8 = GenericExceptionTracker.exceptionPayload("kvarh_q3_t8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}
				else if (obc.equalsIgnoreCase("1.0.2.6.1.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz1 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.1.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz1 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				else if (obc.equalsIgnoreCase("1.0.2.6.2.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz2 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.2.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz2 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				else if (obc.equalsIgnoreCase("1.0.2.6.3.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz3 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.3.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz3 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				else if (obc.equalsIgnoreCase("1.0.2.6.4.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz4 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.4.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz4 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.2.6.5.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz5 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.5.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz5 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}	

				else if (obc.equalsIgnoreCase("1.0.2.6.6.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz6 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.6.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz6 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}	
				else if (obc.equalsIgnoreCase("1.0.2.6.7.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz7 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.7.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz7 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.2.6.8.255") && atr.equalsIgnoreCase("2")) {
					MD_kw_exp_tz8 = GenericExceptionTracker.exceptionPayload("MD_kw_exp_tz8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.2.6.8.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kw_exp_tz8 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}	

				else if (obc.equalsIgnoreCase("1.0.10.6.1.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz1 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz1", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.1.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz1 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.10.6.2.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz2 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz2", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.2.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz2 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.10.6.3.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz3 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz3", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.3.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz3 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}	
				else if (obc.equalsIgnoreCase("1.0.10.6.4.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz4 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz4", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.4.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz4 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}
				else if (obc.equalsIgnoreCase("1.0.10.6.5.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz5 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz5", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.5.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz5 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}

				else if (obc.equalsIgnoreCase("1.0.10.6.6.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz6 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz6", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.6.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz6 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}	

				else if (obc.equalsIgnoreCase("1.0.10.6.7.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz7 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz7", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.7.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz7 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}	

				else if (obc.equalsIgnoreCase("1.0.10.6.8.255") && atr.equalsIgnoreCase("2")) {
					MD_kva_exp_tz8 = GenericExceptionTracker.exceptionPayload("MD_kva_exp_tz8", obc,
							preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
				}else if (obc.equalsIgnoreCase("1.0.10.6.8.255") && atr.equalsIgnoreCase("5")) {
					MD_date_kva_exp_tz8 = parseOffsetDate(preVdt[r].getEnergyQuantity());
				}	

			}	
			
			if (GenericExceptionTracker.hasExceptions()) {
				hasExcpData = true;
				List<String> exceptions = exceptionMap.getOrDefault(scMM.getSeqId(), new ArrayList<>());
				String combinedExceptions = String.join(",", exceptions);
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + ResponseParser.type + "','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + smm.replace("'", "''")
						+ "','" + combinedExceptions + "','" + hesName + "'),");

				GenericExceptionTracker.sendExpToDlq(smm.replace("'", "''").toString());
			}else {
				
			////// Other Project(MCL)
				builder.append("('" + scMM.getSeqId() + "', now(),'" + scMM.getDeviceId() + "','" + billing_date + "','" + sys_PF_Billing + "',"
						+ " " + kwh + "," + kwh_TZ1 + "," + kwh_TZ2 + ", " + kwh_TZ3 + ", " + kwh_TZ4 + ", " + kwh_TZ5 + ", " + kwh_TZ6 + ", " + kwh_TZ7 + ", " + kwh_TZ8 + ","
						+ " " + kvah + "," + kvah_TZ1 + "," + kvah_TZ2 + "," + kvah_TZ3 + ", " + kvah_TZ4 + ", " + kvah_TZ5 + ", " + kvah_TZ6 + ", " + kvah_TZ7 + ", " + kvah_TZ8 + ", " + occDateKw + ", " + demand_kw + ","
						+ " " + kw_TZ1 + "," + kw_TZ2 + ", " + kw_TZ3 + ", " + kw_TZ4 + ", " + kw_TZ5 + ", " + kw_TZ6 + ", " + kw_TZ7 + ", " + kw_TZ8 + ","
						+ " " + dateKwTZ1 + "," + dateKwTZ2 + ", " + dateKwTZ3 + ", " + dateKwTZ4 + ", " + dateKwTZ5 + ", " + dateKwTZ6 + ", " + dateKwTZ7 + ", " + dateKwTZ8 + ","
						+ " " + kva + "," + dateKva + ", " + kva_TZ1 + ", " + kva_TZ2 + ", " + kva_TZ3 + ", " + kva_TZ4 + ", " + kva_TZ5 + ", " + kva_TZ6 + "," + kva_TZ7 + "," + kva_TZ8 + ","
						+ " " + dateKvaTZ1 + "," + dateKvaTZ2 + ", " + dateKvaTZ3 + ", " + dateKvaTZ4 + ", " + dateKvaTZ5 + ", " + dateKvaTZ6 + ", " + dateKvaTZ7 + ", " + dateKvaTZ8 + ","	
						+ " '" + bill_power_on_duration + "','" + bill_kwh_export + "', '" + bill_kvah_export + "', " + reactive_imp_active_imp + ", " + reactive_imp_active_exp + ", " + reactive_exp_active_exp + ", " + reactive_exp_active_imp + ",'"+create_time+"',"
						+ " " + fundamental_kwh + "," + kVArh_Q1_T1 + ", " + kVArh_Q1_T2 + ", " + kVArh_Q1_T3 + ", " + kVArh_Q1_T4 + ", " + kVArh_Q1_T5 + ", " + kVArh_Q1_T6 + ", " + kVArh_Q1_T7 + ", " + kVArh_Q1_T8 + ","
						+ " " + kVArh_Q4_T1 + "," + kVArh_Q4_T2 + ", " + kVArh_Q4_T3 + ", " + kVArh_Q4_T4 + ", " + kVArh_Q4_T5 + ", " + kVArh_Q4_T6 + ", " + kVArh_Q4_T7 + ", " + kVArh_Q4_T8 + ", " + sys_pf_exp_billing + ","
						+ " " + kwh_exp_tz1 + "," + kwh_exp_tz2 + ", " + kwh_exp_tz3 + ", " + kwh_exp_tz4 + ", " + kwh_exp_tz5 + ", " + kwh_exp_tz6 + ", " + kwh_exp_tz7 + ", " + kwh_exp_tz8 + ","
						+ " " + kvah_exp_tz1 + "," + kvah_exp_tz2 + ", " + kvah_exp_tz3 + ", " + kvah_exp_tz4 + ", " + kvah_exp_tz5 + ", " + kvah_exp_tz6 + ", " + kvah_exp_tz7 + ", " + kvah_exp_tz8 + ","
						+ " " + MD_kw_exp + "," + MD_date_kw_exp + ", " 
						+ " " + MD_kva_exp + "," + MD_date_kva_exp + ", " 			
						+ " " + sys_pf_tz1 + "," + sys_pf_tz2 + ", " + sys_pf_tz3 + ", " + sys_pf_tz4 + ", " + sys_pf_tz5 + ", " + sys_pf_tz6 + ", " + sys_pf_tz7 + ", " + sys_pf_tz8 + ","
						+ " " + sys_pf_exp_tz1 + "," + sys_pf_exp_tz2 + ", " + sys_pf_exp_tz3 + ", " + sys_pf_exp_tz4 + ", " + sys_pf_exp_tz5 + ", " + sys_pf_exp_tz6 + ", " + sys_pf_exp_tz7 + ", " + sys_pf_exp_tz8 + ","
						+ " " + kvarh_q3_t1 + "," + kvarh_q3_t2 + ", " + kvarh_q3_t3 + ", " + kvarh_q3_t4 + ", " + kvarh_q3_t5 + ", " + kvarh_q3_t6 + ", " + kvarh_q3_t7 + ", " + kvarh_q3_t8 + ","
						+ " " + kvarh_q2_t1 + "," + kvarh_q2_t2 + ", " + kvarh_q2_t3 + ", " + kvarh_q2_t4 + ", " + kvarh_q2_t5 + ", " + kvarh_q2_t6 + ", " + kvarh_q2_t7 + ", " + kvarh_q2_t8 + ","
						+ " " + MD_kw_exp_tz1 + "," + MD_date_kw_exp_tz1 + ", " + MD_kw_exp_tz2 + ", " + MD_date_kw_exp_tz2 + ", " + MD_kw_exp_tz3 + ", " + MD_date_kw_exp_tz3 + ", " + MD_kw_exp_tz4 + ", " + MD_date_kw_exp_tz4 + ","
						+ " " + MD_kw_exp_tz5 + "," + MD_date_kw_exp_tz5 + ", " + MD_kw_exp_tz6 + ", " + MD_date_kw_exp_tz6 + ", " + MD_kw_exp_tz7 + ", " + MD_date_kw_exp_tz7 + ", " + MD_kw_exp_tz8 + ", " + MD_date_kw_exp_tz8 + ","
						+ " " + MD_kva_exp_tz1 + "," + MD_date_kva_exp_tz1 + ", " + MD_kva_exp_tz2 + ", " + MD_date_kva_exp_tz2 + ", " + MD_kva_exp_tz3 + ", " + MD_date_kva_exp_tz3 + ", " + MD_kva_exp_tz4 + ", " + MD_date_kva_exp_tz4 + ","
						+ " " + MD_kva_exp_tz5 + "," + MD_date_kva_exp_tz5 + ", " + MD_kva_exp_tz6 + ", " + MD_date_kva_exp_tz6 + ", " + MD_kva_exp_tz7 + ", " + MD_date_kva_exp_tz7 + ", " + MD_kva_exp_tz8 + ", " + MD_date_kva_exp_tz8 + ",'"+hesName+"'),");
		
			}
			
				
		} catch (Exception e) {
		    hasExcpData = true;
		    String safeSmm = smm.replace("'", "''"); // escape single quotes
		    excpbuilder.append("(now(),'" + scMM.getSeqId() + "','"+ResponseParser.type+"','" + (scMM.getDeviceId() != null ? scMM.getDeviceId() :"") + "','" + safeSmm +"','REMARKS:" + e.getMessage() + "','"+ hesName + "'),");
		    e.printStackTrace();
		}
		rv=null;
		atr=null;
		resetVariable();
		
	}

}
