/**
 * 
 */
package com.bcits.dataCollection;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

import com.bcits.dataModel.BHESnewSeqData;
import com.bcits.dataModel.DeviceData;
import com.bcits.dataModel.DeviceData.PreVEEData;
import com.bcits.utils.GenericExceptionTracker;
import com.bcits.dataModel.RegisterValue;
import com.bcits.dataModel.RegisterValues;
import com.bcits.dataModel.SchCrystalMeterDataMain;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author TARIK
 *
 */
public class NamePlateProcessor implements DataProcessor{
	
	
	
	private static NamePlateProcessor processor;
	private Timestamp read_time=null ,server_time=null;
	private boolean hasData;
	private boolean hasExcpData;

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

	private StringBuilder builder = new StringBuilder();
	private StringBuilder excpbuilder = new StringBuilder();
	private Gson gson;
	private String meter_serial_number=null,device_id=null,manufacturer_name=null,firmware_version=null,meter_type = null;
	private String meter_catagory = null,current_rating=null,year_of_manufacture=null,ct_ratio=null,pt_ratio=null;
	
	private RegisterValues[] rv;
//	private String[] st,ct;
	private String obc;
	private String val;
	private  List<RegisterValue> dt;
	private String dType;
	private PreVEEData[] preVdt;
	
	public static NamePlateProcessor getInstance() {
		if(processor==null)
			processor = new NamePlateProcessor();
		return processor;
	}

	@Override
	public void init() {
		builder.setLength(0);
		builder.append("insert into meter_data.name_plate "
				+ "(created_time, nflag, meter_serial_number, device_id, manufacturer_name, firmware_version, meter_type,"
				+ " meter_catagory, current_rating, year_of_manufacture,server_time,ct_ratio,pt_ratio,hes_type) values ");	
		excpbuilder.setLength(0);
		excpbuilder.append("insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type, data) values ");

	}

//	@Override
//	public void addData(DeviceData scMM, String hesName) {
//		try {
//			generateNamePlateBatchData(scMM,hesName);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} //Parse Data & Generate Batch Query	
//		hasData = true;
//	}
	
	
	@Override
	public void addData(DeviceData scMM, String hesName) {

		try {
			if (scMM == null) {
				System.out.println("DeviceData is null");
				return;
			}
			GenericExceptionTracker.reset();
			if (scMM.getPreVEE() == null || scMM.getPreVEE().length == 0) {
				hasExcpData = true;
				Gson gson = new Gson();
				String smmJson = gson.toJson(scMM);
				String safeSmm = smmJson.replace("'", "''");
				String remarks = "REMARKS: preVEE payload is empty";
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','NamePlate','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + remarks
						+ "','" + hesName + "'),");
				return;
			} else {
				generateNamePlateBatchData(scMM, hesName);
				hasData = true;
			}
		} catch (ParseException e) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = e.getMessage() == null ? "Parse Exception" : e.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','NamePlate','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "',hes),");
			e.printStackTrace();
		} catch (Exception ex) {
			hasExcpData = true;
			Gson gson = new Gson();
			String safeSmm = gson.toJson(scMM).replace("'", "''");
			String safeMsg = ex.getMessage() == null ? "Unknown Exception" : ex.getMessage().replace("'", "''");
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','NamePlate','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" + safeSmm + "','" + safeMsg + "',hes),");
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
	
	
	private void generateNamePlateBatchData(DeviceData scMM, String hesName) throws ParseException {		
		server_time = parseTimestamp(scMM.getHesDateTime(), new Timestamp(System.currentTimeMillis()));
		preVdt = scMM.getPreVEE();
		gson = new Gson();
		String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {}.getType());
		try {
			for (int r = 0; r < preVdt.length; r++) {
				obc = preVdt[r].getRegisterId();

				if (obc.equalsIgnoreCase("0.0.96.1.0.255")) {
//				String[] s1 = preVdt[r].getEnergyQuantity().replace("T", " ").split("\\+");
					meter_serial_number = preVdt[r].getEnergyQuantity().trim();
				}
				if (obc.equalsIgnoreCase("0.0.96.1.2.255")) {
					device_id = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.96.1.1.255")) {
					manufacturer_name = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("1.0.0.2.0.255")) {
					firmware_version = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.94.91.9.255")) {
					meter_type = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.94.91.11.255")) {
					meter_catagory = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.94.91.12.255")) {
					current_rating = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.96.1.4.255")) {
					year_of_manufacture = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("1.0.0.4.2.255")) {
					ct_ratio = formatNumericOrNull(preVdt[r].getEnergyQuantity());
				} else if (obc.equalsIgnoreCase("1.0.0.4.3.255")) {
					pt_ratio = formatNumericOrNull(preVdt[r].getEnergyQuantity());
				}
				
				obc = null;
			}
			
			String time = (server_time != null) ? "'" + server_time.toString() + "'" : "null";
			String safeCt = (ct_ratio != null && !ct_ratio.isEmpty()) ? ct_ratio : "null";
			String safePt = (pt_ratio != null && !pt_ratio.isEmpty()) ? pt_ratio : "null";
			
			builder.append("( now(),'"+ scMM.getSeqId() +"','" + meter_serial_number + "','" + device_id + "','" + manufacturer_name + "', '" + firmware_version +"', '"+ meter_type +"','"
					+ meter_catagory + "', '" + current_rating + "', '" + year_of_manufacture + "',"+time+","+safeCt+","+safePt+",'"+hesName+"'),");	
			
			

		} catch (Exception e) {
			hasExcpData=true;
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','Bulk Load','" + smm.toString() + "'),");
			// e.printStackTrace();
		}
		
		rv=null;
//		st=null;
//		ct=null;
		resetVariable();
	}
	
	
	private void generateCrystalNamePlateBatchData(SchCrystalMeterDataMain scMM, String flag) throws ParseException {		

		rv = scMM.getRegisterValues();
		gson = new Gson();
		String smm = gson.toJson(scMM, new TypeToken<SchCrystalMeterDataMain>() {}.getType());
		String st = scMM.getServerTime();
		server_time = parseTimestamp(st != null ? zonedDateTimeConverter(st) : null, new Timestamp(System.currentTimeMillis()));

		try {
			for (int r = 0; r < rv.length; r++) {
				obc = rv[r].getFormattedRegisterObisCode();

				if (obc.equalsIgnoreCase("0.0.96.1.0.255")) {
//				String[] s1 = preVdt[r].getEnergyQuantity().replace("T", " ").split("\\+");
					meter_serial_number = preVdt[r].getEnergyQuantity().trim();
				}
				if (obc.equalsIgnoreCase("0.0.96.1.2.255")) {
					device_id = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.96.1.1.255")) {
					manufacturer_name = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("1.0.0.2.0.255")) {
					firmware_version = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.94.91.9.255")) {
					meter_type = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.94.91.11.255")) {
					meter_catagory = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.94.91.12.255")) {
					current_rating = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("0.0.96.1.4.255")) {
					year_of_manufacture = preVdt[r].getEnergyQuantity().trim();
				} else if (obc.equalsIgnoreCase("1.0.0.4.2.255")) {
					ct_ratio = formatNumericOrNull(preVdt[r].getEnergyQuantity());
				} else if (obc.equalsIgnoreCase("1.0.0.4.3.255")) {
					pt_ratio = formatNumericOrNull(preVdt[r].getEnergyQuantity());
				}

				obc = null;
			}
			String safeCt = (ct_ratio != null && !ct_ratio.isEmpty()) ? ct_ratio : "null";
			String safePt = (pt_ratio != null && !pt_ratio.isEmpty()) ? pt_ratio : "null";
			if(flag.equalsIgnoreCase("SCH")) {
			//For Schedule Parsing 
			builder.append("( now(),'"+ scMM.getMeterSampleId() +"','" + meter_serial_number + "','" + device_id + "','" + manufacturer_name + "', '" + firmware_version +"', '"+ meter_type +"','"
					+ meter_catagory + "', '" + current_rating + "', '" + year_of_manufacture + "','"+server_time+"',"+safeCt+","+safePt+"),");		
			}else if(flag.equalsIgnoreCase("OD")) {
			// FOR OD data parsing
			builder.append("( now(),'OD','" + meter_serial_number + "','" + device_id + "','" + manufacturer_name + "', '" + firmware_version +"', '"+ meter_type +"','"
					+ meter_catagory + "', '" + current_rating + "', '" + year_of_manufacture + "','"+server_time+"',"+safeCt+","+safePt+"),");	
			}
		} catch (Exception e) {
			hasExcpData=true;
			excpbuilder.append("(now(),'" + scMM.getMeterSampleId() + "','NamePlate','" + smm.toString() + "'),");
			 e.printStackTrace();
		}
		
		rv=null;
//		st=null;
//		ct=null;
		resetVariable();
	}
	
	public void resetVariable() {
		meter_serial_number=null;device_id=null;manufacturer_name=null;firmware_version=null;meter_type = null;
		meter_catagory = null;current_rating=null;year_of_manufacture=null;
		ct_ratio=null;pt_ratio=null;server_time=null;
	}

	private void generateBHESNamePlateBatchData(BHESnewSeqData scMM) throws ParseException {		

		dt = scMM.getRegisterValues();
		for (int r = 0; r < dt.size(); r++) {
			obc = dt.get(r).getRegisterObisCode();
			 val=dt.get(r).getValue();
			 dType=dt.get(r).getAttributeId();
			
			if (obc.equalsIgnoreCase("0.0.96.1.0.255")) {
//				String[] s1 = preVdt[r].getEnergyQuantity().replace("T", " ").split("\\+");
				meter_serial_number=val.trim();
			}
			if (obc.equalsIgnoreCase("0.0.96.1.2.255")) {
				device_id=val;
			} else if (obc.equalsIgnoreCase("0.0.96.1.1.255")) {
				manufacturer_name=val.trim();
			} else if (obc.equalsIgnoreCase("1.0.0.2.0.255")) {
				firmware_version=val.trim();
			} else if (obc.equalsIgnoreCase("0.0.94.91.9.255")) {
				meter_type=val.trim();
			} else if (obc.equalsIgnoreCase("0.0.94.91.11.255")) {
				meter_catagory=val.trim();
			} else if (obc.equalsIgnoreCase("0.0.94.91.12.255")) {
				current_rating=val.trim();
			} else if (obc.equalsIgnoreCase("0.0.96.1.4.255")) {
				year_of_manufacture=val.trim();
				if(val.trim().length()>4) {
					year_of_manufacture=val.trim().substring(0,4);	
				}
				
			} else if (obc.equalsIgnoreCase("1.0.0.4.2.255")) {
				ct_ratio = formatNumericOrNull(val);
			} else if (obc.equalsIgnoreCase("1.0.0.4.3.255")) {
				pt_ratio = formatNumericOrNull(val);
			}
		
			
			obc=null;
		}	
		String safeCt = (ct_ratio != null && !ct_ratio.isEmpty()) ? ct_ratio : "null";
		String safePt = (pt_ratio != null && !pt_ratio.isEmpty()) ? pt_ratio : "null";
		String time = (server_time != null) ? "'" + server_time.toString() + "'" : "null";
		builder.append("( now(),'"+ scMM.getCounterId() +"','" + meter_serial_number + "','" + device_id + "','" + manufacturer_name + "', '" + firmware_version +"', '"+ meter_type +"','"
				+ meter_catagory + "', '" + current_rating + "', '" + year_of_manufacture + "',"+time+","+safeCt+","+safePt+"),");		

		rv=null;
//		st=null;
//		ct=null;
		resetVariable();
	}

	public static String formatNumericOrNull(String raw) {
		if (raw == null || raw.trim().isEmpty() || "null".equalsIgnoreCase(raw.trim())) {
			return "null";
		}
		try {
			Double d = Double.parseDouble(raw.trim());
			return String.valueOf(d);
		} catch (Exception ex) {
			return "null";
		}
	}

	public String zonedDateTimeConverter(String obisDate) {
		if (obisDate == null || obisDate.trim().isEmpty() || "null".equalsIgnoreCase(obisDate.trim())) {
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
			String timestampGMT5_30 = gmt5_30ZonedDateTime.format(formatter);
			return timestampGMT5_30;
		} catch (Exception ex) {
			return obisDate;
		}
	}

}
