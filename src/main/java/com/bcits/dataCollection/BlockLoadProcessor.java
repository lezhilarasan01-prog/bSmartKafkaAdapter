package com.bcits.dataCollection;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
 * @author Remith
 *
 */
public class BlockLoadProcessor implements DataProcessor {

	private static BlockLoadProcessor processor;

	// MeterInfoManage mtrInfo = MeterInfoManage.getInstance();

	private boolean hasData;
	private boolean hasExcpData;
	private boolean hasNumExcpdata = false;
	private StringBuilder builder = new StringBuilder();
	private StringBuilder excpbuilder = new StringBuilder();
	private Gson gson;
	private static final Map<Integer, List<String>> exceptionMap = new HashMap<>();

	private BigDecimal i_r = null, i_y = null, i_b = null, v_r = null, v_y = null, v_b = null;
	private BigDecimal kwh = null, blockEnergyKwhExp = null, kvah = null, blockEnergyKvahExp = null, kva = null,
			kw = null,
			kw_Exp = null/* , kvar= null,kvarh= null */;
	private BigDecimal kvarh_q1 = null, kvarh_q2 = null, kvarh_q3 = null, kvarh_q4 = null, kvar_lag = null,
			kvar_lead = null, powerFactor = null;
	private BigDecimal averageVoltage = null, averageCurrent = null, neutralCurrent = null;
	private String Kwh = null, kwh_Exp = null, Kvah = null, kvarh_lag = null, kvarh_lead = null;

	private Timestamp read_time = null, sample_time = null, create_time = null;
	private static final ThreadLocal<SimpleDateFormat> format = ThreadLocal
			.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

	private RegisterValues[] rv;
	private String[] st, ct;
	private String obc;
	private String val;
	private List<RegisterValue> dt;
	private PreVEEData[] preVdt;

	public static BlockLoadProcessor getInstance() {
		if (processor == null)
			processor = new BlockLoadProcessor();
		return processor;
	}

	@Override
	public void init() {
		builder.setLength(0);
		builder.append("insert into meter_data.load_survey "
				+ "(time_stamp, flag, sample_or_server_time, create_time, meter_number, read_time, i_r, i_y, i_b, v_r, v_y, v_b,"
				+ "kwh, block_energy_kwh_exp, kvarh_lag, kvarh_q1, kvarh_q2, kvarh_q3, kvarh_q4, kvarh_lead ,"
				+ "kvah, block_energy_kvah_exp, average_voltage, average_current, neutral_current, "
				+ "kw, kva, kvar_lag, kvar_lead, power_factor, kw_exp,hes_type) values ");
		excpbuilder.setLength(0);
		excpbuilder.append(
				"insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");
		hasData = false;
		hasExcpData = false;
	}

	@Override
	public void addData(DeviceData scMM, String hesName) {
		try {
			generateBulkLoadBatchData(scMM, hesName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getQuery() {
		if (builder.length() > 0 && builder.charAt(builder.length() - 1) == ',') {
			return builder.substring(0, builder.length() - 1) + " ON CONFLICT DO NOTHING";
		}
		return builder.toString();
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

	private void generateBulkLoadBatchData(DeviceData scMM, String hesName) throws ParseException {

		int mf = 2;

		try {
			Map<String, Object[]> data = null;// HESintegration.mtrInfo.getMtrInfo().getMtr_ip_period();
			Object[] mtrdata = data.get(scMM.getDeviceId());
			String mtr = mtrdata[0] + "";
			String interval = mtrdata[1] + "";
			if (interval.equalsIgnoreCase("30")) {
				mf = 2;
			} else {
				mf = 4;
			}
		} catch (Exception e) {
			mf = 2;
		}
		preVdt = scMM.getPreVEE();

		String hesDt = scMM.getHesDateTime();
		sample_time = parseTimestamp(hesDt, new Timestamp(System.currentTimeMillis()));
		create_time = sample_time;

		gson = new Gson();
		String smm = gson.toJson(scMM, new TypeToken<DeviceData>() {
		}.getType());
		try {
			GenericExceptionTracker.reset();
			if (preVdt != null) {
				for (int r = 0; r < preVdt.length; r++) {
					if (preVdt[r] == null)
						continue;
					obc = preVdt[r].getRegisterId();
					if (obc == null)
						continue;

					if (obc.equalsIgnoreCase("0.0.1.0.0.255") || obc.equalsIgnoreCase("0.0.0.1.0.255")
							|| obc.equalsIgnoreCase("1.0.0.1.2.255") || obc.equalsIgnoreCase("0.0.99.1.0.255")) {
						String eq = preVdt[r].getEnergyQuantity();
						if (eq != null && !eq.trim().isEmpty()) {
							read_time = parseTimestamp(eq, null);
						}
					} else if (obc.equalsIgnoreCase("1.0.31.27.0.255") || obc.equalsIgnoreCase("1.0.31.7.0.255")) {
						i_r = GenericExceptionTracker.exceptionPayload("i_r", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.51.27.0.255") || obc.equalsIgnoreCase("1.0.51.7.0.255")) {
						i_y = GenericExceptionTracker.exceptionPayload("i_y", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.71.27.0.255") || obc.equalsIgnoreCase("1.0.71.7.0.255")) {
						i_b = GenericExceptionTracker.exceptionPayload("i_b", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.32.27.0.255") || obc.equalsIgnoreCase("1.0.32.7.0.255")) {
						v_r = GenericExceptionTracker.exceptionPayload("v_r", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.52.27.0.255") || obc.equalsIgnoreCase("1.0.52.7.0.255")) {
						v_y = GenericExceptionTracker.exceptionPayload("v_y", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.72.27.0.255") || obc.equalsIgnoreCase("1.0.72.7.0.255")) {
						v_b = GenericExceptionTracker.exceptionPayload("v_b", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.1.29.0.255")) {
						kwh = GenericExceptionTracker.exceptionPayload("kwh", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.2.29.0.255")) {
						String eq = preVdt[r].getEnergyQuantity();
						kwh_Exp = eq != null ? eq.split("\\s+")[0] : null;
						blockEnergyKwhExp = GenericExceptionTracker.exceptionPayload("blockEnergyKwhExp", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.5.29.0.255")) {
						String eq = preVdt[r].getEnergyQuantity();
						kvarh_lag = eq != null ? eq.split("\\s+")[0] : null;
						kvarh_q1 = GenericExceptionTracker.exceptionPayload("kvarh_q1", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.6.29.0.255")) {
						kvarh_q2 = GenericExceptionTracker.exceptionPayload("kvarh_q2", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.7.29.0.255")) {
						kvarh_q3 = GenericExceptionTracker.exceptionPayload("kvarh_q3", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.8.29.0.255")) {
						kvarh_q4 = GenericExceptionTracker.exceptionPayload("kvarh_q4", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
						String eq = preVdt[r].getEnergyQuantity();
						kvarh_lead = eq != null ? eq.split("\\s+")[0] : null;
					} else if (obc.equalsIgnoreCase("1.0.9.29.0.255")) {
						kvah = GenericExceptionTracker.exceptionPayload("kvah", obc, preVdt[r].getEnergyQuantity(),
								scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.10.29.0.255")) {
						blockEnergyKvahExp = GenericExceptionTracker.exceptionPayload("blockEnergyKvahExp", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.12.27.0.255") || obc.equalsIgnoreCase("1.0.12.7.0.255")
							|| obc.equalsIgnoreCase("1.0.12.0.0.255")) {
						averageVoltage = GenericExceptionTracker.exceptionPayload("averageVoltage", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.11.27.0.255") || obc.equalsIgnoreCase("1.0.11.7.0.255")
							|| obc.equalsIgnoreCase("1.0.11.0.0.255")) {
						averageCurrent = GenericExceptionTracker.exceptionPayload("averageCurrent", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					} else if (obc.equalsIgnoreCase("1.0.91.27.0.255") || obc.equalsIgnoreCase("1.0.91.7.0.255")) {
						neutralCurrent = GenericExceptionTracker.exceptionPayload("neutralCurrent", obc,
								preVdt[r].getEnergyQuantity(), scMM.getSeqId(), ResponseParser.type, smm, hesName);
					}

					obc = null;
				}
			}

			// Fallback hierarchy for read_time if not provided in preVEE registers
			if (read_time == null && scMM.getMeterDateTime() != null && !scMM.getMeterDateTime().trim().isEmpty()) {
				read_time = parseTimestamp(scMM.getMeterDateTime(), null);
			}
			if (read_time == null && scMM.getHesDateTime() != null && !scMM.getHesDateTime().trim().isEmpty()) {
				read_time = parseTimestamp(scMM.getHesDateTime(), null);
			}
			if (read_time == null) {
				read_time = sample_time;
			}

			// Derived power calculations
			if (kwh != null) {
				kw = kwh.multiply(BigDecimal.valueOf(mf));
			}
			if (kvah != null) {
				kva = kvah.multiply(BigDecimal.valueOf(mf));
			}
			if (kvarh_lag != null && kvarh_q1 != null) {
				kvar_lag = kvarh_q1.multiply(BigDecimal.valueOf(mf));
			}
			if (kvarh_lead != null && kvarh_q4 != null) {
				kvar_lead = kvarh_q4.multiply(BigDecimal.valueOf(mf));
			}
			if (kwh != null && kvah != null && kvah.compareTo(BigDecimal.ZERO) != 0) {
				powerFactor = kwh.divide(kvah, 4, RoundingMode.HALF_UP);
			}
			if (kwh_Exp != null && blockEnergyKwhExp != null) {
				kw_Exp = blockEnergyKwhExp.multiply(BigDecimal.valueOf(mf));
			}

			if (GenericExceptionTracker.hasExceptions()) {
				hasExcpData = true;
				List<String> exceptions = GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
				String combinedExceptions = String.join(",", exceptions);
				String pType = ResponseParser.type != null ? ResponseParser.type : "Bulk Load";
				excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + pType + "','"
						+ (scMM.getDeviceId() != null ? scMM.getDeviceId().trim() : "") + "','" + smm.replace("'", "''")
						+ "','"
						+ combinedExceptions + "','" + (hesName != null ? hesName : "") + "'),");
				GenericExceptionTracker.sendExpToDlq(smm.replace("'", "''"));
			} else {
				String readTimeSql = read_time != null ? "'" + read_time + "'" : "NULL";
				String sampleTimeSql = sample_time != null ? "'" + sample_time + "'" : "now()";
				String createTimeSql = create_time != null ? "'" + create_time + "'" : "now()";
				String devId = scMM.getDeviceId() != null ? scMM.getDeviceId().trim() : "";

				builder.append("( now(),'" + scMM.getSeqId() + "', " + sampleTimeSql + ", " + createTimeSql + ", '"
						+ devId + "', " + readTimeSql + ","
						+ i_r + ", " + i_y + ", " + i_b + ", " + v_r + ", " + v_y + ", " + v_b + ","
						+ kwh + ", " + blockEnergyKwhExp + ", " + kvarh_q1 + ", " + kvarh_q1 + "," + kvarh_q2 + ","
						+ kvarh_q3 + "," + kvarh_q4 + "," + kvarh_q4 + ","
						+ kvah + ", " + blockEnergyKvahExp + "," + averageVoltage + "," + averageCurrent + ","
						+ neutralCurrent + ","
						+ kw + ", " + kva + "," + kvar_lag + "," + kvar_lead + "," + powerFactor + "," + kw_Exp + ",'"
						+ (hesName != null ? hesName : "") + "'),");
				hasData = true;
			}

		} catch (Exception e) {
			hasExcpData = true;
			String safeSmm = smm.replace("'", "''");
			String pType = ResponseParser.type != null ? ResponseParser.type : "Bulk Load";
			excpbuilder.append("(now(),'" + scMM.getSeqId() + "','" + pType + "','"
					+ (scMM.getDeviceId() != null ? scMM.getDeviceId().trim() : "") + "','" + safeSmm + "','REMARKS:"
					+ (e.getMessage() != null ? e.getMessage().replace("'", "''") : "Error") + "','"
					+ (hesName != null ? hesName : "") + "'),");
			e.printStackTrace();
		}

		rv = null;
		st = null;
		ct = null;
		resetVariable();
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
		kwh = null;
		blockEnergyKwhExp = null;
		kvah = null;
		blockEnergyKvahExp = null;
		kva = null;
		kw = null;
		kw_Exp = null;
		kvarh_q1 = null;
		kvarh_q2 = null;
		kvarh_q3 = null;
		kvarh_q4 = null;
		kvar_lag = null;
		kvar_lead = null;
		powerFactor = null;
		averageVoltage = null;
		averageCurrent = null;
		neutralCurrent = null;
		Kwh = null;
		kwh_Exp = null;
		Kvah = null;
		kvarh_lag = null;
		kvarh_lead = null;
		read_time = null;
		sample_time = null;
		create_time = null;

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
				System.out.println("The value is within the range of a double.");
				return number / 1000;
			}
			// DecimalFormat decimalFormat = new DecimalFormat("#.###");
			//
			// // Parse the String and convert it to a double value
			// return decimalFormat.parse(value).doubleValue();

		} catch (Exception e) {
			return null;
		}
	}

}
