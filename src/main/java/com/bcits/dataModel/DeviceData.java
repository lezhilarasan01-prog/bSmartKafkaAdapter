/**
 * 
 */
package com.bcits.dataModel;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.gson.annotations.SerializedName;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceData {

	@SerializedName(value = "hesId", alternate = {"hes_id", "HesId", "HESID", "hesType", "hes_type", "HES_TYPE"})
	@JsonAlias({"hes_id", "HesId", "HESID", "hesType", "hes_type", "HES_TYPE"})
	private String hesId;

	@SerializedName(value = "seqId", alternate = {"seq_id", "SeqId", "SEQ_ID", "sequenceNumber", "sequence_number", "meterSampleId", "meter_sample_id"})
	@JsonAlias({"seq_id", "SeqId", "SEQ_ID", "sequenceNumber", "sequence_number", "meterSampleId", "meter_sample_id"})
	private Long seqId = 0L;

	@SerializedName(value = "dvcIdN", alternate = {"deviceId", "device_id", "meterNumber", "meter_number", "dvc_id_n", "DvcIdN", "dvcId", "DvcId", "msn", "MSN", "meterNo", "meter_no", "meterSerialNo", "meter_serial_no", "meter_serial_number", "meterSerialNumber"})
	@JsonAlias({"deviceId", "device_id", "meterNumber", "meter_number", "dvc_id_n", "DvcIdN", "dvcId", "DvcId", "msn", "MSN", "meterNo", "meter_no", "meterSerialNo", "meter_serial_no", "meter_serial_number", "meterSerialNumber"})
	private String dvcIdN;

	@SerializedName(value = "HESDTTM", alternate = {"hesDateTime", "hes_datetime", "hes_date_time", "hes_time", "hesTime", "hesDttm", "HesDttm", "sample_time", "sampleTime", "server_time", "serverTime", "timestamp"})
	@JsonAlias({"hesDateTime", "hes_datetime", "hes_date_time", "hes_time", "hesTime", "hesDttm", "HesDttm", "sample_time", "sampleTime", "server_time", "serverTime", "timestamp"})
	@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	private String HESDTTM;

	@SerializedName(value = "exportDTTM", alternate = {"exportDateTime", "export_datetime", "export_date_time", "exportDttm", "ExportDTTM"})
	@JsonAlias({"exportDateTime", "export_datetime", "export_date_time", "exportDttm", "ExportDTTM"})
	private String exportDTTM;

	@SerializedName(value = "MeterDTTM", alternate = {"meterDateTime", "meter_datetime", "meter_date_time", "meter_time", "meterTime", "meterDttm", "MeterDttm", "meterDTTM"})
	@JsonAlias({"meterDateTime", "meter_datetime", "meter_date_time", "meter_time", "meterTime", "meterDttm", "MeterDttm", "meterDTTM"})
	private String MeterDTTM;

	@SerializedName(value = "profileCode", alternate = {"profileObisCode", "profile_code", "profile_obis_code", "profile", "obisCode", "obis_code", "ProfileCode", "ProfileObisCode"})
	@JsonAlias({"profileObisCode", "profile_code", "profile_obis_code", "profile", "obisCode", "obis_code", "ProfileCode", "ProfileObisCode"})
	private String profileCode;

	@SerializedName(value = "preVEE", alternate = {"PreVEE", "pre_vee", "preVee", "PREVEE", "PreVee", "prevEE", "registers", "Registers", "registerValues", "register_values", "RegisterValues", "data", "Data", "readings", "Readings", "preVeeData", "pre_vee_data", "registerData", "load_profile", "values"})
	@JsonAlias({"PreVEE", "pre_vee", "preVee", "PREVEE", "PreVee", "prevEE", "registers", "Registers", "registerValues", "register_values", "RegisterValues", "data", "Data", "readings", "Readings", "preVeeData", "pre_vee_data", "registerData", "load_profile", "values"})
	private PreVEEData[] preVEE;
	
	@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	@SerializedName(value = "alarmTime", alternate = {"alarm_time", "AlarmTime"})
	@JsonAlias({"alarm_time", "AlarmTime"})
	private String alarmTime;
	
	@SerializedName(value = "alarmDescription", alternate = {"alarm_description", "AlarmDescription"})
	@JsonAlias({"alarm_description", "AlarmDescription"})
	private String alarmDescription;

	

	// Getters and Setters
	public String getHesId() {
		return hesId;
	}

	public void setHesId(String hesId) {
		this.hesId = hesId;
	}

	public long getSeqId() {
		return seqId != null ? seqId : 0L;
	}

	public void setSeqId(long seqId) {
		this.seqId = seqId;
	}

	public void setSeqId(Long seqId) {
		this.seqId = seqId != null ? seqId : 0L;
	}
	
	public String getDeviceId() {
		return dvcIdN != null ? dvcIdN.trim() : null;
	}

	public void setDeviceId(String deviceId) {
		this.dvcIdN = deviceId != null ? deviceId.trim() : null;
	}


	public String getHesDateTime() {
		return HESDTTM;
	}

	public void setHesDateTime(String hesDateTime) {
		this.HESDTTM = hesDateTime;
	}

	public String getExportDateTime() {
		return exportDTTM;
	}

	public void setExportDateTime(String exportDateTime) {
		this.exportDTTM = exportDateTime;
	}

	public String getMeterDateTime() {
		return MeterDTTM;
	}

	public void setMeterDateTime(String meterDateTime) {
		this.MeterDTTM = meterDateTime;
	}

	public String getProfileCode() {
		return profileCode;
	}

	public void setProfileCode(String profileCode) {
		this.profileCode = profileCode;
	}

	public PreVEEData[] getPreVEE() {
		return preVEE;
	}

	public void setPreVEE(PreVEEData[] preVEE) {
		this.preVEE = preVEE;
	}
	

	public String getAlarmTime() {
		return alarmTime;
	}

	public void setAlarmTime(String alarmTime) {
		this.alarmTime = alarmTime;
	}

	public String getAlarmDescription() {
		return alarmDescription;
	}

	public void setAlarmDescription(String alarmDescription) {
		this.alarmDescription = alarmDescription;
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class PreVEEData {
		@SerializedName(value = "RegisterID", alternate = {"registerId", "registerID", "RegisterId", "register_id", "REGISTER_ID", "obc", "obisCode", "obis_code", "ObisCode", "param", "paramName", "parameter"})
		@JsonAlias({"registerId", "registerID", "RegisterId", "register_id", "REGISTER_ID", "obc", "obisCode", "obis_code", "ObisCode", "param", "paramName", "parameter"})
		private String RegisterID;

		@SerializedName(value = "enQty", alternate = {"energyQuantity", "energy_quantity", "EnQty", "value", "Value", "val", "reading", "quantity", "qty", "data"})
		@JsonAlias({"energyQuantity", "energy_quantity", "EnQty", "value", "Value", "val", "reading", "quantity", "qty", "data"})
		private String enQty;

		@SerializedName(value = "attribute", alternate = {"Attribute", "attributeId", "attribute_id", "attr"})
		@JsonAlias({"Attribute", "attributeId", "attribute_id", "attr"})
		private String attribute;

		//Getters and Setters
		public String getRegisterId() {
			return RegisterID;
		}

		public void setRegisterId(String registerId) {
			this.RegisterID = registerId;
		}

		public String getEnergyQuantity() {
			return enQty;
		}

		public void setEnergyQuantity(String energyQuantity) {
			this.enQty = energyQuantity;
		}

		public String getAttribute() {
			return attribute;
		}

		public void setAttribute(String attribute) {
			this.attribute = attribute;
		}
		
		
	}


	
}
