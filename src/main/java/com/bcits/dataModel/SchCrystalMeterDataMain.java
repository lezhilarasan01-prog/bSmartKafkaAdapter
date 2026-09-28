/**
 * 
 */
package com.bcits.dataModel;

import java.util.Arrays;

/**
 * @author Tarik Afwas
 * @Created on 06-04-2023
 *
 */
public class SchCrystalMeterDataMain {
	
	private String meterSampleId;
	
	private String sequenceNumber;
	
	private String deviceId;
	
	private String formattedProfileObisCode;
	
	private String time;
	
	private String serverTime;

	private RegisterValues[] registerValues;

	public String getMeterSampleId() {
		return meterSampleId;
	}

	public void setMeterSampleId(String meterSampleId) {
		this.meterSampleId = meterSampleId;
	}

	public String getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(String sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getFormattedProfileObisCode() {
		return formattedProfileObisCode;
	}

	public void setFormattedProfileObisCode(String formattedProfileObisCode) {
		this.formattedProfileObisCode = formattedProfileObisCode;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getServerTime() {
		return serverTime;
	}

	public void setServerTime(String serverTime) {
		this.serverTime = serverTime;
	}

	public RegisterValues[] getRegisterValues() {
		return registerValues;
	}

	public void setRegisterValues(RegisterValues[] registerValues) {
		this.registerValues = registerValues;
	}

	@Override
	public String toString() {
		return "SchCrystalMeterDataMain [meterSampleId=" + meterSampleId + ", sequenceNumber=" + sequenceNumber
				+ ", deviceId=" + deviceId + ", formattedProfileObisCode=" + formattedProfileObisCode + ", time=" + time
				+ ", serverTime=" + serverTime + ", registerValues=" + Arrays.toString(registerValues) + "]";
	}

	

	
}
