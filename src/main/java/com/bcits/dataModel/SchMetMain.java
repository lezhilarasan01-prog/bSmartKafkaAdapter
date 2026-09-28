package com.bcits.dataModel;

public class SchMetMain {
	private String createTime;

	private RegisterValues[] registerValues;

	private String profileObisCode;

	private String nodeId;

	private String sampleTime;

	private String formattedProfileObisCode;

	private String deviceId;

	private String sequenceNumber;

	private String meterSampleId;

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public RegisterValues[] getRegisterValues() {
		return registerValues;
	}

	public void setRegisterValues(RegisterValues[] registerValues) {
		this.registerValues = registerValues;
	}

	public String getProfileObisCode() {
		return profileObisCode;
	}

	public void setProfileObisCode(String profileObisCode) {
		this.profileObisCode = profileObisCode;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getSampleTime() {
		return sampleTime;
	}

	public void setSampleTime(String sampleTime) {
		this.sampleTime = sampleTime;
	}

	public String getFormattedProfileObisCode() {
		return formattedProfileObisCode;
	}

	public void setFormattedProfileObisCode(String formattedProfileObisCode) {
		this.formattedProfileObisCode = formattedProfileObisCode;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(String sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public String getMeterSampleId() {
		return meterSampleId;
	}

	public void setMeterSampleId(String meterSampleId) {
		this.meterSampleId = meterSampleId;
	}

	@Override
	public String toString() {
		return "ClassPojo [createTime = " + createTime + ", registerValues = "
				+ registerValues + ", profileObisCode = " + profileObisCode
				+ ", nodeId = " + nodeId + ", sampleTime = " + sampleTime
				+ ", formattedProfileObisCode = " + formattedProfileObisCode
				+ ", deviceId = " + deviceId + ", sequenceNumber = "
				+ sequenceNumber + ", meterSampleId = " + meterSampleId + "]";
	}
}
