package com.bcits.dataModel;

import java.util.List;

public class BHESnewSeqData {
	public long counterId;
    public String msn;
    public String profileObisCode;
    public List<RegisterValue> registerValues;
    private String serverTime;
	public long getCounterId() {
		return counterId;
	}
	public void setCounterId(int counterId) {
		this.counterId = counterId;
	}
	public String getMsn() {
		return msn;
	}
	public void setMsn(String msn) {
		this.msn = msn;
	}
	public String getProfileObisCode() {
		return profileObisCode;
	}
	public void setProfileObisCode(String profileObisCode) {
		this.profileObisCode = profileObisCode;
	}
	public List<RegisterValue> getRegisterValues() {
		return registerValues;
	}
	public void setRegisterValues(List<RegisterValue> registerValues) {
		this.registerValues = registerValues;
	}
	public String getServerTime() {
		return serverTime;
	}
	public void setServerTime(String serverTime) {
		this.serverTime = serverTime;
	}
}
