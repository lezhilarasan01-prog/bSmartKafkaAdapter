package com.bcits.entity;

import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;

import com.bcits.utils.SchemaUtils;


@Entity
@Table(name="hes_req_tracker", schema=SchemaUtils.SECONDARY_SCHEMA_NAME)
@NamedQueries({
	@NamedQuery(name="HesRequestTrackerEntity.getPendingOndemandDataResponse",query="select H from HesRequestTrackerEntity H where responseBody is not null and updateFlag = false and flag='normal ondemand' and  responseBody != '[]' and  requestTime > cast('2023-09-15 12:30:00' as Timestamp) and  commandType='NAMEPLATE' "),
	@NamedQuery(name="HesRequestTrackerEntity.getRequestEntity",query="select H from HesRequestTrackerEntity H where requestId=:requestId and updateFlag = false and flag='normal ondemand' and message !='Parsing failed' "),
	@NamedQuery(name="HesRequestTrackerEntity.getSatRequestEntity",query="select H from HesRequestTrackerEntity H where requestId=:requestId and updateFlag = false and flag='sat ondemand' and message !='Parsing failed' "),
	//@NamedQuery(name="HesRequestTrackerEntity.getPendingSatOndemandDataResponse",query="select H from HesRequestTrackerEntity H where responseBody is not null and updateFlag = false and flag='sat ondemand' and responseBody != '[]' and  requestTime > cast('2023-09-19 12:00:00' as Timestamp) and  commandType='POWEREVENTLOG'"),
	@NamedQuery(name="HesRequestTrackerEntity.getPendingSatOndemandDataResponse",query="select H from HesRequestTrackerEntity H where responseBody is not null and updateFlag = false and flag='sat ondemand' and responseBody != '[]' and  requestTime > cast('2023-09-19 13:15:00' as Timestamp)"),
	@NamedQuery(name="HesRequestTrackerEntity.getSatRequestEntityData",query="select H from HesRequestTrackerEntity H where requestId=:requestId  and flag='sat ondemand' "),
	@NamedQuery(name="HesRequestTrackerEntity.getmdmRequestEntity",query="select H from HesRequestTrackerEntity H where requestId=:requestId and updateFlag = false"),
	@NamedQuery(name="HesRequestTrackerEntity.getSatRequesthesEntityData",query="select H from HesRequestTrackerEntity H where requestId=:requestId  and meters=:mtrno and flag IN ('sat ondemand','normal ondemand') ")

})

public class HesRequestTrackerEntity {
	
	@Id
	@Column(name="id",  insertable=false, updatable=false)
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	private Integer id;
	
	@Column(name="command_type")
	private String commandType;
	
	@Column(name="meters")
	private String meters;
	
	@Column(name="request_body")
	private String requestBody;
	
	@Column(name="request_time")
	private Timestamp requestTime;
	
	@Column(name="request_id")
	private String requestId;
	
	@Column(name="req_id_time")
	private Timestamp reqidTime;
	
	@Column(name="response_body")
	private String responseBody;
	
	@Column(name="response_time")
	private Timestamp responseTime;
	
	@Column(name="success_flag")
	private boolean successFlag;
	
	@Column(name="update_flag")
	private boolean updateFlag;
	
	@Column(name="message")
	private String message;
	
	@Column(name="resp_code")
	private int respCode;
	
	@Column(name="update_time")
	private Timestamp updateTime;
	
	@Column(name="hes_type")
	private String hesType;
	
	@Column(name="flag")
	private String flag;


	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getCommandType() {
		return commandType;
	}

	public void setCommandType(String commandType) {
		this.commandType = commandType;
	}

	public String getMeters() {
		return meters;
	}

	public void setMeters(String meters) {
		this.meters = meters;
	}

	public String getRequestBody() {
		return requestBody;
	}

	public void setRequestBody(String requestBody) {
		this.requestBody = requestBody;
	}

	public Timestamp getRequestTime() {
		return requestTime;
	}

	public void setRequestTime(Timestamp requestTime) {
		this.requestTime = requestTime;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public Timestamp getReqidTime() {
		return reqidTime;
	}

	public void setReqidTime(Timestamp reqidTime) {
		this.reqidTime = reqidTime;
	}

	public String getResponseBody() {
		return responseBody;
	}

	public void setResponseBody(String responseBody) {
		this.responseBody = responseBody;
	}

	public Timestamp getResponseTime() {
		return responseTime;
	}

	public void setResponseTime(Timestamp responseTime) {
		this.responseTime = responseTime;
	}

	public boolean isSuccessFlag() {
		return successFlag;
	}

	public void setSuccessFlag(boolean successFlag) {
		this.successFlag = successFlag;
	}

	public boolean isUpdateFlag() {
		return updateFlag;
	}

	public void setUpdateFlag(boolean updateFlag) {
		this.updateFlag = updateFlag;
	}

	public Timestamp getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Timestamp updateTime) {
		this.updateTime = updateTime;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public int getRespCode() {
		return respCode;
	}

	public void setRespCode(int respCode) {
		this.respCode = respCode;
	}
	
	public String getHesType() {
		return hesType;
	}

	public void setHesType(String hesType) {
		this.hesType = hesType;
	}
	
	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public String toString() {
		return "HesRequestTrackerEntity [id=" + id + ", commandType=" + commandType + ", meters=" + meters
				+ ", requestBody=" + requestBody + ", requestTime=" + requestTime + ", requestId=" + requestId
				+ ", reqidTime=" + reqidTime + ", responseBody=" + responseBody + ", responseTime=" + responseTime
				+ ", successFlag=" + successFlag + ", updateFlag=" + updateFlag + ", message=" + message + ", respCode="
				+ respCode + ", updateTime=" + updateTime + ", hesType=" + hesType + ", flag=" + flag + "]";
	}

	
	
	
}
