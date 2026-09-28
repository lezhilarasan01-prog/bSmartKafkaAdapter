package com.bcits.dao;

import java.util.List;


import com.bcits.entity.HesRequestTrackerEntity;


public interface HesRequestTrackerDao extends GenericDao<HesRequestTrackerEntity>{

	List<HesRequestTrackerEntity> getPendingOndemandDataResponse();

	HesRequestTrackerEntity getEntityByRequestId(String reqId);
	
	HesRequestTrackerEntity getSatEntityByRequestId(String reqId);
	
	List<HesRequestTrackerEntity> getPendingSatOndemandDataResponse();

	List<?> getPendingSatDataList();

	HesRequestTrackerEntity getSatRequestEntityData(String reqId);

	List<Object[]> getPartialParsedSatDataList(String id);

	List<Object[]> getPartialParsedSatReqList(String type,String reqTime);

	List<Object[]> getPartialParsedSatDataByReqid(String requestId);

	HesRequestTrackerEntity getSingleOdrmtrreq(String mdmRequest);

	HesRequestTrackerEntity getSatRequesthesEntityData(String reqId, String mtrno);

	List<?> getBcitsPendinghesDataList();

	List<?> getMeterIpInterval();

	List<Object[]> getHHUdataBasedOnId(String id);

	List<Object[]> pendingHHUDataParsing();

	List<Object[]> getCyanPendinghesDataList();
}
