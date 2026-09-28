package com.bcits.daoimpl;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.bcits.dao.HesRequestTrackerDao;
import com.bcits.entity.HesRequestTrackerEntity;

@Repository
public class HesRequestTrackerDaoImpl extends GenericDaoImpl<HesRequestTrackerEntity> implements HesRequestTrackerDao{

	@Override
	public 	List<HesRequestTrackerEntity> getPendingOndemandDataResponse() {
		List<HesRequestTrackerEntity> hes=null;
		try {
			hes= entityManager.createNamedQuery("HesRequestTrackerEntity.getPendingOndemandDataResponse",HesRequestTrackerEntity.class).getResultList();
	} catch (Exception e) {
		e.printStackTrace();
	}
	return hes;
	}

	@Override
	public HesRequestTrackerEntity getEntityByRequestId(String requestId) {
		HesRequestTrackerEntity hes=null;
		try {
			hes= entityManager.createNamedQuery("HesRequestTrackerEntity.getRequestEntity",HesRequestTrackerEntity.class).setParameter("requestId", requestId).getSingleResult();
	} catch (Exception e) {
		e.printStackTrace();
	}
	return hes;
	}

	@Override
	public HesRequestTrackerEntity getSatEntityByRequestId(String requestId) {
		HesRequestTrackerEntity hes=null;
		try {
			hes= entityManager.createNamedQuery("HesRequestTrackerEntity.getSatRequestEntity",HesRequestTrackerEntity.class).setParameter("requestId", requestId).getSingleResult();
	} catch (Exception e) {
		e.printStackTrace();
	}
	return hes;
	}

	@Override
	public List<HesRequestTrackerEntity> getPendingSatOndemandDataResponse() {
		List<HesRequestTrackerEntity> hes=null;
		try {
			hes= entityManager.createNamedQuery("HesRequestTrackerEntity.getPendingSatOndemandDataResponse",HesRequestTrackerEntity.class).setMaxResults(3).getResultList();
	} catch (Exception e) {
		e.printStackTrace();
	}
	return hes;
	}

	@Override
	public List<?> getPendingSatDataList() {
		try {

			
			String sql1="SELECT request_id,command_type,response_body,meters,req_id_time,success_flag,update_flag,flag,message FROM ami_master.hes_req_tracker \r\n" + 
					"WHERE command_type like  '%' and  request_id IN ('crystalJob20250103234833533000000') AND success_flag = 't' AND update_flag = 'f' AND response_body != '[]' and   response_body  LIKE '%deviceId%'";

			System.out.println("sql1 "+sql1);
			
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public HesRequestTrackerEntity getSatRequestEntityData(String reqId) {
		HesRequestTrackerEntity hes=null;
		try {
			hes= entityManager.createNamedQuery("HesRequestTrackerEntity.getSatRequestEntityData",HesRequestTrackerEntity.class).setParameter("requestId", reqId).getSingleResult();
	} catch (Exception e) {
		e.printStackTrace();
	}
	return hes;
	}

	@Override
	public List<Object[]> getPartialParsedSatDataList(String type) {
		try {
			
		String commandValue=null;
	
			 switch(type) {
			 case "NONROLLOVEREVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
			
			 case "CONTROLEDEVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
				 
			 case "TRANSACTIONEVENTLOG":  
				 commandValue= " and event_code in ('151','152','153','154','155')";			
				 break;
				 
			 case "CURRENTEVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
				 
			 case "VOLTAGEEVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
			 case "OTHEREVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
			 case "POWEREVENTLOG": 
				 commandValue= " and event_code in ('102','101')";
				 break;
			
			 }
			String sql1="select request_id,command_type,response_body,meters from \n" +
					"					ami_master.hes_req_tracker where flag='sat ondemand' and req_id_time>='2023-09-19 09:30:00' and command_type='"+type+"'\n" +
					"					and  response_body is not null and  response_body != '[]' and request_id is not null and message !='Parsing failed Json issue' \n" +
					"					and meters not in(\n" +
					"					select distinct(meter_number) from  meter_data.sat_events where time_stamp >'2023-09-19 09:30:00' "+commandValue+" GROUP BY \n" +
					"		meter_number HAVING count(id) < 20) and success_flag=true and update_flag=true limit 3";
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public List<Object[]> getPartialParsedSatReqList(String type, String reqTime) {
		try {
			
		String commandValue=null;
	
			 switch(type) {
			 case "NONROLLOVEREVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
			
			 case "CONTROLEDEVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
				 
			 case "TRANSACTIONEVENTLOG":  
				 commandValue= " and event_code in ('151','152','153','154','155')";			
				 break;
				 
			 case "CURRENTEVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
				 
			 case "VOLTAGEEVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
			 case "OTHEREVENTLOG": 
				 commandValue= " and event_code in ('10','9')";
				 break;
			 case "POWEREVENTLOG": 
				 commandValue= " and event_code in ('102','101')";
				 break;
			
			 }
			 
			String sql1="select request_id,command_type from \n" +
					"					ami_master.hes_req_tracker where flag='sat ondemand' and req_id_time>='"+reqTime+"' and command_type='"+type+"'\n" +
					"					and  response_body is not null and  response_body != '[]' and request_id is not null and message !='Parsing failed Json issue' \n" +
					"					and meters not in(\n" +
					"					select distinct(meter_number) from  meter_data.sat_events where time_stamp >'"+reqTime+"' "+commandValue+" GROUP BY \n" +
					"		meter_number HAVING count(id) < 20) and success_flag=true and update_flag=true";
			return entityManager.createNativeQuery(sql1).getResultList();
			
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public List<Object[]> getPartialParsedSatDataByReqid(String requestId) {
		try {
			String sql1="select request_id,command_type,response_body,meters from \n" +
					"ami_master.hes_req_tracker where flag='sat ondemand' and request_id='"+requestId+"'";
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public HesRequestTrackerEntity getSingleOdrmtrreq(String mdmRequest) {
		System.out.println("mdmRequest "+mdmRequest);
		HesRequestTrackerEntity hes=null;
		try {
			hes= entityManager.createNamedQuery("HesRequestTrackerEntity.getmdmRequestEntity",HesRequestTrackerEntity.class).setParameter("requestId", mdmRequest).getSingleResult();
	} catch (Exception e) {
		e.printStackTrace();
	}
		return hes;
	}

	@Override
	public HesRequestTrackerEntity getSatRequesthesEntityData(String reqId, String mtrno) {
		HesRequestTrackerEntity hes=null;
		try {
			hes= entityManager.createNamedQuery("HesRequestTrackerEntity.getSatRequesthesEntityData",HesRequestTrackerEntity.class).setParameter("requestId", reqId).setParameter("mtrno", mtrno).getSingleResult();
	} catch (Exception e) {
		e.printStackTrace();
	}
	return hes;
	}

	@Override
	public List<?> getBcitsPendinghesDataList() {
		try {

			String sql1=" SELECT request_id,command_type,response_body,meters,req_id_time,success_flag,update_flag,flag,message FROM ami_master.hes_req_tracker WHERE hes_type='BCITS' and  request_id ='Bcits20241212175726035000000'  \r\n" + 
					"  and success_flag = 't' and  update_flag = 'f' and response_body  like '%RegisterObisCode%' limit 20\r\n" + 
					"  ";
			//System.out.println(sql1);
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public List<?> getMeterIpInterval() {
		try {
			String sql1 = "select meterno,meter_ip_period from ami_master.meter_inventory";
			//System.out.println(sql1);
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<Object[]> getHHUdataBasedOnId(String id) {
		try {
			String sql1="select mtrno,data from ami_master.hhu_tracker where id='"+id+"' and update_flag=false";
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public List<Object[]> pendingHHUDataParsing() {
		try {
			String sql1="select mtrno,data,id from ami_master.hhu_tracker where update_flag=false and message='Data Saved Successfully'";
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Object[]> getCyanPendinghesDataList() {
		try {
			String sql1=" SELECT request_id,command_type,response_body,meters,req_id_time,success_flag,update_flag,flag,message FROM ami_master.hes_req_tracker WHERE request_id ='Cyan20241127170506601000000'  \r\n" + 
					"  and success_flag = 't' and  update_flag = 'f' and response_body  like '%samples%' limit 500\r\n" + 
					"  ";
			
			System.out.println("sql "+sql1);
			return entityManager.createNativeQuery(sql1).getResultList();
		} catch (Exception e) {
			return null;
		}
	}


	

}
