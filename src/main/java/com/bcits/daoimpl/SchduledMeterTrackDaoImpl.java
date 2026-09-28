/**
 * 
 */
package com.bcits.daoimpl;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import jakarta.persistence.Query;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.bcits.dao.SchduledMeterTrackDao;
import com.bcits.entity.SchduledMeterTrackEntity;
import com.bcits.utils.SchemaUtils;

/**
 * @author Tarik Afwas
 *
 */
@Repository
public class SchduledMeterTrackDaoImpl extends GenericDaoImpl<SchduledMeterTrackEntity>
		implements SchduledMeterTrackDao {

//	@Scheduled(cron="0 0/2 * * * ?")
//	public void test() {
//		SchduledMeterTrackEntity smte = new SchduledMeterTrackEntity();
//		smte.setSeqId(Integer.parseInt("1"));
//		smte.setType("Billing");
//		smte.setMeterNumber("156565556");
//		int count=queryStringExecute("insert into meter_data.sch_meter_track(seq_id,type,meter_number) values(33,'Bulk Load','4623052')");
//		System.out.println("Insert Count- "+count);
//		List<Object[]> l=entityManager.createNativeQuery("select * from meter_data.load_survey limit 10").getResultList();
//		save(smte);
//	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long highcount() {
		BigDecimal hn;
		long i = 0;

		try {
			hn = (BigDecimal) entityManager.createNativeQuery("select max(seq_id) from meter_data.sch_meter_trackload")
					.getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public Long cyanCounterId(String apiName, String hes) {
		BigDecimal hn;
		long i = 0;

		try {
			hn = (BigDecimal) entityManager
					.createNativeQuery("select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
							+ ".max_sequence where  type='" + apiName + "' and hes='" + hes + "'")
					.getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return i;

	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long bhesHighcount(String profileCode) {
		BigDecimal hn;
		long i = 0;
		String url = "";
		try {
			if ("1.0.98.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Billing'";
			}
			if ("1.0.99.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Bulk Load'";
			}
			if ("1.0.99.2.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Daily Load'";
			}
			if ("1.0.94.91.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Instantaneous Pull'";
			}
			if ("0.0.99.98.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Voltage Event Log'";
			}
			if ("0.0.99.98.1.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Current Event Log'";
			}
			if ("0.0.99.98.2.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Power Event Log'";
			}
			if ("0.0.99.98.3.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Transaction Event Log'";
			}
			if ("0.0.99.98.4.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Other Event Log'";
			}
			if ("0.0.99.98.5.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Non Roll Over Event Log'";
			}
			if ("0.0.99.98.6.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Controled Event Log'";
			}
			if ("0.0.94.91.10.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Name Plate'";
			}
			if ("0.0.25.9.0.255".equalsIgnoreCase(profileCode)) { // Instant Push
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Instantaneous'";
			}
			if ("getDCUEventsInfo".equalsIgnoreCase(profileCode)) { // Gateway
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='getDCUEventsInfo'";
			}
			if ("0.4.25.9.0.255".equalsIgnoreCase(profileCode)) { // Meter Alarm
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Meter Alarm'";
			}

			hn = (BigDecimal) entityManager.createNativeQuery(url).getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long hesWiseMaxCounterId(String profileCode, String hes) {
		BigDecimal hn;
		long i = 0;
		String url = "";
		try {
			if ("1.0.98.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Billing' and hes='" + hes + "'";
			}
			if ("1.0.99.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Bulk Load' and hes='" + hes + "'";
			}
			if ("1.0.99.2.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Daily Load' and hes='" + hes + "'";
			}
			if ("1.0.94.91.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Instantaneous' and hes='" + hes + "'";
			}
			if ("0.0.99.98.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Voltage Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.1.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Current Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.2.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Power Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.3.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Transaction Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.4.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Other Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.5.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Non Roll Over Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.6.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Controled Event Log' and hes='" + hes + "'";
			}
			if ("0.0.94.91.10.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Name Plate' and hes='" + hes + "'";
			}
			if ("0.0.25.9.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Instantaneous Push' and hes='" + hes + "'";
			}

			hn = (BigDecimal) entityManager.createNativeQuery(url).getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Override
	public int bulkInsert(String bulkdata) {
		// TODO Auto-generated method stub
		return queryStringExecute(bulkdata);
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long highAlarmCount() {
		BigDecimal hn;
		long i = 0;

		try {
			hn = (BigDecimal) entityManager.createNativeQuery("select max(meter_alarm_id) from meter_data.meter_alarm")
					.getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public List<?> getmissingSeq() {
		List<?> list = null;
		try {
			String sql = "select (sid-countf+1) as strtid,countf-1 as count from (\n"
					+ "select case when seq_f>1 then seq_id end as sid,case when seq_f>1 then seq_f end as  countf from\n"
					+ "(select seq_id,seq_id-lag(seq_id) OVER (ORDER BY seq_id) as seq_f\n"
					+ "from meter_data.sch_meter_trackload where seq_id>(select max(seq_id) from meter_data.sch_meter_trackload )-70000000) a where seq_f>1 group by seq_id,seq_f   ORDER BY seq_id) b;";

//			System.out.println("Final Missing Query ===" + sql);

			list = entityManager.createNativeQuery(sql).getResultList();

		} catch (Exception e) {
			list = null;
			e.printStackTrace();
		}

		return list;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public List<?> getmin_maxSeq() {
		List<?> list = null;
		try {
			String sql = "select min(seq_id) as minSeq,max(seq_id) as maxSeq \n"
					+ "from meter_data.sch_meter_trackload \n"
					+ "where time_stamp between CURRENT_DATE-1 and CURRENT_DATE;";
//			String sql="select case when seq_f>1 then seq_id-1 end as seq_num,case when seq_f>1 then seq_f-1 end as lim from\n" +
//					"(select seq_id,seq_id-lag(\"seq_id\") OVER (ORDER BY seq_id) as seq_f\n" +
//					"from meter_data.sch_meter_trackload where seq_id>(select max(seq_id) from meter_data.sch_meter_trackload )-10000000) a where seq_f>1 group by seq_id,seq_f  limit 1";

//			System.out.println("Final Missing Query ===" + sql);

			list = entityManager.createNativeQuery(sql).getResultList();

		} catch (Exception e) {
			list = null;
			e.printStackTrace();
		}

		return list;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long genusHighcount(String profileCode) {
		BigDecimal hn;
		long i = 0;
		String url = "";
		try {
			if ("1.0.98.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Billing'";
			}
			if ("1.0.99.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Bulk Load'";
			}
			if ("1.0.99.2.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Daily Load'";

			}
			if ("1.0.94.91.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Instantaneous'";
			}
			if ("0.0.99.98.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Voltage Event Log'";
			}
			if ("0.0.99.98.1.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Current Event Log'";
			}
			if ("0.0.99.98.2.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Power Event Log'";
			}
			if ("0.0.99.98.3.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Transaction Event Log'";
			}
			if ("0.0.99.98.4.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Other Event Log'";
			}
			if ("0.0.99.98.5.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Non Roll Over Event Log'";
			}
			if ("0.0.99.98.6.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Controled Event Log'";
			}
			if ("0.0.94.91.10.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME
						+ ".max_sequence where type='Name Plate'";
			}

			// System.out.println("url : "+url);

			hn = (BigDecimal) entityManager.createNativeQuery(url).getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long genusHighAlarmCount() {
		BigDecimal hn;
		long i = 0;

		try {
			hn = (BigDecimal) entityManager.createNativeQuery("select max(meter_alarm_id) from meter_data.meter_alarm")
					.getSingleResult();
			// System.out.println("hn : "+hn);
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long crystalMaxCounterId(String apiName) {
		BigDecimal hn;
		long i = 0;

		try {
			hn = (BigDecimal) entityManager.createNativeQuery("select max(seq_id) from "
					+ SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where  type='" + apiName + "'")
					.getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long crystalGatewayMaxCounterId(String apiName) {
		BigDecimal hn;
		long i = 0;

		try {
			hn = (BigDecimal) entityManager
					.createNativeQuery("select max(seq_id) from " + SchemaUtils.SECONDARY_SCHEMA_NAME + ".search_nodes")
					.getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Transactional(propagation = Propagation.REQUIRED)
	public long hesAssetTypeWiseMaxCounterId(String profileCode, String assetType, String hes) {
		BigDecimal hn;
		long i = 0;
		String url = "";
		try {
			if ("1.0.98.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Billing' and hes='" + hes + "'";
			}
			if ("1.0.99.1.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Bulk Load' and hes='" + hes + "'";
			}
			if ("1.0.99.2.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Daily Load' and hes='" + hes + "'";
			}
			if ("1.0.94.91.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Instantaneous' and hes='" + hes + "'";
			}
			if ("0.0.99.98.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Voltage Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.1.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Current Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.2.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Power Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.3.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Transaction Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.4.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Other Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.5.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Non Roll Over Event Log' and hes='" + hes + "'";
			}
			if ("0.0.99.98.6.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Controled Event Log' and hes='" + hes + "'";
			}
			if ("0.0.94.91.10.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Name Plate' and hes='" + hes + "'";
			}
			if ("0.0.25.9.0.255".equalsIgnoreCase(profileCode)) {
				url = "select max(seq_id) from " + SchemaUtils.PRIMARY_SCHEMA_NAME + ".max_sequence where type='"
						+ assetType + "_Instantaneous Push' and hes='" + hes + "'";
			}

			hn = (BigDecimal) entityManager.createNativeQuery(url).getSingleResult();
			i = hn.longValue();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return i;
	}

	@Override
	public long getSavedOffset(String topic, int partition) {
		
		String sql = "SELECT offset_value FROM meter_data.kafka_offsets WHERE topic = :topic AND partition_id = :partition";
		Query query = entityManager.createNativeQuery(sql);
		query.setParameter("topic", topic);
		query.setParameter("partition", partition);
		Object result = null;
		try {
			result = query.getSingleResult();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		// Null-safe conversion
		return result != null ? ((Number) result).longValue() + 1 : 1L;
	}

	@Override
	public void saveOffsets(Map<TopicPartition, Long> partitionOffsetMap, String hes) {
		for (Map.Entry<TopicPartition, Long> entry : partitionOffsetMap.entrySet()) {
            String sql = "INSERT INTO meter_data.kafka_offsets (topic, partition_id, offset_value ,hes) " +
                         "VALUES (:topic, :partitionId, :offsetValue, :hesName ) " +
                         "ON CONFLICT(topic, partition_id) DO UPDATE set offset_value = :offsetValue , last_updated_time = now() , hes =:hesName";

            Query query = entityManager.createNativeQuery(sql);
            query.setParameter("topic", entry.getKey().topic());
            query.setParameter("partitionId", entry.getKey().partition());
            query.setParameter("offsetValue", entry.getValue());
            query.setParameter("hesName", hes);
//            query.setParameter("last_updated_time", now());
            
            query.executeUpdate();
        }
		
	}

}
