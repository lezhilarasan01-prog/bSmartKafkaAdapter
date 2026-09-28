/**
 * 
 */
package com.bcits.dao;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import com.bcits.entity.SchduledMeterTrackEntity;

/**
 * @author Tarik
 *
 */
public interface SchduledMeterTrackDao extends GenericDao<SchduledMeterTrackEntity> {

	public long highcount();

	public long bhesHighcount(String profileCode);

	public int bulkInsert(String bulkdata);

	public long highAlarmCount();

	public List<?> getmissingSeq();

	public List<?> getmin_maxSeq();

	public long genusHighcount(String profileCode);

	public long genusHighAlarmCount();

	public long crystalMaxCounterId(String apiName);

	public long crystalGatewayMaxCounterId(String apiName);

	public long hesWiseMaxCounterId(String profileCode, String hes);

	public Long cyanCounterId(String apiName, String hes);

	public long hesAssetTypeWiseMaxCounterId(String profileCode, String assetType, String hes);

	public long getSavedOffset(String topic, int partition);

	public void saveOffsets(Map<TopicPartition, Long> partitionOffsetMap , String hes);

}
