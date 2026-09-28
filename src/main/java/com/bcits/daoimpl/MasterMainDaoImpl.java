package com.bcits.daoimpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Repository;

import com.bcits.dao.MasterMainDao;
import com.bcits.entity.MasterMainEntity;
import com.bcits.utils.SchemaUtils;
@Repository
public class MasterMainDaoImpl extends GenericDaoImpl<MasterMainEntity> implements MasterMainDao{
	
	@Override
	public Map<String,String> getMeterHESNames(String mt){
		mt=mt.replaceAll(",", "','");
		Map<String,String> m=new HashMap<String, String>();
		List<Object[]> l=entityManager.createNativeQuery("select mtrno,hes_type from "+SchemaUtils.SECONDARY_SCHEMA_NAME+".master_main where mtrno in ('"+mt+"')").getResultList();
		for(Object[] o:l) {
			m.put(o[0].toString(), o[1]==null?"0":o[1].toString());
		}
		return m;
	}

}
