/**
 * 
 */
package com.bcits.daoimpl;

import org.springframework.stereotype.Repository;

import com.bcits.dao.NamePlateDao;
import com.bcits.entity.NamePlate;

/**
 * @author Tarik Afwas
 *
 */
@Repository
public class NamePlateDaoImpl extends GenericDaoImpl<NamePlate> implements NamePlateDao {
	
	@Override
	public NamePlate getNameplateDetails(String mtrno) {
		NamePlate np = null;
		try {
			np = entityManager.createNamedQuery("NamePlate.getMtrInfo",NamePlate.class).setParameter("meterNo", mtrno)
					.getSingleResult();
		} catch (Exception e) {
			// TODO: handle exception
		}
		return np;
	}
}
