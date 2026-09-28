/**
 * 
 */
package com.bcits.daoimpl;

import org.springframework.stereotype.Repository;

import com.bcits.dao.NamePlateDao;
import com.bcits.dao.NamePlateHstDao;
import com.bcits.entity.NamePlate;
import com.bcits.entity.NamePlateHistory;

/**
 * @author Tarik
 *
 */
@Repository
public class NamePlateHstDaoImpl extends GenericDaoImpl<NamePlateHistory> implements NamePlateHstDao {

}
