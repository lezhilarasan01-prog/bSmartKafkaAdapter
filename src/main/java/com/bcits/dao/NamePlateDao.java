/**
 * 
 */
package com.bcits.dao;

import com.bcits.entity.NamePlate;

/**
 * @author Tarik Afwas
 *
 */
public interface NamePlateDao extends GenericDao<NamePlate> {

	NamePlate getNameplateDetails(String mtrno);

}
