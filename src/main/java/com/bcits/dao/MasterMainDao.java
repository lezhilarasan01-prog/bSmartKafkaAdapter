package com.bcits.dao;

import java.util.Map;

import com.bcits.entity.MasterMainEntity;

public interface MasterMainDao extends GenericDao<MasterMainEntity> {

	Map<String, String> getMeterHESNames(String mt);

}
