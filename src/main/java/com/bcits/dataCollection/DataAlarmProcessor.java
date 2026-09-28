package com.bcits.dataCollection;

import com.bcits.dataModel.DeviceData;

interface DataAlarmProcessor {
	
	/**
	 *  Initiate the values and query header
	 */
	void init();

	/** Final Query to process
	 * @return string query
	 */
	String getQuery();

	/** Return true if has data for processing
	 * @return boolean
	 */
	boolean hasData();

	/**
	 * Reset the values to avoid memory issues
	 */
	void reset();


	void addData(DeviceData scMM);

	boolean hasExcpData();

	String getExcepQuery();
	
	/** Add Alarm data to for appending to builder
	 * @param scMA
	 */

	
}
