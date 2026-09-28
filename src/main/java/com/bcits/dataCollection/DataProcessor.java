package com.bcits.dataCollection;

import com.bcits.dataModel.DeviceData;

/**
 * @author REMITH
 *
 */
interface DataProcessor {
	
	/**
	 *  Initiate the values and query header
	 */
	void init();

	/** Add meter data to for appending to builder
	 * @param scMM
	 */

	void addData(DeviceData scMM , String hesName);

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

	String getExcepQuery();

	boolean hasExcpData();
	
}
