/**
 * 
 */
package com.bcits.dataModel;

import java.util.List;

import com.bcits.dataModel.DeviceData.PreVEEData;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author BCITS
 *
 */
public class JioDataModel {
	
	@JsonProperty("hesId")
    private String hesId;

    @JsonProperty("seqId")
    private long seqId;

    @JsonProperty("dvcIdN")
    private String deviceId;

    @JsonProperty("HESDTTM")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private String hesDateTime;

    @JsonProperty("exportDTTM")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private String exportDateTime;

    @JsonProperty("MeterDTTM")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private String meterDateTime;

    @JsonProperty("profileCode")
    private String profileCode;
    
    private JioPreVEE[] preVEE;

}
