package com.boncfc.ide.plugin.task.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobInstanceParams {

    private int jfiId;
    private int jiId;
    private int jobParamId;
    private int paramId;
    private String isUpstreamOutput;
    private int sortIndex;
    private String paramName;
    private String isPreset;
    private String jobParamType;
    private String dataType;
    private String paramType;
    private String paramValue;

}
