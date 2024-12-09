package com.boncfc.ide.plugin.task.conditionalbranch;

import com.boncfc.ide.plugin.task.api.model.JobConf;
import lombok.Data;

@Data
public class ConditionalBranchJobConf implements JobConf {
    String paramName;
    String operator;
    String threshold;
    String thresholdType;
    String validationType;
}
