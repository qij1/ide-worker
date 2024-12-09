package com.boncfc.ide.plugin.task.dataquality;

import com.boncfc.ide.plugin.task.api.model.JobConf;
import lombok.Data;

@Data
public class DataQualityJobConf implements JobConf {
    int dsId;
    String checkSql;
    String operator;
    String threshold;
    String thresholdType;
    String validationType;
}
