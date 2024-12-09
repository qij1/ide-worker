package com.boncfc.ide.plugin.task.sql;

import com.boncfc.ide.plugin.task.api.model.JobConf;
import lombok.Data;

@Data
public class SqlJobConf implements JobConf {
    int dsId;
    String querySql;
}
