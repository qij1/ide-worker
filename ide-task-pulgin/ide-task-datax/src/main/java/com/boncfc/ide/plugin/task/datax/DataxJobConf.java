package com.boncfc.ide.plugin.task.datax;

import com.boncfc.ide.plugin.task.api.constants.JobType;
import com.boncfc.ide.plugin.task.api.model.JobConf;
import lombok.Data;

import java.util.List;

@Data
public class DataxJobConf implements JobConf {

    private JobType jobType = JobType.DT;

    private List<Readers> readers;
    private List<Writers> writers;

    @Data
    public static class Readers {
        String dsName;
        int dsId;
        String querySql;
        String dsType;
    }

    @Data
    public static class Writers {
        String dsName;
        int dsId;
        String tableName;
        String preSql;
        String postSql;
        String batchSize;
    }
}
