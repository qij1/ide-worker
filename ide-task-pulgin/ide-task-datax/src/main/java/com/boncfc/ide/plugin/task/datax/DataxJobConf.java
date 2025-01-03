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
        String filePath;
        String fileName;
        String colSplit;
        String fileType;
        boolean skipHeader;
    }

    @Data
    public static class Writers {
        String dsName;
        int dsId;
        String tableName;
        String preSql;
        String postSql;
        String batchSize;
        String hivePartition;
        String fileName;
        String setSql; //hive
        String filePath;
        String colSplit;  // ftp hdfs
        boolean notAllowControlCharacter;  // ftp
        String path;  // ftp
        String split;  // ftp
        String hbaseColumns;

        String writeMode; // hdfs
        String fileType; // hdfs
        String compress; // hdfs
    }
}
