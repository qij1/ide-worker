package com.boncfc.ide.plugin.task.datax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.boncfc.ide.plugin.task.api.TaskException;
import com.boncfc.ide.plugin.task.api.datasource.BaseConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.ConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.ftp.FTPConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.hbase.HBaseXSQLConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.hdfs.HDFSConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.hive.HiveImpalaConnectionParam;
import com.boncfc.ide.plugin.task.api.model.JobDetails;
import com.boncfc.ide.plugin.task.api.model.JobInstance;
import com.boncfc.ide.plugin.task.api.model.JobInstanceParams;
import com.boncfc.ide.plugin.task.api.model.ParamDataType;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import static com.boncfc.ide.plugin.task.api.constants.Constants.YES;
import static com.boncfc.ide.plugin.task.api.constants.DataSourceConstants.DATAX_TARGET_TABLE_TEMP_HDFS_PATH;
import static com.boncfc.ide.plugin.task.api.constants.DateConstants.YYYYMMDD;
import static com.boncfc.ide.plugin.task.api.model.JobParamType.IN;
import static com.boncfc.ide.plugin.task.datax.DataXKey.*;

public class DataxTemplateHandler {

    private JobDetails jobDetails;

    private List<JobInstanceParams> jobInstanceParamsList;

    private JobInstance jobInstance;


    public DataxTemplateHandler(JobInstance jobInstance, JobDetails jobDetails, List<JobInstanceParams> jobInstanceParamsList) {
        this.jobInstance = jobInstance;
        this.jobDetails = jobDetails;
        this.jobInstanceParamsList = jobInstanceParamsList;
    }

    public void fillVerticaReaderJobConf(ConnectionParam connectionparam, DataxJobConf.Readers reader, JSONObject readerTemplate) {
        fillRDMSReaderJobConf(connectionparam, reader, readerTemplate);
    }

    public void fillHDFSReaderJobConf(ConnectionParam connectionparam, DataxJobConf.Readers reader, JSONObject readerTemplate) {
        HDFSConnectionParam hdfsConnectionParam = (HDFSConnectionParam) connectionparam;
        JSONObject readerParameter = readerTemplate.getJSONObject(PARAMETER);
        readerParameter.fluentPut(HADOOP_CONFIG_PATH, hdfsConnectionParam.getHadoopConfDir())
                .fluentPut(HAVE_KERBEROS, YES.equals(hdfsConnectionParam.getIsSecurity()))
                .fluentPut(KRB_CONF_FILEPATH, hdfsConnectionParam.getKrbConfPath())
                .fluentPut(KRB_KEYTAB_FILEPATH, hdfsConnectionParam.getKrbKeytabsDir())
                .fluentPut(KRB_PRINCIPAL, hdfsConnectionParam.getPrincipalName())
                .fluentPut(FIELD_DELIMITER, reader.getColSplit())
                .fluentPut(PATH, reader.getFilePath())
                .fluentPut(FILE_TYPE, reader.getFileType());
    }

    public void fillHBaseReaderJobConf(ConnectionParam connectionparam, DataxJobConf.Readers reader, JSONObject readerTemplate) {
        HBaseXSQLConnectionParam hBaseXSQLConnectionParam = (HBaseXSQLConnectionParam) connectionparam;
        JSONObject readerParameter = readerTemplate.getJSONObject(PARAMETER);
        readerParameter.fluentPut(JDBCURL, hBaseXSQLConnectionParam.getJdbcUrl())
                .fluentPut(HAVE_KERBEROS, YES.equals(hBaseXSQLConnectionParam.getIsSecurity()))
                .fluentPut(KRB_CONF_FILEPATH, hBaseXSQLConnectionParam.getKrbConfPath())
                .fluentPut(KRB_KEYTAB_FILEPATH, hBaseXSQLConnectionParam.getKrbKeytabsDir())
                .fluentPut(KRB_PRINCIPAL, hBaseXSQLConnectionParam.getPrincipalName());

        String querySql = replaceCustomParams(reader.getQuerySql());
        querySql = querySql.trim().replace("\r", "").replace("\t", " ");
        List<String> querySqlList = Lists.newArrayList(querySql.split("(?<!\\\\);"));
        querySqlList.removeIf((Predicate<String>) StringUtils::isBlank);
        JSONArray querySqlJsonList = new JSONArray(querySqlList);
        readerParameter.fluentPut(QUERYSQL, querySqlJsonList);
    }

    public void fillFtpReaderJobConf(ConnectionParam connectionparam, DataxJobConf.Readers reader, JSONObject readerTemplate) {
        FTPConnectionParam ftpConnectionParam = (FTPConnectionParam) connectionparam;
        JSONObject readerParameter = readerTemplate.getJSONObject(PARAMETER);
        readerParameter.fluentPut(HOST, ftpConnectionParam.getIp())
                .fluentPut(PORT, ftpConnectionParam.getPort())
                .fluentPut(USERNAME, ftpConnectionParam.getUserName())
                .fluentPut(PASSWORD, ftpConnectionParam.getPassword())
                .fluentPut(PROTOCOL, ftpConnectionParam.getPort().equals("21") ? "FTP" : "SFTP")
                .fluentPut(FIELD_DELIMITER, reader.getColSplit())
                .fluentPut(SKIP_HEADER, reader.isSkipHeader());

    }


    public void fillHiveImpalaReaderJobConf(ConnectionParam connectionparam, DataxJobConf.Readers reader, JSONObject readerTemplateJSON) {
        HiveImpalaConnectionParam hiveImpalaConnectionParam = (HiveImpalaConnectionParam) connectionparam;
        JSONObject readerParameter = readerTemplateJSON.getJSONObject(PARAMETER);
        readerParameter.fluentPut(HADOOP_CONFIG_PATH, hiveImpalaConnectionParam.getHadoopConfDir())
                .fluentPut(HAVE_KERBEROS, YES.equals(hiveImpalaConnectionParam.getIsSecurity()))
                .fluentPut(KRB_CONF_FILEPATH, hiveImpalaConnectionParam.getKrbConfPath())
                .fluentPut(KRB_KEYTAB_FILEPATH, hiveImpalaConnectionParam.getKrbKeytabsDir())
                .fluentPut(KRB_PRINCIPAL, hiveImpalaConnectionParam.getPrincipalName());
        JSONObject connection = readerParameter.getJSONObject(CONNECTION);
        connection.fluentPut(APP_NAME, jobDetails.getCreator() + "_" + jobDetails.getJobName())
                .fluentPut(QUERYSQL, replaceCustomParams(reader.getQuerySql()))
                .fluentPut(USERNAME, hiveImpalaConnectionParam.getUserName())
                .fluentPut(PASSWORD, hiveImpalaConnectionParam.getPassword())
                .fluentPut(JDBCURL, hiveImpalaConnectionParam.getJdbcUrl());
    }

    //      "parameter": {
//        "connection": [
//           {
//               "username": "",
//                   "password": "",
//                   "jdbcUrl": "",
//                   "querySql": [
//                          ""
//                    ]
//           }
//         ]
//    }
    public void fillRDMSReaderJobConf(ConnectionParam connectionParam, DataxJobConf.Readers reader, JSONObject readerTemplateJSON) {
        JSONObject readerParameter = readerTemplateJSON.getJSONObject(PARAMETER);
        JSONArray connectionArray = readerParameter.getJSONArray(CONNECTION);

        // connetion json
        JSONObject connectionJson = new JSONObject();
        connectionJson
                .fluentPut(USERNAME, ((BaseConnectionParam) connectionParam).getUserName())
                .fluentPut(PASSWORD, ((BaseConnectionParam) connectionParam).getPassword())
                .fluentPut(QUERYSQL, dealWithSqlList(reader.getQuerySql()))
                .fluentPut(JDBCURL, new JSONArray(Lists.newArrayList(((BaseConnectionParam) connectionParam).getJdbcUrl())));
        connectionArray.add(connectionJson);
    }

    public void fillRDMSWriterJobConf(ConnectionParam connectionParam, DataxJobConf.Writers writer, JSONObject readerTemplateJSON) {
        JSONObject readerParameter = readerTemplateJSON.getJSONObject(PARAMETER);
        // connetion json
        JSONArray connectionArray = new JSONArray();
        JSONObject connectionJson = new JSONObject();
        JSONArray table = new JSONArray();
        table.add(writer.getTableName());
        connectionJson.fluentPut(JDBCURL, ((BaseConnectionParam) connectionParam).getJdbcUrl())
                .fluentPut(TABLE, table);
        connectionArray.add(connectionJson);
        readerParameter
                .fluentPut(BATCH_SIZE, writer.getBatchSize())
                .fluentPut(USERNAME, ((BaseConnectionParam) connectionParam).getUserName())
                .fluentPut(PASSWORD, ((BaseConnectionParam) connectionParam).getPassword())
                .fluentPut(CONNECTION, connectionArray)
                .fluentPut(PRE_SQL, dealWithSqlList(writer.getPreSql()))
                .fluentPut(POST_SQL, dealWithSqlList(writer.getPostSql()))
        ;
    }


    public void fillHiveWriterJobConf(ConnectionParam connectionparam, DataxJobConf.Writers writer, JSONObject writerTemplate) {
        fillImpalaWriterJobConf(connectionparam, writer, writerTemplate);
        JSONObject writerParameter = writerTemplate.getJSONObject(PARAMETER);
        JSONObject connection = writerParameter.getJSONObject(CONNECTION);
        connection.fluentPut(SET_SQL, replaceCustomParams(writer.getSetSql()));
    }

    public void fillImpalaWriterJobConf(ConnectionParam connectionparam, DataxJobConf.Writers writer, JSONObject writerTemplate) {
        HiveImpalaConnectionParam hiveImpalaConnectionParam = (HiveImpalaConnectionParam) connectionparam;
        JSONObject writerParameter = writerTemplate.getJSONObject(PARAMETER);
        writerParameter.fluentPut(HADOOP_CONFIG_PATH, hiveImpalaConnectionParam.getHadoopConfDir())
                .fluentPut(HAVE_KERBEROS, YES.equals(hiveImpalaConnectionParam.getIsSecurity()))
                .fluentPut(KRB_CONF_FILEPATH, hiveImpalaConnectionParam.getKrbConfPath())
                .fluentPut(KRB_KEYTAB_FILEPATH, hiveImpalaConnectionParam.getKrbKeytabsDir())
                .fluentPut(KRB_PRINCIPAL, hiveImpalaConnectionParam.getPrincipalName())
                .fluentPut(FILE_NAME, writer.getHivePartition())
                .fluentPut(PATH, generateHDFSTmpFilePath(writer.getFileName()))
                .fluentPut(METASTORE, hiveImpalaConnectionParam.getMetastoreUrl());
        JSONObject connection = writerParameter.getJSONObject(CONNECTION);
        connection
                .fluentPut(HIVE_PARTITION, replaceCustomParams(writer.getHivePartition()))
                .fluentPut(USERNAME, hiveImpalaConnectionParam.getUserName())
                .fluentPut(PASSWORD, hiveImpalaConnectionParam.getPassword())
                .fluentPut(JDBCURL, hiveImpalaConnectionParam.getJdbcUrl())
                .fluentPut(PRE_SQL, replaceCustomParams(writer.getPreSql()))
                .fluentPut(POST_SQL, replaceCustomParams(writer.getPostSql()))
        ;
    }

    public void fillFtpWriterJobConf(ConnectionParam connectionparam, DataxJobConf.Writers writer, JSONObject writerTemplate) {
        FTPConnectionParam ftpConnectionParam = (FTPConnectionParam) connectionparam;
        JSONObject writerParameter = writerTemplate.getJSONObject(PARAMETER);
        writerParameter.fluentPut(HOST, ftpConnectionParam.getIp())
                .fluentPut(PORT, ftpConnectionParam.getPort())
                .fluentPut(USERNAME, ftpConnectionParam.getUserName())
                .fluentPut(PASSWORD, ftpConnectionParam.getPassword())
                .fluentPut(PROTOCOL, ftpConnectionParam.getPort().equals("21") ? "FTP" : "SFTP")
                .fluentPut(PATH, writer.getPath())
                .fluentPut(FILE_NAME, writer.getFileName())
                .fluentPut(FIELD_DELIMITER, writer.getColSplit())
                ;
        if(writer.getSplit() != null) {
            JSONObject splitJson = new JSONObject();
            JSONObject split = JSONObject.parseObject(writer.getSplit());
            splitJson.fluentPut(SPLIT_UNIT, split.get(SPLIT_UNIT))
                    .fluentPut(SPLIT_AVERAGE_UNIT_NUMBER, split.getOrDefault(SPLIT_AVERAGE_UNIT_NUMBER, 0))
                    .fluentPut(SPLIT_MAX_FILE_COUNT, split.getOrDefault(SPLIT_MAX_FILE_COUNT, 0));
            writerParameter.fluentPut(SPLIT, splitJson);
        }

    }

    public void fillHBaseWriterJobConf(ConnectionParam connectionparam, DataxJobConf.Writers writer, JSONObject readerTemplate) {
        HBaseXSQLConnectionParam hBaseXSQLConnectionParam = (HBaseXSQLConnectionParam) connectionparam;
        JSONObject readerParameter = readerTemplate.getJSONObject(PARAMETER);

        List<String> colList = Lists.newArrayList(writer.getHbaseColumns().split("(?<!\\\\);"));
        colList.removeIf((Predicate<String>) StringUtils::isBlank);

        readerParameter.fluentPut(JDBCURL, hBaseXSQLConnectionParam.getJdbcUrl())
                .fluentPut(HAVE_KERBEROS, YES.equals(hBaseXSQLConnectionParam.getIsSecurity()))
                .fluentPut(KRB_CONF_FILEPATH, hBaseXSQLConnectionParam.getKrbConfPath())
                .fluentPut(KRB_KEYTAB_FILEPATH, hBaseXSQLConnectionParam.getKrbKeytabsDir())
                .fluentPut(KRB_PRINCIPAL, hBaseXSQLConnectionParam.getPrincipalName())
                .fluentPut(BATCH_SIZE, writer.getBatchSize())
                .fluentPut(TABLE, writer.getTableName())
                .fluentPut(COLUMN, new JSONArray(colList));

    }

    public void fillHDFSWriterJobConf(ConnectionParam connectionparam, DataxJobConf.Writers writer, JSONObject writerTemplate) {
        HDFSConnectionParam hdfsConnectionParam = (HDFSConnectionParam) connectionparam;
        JSONObject readerParameter = writerTemplate.getJSONObject(PARAMETER);
        readerParameter.fluentPut(HADOOP_CONFIG_PATH, hdfsConnectionParam.getHadoopConfDir())
                .fluentPut(HAVE_KERBEROS, YES.equals(hdfsConnectionParam.getIsSecurity()))
                .fluentPut(KRB_CONF_FILEPATH, hdfsConnectionParam.getKrbConfPath())
                .fluentPut(KRB_KEYTAB_FILEPATH, hdfsConnectionParam.getKrbKeytabsDir())
                .fluentPut(KRB_PRINCIPAL, hdfsConnectionParam.getPrincipalName())
                .fluentPut(FIELD_DELIMITER, writer.getColSplit())
                .fluentPut(PATH, writer.getFilePath())
                .fluentPut(FILE_NAME, writer.getFileName())
                .fluentPut(FIELD_DELIMITER, writer.getColSplit())
                .fluentPut(FILE_TYPE, writer.getFileType())
                .fluentPut(COMPRESS, writer.getCompress())
              ;
    }

    public void fillVerticaWriterJobConf(ConnectionParam connectionparam, DataxJobConf.Writers writer, JSONObject readerTemplate) {
    }

    private JSONArray dealWithSqlList(String sql) {
        String replacedSql = replaceCustomParams(sql);
        replacedSql = replacedSql.trim().replace("\r", "").replace("\t", " ");
        List<String> sqlList = Lists.newArrayList(replacedSql.split("(?<!\\\\);"));
        sqlList.removeIf((Predicate<String>) StringUtils::isBlank);
        return new JSONArray(sqlList);

    }

    public String replaceCustomParams(String content) {
        // 此处必须返回空字符串,确保json不会忽略key
        if (StringUtils.isBlank(content)) {
            return "";
        }
        List<JobInstanceParams> inputParams = jobInstanceParamsList.stream()
                .filter(jp -> jp.getJobParamType().equals(IN.name()))
                .collect(Collectors.toList());
        for (JobInstanceParams jp : inputParams) {
            ParamDataType dataType = ParamDataType.valueOf(jp.getDataType());
            switch (dataType) {
                case INT:
                case FLOAT:
                case BOOLEAN:
                    content = content.replace("${" + jp.getParamName() + "}", jp.getParamValue());
                    break;
                case STRING:
                    content = content.replace("${" + jp.getParamName() + "}",
                            "'" + jp.getParamValue() + "'");
                    break;
                default:
                    throw new TaskException("unknown param data type!");
            }
        }
        return content;
    }

    private String generateHDFSTmpFilePath(String tablename) {
        String path;
        if (tablename.contains(".")) {
            path = String.format("%s_%s_%s_%s_%s", DATAX_TARGET_TABLE_TEMP_HDFS_PATH, tablename.split("\\.")[0],
                    tablename.split("\\.")[1], jobInstance.getJiId(), LocalDateTime.now().format(DateTimeFormatter.ofPattern(YYYYMMDD)));
        } else {
            path = String.format("%s_%s_%s_%s", DATAX_TARGET_TABLE_TEMP_HDFS_PATH, tablename,
                    jobInstance.getJiId(), LocalDateTime.now().format(DateTimeFormatter.ofPattern(YYYYMMDD)));
        }
        return path;
    }



}
