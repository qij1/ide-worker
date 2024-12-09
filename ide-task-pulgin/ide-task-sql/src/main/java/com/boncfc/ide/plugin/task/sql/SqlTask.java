package com.boncfc.ide.plugin.task.sql;

import com.boncfc.ide.plugin.task.api.*;
import com.boncfc.ide.plugin.task.api.datasource.ConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.DataSourceProcessorProvider;
import com.boncfc.ide.plugin.task.api.datasource.DbType;
import com.boncfc.ide.plugin.task.api.datasource.mysql.MySQLDataSourceProcessor;
import com.boncfc.ide.plugin.task.api.datasource.oracle.OracleDataSourceProcessor;
import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import com.boncfc.ide.plugin.task.api.model.JobInstance;
import com.boncfc.ide.plugin.task.api.model.JobInstanceParams;
import com.boncfc.ide.plugin.task.api.model.ParamDataType;
import com.boncfc.ide.plugin.task.api.utils.DataSourceUtils;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.boncfc.ide.plugin.task.api.utils.ParameterUtils;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.boncfc.ide.plugin.task.api.TaskConstants.EXIT_CODE_KILL;
import static com.boncfc.ide.plugin.task.api.constants.Constants.NO;
import static com.boncfc.ide.plugin.task.api.constants.Constants.YES;
import static com.boncfc.ide.plugin.task.api.model.JobParamType.OUT;
import static com.boncfc.ide.plugin.task.api.model.ParamType.RUNTIME;

@Slf4j
public class SqlTask extends AbstractTask {

    private TaskExecutionContext taskExecutionContext;

    private final DbType dbType;

    private ConnectionParam connectionParam;

    private SqlJobConf sqlJobConf;

    /**
     * cancel
     */
    protected volatile boolean cancel = false;


    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    public SqlTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
        this.sqlJobConf = (SqlJobConf) taskExecutionContext.getJobConf();
        if (taskExecutionContext.getDatasourceDetailInfoMap() == null ||
                taskExecutionContext.getDatasourceDetailInfoMap().get(sqlJobConf.getDsId()) == null) {
            throw new TaskException("unbound data source");
        }
        DatasourceDetailInfo detailInfo = taskExecutionContext.getDatasourceDetailInfoMap().get(sqlJobConf.getDsId());
        dbType = DbType.valueOf(detailInfo.getDsType());
        connectionParam = DataSourceUtils.buildConnectionParams(dbType, detailInfo);
    }


    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        List<String> subSqls = DataSourceProcessorProvider.getDataSourceProcessor(dbType)
                .splitAndRemoveComment(sqlJobConf.getQuerySql());
        List<SqlBinds> mainStatementSqlBinds = subSqls
                .stream()
                .map(this::getSqlAndSqlParamsMap)
                .collect(Collectors.toList());
        executeSql(mainStatementSqlBinds);
    }

    private void executeSql(List<SqlBinds> statementSqlBinds) {
        try (Connection connection =
                     DataSourceProcessorProvider.getDataSourceProcessor(dbType).getConnection(connectionParam);) {
            for (int i = 0; i < statementSqlBinds.size(); i++) {
                SqlBinds sqlBind = statementSqlBinds.get(i);
                // 任务终止标记，如果cancel的值被设置为true，说明当前任务已被手动“杀死”，任务结束执行
                if (cancel) {
                    exitStatusCode = EXIT_CODE_KILL;
                    break;
                }
                if (i == statementSqlBinds.size() - 1) {
                    if (sqlBind.getSql().toUpperCase().startsWith("SELECT")) {
                        executeQuery(connection, sqlBind, "main");
                    } else {
                        executeUpdate(connection, sqlBind, "main");
                    }
                } else {
                    executeUpdate(connection, sqlBind, "main");
                }
                Thread.sleep(300);
            }
            setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
        } catch (Exception e) {
            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            throw new RuntimeException(e);
        }
    }

    private void executeQuery(Connection connection, SqlBinds sqlBinds, String handlerType) throws Exception {
        try (PreparedStatement statement = prepareStatementAndBind(connection, sqlBinds)) {
            log.info("{} statement execute query, for sql: {}", handlerType, sqlBinds.getSql());
            ResultSet resultSet = statement.executeQuery();
            resultProcess(resultSet);
        }
    }

    private void resultProcess(ResultSet resultSet) throws SQLException {
        List<JobInstanceParams> jobInstanceParams = this.taskExecutionContext.getJobInstanceParamsList().stream()
                .filter(jp -> OUT.name().equals(jp.getJobParamType()) && RUNTIME.name().equals(jp.getParamType()))
                .sorted(Comparator.comparing(JobInstanceParams::getSortIndex))
                .collect(Collectors.toList());

        if (resultSet != null) {
            ResultSetMetaData md = resultSet.getMetaData();
            int num = md.getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= num; i++) {
                    jobInstanceParams.get(i).setParamValue(
                            String.valueOf(JSONUtils.toJsonNode(resultSet.getObject(i))));
                }
                break;
            }
        }
    }

    private String executeUpdate(Connection connection, SqlBinds statementsBinds,
                                 String handlerType) throws Exception {
        int result = 0;
        try (PreparedStatement statement = prepareStatementAndBind(connection, statementsBinds)) {
            result = statement.executeUpdate();
            log.info("{} statement execute update result: {}, for sql: {}", handlerType, result,
                    statementsBinds.getSql());
        }
        return String.valueOf(result);
    }

    /**
     * preparedStatement bind
     *
     * @param connection connection
     * @param sqlBinds   sqlBinds
     * @return PreparedStatement
     * @throws Exception Exception
     */
    private PreparedStatement prepareStatementAndBind(Connection connection, SqlBinds sqlBinds) {
        try {
            PreparedStatement stmt = connection.prepareStatement(sqlBinds.getSql());
            stmt.setQueryTimeout(taskExecutionContext.getTaskTimeout());
            Map<Integer, JobInstanceParams> params = sqlBinds.getParamsMap();
            if (params != null) {
                for (Map.Entry<Integer, JobInstanceParams> entry : params.entrySet()) {
                    JobInstanceParams prop = entry.getValue();
                    ParameterUtils.setInParameter(entry.getKey(), stmt, ParamDataType.valueOf(prop.getDataType()),
                            prop.getParamValue());
                }
            }
            log.info("prepare statement replace sql : {}, sql parameters : {}", sqlBinds.getSql(),
                    sqlBinds.getParamsMap());
            return stmt;
        } catch (Exception exception) {
            throw new TaskException("SQL task prepareStatementAndBind error", exception);
        }
    }

    @Override
    public void cancel() throws TaskException {

    }


    private SqlBinds getSqlAndSqlParamsMap(String sql) {
        Map<Integer, JobInstanceParams> sqlParamsMap = new HashMap<>();
        StringBuilder sqlBuilder = new StringBuilder();
        List<JobInstanceParams> jobInstanceParamsList = this.taskExecutionContext.getJobInstanceParamsList();
        Map<String, JobInstanceParams> paramsMap = jobInstanceParamsList.stream().
                collect(Collectors.toMap(JobInstanceParams::getParamName, Function.identity()));

        // spell SQL according to the final user-defined variable
        if (paramsMap == null) {
            sqlBuilder.append(sql);
            return new SqlBinds(sqlBuilder.toString(), sqlParamsMap);
        }

        // special characters need to be escaped, ${} needs to be escaped
        setSqlParamsMap(sql, rgex, sqlParamsMap, paramsMap, taskExecutionContext.getJobInstance().getJiId());
        // Replace the original value in sql ！{...} ，Does not participate in precompilation
        String rgexo = "['\"]*\\!\\{(.*?)\\}['\"]*";
        sql = replaceOriginalValue(sql, rgexo, paramsMap);
        // replace the ${} of the SQL statement with the Placeholder
        String formatSql = sql.replaceAll(rgex, "?");
        sqlBuilder.append(formatSql);
        // print replace sql
        printReplacedSql(sql, formatSql, rgex, sqlParamsMap);
        return new SqlBinds(sqlBuilder.toString(), sqlParamsMap);
    }

    private String replaceOriginalValue(String content, String rgex, Map<String, JobInstanceParams> sqlParamsMap) {
        Pattern pattern = Pattern.compile(rgex);
        while (true) {
            Matcher m = pattern.matcher(content);
            if (!m.find()) {
                break;
            }
            String paramName = m.group(1);
            String paramValue = sqlParamsMap.get(paramName).getParamValue();
            content = m.replaceFirst(paramValue);
        }
        return content;
    }

    /**
     * print replace sql
     *
     * @param content      content
     * @param formatSql    format sql
     * @param rgex         rgex
     * @param sqlParamsMap sql params map
     */
    private void printReplacedSql(String content, String formatSql, String rgex, Map<Integer, JobInstanceParams> sqlParamsMap) {
        // parameter print style
        log.info("after replace sql , preparing : {}", formatSql);
        StringBuilder logPrint = new StringBuilder("replaced sql , parameters:");
        if (sqlParamsMap == null) {
            log.info("printReplacedSql: sqlParamsMap is null.");
        } else {
            for (int i = 1; i <= sqlParamsMap.size(); i++) {
                logPrint.append(sqlParamsMap.get(i).getParamValue()).append("(").append(sqlParamsMap.get(i).getParamType())
                        .append(")");
            }
        }
        log.info("Sql Params are {}", logPrint);
    }


}
