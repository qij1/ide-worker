package com.boncfc.ide.server.worker.thread;


import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import com.boncfc.ide.plugin.task.api.model.*;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.boncfc.ide.plugin.task.conditionalbranch.ConditionalBranchJobConf;
import com.boncfc.ide.plugin.task.dataquality.DataQualityJobConf;
import com.boncfc.ide.plugin.task.datax.DataxJobConf;
import com.boncfc.ide.plugin.task.http.HttpJobConf;
import com.boncfc.ide.plugin.task.shell.ShellJobConf;
import com.boncfc.ide.plugin.task.sql.SqlJobConf;
import com.boncfc.ide.server.worker.common.lifecycle.ServerLifeCycleManager;
import com.boncfc.ide.server.worker.config.WorkerConfig;
import com.boncfc.ide.server.worker.constants.Constants;
import com.boncfc.ide.plugin.task.api.constants.JobType;
import com.boncfc.ide.server.worker.mapper.WorkerMapper;
import com.boncfc.ide.server.worker.metrics.MetricsProvider;
import com.boncfc.ide.server.worker.metrics.WorkerServerMetrics;
import com.boncfc.ide.server.worker.registry.RegistryClient;
import com.boncfc.ide.server.worker.registry.enums.RegistryNodeType;
import com.boncfc.ide.server.worker.runner.WorkerTaskExecutor;
import com.boncfc.ide.server.worker.runner.WorkerTaskExecutorFactoryBuilder;
import com.boncfc.ide.server.worker.runner.WorkerTaskExecutorThreadPool;
import com.boncfc.ide.server.worker.utils.ThreadUtils;
import com.boncfc.ide.server.worker.utils.TimeParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.boncfc.ide.plugin.task.api.AbstractTask.groupName;
import static com.boncfc.ide.plugin.task.api.AbstractTask.rgex;
import static com.boncfc.ide.plugin.task.api.constants.Constants.NO;

@Slf4j
@Component
public class TaskThread implements Runnable {


    @Autowired
    private WorkerConfig workerConfig;

    @Autowired
    private WorkerMapper workerMapper;

    @Autowired
    RegistryClient registryClient;

    @Autowired
    private MetricsProvider metricsProvider;

    @Autowired
    WorkerTaskExecutorThreadPool workerTaskExecutorThreadPool;

    @Autowired
    WorkerTaskExecutorFactoryBuilder workerTaskExecutorFactoryBuilder;

    static Map<String, Map<String, String>> workspaces = new HashMap<>();

    @Override
    public void run() {
        initTaskThread();
        while (!ServerLifeCycleManager.isStopped()) {
            try {
                String lockPath = RegistryNodeType.WORKER_JOB_LOCK.getRegistryPath();
                // 竞争任务锁
                if (lockPath != null && registryClient.getLock(lockPath)) {
                    List<String> jobTypeList = workerMapper.getWorkerExecutableJobType(workerConfig.getWorkerHost());

                    for (int i = 0; i < workerConfig.getBatchJobNum(); i++) {
                        if (workerConfig.getServerLoadProtection().isOverload(metricsProvider.getSystemMetrics())) {
                            log.warn("The current server is overload, cannot consumes commands.");
                            WorkerServerMetrics.incWorkerOverloadCount();
                            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                            break;
                        }

                        if (workerTaskExecutorThreadPool.isOverload()) {
                            log.warn("workerTaskExecutorThreadPoll is full, please wait");
                            WorkerServerMetrics.incWorkerSubmitQueueIsFullCount();
                            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                            break;
                        }

                        String jobInstId = workerMapper.getJobInstId(jobTypeList);
                        if (jobInstId != null) {
                            JobInstance jobInstance = workerMapper.getJobInstanceInfo(jobInstId);
                            jobInstance.setWorkerIp(workerConfig.getWorkerHost());
                            // 获取任务实例参数信息并解析成实际值
                            List<JobInstanceParams> jobInstanceParamsList = workerMapper.getJobAllParams(jobInstance);
                            // 获取任务详情
                            JobDetails jobDetails = workerMapper.getJobDetails(jobInstance);
                            addJobConfPersetParam(jobDetails.getJobConf(), jobInstanceParamsList);
                            parseJobInstanceParams(jobInstance, jobInstanceParamsList);
                            // 任务上下文环境
                            TaskExecutionContext taskExecutionContext = TaskExecutionContext.builder().
                                    jobDetails(jobDetails).
                                    jobType(jobDetails.getJobType()).
                                    jobInstance(jobInstance).
                                    jobInstanceParamsList(jobInstanceParamsList).
                                    build();
                            // 解析任务配置和数据源信息
                            parseJobConf(taskExecutionContext, jobDetails.getJobConf(),
                                    JobType.valueOf(jobDetails.getJobType()));
                            WorkerTaskExecutor workerTaskExecutor = workerTaskExecutorFactoryBuilder
                                    .createWorkerTaskExecutorFactory(taskExecutionContext).createWorkerTaskExecutor();
                            if (!workerTaskExecutorThreadPool.submitWorkerTaskExecutor(workerTaskExecutor)) {
                                log.error("server is not running. reject task: jobId {} jiId {}",
                                        jobInstance.getJobId(), jobInstance.getJiId());
                            } else {
                                workerMapper.deleteFromJobInstanceQueue(jobInstance);
                                log.info("worker从队列领取任务实例ID:[{}], 类型:[{}]", jobInstId, jobInstance.getJiType());
                            }
                        }


                    }
                }
            } catch (Exception e) {
                log.error("{} 领取任务过程中发生错误", workerConfig.getWorkerAddress(), e);
                // sleep for 1s here to avoid the database down cause the exception boom
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            } finally {
                registryClient.releaseLock(RegistryNodeType.WORKER_JOB_LOCK.getRegistryPath());
            }
        }
    }

    private void parseJobInstanceParams(JobInstance jobInstance, List<JobInstanceParams> jobInstanceParamsList) {
        for (JobInstanceParams jobParam : jobInstanceParamsList) {
            jobParam.setJfiId(jobInstance.getJfiId());
            jobParam.setJiId(jobInstance.getJiId());
            if (NO.equals(jobParam.getIsUpstreamOutput())) {
                if (ParamType.TIME.name().equals(jobParam.getParamType())) {
                    try {
                        jobParam.setParamValue(TimeParser.formatExpressionResult(jobInstance.getBatchTime(), jobParam.getParamValue()));
                    } catch (ParseException e) {
                        log.error("解析TIME类型的参数表达式报错", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private void addJobConfPersetParam(String jobConf, List<JobInstanceParams> jobInstanceParamsList) {
        Map<String, JobInstanceParams> paramsPropsMap = jobInstanceParamsList.stream().
                collect(Collectors.toMap(JobInstanceParams::getParamName, Function.identity()));
        Pattern pattern = Pattern.compile(rgex);
        Matcher m = pattern.matcher(jobConf);
        while (m.find()) {
            String paramName = m.group(groupName);
            JobInstanceParams prop = paramsPropsMap.get(paramName);
            if(prop == null) {
                // 去永久参数里找
                JobInstanceParams jobInstanceParams = workerMapper.getPersetParams(paramName);
                if(jobInstanceParams != null) jobInstanceParamsList.add(jobInstanceParams);

            }
        }
    }
    public static void main(String[] args) {
        String jsonDatax = "{\n" +
                "   \"readers\":[{\"dsName\":\"mall1\", \"dsId\":\"3659582275830784\", \"querySql\": \"select 1\", \"dsType\":\"oracle\"}," +
                "                {\"dsName\": \"mall2\", \"dsId\": \"3659582275830784\", \"querySql\": \"select 1\", \"dsType\": \"oracle\"}]," +
                "    \"writers\":[{\"dsName\":\"test1\", \"dsId\": \"3553264497476608\", \"tableName\": \"test\", \"preSql\":\" \",\"postSql\":\" \",\"batchSize\":\"512\"}," +
                "                 {\"dsName\":\"test2\",\"dsId\": \"3553264497476608\", \"tableName\": \"test\",  \"preSql\":\" \",\"postSql\":\"\",\"batchSize\":\"512\"}]\n" +
                "    }";
        String jsonSql = "{\n" +
                " \"dsId\": 1,\n" +
                " \"querySql\":\"insert into TEST_TABLE(id, name, sex) VALUES (1, 'aaaaaa', '0');\\n" +
                "                select name from TEST_TABLE where sex = 0;\"\n" +
                "}";
        String jsonConditional = "";
        String jsonCheck = "{" +
                "\"dsId\":\"test1\"," +
                "\"checkSql\":\"\"," +
                "\"operator\":\">\", " +
                "\"threshold\":\"\"," +
                "\"thresholdType\":\"string\",\n" +
                "\"validationType\":\"strong\"\n" +
                "}";
        String jsonShell = "{" +
                "  \"user\":\"test\"," +
                "  \"shell\":\"\"" +
                "}";
        String jsonHttp = "{\n" +
                "   \"requestUrl\":\"\",\n" +
                "   \"requestMethod\":\"GET\",\n" +
                "   \"requestHeaders\":[{\"content-type\":\"\"}],\n" +
                "   \"requestBody\":\"\",\n" +
                "   \"responseHeaders\":[{\"content-type\":\"\"}],\n" +
                "   \"ack\":true,\n" +
                "   \"ackKey\":\"\",\n" +
                "   \"ackValue\":\"\",\n" +
                "   \"ackValueType\":\"string|number|boolean\",\n" +
                "   \"stopRequestUrl\":\"\",\n" +
                "   \"stopRequestMethod\":\"GET\",\n" +
                "   \"stopRequestHeaders\":[{\"content-type\":\"\"}]\n" +
                "}";
        SqlJobConf jsonObject = JSONUtils.parseObject(jsonSql, SqlJobConf.class);
        System.out.println(jsonObject);
    }

    private void parseJobConf(TaskExecutionContext taskExecutionContext, String jobConfJson, JobType jobType) {
        List<Integer> dsIds = new LinkedList<>();
        JobConf jobConf = null;
        switch (jobType) {
            case HTTP:
                jobConf = JSONUtils.parseObject(jobConfJson, HttpJobConf.class);
                break;
            case SQL:
                jobConf = JSONUtils.parseObject(jobConfJson, SqlJobConf.class);
                if (jobConf != null) {
                    dsIds.add(((SqlJobConf) jobConf).getDsId());
                }
                break;
            case SH:
                jobConf = JSONUtils.parseObject(jobConfJson, ShellJobConf.class);
                break;
            case QV:
                jobConf = JSONUtils.parseObject(jobConfJson, DataQualityJobConf.class);
                if (jobConf != null) {
                    dsIds.add(((DataQualityJobConf) jobConf).getDsId());
                }
                break;
            case CK:
                jobConf = JSONUtils.parseObject(jobConfJson, ConditionalBranchJobConf.class);
                break;
            case DT:
                jobConf = JSONUtils.parseObject(jobConfJson, DataxJobConf.class);
                if (jobConf != null) {
                    ((DataxJobConf) jobConf).getReaders().forEach(reader -> {
                        dsIds.add(reader.getDsId());
                    });
                    ((DataxJobConf) jobConf).getWriters().forEach(writer -> {
                        dsIds.add(writer.getDsId());
                    });
                }
                break;
            default:
                log.warn("unsupported jobType");
        }

        if (jobConf != null) {
            taskExecutionContext.setJobConf(jobConf);
        }

        if (!dsIds.isEmpty()) {
            List<DatasourceDetailInfo> datasourceDetailInfoList = workerMapper.getDatasourceDetailInfoList(dsIds);
            datasourceDetailInfoList.forEach(datasourceDetailInfo -> {
                datasourceDetailInfo.setDsPasswordAESKey(workerConfig.getDatasourcePasswordAesKey());
            });
            Map<Integer, DatasourceDetailInfo> datasourceDetailInfoMap = datasourceDetailInfoList.stream().collect(
                    Collectors.toMap(DatasourceDetailInfo::getDsId, Function.identity()));
            taskExecutionContext.setDatasourceDetailInfoMap(datasourceDetailInfoMap);
        }
    }

    private void initTaskThread() {
        loadWorkSpaceInfo();
    }

    private void loadWorkSpaceInfo() {
        workspaces = workerMapper.getWorkspace();
    }
}
