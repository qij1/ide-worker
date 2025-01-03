package com.boncfc.ide.server.worker;


import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import com.boncfc.ide.plugin.task.api.lifecycle.ServerLifeCycleManager;
import com.boncfc.ide.plugin.task.api.model.*;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.boncfc.ide.plugin.task.conditionalbranch.ConditionalBranchJobConf;
import com.boncfc.ide.plugin.task.dataquality.DataQualityJobConf;
import com.boncfc.ide.plugin.task.datax.DataxJobConf;
import com.boncfc.ide.plugin.task.http.HttpJobConf;
import com.boncfc.ide.plugin.task.shell.ShellJobConf;
import com.boncfc.ide.plugin.task.sql.SqlJobConf;
import com.boncfc.ide.server.worker.config.WorkerConfig;
import com.boncfc.ide.server.worker.config.YarnConfig;
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
import static com.boncfc.ide.plugin.task.api.constants.JobType.DT;

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

    @Autowired
    YarnConfig yarnConfig;

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
                            TaskExecutionContext taskExecutionContext = TaskExecutionContext.builder()
                                    .jobDetails(jobDetails)
                                    .jobType(jobDetails.getJobType())
                                    .jobInstance(jobInstance)
                                    .jobInstanceParamsList(jobInstanceParamsList)
                                    .dataxProperties(workerConfig.getDatax())
                                    .taskTimeout(workerConfig.getJobTimeout())
                                    .applicationState(yarnConfig.getApplicationState())
                                    .build();
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
            if (prop == null) {
                // 去永久参数里找
                JobInstanceParams jobInstanceParams = workerMapper.getPersetParams(paramName);
                if (jobInstanceParams != null) jobInstanceParamsList.add(jobInstanceParams);

            }
        }
    }

    /**
     * 解析任务的配置信息：jobConf、datasource etc.
     *
     * @param taskExecutionContext
     * @param jobConfJson
     * @param jobType
     */
    private void parseJobConf(TaskExecutionContext taskExecutionContext, String jobConfJson, JobType jobType) {
        JobConf jobConf = null;
        Set<Integer> dsIds = new HashSet<>();
        Map<String, Set<Integer>> dataxDsIdMap = new HashMap<>();
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
                    Set<Integer> dsIdReader = new HashSet<>();
                    ((DataxJobConf) jobConf).getReaders().forEach(reader -> {
                        dsIdReader.add(reader.getDsId());
                    });
                    dataxDsIdMap.put(DataxPluginType.READER.name(), dsIdReader);
                    Set<Integer> dsIdWriter = new HashSet<>();
                    ((DataxJobConf) jobConf).getWriters().forEach(writer -> {
                        dsIdWriter.add(writer.getDsId());
                    });
                    dataxDsIdMap.put(DataxPluginType.WRITER.name(), dsIdWriter);

                }
                break;
            default:
                log.warn("unsupported jobType");
        }

        if (jobConf != null) {
            taskExecutionContext.setJobConf(jobConf);
        }

        List<DatasourceDetailInfo> datasourceDetailInfoList = new ArrayList<>();
        if (!dsIds.isEmpty() || !dataxDsIdMap.isEmpty()) {
            if (jobType != DT) {
                datasourceDetailInfoList = workerMapper.getDatasourceDetailInfoList(dsIds, null);
            } else {
                getDataxJobDsInfo(dataxDsIdMap, datasourceDetailInfoList);
            }
            datasourceDetailInfoList.forEach(datasourceDetailInfo -> {
                datasourceDetailInfo.setDsPasswordAESKey(workerConfig.getDatasourcePasswordAesKey());
            });

            taskExecutionContext.setDatasourceDetailInfoList(datasourceDetailInfoList);
        }

    }

    private void getDataxJobDsInfo(Map<String, Set<Integer>> dataxDsIdMap, List<DatasourceDetailInfo> datasourceDetailInfoList) {
        List<DatasourceDetailInfo> datasourceDetailInfoReaderList = workerMapper.getDatasourceDetailInfoList(dataxDsIdMap.get(DataxPluginType.READER.name()), DataxPluginType.READER.name());
        List<DatasourceDetailInfo> datasourceDetailInfoWriterList = workerMapper.getDatasourceDetailInfoList(dataxDsIdMap.get(DataxPluginType.WRITER.name()), DataxPluginType.WRITER.name());
        datasourceDetailInfoList.addAll(datasourceDetailInfoReaderList);
        datasourceDetailInfoList.addAll(datasourceDetailInfoWriterList);
    }

    private void initTaskThread() {
        loadWorkSpaceInfo();
    }

    private void loadWorkSpaceInfo() {
        workspaces = workerMapper.getWorkspace();
    }
}
