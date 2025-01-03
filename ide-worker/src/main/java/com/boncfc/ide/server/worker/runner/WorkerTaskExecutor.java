/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.boncfc.ide.server.worker.runner;

import com.boncfc.ide.plugin.task.api.*;
import com.boncfc.ide.plugin.task.api.model.*;
import com.boncfc.ide.plugin.task.api.utils.DateUtils;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.boncfc.ide.server.worker.utils.LogUtils;
import com.boncfc.ide.plugin.task.api.utils.ProcessUtils;
import com.boncfc.ide.server.worker.common.log.TaskInstanceLogHeader;
import com.boncfc.ide.server.worker.config.WorkerConfig;
import com.boncfc.ide.server.worker.config.YarnConfig;
import com.boncfc.ide.server.worker.mapper.WorkerMapper;
import com.boncfc.ide.server.worker.registry.RegistryClient;
import com.boncfc.ide.server.worker.registry.WorkerRegistryClient;
import com.boncfc.ide.server.worker.registry.enums.RegistryNodeType;
import com.boncfc.ide.server.worker.utils.*;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static com.boncfc.ide.plugin.task.api.constants.Constants.*;
import static com.boncfc.ide.plugin.task.api.model.JobInstanceExtStatus.fail_normal;
import static com.boncfc.ide.plugin.task.api.model.JobParamType.IN;
import static com.boncfc.ide.plugin.task.api.model.JobParamType.OUT;
import static com.boncfc.ide.plugin.task.api.model.ParamType.RUNTIME;

public abstract class WorkerTaskExecutor implements Runnable {

    protected static final Logger log = LoggerFactory.getLogger(WorkerTaskExecutor.class);

    protected final TaskExecutionContext taskExecutionContext;
    protected final WorkerConfig workerConfig;
    protected final YarnConfig yarnConfig;
    protected final WorkerRegistryClient workerRegistryClient;
    protected final WorkerMapper workerMapper;
    protected final RegistryClient registryClient;

    protected @Nullable AbstractTask task;

    protected final String jobInstancePath;

    protected WorkerTaskExecutor(
            @NonNull TaskExecutionContext taskExecutionContext,
            @NonNull WorkerConfig workerConfig,
            @NonNull YarnConfig yarnConfig,
            @NonNull WorkerRegistryClient workerRegistryClient,
            @NonNull WorkerMapper workerMapper,
            @NonNull RegistryClient registryClient) {
        this.taskExecutionContext = taskExecutionContext;
        this.workerConfig = workerConfig;
        this.yarnConfig = yarnConfig;
        this.workerRegistryClient = workerRegistryClient;
        this.workerMapper = workerMapper;
        this.registryClient = registryClient;
        JobInstance jobInstance = taskExecutionContext.getJobInstance();
        this.jobInstancePath = String.join("/", RegistryNodeType.JOB_INSTANCE_BASE.getRegistryPath(),
                String.valueOf(jobInstance.getJfiId()), String.valueOf(jobInstance.getJiId()));

    }

    protected abstract void executeTask(TaskCallBack taskCallBack);

    protected void afterExecute() throws TaskException {
        if (task == null) {
            throw new TaskException("The current task instance is null");
        }
//        sendAlertIfNeeded();
        processTaskResult();
        WorkerTaskExecutorHolder.remove(taskExecutionContext.getJobInstance().getJiId());
        log.info("Remove the current task execute context from worker cache");

    }

    protected void afterThrowing(Throwable throwable) throws TaskException {
        if (cancelTask()) {
            log.info("Cancel the task successfully");
        }
        WorkerTaskExecutorHolder.remove(taskExecutionContext.getJobInstance().getJiId());
        taskExecutionContext.getJobInstance().setFinishTime(DateUtils.getCurrentDate());
        updateJobInstanceStatus(JobInstanceStatus.fail, fail_normal);
    }


    protected boolean cancelTask() {
        // cancel the task
        if (task == null) {
            return true;
        }
        try {
            task.cancel();
            // 如果有提交上 Yarn，需要补充杀死一下
            if (taskExecutionContext.getAppId() != null && !taskExecutionContext.getAppId().isEmpty()) {
                ProcessUtils.stopYarnJob(yarnConfig.getApplicationState(), taskExecutionContext.getAppId());
            }
            return true;
        } catch (Exception e) {
            log.error("Cancel task failed, this will not affect the taskInstance status, but you need to check manual",
                    e);
            return false;
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName("TaskLog-" + taskExecutionContext.getJobInstance().getJiId());
        try {
            JobInstance jobInstance = taskExecutionContext.getJobInstance();
            LogUtils.setJobInstanceIDMDC(jobInstance.getJiId());
            TaskInstanceLogHeader.printInitializeTaskContextHeader();
            initializeTask();
            TaskInstanceLogHeader.printLoadTaskInstancePluginHeader();
            beforeExecute();
            TaskCallBack taskCallBack = TaskCallbackImpl.builder()
                    .taskExecutionContext(taskExecutionContext)
                    .workerMapper(workerMapper)
                    .build();
            TaskInstanceLogHeader.printExecuteTaskHeader();
            updateJobInstanceStatus(JobInstanceStatus.inp, JobInstanceExtStatus.inp_running);
            executeTask(taskCallBack);
            TaskInstanceLogHeader.printFinalizeTaskHeader();
            afterExecute();
        } catch (Throwable ex) {
            log.error("Task execute failed, due to meet an exception", ex);
            afterThrowing(ex);
        } finally {
            LogUtils.removeJobInstanceIdMDC();
        }
    }

    public void updateJobInstanceStatus(JobInstanceStatus status, JobInstanceExtStatus jobInstanceExtStatus) {
        Map<String, String> jobInstanceStatus = new HashMap<>();
        jobInstanceStatus.put("jobInstanceStatus", status.name());
        jobInstanceStatus.put("jobInstanceExtStatus", jobInstanceExtStatus.name());
        registryClient.persistEphemeral(this.jobInstancePath, JSONUtils.toPrettyJsonString(jobInstanceStatus));

        this.taskExecutionContext.getJobInstance().setExtendedStatus(jobInstanceExtStatus.name());
        this.taskExecutionContext.getJobInstance().setStatus(status.name());
        log.info("Set job status: {} , ext status: {}", status.name(), jobInstanceExtStatus.name());
        workerMapper.updateJobInstance(taskExecutionContext.getJobInstance());
    }

    protected void initializeTask() {
        log.info("Begin to initialize task");
        Date takenTime = DateUtils.getCurrentDate();
        taskExecutionContext.getJobInstance().setTakenTime(takenTime);
        log.info("Set job startTime: {}", takenTime);
        log.info("End initialize task {}", JSONUtils.toPrettyJsonString(taskExecutionContext));
    }


    protected void beforeExecute() {
        updateJobInstanceStatus(JobInstanceStatus.inp, JobInstanceExtStatus.inp_torun);
        log.info("add jobinstance params,jiId {}", taskExecutionContext.getJobInstance().getJiId());
        workerMapper.deleteJobInstanceParams(String.valueOf(taskExecutionContext.getJobInstance().getJiId()));
        List<JobInstanceParams> jobInstanceParamsList = taskExecutionContext.getJobInstanceParamsList().stream()
                .filter(jobInstanceParams -> NO.equals(jobInstanceParams.getIsPreset()) &&
                        (IN.name().equals(jobInstanceParams.getJobParamType()) ||
                                !RUNTIME.name().equals(jobInstanceParams.getParamType())))
                .collect(Collectors.toList());
        if(!jobInstanceParamsList.isEmpty()) workerMapper.addJobInstanceParams(jobInstanceParamsList);

        TaskChannel taskChannel =
                Optional.ofNullable(TaskPluginManager.getTaskChannelMap().get(taskExecutionContext.getJobType()))
                        .orElseThrow(() -> new TaskPluginException(taskExecutionContext.getJobType()
                                + " task plugin not found, please check the task type is correct."));

        log.info("Create TaskChannel: {} successfully", taskChannel.getClass().getName());

        task = taskChannel.createTask(taskExecutionContext);
        log.info("Task plugin instance: {} create successfully", taskExecutionContext.getJobInstance().getJiId());

        // todo: remove the init method, this should initialize in constructor method
        task.init();
        log.info("Success initialized task plugin instance successfully");
    }

    protected void sendAlertIfNeeded() {
        if (!task.getNeedAlert()) {
            return;
        }

        // todo: We need to send the alert to the master rather than directly send to the alert server
        Optional<Host> alertServerAddressOptional = workerRegistryClient.getAlertServerAddress();
        if (!alertServerAddressOptional.isPresent()) {
            log.error("Cannot get alert server address, please check the alert server is running");
            return;
        }
        Host alertServerAddress = alertServerAddressOptional.get();


    }

    protected void processTaskResult() {
        // 状态更新
        taskExecutionContext.getJobInstance().setFinishTime(DateUtils.getCurrentDate());
        taskExecutionContext.getJobInstance().setUpdateTime(DateUtils.getCurrentDate());
        TaskExecutionStatus taskExecutionStatus = task.getExitStatus();

        if (taskExecutionStatus.isSuccess()) {
            updateJobInstanceStatus(JobInstanceStatus.success, JobInstanceExtStatus.success_normal);
        } else if (taskExecutionStatus.isFailure()) {
            updateJobInstanceStatus(JobInstanceStatus.fail, JobInstanceExtStatus.fail_normal);
        } else if (taskExecutionStatus.isKill()) {
            updateJobInstanceStatus(JobInstanceStatus.fail, JobInstanceExtStatus.fail_killed);
        }

        List<JobInstanceIds> jobInstanceIdsList = new LinkedList<>();
        if (taskExecutionContext.getAppId() != null && !taskExecutionContext.getAppId().isEmpty()) {
            JobInstanceIds jobInstanceIds = JobInstanceIds.builder()
                    .jiId(taskExecutionContext.getJobInstance().getJiId())
                    .idType(APPLICATION_ID)
                    .idValue(taskExecutionContext.getAppId())
                    .build();
            jobInstanceIdsList.add(jobInstanceIds);
        }
        if (!jobInstanceIdsList.isEmpty()) workerMapper.addJobInstanceIds(jobInstanceIdsList);

        // 输出参数
        List<JobInstanceParams> jobInstanceParams = this.taskExecutionContext.getJobInstanceParamsList().stream()
                .filter(jp -> OUT.name().equals(jp.getJobParamType()) && RUNTIME.name().equals(jp.getParamType()))
                .sorted(Comparator.comparing(JobInstanceParams::getSortIndex))
                .collect(Collectors.toList());
        if (!jobInstanceParams.isEmpty()) workerMapper.addJobInstanceParams(jobInstanceParams);
    }

    protected void closeLogAppender() {

    }

    public @NonNull TaskExecutionContext getTaskExecutionContext() {
        return taskExecutionContext;
    }

    public @Nullable AbstractTask getTask() {
        return task;
    }

}
