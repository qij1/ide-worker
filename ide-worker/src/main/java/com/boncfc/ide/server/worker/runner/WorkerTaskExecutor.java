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
import com.boncfc.ide.plugin.task.api.model.JobInstanceExtStatus;
import com.boncfc.ide.plugin.task.api.model.JobInstance;
import com.boncfc.ide.plugin.task.api.model.TaskExecutionStatus;
import com.boncfc.ide.server.worker.common.log.TaskInstanceLogHeader;
import com.boncfc.ide.server.worker.config.WorkerConfig;
import com.boncfc.ide.server.worker.mapper.WorkerMapper;
import com.boncfc.ide.server.worker.registry.RegistryClient;
import com.boncfc.ide.server.worker.registry.WorkerRegistryClient;
import com.boncfc.ide.server.worker.utils.*;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.boncfc.ide.plugin.task.api.constants.Constants.YES;
import static com.boncfc.ide.plugin.task.api.model.JobInstanceExtStatus.fail_normal;
import static com.boncfc.ide.plugin.task.api.model.JobInstanceStatus.fail;
import static com.boncfc.ide.plugin.task.api.model.JobParamType.IN;
import static com.boncfc.ide.plugin.task.api.model.JobParamType.OUT;
import static com.boncfc.ide.plugin.task.api.model.ParamType.RUNTIME;

public abstract class WorkerTaskExecutor implements Runnable {

    protected static final Logger log = LoggerFactory.getLogger(WorkerTaskExecutor.class);

    protected final TaskExecutionContext taskExecutionContext;
    protected final WorkerConfig workerConfig;
    protected final WorkerRegistryClient workerRegistryClient;
    protected final WorkerMapper workerMapper;
    protected final RegistryClient registryClient;

    protected @Nullable AbstractTask task;

    protected WorkerTaskExecutor(
            @NonNull TaskExecutionContext taskExecutionContext,
            @NonNull WorkerConfig workerConfig,
            @NonNull WorkerRegistryClient workerRegistryClient,
            @NonNull WorkerMapper workerMapper,
            @NonNull RegistryClient registryClient) {
        this.taskExecutionContext = taskExecutionContext;
        this.workerConfig = workerConfig;
        this.workerRegistryClient = workerRegistryClient;
        this.workerMapper = workerMapper;
        this.registryClient = registryClient;
    }

    protected abstract void executeTask(TaskCallBack taskCallBack);

    protected void afterExecute() throws TaskException {
        if (task == null) {
            throw new TaskException("The current task instance is null");
        }
//        sendAlertIfNeeded();

        sendTaskResult();
        WorkerTaskExecutorHolder.remove(taskExecutionContext.getJobInstance().getJiId());
        log.info("Remove the current task execute context from worker cache");

    }

    protected void afterThrowing(Throwable throwable) throws TaskException {
        if (cancelTask()) {
            log.info("Cancel the task successfully");
        }
        WorkerTaskExecutorHolder.remove(taskExecutionContext.getJobInstance().getJiId());
        taskExecutionContext.getJobInstance().setFinishTime(DateUtils.getCurrentDate());
        taskExecutionContext.getJobInstance().setStatus(fail.name());
        taskExecutionContext.getJobInstance().setExtendedStatus(fail_normal.name());

    }

    protected boolean cancelTask() {
        // cancel the task
        if (task == null) {
            return true;
        }
        try {
            task.cancel();
            ProcessUtils.cancelApplication(taskExecutionContext);
            return true;
        } catch (Exception e) {
            log.error("Cancel task failed, this will not affect the taskInstance status, but you need to check manual",
                    e);
            return false;
        }
    }

    @Override
    public void run() {
        try {
            JobInstance jobInstance = taskExecutionContext.getJobInstance();
            LogUtils.setJobInstanceIDMDC(jobInstance.getJiId());
            TaskInstanceLogHeader.printInitializeTaskContextHeader();
            initializeTask();
            TaskInstanceLogHeader.printLoadTaskInstancePluginHeader();
            beforeExecute();

            TaskCallBack taskCallBack = TaskCallbackImpl.builder()
                    .taskExecutionContext(taskExecutionContext)
                    .build();

            TaskInstanceLogHeader.printExecuteTaskHeader();
            executeTask(taskCallBack);

            TaskInstanceLogHeader.printFinalizeTaskHeader();
            afterExecute();
        } catch (Throwable ex) {
            log.error("Task execute failed, due to meet an exception", ex);
            afterThrowing(ex);
            closeLogAppender();
        } finally {
            LogUtils.removeJobInstanceIdMDC();
        }
    }


    protected void initializeTask() {
        log.info("Begin to initialize task");
        Date takenTime = DateUtils.getCurrentDate();
        taskExecutionContext.getJobInstance().setTakenTime(takenTime);
        log.info("Set job startTime: {}", takenTime);
        log.info("End initialize task {}", JSONUtils.toPrettyJsonString(taskExecutionContext));
    }


    protected void beforeExecute() {
        taskExecutionContext.getJobInstance().setExtendedStatus(JobInstanceExtStatus.inp_torun.name());
        log.info("Set job ext status: {}", JobInstanceExtStatus.inp_torun.name());
        workerMapper.updateJobInstance(taskExecutionContext.getJobInstance());

        log.info("add jobinstance params,jiId {}", taskExecutionContext.getJobInstance().getJiId());

        workerMapper.addJobInstanceParams(taskExecutionContext.getJobInstanceParamsList().stream()
                .filter(jobInstanceParams -> IN.name().equals(jobInstanceParams.getJobParamType()) ||
                                             !RUNTIME.name().equals(jobInstanceParams.getParamType()))
                .collect(Collectors.toList()));

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

    protected void sendTaskResult() {
        TaskExecutionStatus taskExecutionStatus = task.getExitStatus();
        JobInstance jobInstance = taskExecutionContext.getJobInstance();
        jobInstance.setFinishTime(DateUtils.getCurrentDate());

//        taskExecutionContext.setProcessId(task.getProcessId());
//        taskExecutionContext.setAppIds(task.getAppIds());
//        taskExecutionContext.setVarPool(JSONUtils.toJsonString(task.getParameters().getVarPool()));
//        taskExecutionContext.setEndTime(System.currentTimeMillis());
//
//        log.info("Send task execute status: {} to master : {}", taskExecutionContext.getCurrentExecutionStatus().name(),
//                taskExecutionContext.getHost());
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
