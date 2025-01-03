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

package com.boncfc.ide.plugin.task.api;

import com.boncfc.ide.plugin.task.api.constants.Constants;
import com.boncfc.ide.plugin.task.api.model.TaskResponse;
import com.boncfc.ide.plugin.task.api.shell.IShellInterceptor;
import com.boncfc.ide.plugin.task.api.shell.IShellInterceptorBuilder;
import com.boncfc.ide.plugin.task.api.thread.ThreadUtils;
import com.boncfc.ide.plugin.task.api.utils.OSUtils;
import com.boncfc.ide.plugin.task.api.utils.ProcessUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static com.boncfc.ide.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;
import static com.boncfc.ide.plugin.task.api.TaskConstants.EXIT_CODE_KILL;
import static com.boncfc.ide.plugin.task.api.constants.Constants.EMPTY_STRING;
import static com.boncfc.ide.plugin.task.api.constants.Constants.TASK_INSTANCE_ID_MDC_KEY;

/**
 * abstract command executor
 */
@Slf4j
public abstract class AbstractCommandExecutor {

    protected volatile Map<String, String> taskOutputParams = new HashMap<>();
    /**
     * process
     */
    private Process process;

    /**
     * log handler
     */
    protected Consumer<String> logHandler;

    /**
     * log list
     */
    protected LinkedBlockingQueue<String> logBuffer;

    protected boolean processLogOutputIsSuccess = false;

    protected boolean podLogOutputIsFinished = false;

    /**
     * taskRequest
     */
    protected TaskExecutionContext taskRequest;

    protected Future<?> taskOutputFuture;

    protected Future<?> podLogOutputFuture;

    public AbstractCommandExecutor(Consumer<String> logHandler,
                                   TaskExecutionContext taskRequest) {
        this.logHandler = logHandler;
        this.taskRequest = taskRequest;
        this.logBuffer = new LinkedBlockingQueue<>();
        this.logBuffer.add(EMPTY_STRING);

    }

    // todo: We need to build the IShellActuator in outer class, since different task may have specific logic to build
    // the IShellActuator
    public TaskResponse run(IShellInterceptorBuilder iShellInterceptorBuilder,
                            TaskCallBack taskCallBack) throws Exception {
        int exitCode;
        TaskResponse result = new TaskResponse();
        int taskInstanceId = taskRequest.getJobInstance().getJiId();
        // todo: we need to use state like JDK Thread to make sure the killed task should not be executed
        iShellInterceptorBuilder = iShellInterceptorBuilder
                .shellDirectory(taskRequest.getDataxProperties().getExecuteJobPath())
                .shellName(taskRequest.getTaskAppId() + "_sh");

        // Set sudo (This is only work in Linux)
        iShellInterceptorBuilder.sudoMode(OSUtils.isSudoEnable());
        // Set tenant (This is only work in Linux)
        iShellInterceptorBuilder.runUser(taskRequest.getTenantCode());

        IShellInterceptor iShellInterceptor = iShellInterceptorBuilder.build();
        process = iShellInterceptor.execute();

        // parse process output
        parseProcessOutput(this.process);

        int processId = getProcessId(this.process);

        result.setProcessId(processId);

        // cache processId
        taskRequest.setProcessId(processId);

        // print process id
        log.info("process start, process id is: {}", processId);

        // update pid before waiting for the run to finish
        if (null != taskCallBack) {
            taskCallBack.updateTaskInstanceInfo(taskInstanceId);
        }
        // if timeout occurs, exit directly
        long remainTime = getRemainTime();

        // waiting for the run to finish
        boolean status = this.process.waitFor(remainTime, TimeUnit.SECONDS);

        if (taskOutputFuture != null) {
            try {
                // Wait the task log process finished.
                taskOutputFuture.get();
            } catch (ExecutionException e) {
                log.error("Handle task log error", e);
            }
        }

        // if SHELL task exit
        if (status) {
            if (this.process != null) {
                exitCode = this.process.exitValue();
            } else {
                exitCode = EXIT_CODE_KILL;
            }
        } else {
            log.error("process has failure, the task timeout configuration value is:{}, ready to kill ...",
                    taskRequest.getTaskTimeout());
            exitCode = EXIT_CODE_FAILURE;
            cancelApplication();
        }
        result.setExitStatusCode(exitCode);
        String exitLogMessage = EXIT_CODE_KILL == exitCode ? "process has killed." : "process has exited.";
        log.info("{} execute path:{}, processId:{} ,exitStatusCode:{} ,processWaitForStatus:{} ,processExitValue:{}",
                exitLogMessage, taskRequest.getDataxProperties().getExecuteJobPath(), processId, result.getExitStatusCode(), status, exitCode);
        return result;

    }

    public Map<String, String> getTaskOutputParams() {
        return taskOutputParams;
    }

    public void cancelApplication() throws InterruptedException {
        if (process == null) {
            return;
        }

        // soft kill
        log.info("Begin to kill process process, pid is : {}", taskRequest.getProcessId());
        process.destroy();
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            process.destroyForcibly();
        }
        log.info("Success kill task: {}, pid: {}", taskRequest.getAppId(), taskRequest.getProcessId());
    }

    private void parseProcessOutput(Process process) {
        String jobInstId = String.valueOf(taskRequest.getJobInstance().getJiId());
        ExecutorService getOutputLogService = ThreadUtils
                .newSingleDaemonScheduledExecutorService(String.format("TaskLog-%s", jobInstId));
        taskOutputFuture = getOutputLogService.submit(() -> {
            try(BufferedReader inReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                MDC.put(TASK_INSTANCE_ID_MDC_KEY, jobInstId);
                String line;
                while ((line = inReader.readLine()) != null) {
                    logHandler.accept(line);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                MDC.remove(Constants.TASK_INSTANCE_ID_MDC_KEY);
            }
        });
        getOutputLogService.shutdown();
    }

    /**
     * get remain time（s）
     *
     * @return remain time
     */
    private long getRemainTime() {
        long usedTime = (System.currentTimeMillis() - taskRequest.getJobInstance().getTakenTime().getTime()) / 1000;
        long remainTime = taskRequest.getTaskTimeout() - usedTime;

        if (remainTime < 0) {
            throw new RuntimeException("task execution time out");
        }

        return remainTime;
    }

    /**
     * get process id
     *
     * @param process process
     * @return process id
     */
    private int getProcessId(Process process) {
        int processId = 0;

        try {
            Field f = process.getClass().getDeclaredField(TaskConstants.PID);
            f.setAccessible(true);

            processId = f.getInt(process);
        } catch (Exception e) {
            log.error("Get task pid failed", e);
        }

        return processId;
    }

}
