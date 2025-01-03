package com.boncfc.ide.server.worker.api;

import com.boncfc.ide.plugin.task.api.AbstractTask;
import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import com.boncfc.ide.plugin.task.api.model.JobInstanceExtStatus;
import com.boncfc.ide.plugin.task.api.model.JobInstanceStatus;
import com.boncfc.ide.plugin.task.api.utils.ProcessUtils;
import com.boncfc.ide.server.worker.common.model.Result;
import com.boncfc.ide.server.worker.config.YarnConfig;
import com.boncfc.ide.server.worker.runner.WorkerTaskExecutor;
import com.boncfc.ide.server.worker.runner.WorkerTaskExecutorHolder;
import com.boncfc.ide.server.worker.runner.WorkerTaskExecutorThreadPool;
import com.boncfc.ide.server.worker.utils.LogUtils;
import com.boncfc.ide.plugin.task.api.utils.OSUtils;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/ide/worker/job-instances")
public class JobInstanceOperator {

    @Autowired
    YarnConfig yarnConfig;

    @Autowired
    private WorkerTaskExecutorThreadPool workerManager;

    @PostMapping(value = "/{jiId}/cancel")
    @ResponseStatus(HttpStatus.OK)
    public Result<Void> killOperate(@PathVariable(value = "jiId") Integer jiId) {
        log.info("Receive JobInstanceKillRequest: {}", jiId);
        try {
            LogUtils.setJobInstanceIDMDC(jiId);
            // 停止动作，任务、进程、yarnapp处理
            WorkerTaskExecutor workerTaskExecutor = WorkerTaskExecutorHolder.get(jiId);
            if (workerTaskExecutor == null) {
                log.warn("Cannot find WorkerTaskExecutor for jobInstance: {}", jiId);
                return Result.success();
            }
            cancelApplication(workerTaskExecutor);
            TaskExecutionContext taskExecutionContext = workerTaskExecutor.getTaskExecutionContext();
            killProcess(taskExecutionContext.getTenantCode(), taskExecutionContext.getProcessId());
            cancelYarnApp(taskExecutionContext.getAppId());

            // 清除动作：线程池、内存等
            workerManager.killTaskBeforeExecuteByInstanceId(jiId);
            WorkerTaskExecutorHolder.remove(jiId);

            // 任务状态更新
            workerTaskExecutor.updateJobInstanceStatus(JobInstanceStatus.fail, JobInstanceExtStatus.fail_killed);
        } finally {
            LogUtils.removeJobInstanceIdMDC();
        }
        return Result.success();
    }


    protected void cancelApplication(WorkerTaskExecutor workerTaskExecutor) {
        AbstractTask task = workerTaskExecutor.getTask();
        if (task == null) {
            log.warn("jobinstance not found, jobInstanceId: {}",
                    workerTaskExecutor.getTaskExecutionContext().getJobInstance().getJiId());
            return;
        }
        try {
            task.cancel();
        } catch (Exception e) {
            log.error("kill task error", e);
        }
        log.info("kill task by cancelApplication, taskInstanceId: {}",
                workerTaskExecutor.getTaskExecutionContext().getJobInstance().getJiId());
    }

    private void cancelYarnApp(String appIds) {
        if (appIds != null && !appIds.isEmpty()) {
            List<String> appList = Arrays.asList(appIds.split(","));
            appList.forEach(appId -> ProcessUtils.stopYarnJob(yarnConfig.getApplicationState(), appId));
        }
    }

    private void killProcess(String tenantCode, Integer processId) {
        // todo: directly interrupt the process
        if (processId == null || processId.equals(0)) {
            return;
        }
        try {
            String pidsStr = ProcessUtils.getPidsStr(processId);
            if (!Strings.isNullOrEmpty(pidsStr)) {
                String cmd = String.format("kill -9 %s", pidsStr);
                cmd = OSUtils.getSudoCmd(tenantCode, cmd);
                log.info("process id:{}, cmd:{}", processId, cmd);
                OSUtils.exeCmd(cmd);
            }
        } catch (Exception e) {
            log.error("kill task error", e);
        }
    }


}
