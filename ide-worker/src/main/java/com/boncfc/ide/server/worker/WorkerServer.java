package com.boncfc.ide.server.worker;

import com.boncfc.ide.plugin.task.api.TaskPluginManager;
import com.boncfc.ide.plugin.task.api.datasource.DataSourceProcessorProvider;
import com.boncfc.ide.server.worker.common.IStoppable;
import com.boncfc.ide.server.worker.common.lifecycle.ServerLifeCycleManager;
import com.boncfc.ide.server.worker.constants.Constants;
import com.boncfc.ide.server.worker.registry.RegistryClient;
import com.boncfc.ide.server.worker.registry.WorkerRegistryClient;
import com.boncfc.ide.server.worker.thread.TaskThread;
import com.boncfc.ide.server.worker.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;


@Slf4j
@MapperScan("com.boncfc.ide.server.worker.mapper")
@SpringBootApplication
public class WorkerServer implements IStoppable {


    @Autowired
    private RegistryClient registryClient;

    @Autowired
    private WorkerRegistryClient workerRegistryClient;

    @Autowired
    TaskThread taskThread;

    private ExecutorService jobExecutorService;

    private ExecutorService cacheExecutorService;

    public static void main(String[] args) {
        Thread.currentThread().setName(Constants.THREAD_NAME_WORKER_SERVER);
        SpringApplication.run(WorkerServer.class);
    }

    @PostConstruct
    public void run() {
        TaskPluginManager.loadPlugin();
        DataSourceProcessorProvider.initialize();
        this.workerRegistryClient.setRegistryStoppable(this);
        this.workerRegistryClient.start();

        jobExecutorService = ThreadUtils.newSingleDaemonScheduledExecutorService("Worker-Fetch-Task-Thread");
        jobExecutorService.execute(taskThread);

        /*
         * registry hooks, which are called before the process exits
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!ServerLifeCycleManager.isStopped()) {
                close("WorkerServer shutdown hook");
            }
        }));


    }

    @Override
    public void stop(String cause) {
        close(cause);
    }

    public void close(String cause) {
        if (!ServerLifeCycleManager.toStopped()) {
            log.warn("WorkerServer is already stopped, current cause: {}", cause);
            return;
        }
        ThreadUtils.sleep(Constants.SERVER_CLOSE_WAIT_TIME.toMillis());

        try (WorkerRegistryClient closedRegistryClient = workerRegistryClient) {
            log.info("Worker server is stopping, current cause : {}", cause);
            // todo: we need to remove this method
            // since for some task, we need to take-over the remote task after the worker restart
            // and if the worker crash, the `killAllRunningTasks` will not be execute, this will cause there exist two
            // kind of situation:
            // 1. If the worker is stop by kill, the tasks will be kill.
            // 2. If the worker is stop by kill -9, the tasks will not be kill.
            // So we don't need to kill the tasks.
//            this.killAllRunningTasks();
        } catch (Exception e) {
            log.error("Worker server stop failed, current cause: {}", cause, e);
            return;
        }
        log.info("Worker server stopped, current cause: {}", cause);
    }

}
