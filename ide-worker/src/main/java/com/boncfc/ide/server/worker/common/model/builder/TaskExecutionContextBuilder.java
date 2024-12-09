package com.boncfc.ide.server.worker.common.model.builder;

import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskExecutionContextBuilder {
    public static TaskExecutionContextBuilder get() {
        return new TaskExecutionContextBuilder();
    }

    private TaskExecutionContext taskExecutionContext = new TaskExecutionContext();


}
