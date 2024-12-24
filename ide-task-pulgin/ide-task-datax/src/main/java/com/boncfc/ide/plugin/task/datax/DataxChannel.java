package com.boncfc.ide.plugin.task.datax;

import com.boncfc.ide.plugin.task.api.AbstractTask;
import com.boncfc.ide.plugin.task.api.TaskChannel;
import com.boncfc.ide.plugin.task.api.TaskExecutionContext;

public class DataxChannel implements TaskChannel {
    @Override
    public void cancelApplication(boolean status) {

    }

    @Override
    public AbstractTask createTask(TaskExecutionContext taskRequest) {
        return new DataxTask(taskRequest);
    }
}
