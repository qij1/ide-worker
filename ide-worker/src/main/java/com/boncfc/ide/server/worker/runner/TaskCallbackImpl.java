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

import com.boncfc.ide.plugin.task.api.TaskCallBack;
import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import com.boncfc.ide.plugin.task.api.model.ApplicationInfo;
import com.boncfc.ide.plugin.task.api.model.JobInstance;
import com.boncfc.ide.plugin.task.api.model.JobInstanceIds;
import com.boncfc.ide.server.worker.mapper.WorkerMapper;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;

import static com.boncfc.ide.plugin.task.api.constants.Constants.APPLICATION_ID;
import static com.boncfc.ide.plugin.task.api.constants.Constants.PID;

@Slf4j
@Builder
public class TaskCallbackImpl implements TaskCallBack {

    private final TaskExecutionContext taskExecutionContext;

    private final WorkerMapper workerMapper;


    public TaskCallbackImpl(TaskExecutionContext taskExecutionContext, WorkerMapper workerMapper) {
        this.taskExecutionContext = taskExecutionContext;
        this.workerMapper = workerMapper;
    }

    @Override
    public void updateRemoteApplicationInfo(int taskInstanceId, ApplicationInfo applicationInfo) {
        // todo: use listener
    }

    @Override
    public void updateTaskInstanceInfo(int taskInstanceId) {
        List<JobInstanceIds> jobInstanceIdsList = new LinkedList<>();
        if (0 != taskExecutionContext.getProcessId()) {
            JobInstanceIds jobInstanceIds = JobInstanceIds.builder()
                    .jiId(taskExecutionContext.getJobInstance().getJiId())
                    .idType(PID)
                    .idValue(String.valueOf(taskExecutionContext.getProcessId()))
                    .build();
            jobInstanceIdsList.add(jobInstanceIds);
        }

        if (!jobInstanceIdsList.isEmpty()) workerMapper.addJobInstanceIds(jobInstanceIdsList);
    }

}
