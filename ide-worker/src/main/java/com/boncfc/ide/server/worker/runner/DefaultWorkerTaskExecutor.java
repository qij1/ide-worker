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
import com.boncfc.ide.plugin.task.api.TaskException;
import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import com.boncfc.ide.server.worker.config.WorkerConfig;
import com.boncfc.ide.server.worker.config.YarnConfig;
import com.boncfc.ide.server.worker.mapper.WorkerMapper;
import com.boncfc.ide.server.worker.registry.RegistryClient;
import com.boncfc.ide.server.worker.registry.WorkerRegistryClient;
import lombok.NonNull;

public class DefaultWorkerTaskExecutor extends WorkerTaskExecutor {

    public DefaultWorkerTaskExecutor(@NonNull TaskExecutionContext taskExecutionContext,
                                     @NonNull WorkerConfig workerConfig,
                                     @NonNull YarnConfig yarnConfig,
                                     @NonNull WorkerRegistryClient workerRegistryClient,
                                     @NonNull WorkerMapper workerMapper,
                                     @NonNull RegistryClient registryClient) {
        super(taskExecutionContext,
                workerConfig,
                yarnConfig,
                workerRegistryClient,
                workerMapper,
                registryClient);
    }

    @Override
    public void executeTask(TaskCallBack taskCallBack) throws TaskException {
        if (task == null) {
            throw new IllegalArgumentException("The task plugin instance is not initialized");
        }
        task.handle(taskCallBack);
    }

    @Override
    protected void afterExecute() {
        super.afterExecute();
    }

    @Override
    protected void afterThrowing(Throwable throwable) throws TaskException {
        super.afterThrowing(throwable);
    }
}
