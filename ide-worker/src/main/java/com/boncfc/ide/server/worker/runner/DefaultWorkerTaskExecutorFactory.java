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

import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import com.boncfc.ide.server.worker.config.WorkerConfig;
import com.boncfc.ide.server.worker.mapper.WorkerMapper;
import com.boncfc.ide.server.worker.registry.RegistryClient;
import com.boncfc.ide.server.worker.registry.WorkerRegistryClient;
import lombok.NonNull;
import javax.annotation.Nullable;

public class DefaultWorkerTaskExecutorFactory
        implements
            WorkerTaskExecutorFactory<DefaultWorkerTaskExecutor> {

    private final @NonNull TaskExecutionContext taskExecutionContext;
    private final @NonNull WorkerConfig workerConfig;

    private final @NonNull RegistryClient registryClient;

    private final @NonNull WorkerMapper workerMapper;
    public DefaultWorkerTaskExecutorFactory(@NonNull TaskExecutionContext taskExecutionContext,
                                            @NonNull WorkerConfig workerConfig,
                                            @NonNull WorkerRegistryClient workerRegistryClient,
                                            @NonNull RegistryClient registryClient,
                                            @NonNull WorkerMapper workerMapper) {
        this.taskExecutionContext = taskExecutionContext;
        this.workerConfig = workerConfig;
        this.workerRegistryClient = workerRegistryClient;
        this.workerMapper = workerMapper;
        this.registryClient = registryClient;
    }

    private final @NonNull WorkerRegistryClient workerRegistryClient;

    @Override
    public DefaultWorkerTaskExecutor createWorkerTaskExecutor() {
        return new DefaultWorkerTaskExecutor(
                taskExecutionContext,
                workerConfig,
                workerRegistryClient,
                workerMapper,
                registryClient);
    }
}
