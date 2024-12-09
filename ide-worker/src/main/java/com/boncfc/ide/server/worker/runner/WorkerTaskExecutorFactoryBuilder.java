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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkerTaskExecutorFactoryBuilder {

    @Autowired
    private WorkerConfig workerConfig;

    @Autowired
    private WorkerRegistryClient workerRegistryClient;

    @Autowired
    private WorkerMapper workerMapper;

    @Autowired
    private RegistryClient registryClient;

    public WorkerTaskExecutorFactory<? extends WorkerTaskExecutor> createWorkerTaskExecutorFactory(TaskExecutionContext taskExecutionContext) {
        return new DefaultWorkerTaskExecutorFactory(taskExecutionContext,
                workerConfig,
                workerRegistryClient,
                registryClient,
                workerMapper);
    }

}
