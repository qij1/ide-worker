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

package com.boncfc.ide.server.worker.registry.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum RegistryNodeType {

    ALL_SERVERS("nodes", "/nodes"),
    MASTER("Master", "/nodes/master"),
    WORKER("Worker", "/nodes/worker"),
    WORKER_JOB_LOCK("WorkerJobLock", "/lock/worker"),
    WORKER_DATX_JOB_LOCK("WorkerDataxJobLock", "/lock/worker-datax"),
    WORKER_SQL_JOB_LOCK("WorkerSQLJobLock", "/lock/worker-sql"),
    WORKER_SHELL_JOB_LOCK("WorkerShellJobLock", "/lock/worker-shell"),
    WORKER_HTTP_JOB_LOCK("WorkerHttpJobLock", "/lock/worker-http"),
    WORKER_QV_JOB_LOCK("WorkerQvJobLock", "/lock/worker-qualityvalid"),
    WORKER_CK_JOB_LOCK("WorkerCKJobLock", "/lock/worker-check"),
    ALERT_SERVER("AlertServer", "/nodes/alert-server"),
    ALERT_LOCK("AlertNodeLock", "/lock/alert"),
    JOB_INSTANCE_BASE("JobInstanceBase", "/jobs"),
    ;

    private final String name;

    private final String registryPath;
}
