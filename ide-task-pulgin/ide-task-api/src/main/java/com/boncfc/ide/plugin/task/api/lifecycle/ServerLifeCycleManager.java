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

package com.boncfc.ide.plugin.task.api.lifecycle;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class ServerLifeCycleManager {

    private static volatile ServerLifeCycleStatus serverStatus = ServerLifeCycleStatus.RUNNING;

    private static long serverStartupTime = System.currentTimeMillis();

    public static long getServerStartupTime() {
        return serverStartupTime;
    }

    public static boolean isRunning() {
        return serverStatus == ServerLifeCycleStatus.RUNNING;
    }

    public static boolean isStopped() {
        return serverStatus == ServerLifeCycleStatus.STOPPED;
    }

    public static ServerLifeCycleStatus getServerStatus() {
        return serverStatus;
    }

    /**
     * Change the current server state to {@link ServerLifeCycleStatus#WAITING}, only {@link ServerLifeCycleStatus#RUNNING} can change to {@link ServerLifeCycleStatus#WAITING}.
     *
     * @throws ServerLifeCycleException if change failed.
     */
    public static synchronized void toWaiting() throws ServerLifeCycleException {
        if (isStopped()) {
            throw new ServerLifeCycleException("The current server is already stopped, cannot change to waiting");
        }

        if (serverStatus == ServerLifeCycleStatus.WAITING) {
            log.warn("The current server is already at waiting status, cannot change to waiting");
            return;
        }
        serverStatus = ServerLifeCycleStatus.WAITING;
    }

    /**
     * Recover from {@link ServerLifeCycleStatus#WAITING} to {@link ServerLifeCycleStatus#RUNNING}.
     */
    public static synchronized void recoverFromWaiting() throws ServerLifeCycleException {
        if (isStopped()) {
            throw new ServerLifeCycleException("The current server is already stopped, cannot recovery");
        }

        if (serverStatus == ServerLifeCycleStatus.RUNNING) {
            log.warn("The current server status is already running, cannot recover form waiting");
            return;
        }
        serverStartupTime = System.currentTimeMillis();
        serverStatus = ServerLifeCycleStatus.RUNNING;
    }

    public static synchronized boolean toStopped() {
        if (serverStatus == ServerLifeCycleStatus.STOPPED) {
            return false;
        }
        serverStatus = ServerLifeCycleStatus.STOPPED;
        return true;
    }

}
