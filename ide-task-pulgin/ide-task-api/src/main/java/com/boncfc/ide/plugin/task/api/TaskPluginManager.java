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

package com.boncfc.ide.plugin.task.api;

import com.boncfc.ide.plugin.task.api.spi.PrioritySPIFactory;
import lombok.extern.slf4j.Slf4j;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TaskPluginManager {

    private static final Map<String, TaskChannelFactory> taskChannelFactoryMap = new HashMap<>();
    private static final Map<String, TaskChannel> taskChannelMap = new HashMap<>();

    private static final AtomicBoolean loadedFlag = new AtomicBoolean(false);



    /**
     * Load task plugins from classpath.
     */
    public static void loadPlugin() {
        if (!loadedFlag.compareAndSet(false, true)) {
            log.warn("The task plugin has already been loaded");
            return;
        }
        PrioritySPIFactory<TaskChannelFactory> prioritySPIFactory = new PrioritySPIFactory<>(TaskChannelFactory.class);
        for (Map.Entry<String, TaskChannelFactory> entry : prioritySPIFactory.getSPIMap().entrySet()) {
            String factoryName = entry.getKey();
            TaskChannelFactory factory = entry.getValue();

            log.info("Registering task plugin: {} - {}", factoryName, factory.getClass().getSimpleName());

            taskChannelFactoryMap.put(factoryName, factory);
            taskChannelMap.put(factoryName, factory.create());

            log.info("Registered task plugin: {} - {}", factoryName, factory.getClass().getSimpleName());
        }

    }

    public static Map<String, TaskChannel> getTaskChannelMap() {
        return Collections.unmodifiableMap(taskChannelMap);
    }

    public static Map<String, TaskChannelFactory> getTaskChannelFactoryMap() {
        return Collections.unmodifiableMap(taskChannelFactoryMap);
    }

    public static TaskChannel getTaskChannel(String type) {
        return getTaskChannelMap().get(type);
    }


}
