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

package com.boncfc.ide.plugin.task.api.shell;

import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseShellInterceptorBuilder<T extends BaseShellInterceptorBuilder<T, Y>, Y extends BaseShellInterceptor>
        implements
            IShellInterceptorBuilder<T, Y> {

    protected String shellDirectory;

    protected String shellName;

    protected String runUser;

    protected Map<String, String> propertyMap = new HashMap<>();

    protected boolean sudoEnable;

    protected List<String> scripts = new ArrayList<>();

    protected BaseShellInterceptorBuilder() {
    }

    @Override
    public T newBuilder(T builder) {
        T newBuilder = newBuilder();
        newBuilder.shellDirectory = builder.shellDirectory;
        newBuilder.shellName = builder.shellName;
        newBuilder.runUser = builder.runUser;
        newBuilder.propertyMap = builder.propertyMap;
        newBuilder.sudoEnable = builder.sudoEnable;
        newBuilder.scripts = builder.scripts;
        return newBuilder;
    }

    @Override
    public T shellDirectory(String shellDirectory) {
        this.shellDirectory = shellDirectory;
        return (T) this;
    }

    @Override
    public T shellName(String shellFilename) {
        this.shellName = shellFilename;
        return (T) this;
    }

    @Override
    public T runUser(String systemUser) {
        this.runUser = systemUser;
        return (T) this;
    }

    @Override
    public T properties(Map<String, String> propertyMap) {
        if (MapUtils.isNotEmpty(propertyMap)) {
            this.propertyMap.putAll(propertyMap);
        }
        return (T) this;
    }

    @Override
    public T sudoMode(boolean sudoEnable) {
        this.sudoEnable = sudoEnable;
        return (T) this;
    }

    @Override
    public T appendScript(String script) {
        scripts.add(script);
        return (T) this;
    }

}
