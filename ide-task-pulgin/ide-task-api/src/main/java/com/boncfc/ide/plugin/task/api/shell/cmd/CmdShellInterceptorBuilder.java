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

package com.boncfc.ide.plugin.task.api.shell.cmd;


import com.boncfc.ide.plugin.task.api.shell.BaseWindowsShellInterceptorBuilder;

import java.io.IOException;
import java.util.List;

public class CmdShellInterceptorBuilder
        extends
        BaseWindowsShellInterceptorBuilder<CmdShellInterceptorBuilder, CmdShellInterceptor> {

    @Override
    protected String shellHeader() {
        return "@echo off";
    }

    @Override
    protected String shellInterpreter() {
        return "cmd.exe";
    }

    @Override
    protected String shellExtension() {
        return ".bat";
    }

    @Override
    public CmdShellInterceptorBuilder newBuilder() {
        return new CmdShellInterceptorBuilder();
    }

    @Override
    public CmdShellInterceptor build() throws IOException {
        generateShellScript();
        List<String> bootstrapCommand = generateBootstrapCommand();
        return new CmdShellInterceptor(bootstrapCommand, shellDirectory);
    }
}
