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

import com.boncfc.ide.plugin.task.api.utils.FileUtils;
import com.boncfc.ide.plugin.task.api.utils.PropertyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import com.boncfc.ide.plugin.task.api.utils.ParameterUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.boncfc.ide.plugin.task.api.constants.Constants.TASK_RESOURCE_LIMIT_STATE;

@Slf4j
public abstract class BaseLinuxShellInterceptorBuilder<T extends BaseLinuxShellInterceptorBuilder<T, Y>, Y extends BaseShellInterceptor>
        extends
            BaseShellInterceptorBuilder<T, Y> {

    protected void generateShellScript() throws IOException {
        List<String> finalScripts = new ArrayList<>();
        // add shell header
        finalScripts.add(shellHeader());
        finalScripts.add("BASEDIR=$(cd `dirname $0`; pwd)");
        finalScripts.add("cd $BASEDIR");
        // add shell body
        finalScripts.add(shellBody());
        // create shell file
        String finalScript = finalScripts.stream().collect(Collectors.joining(System.lineSeparator()));
        Path shellAbsolutePath = shellAbsolutePath();
        FileUtils.createFileWith755(shellAbsolutePath);
        Files.write(shellAbsolutePath, finalScript.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        log.info("Final Shell file is: ");
        log.info(
                "****************************** Script Content *****************************************************************");
        log.info(finalScript);
        log.info(
                "****************************** Script Content *****************************************************************");
    }

    protected abstract String shellHeader();

    protected abstract String shellInterpreter();

    protected abstract String shellExtension();


    private String shellBody() {
        if (CollectionUtils.isEmpty(scripts)) {
            return StringUtils.EMPTY;
        }
        String scriptBody = scripts
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));
        scriptBody = scriptBody.replaceAll("\\r\\n", System.lineSeparator());
        return ParameterUtils.convertParameterPlaceholders(scriptBody, propertyMap);
    }

    private Path shellAbsolutePath() {
        return Paths.get(shellDirectory, shellName + shellExtension());
    }

    protected List<String> generateBootstrapCommand() {
        if (sudoEnable) {
            return bootstrapCommandInSudoMode();
        }
        return bootstrapCommandInNormalMode();
    }

    private List<String> bootstrapCommandInSudoMode() {
        List<String> bootstrapCommand = new ArrayList<>();
        bootstrapCommand.add("sudo");
        if (StringUtils.isNotBlank(runUser)) {
            bootstrapCommand.add("-u");
            bootstrapCommand.add(runUser);
        }
        bootstrapCommand.add("-i");
        bootstrapCommand.add(shellAbsolutePath().toString());
        return bootstrapCommand;
    }

    private List<String> bootstrapCommandInNormalMode() {
        List<String> bootstrapCommand = new ArrayList<>();
        bootstrapCommand.add(shellInterpreter());
        bootstrapCommand.add(shellAbsolutePath().toString());
        return bootstrapCommand;
    }
}
