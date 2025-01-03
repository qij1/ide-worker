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

import com.boncfc.ide.plugin.task.api.constants.Constants;
import com.boncfc.ide.plugin.task.api.model.JobInstanceParams;
import com.boncfc.ide.plugin.task.api.model.TaskExecutionStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.boncfc.ide.plugin.task.api.constants.Constants.APPLICATION_REGEX;

@Slf4j
public abstract class AbstractTask {

    @Getter
    @Setter
    protected Map<String, String> taskOutputParams;

    /**
     * taskExecutionContext
     **/
    protected TaskExecutionContext taskRequest;

    /**
     * SHELL process pid
     */
    protected int processId;

    /**
     * other resource manager appId , for example : YARN etc
     */
    protected String appIds;

    /**
     * exit code
     */
    protected volatile int exitStatusCode = -1;

    protected boolean needAlert = false;

    protected boolean cancel = false;


    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    protected AbstractTask(TaskExecutionContext taskExecutionContext) {
        this.taskRequest = taskExecutionContext;
    }

    /**
     * init task
     */
    public void init() {
    }

    // todo: return TaskResult rather than store the result in Task
    public abstract void handle(TaskCallBack taskCallBack) throws TaskException;

    public abstract void cancel() throws TaskException;

    /**
     * get exit status code
     *
     * @return exit status code
     */
    public int getExitStatusCode() {
        return exitStatusCode;
    }

    public void setExitStatusCode(int exitStatusCode) {
        this.exitStatusCode = exitStatusCode;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public String getAppIds() {
        return appIds;
    }

    public void setAppIds(String appIds) {
        this.appIds = appIds;
    }

    public boolean getNeedAlert() {
        return needAlert;
    }

    public void setNeedAlert(boolean needAlert) {
        this.needAlert = needAlert;
    }

    /**
     * get exit status according to exitCode
     *
     * @return exit status
     */
    public TaskExecutionStatus getExitStatus() {
        switch (getExitStatusCode()) {
            case TaskConstants.EXIT_CODE_SUCCESS:
                return TaskExecutionStatus.SUCCESS;
            case TaskConstants.EXIT_CODE_KILL:
                return TaskExecutionStatus.KILL;
            default:
                return TaskExecutionStatus.FAILURE;
        }
    }

    /**
     * log handle
     *
     * @param logs log list
     */
    public void logHandle(String logs) {
        findAppId(logs);
        log.info(logs);
    }

    public static String groupName = "paramName1";
    public static String rgex = String.format("\\$\\{(?<%s>.*?)}", groupName);

    /**
     * regular expressions match the contents between two specified strings
     *
     * @param content        content
     * @param rgex           rgex
     * @param sqlParamsMap   sql params map
     * @param paramsPropsMap params props map
     */
    public static void setSqlParamsMap(String content, String rgex, Map<Integer, JobInstanceParams> sqlParamsMap,
                                       Map<String, JobInstanceParams> paramsPropsMap, int taskInstanceId) {
        if (paramsPropsMap == null) {
            return;
        }
        Pattern pattern = Pattern.compile(rgex);
        Matcher m = pattern.matcher(content);
        int index = 1;
        while (m.find()) {
            String paramName = m.group(groupName);
            JobInstanceParams prop = paramsPropsMap.get(paramName);
            if (prop == null) {
                log.error(
                        "setSqlParamsMap: No Property with paramName: {} is found in paramsPropsMap of task instance"
                                + " with id: {}. So couldn't put Property in sqlParamsMap.",
                        paramName, taskInstanceId);
            } else {
                sqlParamsMap.put(index, prop);
                index++;
                log.info(
                        "setSqlParamsMap: Property with paramName: {} put in sqlParamsMap of content {} successfully.",
                        paramName, content);
            }
        }
    }

    private static final char PARAM_REPLACE_CHAR = '?';

    public static String expandListParameter(Map<Integer, JobInstanceParams> params, String sql) {
        Map<Integer, JobInstanceParams> expandMap = new HashMap<>();
        if (params == null || params.isEmpty()) {
            return sql;
        }
        String[] split = sql.split("\\?");
        if (split.length == 0) {
            return sql;
        }
        StringBuilder ret = new StringBuilder(split[0]);
        int index = 1;
        for (int i = 1; i < split.length; i++) {
            JobInstanceParams property = params.get(i);
            String value = property.getParamValue();
            ret.append(PARAM_REPLACE_CHAR);
            expandMap.put(index++, property);
            ret.append(split[i]);
        }
        if (PARAM_REPLACE_CHAR == sql.charAt(sql.length() - 1)) {
            ret.append(PARAM_REPLACE_CHAR);
            expandMap.put(index, params.get(split.length));
        }
        params.clear();
        params.putAll(expandMap);
        return ret.toString();
    }


    private static final Pattern APPLICATION_REGEX = Pattern.compile(Constants.APPLICATION_REGEX);

    /**
     * 匹配applicationId
     *
     * @return appid
     */
    protected void findAppId(String line) {
        // 通过正则表达式找到yarn上job的appId
        Matcher matcher = APPLICATION_REGEX.matcher(line);
        if (matcher.find()) {
            taskRequest.setAppId(taskRequest.getAppId().isEmpty() ? matcher.group() : taskRequest.getAppId() + "," +matcher.group());
        }
    }
}
