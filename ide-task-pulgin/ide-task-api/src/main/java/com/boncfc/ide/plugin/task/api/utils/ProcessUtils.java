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

package com.boncfc.ide.plugin.task.api.utils;

import com.boncfc.ide.plugin.task.api.TaskConstants;
import com.boncfc.ide.plugin.task.api.TaskExecutionContext;
import com.boncfc.ide.plugin.task.api.constants.Constants;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public final class ProcessUtils {

    private ProcessUtils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Initialization regularization, solve the problem of pre-compilation performance,
     * avoid the thread safety problem of multi-thread operation
     */
    private static final Pattern MACPATTERN = Pattern.compile("-[+|-][-|=]\\s(\\d+)");

    /**
     * Expression of PID recognition in Windows scene
     */
    private static final Pattern WINDOWSPATTERN = Pattern.compile("(\\d+)");

    /**
     * Expression of PID recognition in Linux scene
     */
    private static final Pattern LINUXPATTERN = Pattern.compile("\\((\\d+)\\)");

    /**
     * kill tasks according to different task types.
     */
    @Deprecated
    public static boolean kill(@NonNull TaskExecutionContext request) {
        try {
            log.info("Begin kill task instance, processId: {}", request.getProcessId());
            int processId = request.getProcessId();
            if (processId == 0) {
                log.error("Task instance kill failed, processId is not exist");
                return false;
            }

            String cmd = String.format("kill -9 %s", getPidsStr(processId));
//            cmd = OSUtils.getSudoCmd(request.getTenantCode(), cmd);
            log.info("process id:{}, cmd:{}", processId, cmd);

            OSUtils.exeCmd(cmd);
            log.info("Success kill task instance, processId: {}", request.getProcessId());
            return true;
        } catch (Exception e) {
            log.error("Kill task instance error, processId: {}", request.getProcessId(), e);
            return false;
        }
    }

    /**
     * get pids str.
     *
     * @param processId process id
     * @return pids pid String
     * @throws Exception exception
     */
    public static String getPidsStr(int processId) throws Exception {

        String rawPidStr;

        // pstree pid get sub pids
        if (SystemUtils.IS_OS_MAC) {
            rawPidStr = OSUtils.exeCmd(String.format("%s -sp %d", TaskConstants.PSTREE, processId));
        } else if (SystemUtils.IS_OS_LINUX) {
            rawPidStr = OSUtils.exeCmd(String.format("%s -p %d", TaskConstants.PSTREE, processId));
        } else {
            rawPidStr = OSUtils.exeCmd(String.format("%s -p %d", TaskConstants.PSTREE, processId));
        }

        return parsePidStr(rawPidStr);
    }

    public static String parsePidStr(String rawPidStr) {

        log.info("prepare to parse pid, raw pid string: {}", rawPidStr);
        ArrayList<String> allPidList = new ArrayList<>();
        Matcher mat = null;
        if (SystemUtils.IS_OS_MAC) {
            if (StringUtils.isNotEmpty(rawPidStr)) {
                mat = MACPATTERN.matcher(rawPidStr);
            }
        } else if (SystemUtils.IS_OS_LINUX) {
            if (StringUtils.isNotEmpty(rawPidStr)) {
                mat = LINUXPATTERN.matcher(rawPidStr);
            }
        } else {
            if (StringUtils.isNotEmpty(rawPidStr)) {
                mat = WINDOWSPATTERN.matcher(rawPidStr);
            }
        }
        if (null != mat) {
            while (mat.find()) {
                allPidList.add(mat.group(1));
            }
        }
        return String.join(" ", allPidList).trim();
    }

    public static void cancelApplication(String url, String appId) {
        try {
            stopYarnJob(url, appId);
        } catch (Exception e) {
            log.error("Cancel application failed: {}", e.getMessage());
        }
    }


    public static void stopYarnJob(String url, String appId) {
        String yarnUrl = String.format(url, appId);
        String result = HttpUtils.stopYarnJob(yarnUrl);
        // 如果yarn上的job没有被杀死，就终止sql的执行来补刀
        if (Constants.YARN_JOB_KILLED.equalsIgnoreCase(result)) {
            log.info("SqlJob stopYarnJob成功!");
        } else {
            log.info("SqlJob stopYarnJob code=[{}]", result);
        }
    }
}
