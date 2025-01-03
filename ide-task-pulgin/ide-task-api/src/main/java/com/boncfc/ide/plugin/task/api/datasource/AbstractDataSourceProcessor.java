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

package com.boncfc.ide.plugin.task.api.datasource;

import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.boncfc.ide.plugin.task.api.utils.PasswordUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public abstract class AbstractDataSourceProcessor implements DataSourceProcessor {

    private static final Pattern IPV4_PATTERN = Pattern.compile("^[a-zA-Z0-9\\_\\-\\.\\,]+$");

    private static final Pattern IPV6_PATTERN = Pattern.compile("^[a-zA-Z0-9\\_\\-\\.\\:\\[\\]\\,]+$");

    private static final Pattern DATABASE_PATTER = Pattern.compile("^[a-zA-Z0-9\\_\\-\\.]+$");

    private static final Pattern PARAMS_PATTER = Pattern.compile("^[a-zA-Z0-9\\-\\_\\/\\@\\.\\:]+$");

    private static final Set<String> POSSIBLE_MALICIOUS_KEYS = Sets.newHashSet("allowLoadLocalInfile");

    protected void checkDatasourceParam(BaseConnectionParam baseConnectionParam) {
        checkDatabasePatter(baseConnectionParam.getDatabase());
    }
    /**
     * Check the host is valid
     *
     * @param host datasource host
     */
    protected void checkHost(String host) {
        if (com.google.common.net.InetAddresses.isInetAddress(host)) {
        } else if (!IPV4_PATTERN.matcher(host).matches() || !IPV6_PATTERN.matcher(host).matches()) {
            throw new IllegalArgumentException("datasource host illegal");
        }
    }

    /**
     * check database name is valid
     *
     * @param database database name
     */
    protected void checkDatabasePatter(String database) {
        if (!DATABASE_PATTER.matcher(database).matches()) {
            throw new IllegalArgumentException("database name illegal");
        }
    }

    /**
     * check other is valid
     *
     * @param other other
     */
    protected void checkOther(Map<String, String> other) {
        if (MapUtils.isEmpty(other)) {
            return;
        }

        if (!Sets.intersection(other.keySet(), POSSIBLE_MALICIOUS_KEYS).isEmpty()) {
            throw new IllegalArgumentException("Other params include possible malicious keys.");
        }

        for (Map.Entry<String, String> entry : other.entrySet()) {
            if (!PARAMS_PATTER.matcher(entry.getKey()).matches()) {
                throw new IllegalArgumentException("datasource other params: " + entry.getKey() + " illegal");
            }
        }
    }

    protected Map<String, String> transformOtherParamToMap(String other) {
        if (StringUtils.isBlank(other)) {
            return Collections.emptyMap();
        }
        return JSONUtils.parseObject(other, new TypeReference<Map<String, String>>() {
        });
    }

    @Override
    public String getDatasourceUniqueId(ConnectionParam connectionParam, DbType dbType) {
        BaseConnectionParam baseConnectionParam = (BaseConnectionParam) connectionParam;
        return MessageFormat.format("{0}@{1}@{2}@{3}", dbType.name(), baseConnectionParam.getUserName(),
                PasswordUtils.encodePassword(baseConnectionParam.getPassword()), baseConnectionParam.getJdbcUrl());
    }

    @Override
    public boolean checkDataSourceConnectivity(ConnectionParam connectionParam) {
        try (Connection connection = getConnection(connectionParam)) {
            return true;
        } catch (Exception e) {
            log.error("Check datasource connectivity for: {} error", getDbType().name(), e);
            return false;
        }
    }

    @Override
    public String getValidationQuery() {
        return "";
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        return "";
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException, IOException {
        return null;
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return null;
    }

    @Override
    public String getDatasourceDriver() {
        return null;
    }
}
