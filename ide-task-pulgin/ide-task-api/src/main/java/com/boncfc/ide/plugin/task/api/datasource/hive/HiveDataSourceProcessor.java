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

package com.boncfc.ide.plugin.task.api.datasource.hive;

import com.boncfc.ide.plugin.task.api.TaskException;
import com.boncfc.ide.plugin.task.api.constants.DataSourceConstants;
import com.boncfc.ide.plugin.task.api.datasource.*;
import com.boncfc.ide.plugin.task.api.datasource.mysql.MySQLConnectionParam;
import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import com.boncfc.ide.plugin.task.api.utils.AESUtil;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class HiveDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public void checkDatasourceParam(BaseConnectionParam baseConnectionParam) {
        checkDatabasePatter(baseConnectionParam.getDatabase());
        if (baseConnectionParam.getUserName() == null || baseConnectionParam.getPassword() == null ||
                baseConnectionParam.getDriverClassName() == null || baseConnectionParam.getDriverLocation() == null) {
            throw new IllegalArgumentException("hive datasource param is not valid");
        }
    }

    @Override
    public ConnectionParam createConnectionParams(DatasourceDetailInfo detailInfo) {
        HiveImpalaConnectionParam hiveImpalaConnectionParam = JSONUtils.parseObject(detailInfo.getDsConf(), HiveImpalaConnectionParam.class);
        hiveImpalaConnectionParam.setDriverClassName(detailInfo.getClassName());
        hiveImpalaConnectionParam.setDriverLocation(detailInfo.getJarDir() + File.separator + detailInfo.getJarFileName());
        hiveImpalaConnectionParam.setPassword(AESUtil.decrypt(hiveImpalaConnectionParam.getPassword(),
                detailInfo.getDsPasswordAESKey().getBytes(StandardCharsets.UTF_8)));
        checkDatasourceParam(hiveImpalaConnectionParam);
        return hiveImpalaConnectionParam;
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.ORG_APACHE_HIVE_JDBC_HIVE_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.HIVE_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        MySQLConnectionParam mysqlConnectionParam = (MySQLConnectionParam) connectionParam;
        return mysqlConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        HiveImpalaConnectionParam hiveImpalaConnectionParam = (HiveImpalaConnectionParam) connectionParam;
        Driver driver;
        try {
            URL[] urls = new URL[]{new URL("file:" + hiveImpalaConnectionParam.getDriverLocation())};
            URLClassLoader classLoader = new URLClassLoader(urls);
            Class<?> driverClass = classLoader.loadClass(hiveImpalaConnectionParam.getDriverClassName());
            driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
        } catch (MalformedURLException | InvocationTargetException |
                 InstantiationException | IllegalAccessException |
                 NoSuchMethodException e) {
            log.error("获取Hive数据库连接失败", e);
            throw new TaskException("获取连接失败: " + e.getMessage());
        }
        String user = hiveImpalaConnectionParam.getUserName();
        String password = hiveImpalaConnectionParam.getPassword();
        Properties connectionProperties = getConnectionProperties(hiveImpalaConnectionParam, user, password);
        return driver.connect(hiveImpalaConnectionParam.getJdbcUrl(), connectionProperties);
    }

    private Properties getConnectionProperties(HiveImpalaConnectionParam hiveImpalaConnectionParam, String user,
                                               String password) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        return connectionProperties;
    }

    @Override
    public DbType getDbType() {
        return DbType.HIVE;
    }

    @Override
    public DataSourceProcessor create() {
        return new HiveDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return null;
    }

}
