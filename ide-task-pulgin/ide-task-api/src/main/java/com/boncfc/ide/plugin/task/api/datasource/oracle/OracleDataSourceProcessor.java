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

package com.boncfc.ide.plugin.task.api.datasource.oracle;

import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.boncfc.ide.plugin.task.api.TaskException;
import com.boncfc.ide.plugin.task.api.constants.DataSourceConstants;
import com.boncfc.ide.plugin.task.api.datasource.*;
import com.boncfc.ide.plugin.task.api.datasource.mysql.MySQLConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.mysql.MySQLDataSourceProcessor;
import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import com.boncfc.ide.plugin.task.api.utils.AESUtil;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@AutoService(DataSourceProcessor.class)
public class OracleDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public ConnectionParam createConnectionParams(DatasourceDetailInfo detailInfo) {
        OracleConnectionParam oracleConnectionParam = JSONUtils.parseObject(detailInfo.getDsConf(), OracleConnectionParam.class);
        oracleConnectionParam.setDriverClassName(detailInfo.getClassName());
        oracleConnectionParam.setDriverLocation(detailInfo.getJarDir() + File.separator + detailInfo.getJarFileName());
        oracleConnectionParam.setPassword(AESUtil.decrypt(oracleConnectionParam.getPassword(),
                detailInfo.getDsPasswordAESKey().getBytes(StandardCharsets.UTF_8)));
        return oracleConnectionParam;
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_ORACLE_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.ORACLE_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        OracleConnectionParam mysqlConnectionParam = (OracleConnectionParam) connectionParam;
        return mysqlConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        OracleConnectionParam oracleConnectionParam = (OracleConnectionParam) connectionParam;
        Driver driver;
        try {
            URL[] urls = new URL[]{new URL("file:" + oracleConnectionParam.getDriverLocation())};
            URLClassLoader classLoader = new URLClassLoader(urls);
            Class<?> driverClass = classLoader.loadClass(oracleConnectionParam.getDriverClassName());
            driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
        } catch (MalformedURLException | InvocationTargetException |
                 InstantiationException | IllegalAccessException |
                 NoSuchMethodException e) {
            log.error("获取Oracle数据库连接失败", e);
            throw new TaskException("获取连接失败: " + e.getMessage());
        }
        String user = oracleConnectionParam.getUserName();
        String password = oracleConnectionParam.getPassword();
        Properties connectionProperties = getConnectionProperties(oracleConnectionParam, user, password);
        return driver.connect(oracleConnectionParam.getJdbcUrl(), connectionProperties);
    }

    private Properties getConnectionProperties(OracleConnectionParam oracleConnectionParam, String user,
                                               String password) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        return connectionProperties;
    }



    @Override
    public DbType getDbType() {
        return DbType.ORACLE;
    }

    @Override
    public DataSourceProcessor create() {
        return new OracleDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        //删除单行注释
        sql = sql.replaceAll("--.*", "");
        // 匹配/* */形式的多行注释
        Pattern pattern = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        sql = matcher.replaceAll("");
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.oracle);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.oracle);
    }

}
