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

package com.boncfc.ide.plugin.task.api.datasource.mysql;

import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.boncfc.ide.plugin.task.api.TaskException;
import com.boncfc.ide.plugin.task.api.constants.DataSourceConstants;
import com.boncfc.ide.plugin.task.api.datasource.*;
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

@AutoService(DataSourceProcessor.class)
@Slf4j
public class MySQLDataSourceProcessor extends AbstractDataSourceProcessor {

    private static final String ALLOW_LOAD_LOCAL_IN_FILE_NAME = "allowLoadLocalInfile";

    private static final String AUTO_DESERIALIZE = "autoDeserialize";

    private static final String ALLOW_LOCAL_IN_FILE_NAME = "allowLocalInfile";

    private static final String ALLOW_URL_IN_LOCAL_IN_FILE_NAME = "allowUrlInLocalInfile";

    @Override
    public void checkDatasourceParam(BaseConnectionParam baseConnectionParam) {
        checkDatabasePatter(baseConnectionParam.getDatabase());
        if (baseConnectionParam.getUserName() == null || baseConnectionParam.getPassword() == null ||
                baseConnectionParam.getDriverClassName() == null || baseConnectionParam.getDriverLocation() == null) {
            throw new IllegalArgumentException("mysql datasource param is not valid");
        }
    }

    @Override
    public ConnectionParam createConnectionParams(DatasourceDetailInfo detailInfo) {
        MySQLConnectionParam mySQLConnectionParam = JSONUtils.parseObject(detailInfo.getDsConf(), MySQLConnectionParam.class);
        mySQLConnectionParam.setDriverClassName(detailInfo.getClassName());
        mySQLConnectionParam.setDriverLocation(detailInfo.getJarDir() + File.separator + detailInfo.getJarFileName());
        mySQLConnectionParam.setPassword(AESUtil.decrypt(mySQLConnectionParam.getPassword(),
                detailInfo.getDsPasswordAESKey().getBytes(StandardCharsets.UTF_8)));
        checkDatasourceParam(mySQLConnectionParam);
        return mySQLConnectionParam;
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.MYSQL_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        MySQLConnectionParam mysqlConnectionParam = (MySQLConnectionParam) connectionParam;
        return mysqlConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        MySQLConnectionParam mysqlConnectionParam = (MySQLConnectionParam) connectionParam;
        Driver driver;
        try {
            URL[] urls = new URL[]{new URL("file:" + mysqlConnectionParam.getDriverLocation())};
            URLClassLoader classLoader = new URLClassLoader(urls);
            Class<?> driverClass = classLoader.loadClass(mysqlConnectionParam.getDriverClassName());
            driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
        } catch (MalformedURLException | InvocationTargetException |
                 InstantiationException | IllegalAccessException |
                 NoSuchMethodException e) {
            log.error("获取MySql数据库连接失败", e);
            throw new TaskException("获取连接失败: " + e.getMessage());
        }
        String user = mysqlConnectionParam.getUserName();
        String password = mysqlConnectionParam.getPassword();
        Properties connectionProperties = getConnectionProperties(mysqlConnectionParam, user, password);
        return driver.connect(mysqlConnectionParam.getJdbcUrl(), connectionProperties);
    }

    public static void main(String[] args) {
        try {
            URL[] urls = new URL[]{new URL("file:C:\\Users\\12415\\Desktop\\drivers\\ojdbc8-19.3.0.0.jar")};
            URLClassLoader classLoader = new URLClassLoader(urls);
            Class<?> driverClass = classLoader.loadClass("oracle.jdbc.OracleDriver");
            Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(driver);
            Properties properties = new Properties();
            properties.setProperty("user", "ide");
            properties.setProperty("password", "ide");
            Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:ORCL", properties);

//            Connection conn = driver.connect("jdbc:oracle:thin:@localhost:1521:ORCL", properties);
            System.out.println(conn.isClosed());
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Properties getConnectionProperties(MySQLConnectionParam mysqlConnectionParam, String user,
                                               String password) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        Map<String, String> paramMap = mysqlConnectionParam.getOther();
        if (MapUtils.isNotEmpty(paramMap)) {
            paramMap.forEach((k, v) -> {
                if (!checkKeyIsLegitimate(k)) {
                    log.info("Key `{}` is not legitimate for security reason", k);
                    return;
                }
                connectionProperties.put(k, v);
            });
        }
        connectionProperties.put(AUTO_DESERIALIZE, "false");
        connectionProperties.put(ALLOW_LOAD_LOCAL_IN_FILE_NAME, "false");
        connectionProperties.put(ALLOW_LOCAL_IN_FILE_NAME, "false");
        connectionProperties.put(ALLOW_URL_IN_LOCAL_IN_FILE_NAME, "false");
        return connectionProperties;
    }

    @Override
    public DbType getDbType() {
        return DbType.MYSQL;
    }

    @Override
    public DataSourceProcessor create() {
        return new MySQLDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        //删除单行注释
        sql = sql.replaceAll("--.*", "");
        // 匹配/* */形式的多行注释
        Pattern pattern = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        sql = matcher.replaceAll("");
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.mysql);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.mysql);
    }

    private static boolean checkKeyIsLegitimate(String key) {
        return !key.contains(ALLOW_LOAD_LOCAL_IN_FILE_NAME)
                && !key.contains(AUTO_DESERIALIZE)
                && !key.contains(ALLOW_LOCAL_IN_FILE_NAME)
                && !key.contains(ALLOW_URL_IN_LOCAL_IN_FILE_NAME);
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isNotEmpty(otherMap)) {
            List<String> list = new ArrayList<>(otherMap.size());
            otherMap.forEach((key, value) -> list.add(String.format("%s=%s", key, value)));
            return String.join("&", list);
        }
        return null;
    }

}


