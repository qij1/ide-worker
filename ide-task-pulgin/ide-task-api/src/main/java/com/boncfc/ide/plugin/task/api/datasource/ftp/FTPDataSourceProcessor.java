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

package com.boncfc.ide.plugin.task.api.datasource.ftp;

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
import java.io.IOException;
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
public class FTPDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public void checkDatasourceParam(BaseConnectionParam baseConnectionParam) {
        if (baseConnectionParam.getUserName() == null || baseConnectionParam.getPassword() == null) {
            throw new IllegalArgumentException("hive datasource param is not valid");
        }
    }

    @Override
    public ConnectionParam createConnectionParams(DatasourceDetailInfo detailInfo) {
        FTPConnectionParam ftpConnectionParam = JSONUtils.parseObject(detailInfo.getDsConf(), FTPConnectionParam.class);
        ftpConnectionParam.setPassword(AESUtil.decrypt(ftpConnectionParam.getPassword(),
                detailInfo.getDsPasswordAESKey().getBytes(StandardCharsets.UTF_8)));
        checkDatasourceParam(ftpConnectionParam);
        return ftpConnectionParam;
    }


    @Override
    public DbType getDbType() {
        return DbType.FTP;
    }


    @Override
    public DataSourceProcessor create() {
        return new FTPDataSourceProcessor();
    }


}
