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

package com.boncfc.ide.plugin.task.api.datasource.hbase;

import com.boncfc.ide.plugin.task.api.datasource.*;
import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import com.boncfc.ide.plugin.task.api.utils.AESUtil;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class HBaseDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public void checkDatasourceParam(BaseConnectionParam baseConnectionParam) {
        if (baseConnectionParam.getUserName() == null || baseConnectionParam.getPassword() == null) {
            throw new IllegalArgumentException("hive datasource param is not valid");
        }
    }

    @Override
    public ConnectionParam createConnectionParams(DatasourceDetailInfo detailInfo) {
        HBaseXSQLConnectionParam HBaseXSQLConnectionParam = JSONUtils.parseObject(detailInfo.getDsConf(), HBaseXSQLConnectionParam.class);
        HBaseXSQLConnectionParam.setPassword(AESUtil.decrypt(HBaseXSQLConnectionParam.getPassword(),
                detailInfo.getDsPasswordAESKey().getBytes(StandardCharsets.UTF_8)));
        checkDatasourceParam(HBaseXSQLConnectionParam);
        return HBaseXSQLConnectionParam;
    }


    @Override
    public DbType getDbType() {
        return DbType.HBASE;
    }


    @Override
    public DataSourceProcessor create() {
        return new HBaseDataSourceProcessor();
    }


}
