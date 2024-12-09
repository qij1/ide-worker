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

import com.boncfc.ide.plugin.task.api.datasource.ConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.DataSourceProcessor;
import com.boncfc.ide.plugin.task.api.datasource.DataSourceProcessorProvider;
import com.boncfc.ide.plugin.task.api.datasource.DbType;
import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Map;

@Slf4j
public class DataSourceUtils {

    public DataSourceUtils() {
    }


    public static ConnectionParam buildConnectionParams(DbType dbType, DatasourceDetailInfo datasourceDetailInfo) {
        return getDatasourceProcessor(dbType).createConnectionParams(datasourceDetailInfo);
    }

    public static Connection getConnection(DbType dbType, ConnectionParam connectionParam) {
        try {
            return getDatasourceProcessor(dbType).getConnection(connectionParam);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDatasourceDriver(DbType dbType) {
        return getDatasourceProcessor(dbType).getDatasourceDriver();
    }

    public static DataSourceProcessor getDatasourceProcessor(DbType dbType) {
        Map<String, DataSourceProcessor> dataSourceProcessorMap =
                DataSourceProcessorProvider.getDataSourceProcessorMap();
        if (!dataSourceProcessorMap.containsKey(dbType.name())) {
            throw new IllegalArgumentException("illegal datasource type");
        }
        return dataSourceProcessorMap.get(dbType.name());
    }

    /**
     * get datasource UniqueId
     */
    public static String getDatasourceUniqueId(ConnectionParam connectionParam, DbType dbType) {
        return getDatasourceProcessor(dbType).getDatasourceUniqueId(connectionParam, dbType);
    }
}
