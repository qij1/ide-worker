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

import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface DataSourceProcessor {

    /**
     * get Datasource Client UniqueId
     *
     * @return UniqueId
     */
    String getDatasourceUniqueId(ConnectionParam connectionParam, DbType dbType);

    /**
     * get datasource Driver
     */
    String getDatasourceDriver();

    /**
     * deserialize json to datasource connection param
     *
     * @param connectionJson {@code org.apache.dolphinscheduler.dao.entity.DataSource.connectionParams}
     * @return {@link BaseConnectionParam}
     */
    ConnectionParam createConnectionParams(DatasourceDetailInfo connectionJson);

    /**
     * get validation Query
     */
    String getValidationQuery();

    /**
     * get jdbcUrl by connection param, the jdbcUrl is different with ConnectionParam.jdbcUrl, this method will inject
     * other to jdbcUrl
     *
     * @param connectionParam connection param
     */
    String getJdbcUrl(ConnectionParam connectionParam);

    /**
     * get connection by connectionParam
     *
     * @param connectionParam connectionParam
     * @return {@link Connection}
     */
    // todo: Change to return a ConnectionWrapper
    Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException, IOException;

    /**
     * test connection
     *
     * @param connectionParam connectionParam
     * @return true if connection is valid
     */
    boolean checkDataSourceConnectivity(ConnectionParam connectionParam);

    /**
     * @return {@link DbType}
     */
    DbType getDbType();

    /**
     * get datasource processor
     */
    DataSourceProcessor create();

    List<String> splitAndRemoveComment(String sql);
}
