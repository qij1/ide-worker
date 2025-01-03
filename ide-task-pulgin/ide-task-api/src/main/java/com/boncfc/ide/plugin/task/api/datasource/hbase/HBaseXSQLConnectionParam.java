package com.boncfc.ide.plugin.task.api.datasource.hbase;

import com.boncfc.ide.plugin.task.api.datasource.BaseConnectionParam;
import lombok.Data;

@Data
public class HBaseXSQLConnectionParam extends BaseConnectionParam {
    String jdbcUrl;
    String clusterId;
    String clusterName;
    String isSecurity;
    String principalName;
    String krbConfPath;
    String krbKeytabsDir;
    String dsPasswordAESKey;
}
