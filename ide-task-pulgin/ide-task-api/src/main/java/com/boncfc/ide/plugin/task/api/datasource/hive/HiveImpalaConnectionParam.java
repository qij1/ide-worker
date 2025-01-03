package com.boncfc.ide.plugin.task.api.datasource.hive;

import com.boncfc.ide.plugin.task.api.datasource.BaseConnectionParam;
import lombok.Data;

@Data
public class HiveImpalaConnectionParam extends BaseConnectionParam {
    String clusterId;
    String clusterName;
    String hadoopConfDir;
    String metastoreUrl;
    String isSecurity;
    String principalName;
    String krbConfPath;
    String krbKeytabsDir;
    String dsPasswordAESKey;
}
