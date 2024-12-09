package com.boncfc.ide.plugin.task.api.datasource.hive;

import com.boncfc.ide.plugin.task.api.datasource.BaseConnectionParam;
import lombok.Data;

@Data
public class HiveConnectionParam extends BaseConnectionParam {
    String clusterId;
    String clusterName;
    String hadoopConfDir;
    String krbConfPath;
    String krbKeytabsDir;
    String dsPasswordAESKey;
}
