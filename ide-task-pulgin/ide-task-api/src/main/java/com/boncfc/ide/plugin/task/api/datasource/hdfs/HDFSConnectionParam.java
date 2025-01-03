package com.boncfc.ide.plugin.task.api.datasource.hdfs;

import com.boncfc.ide.plugin.task.api.datasource.BaseConnectionParam;
import lombok.Data;

@Data
public class HDFSConnectionParam extends BaseConnectionParam {
    String clusterId;
    String clusterName;
    String hadoopConfDir;
    String isSecurity;
    String principalName;
    String krbConfPath;
    String krbKeytabsDir;
    String dsPasswordAESKey;
}
