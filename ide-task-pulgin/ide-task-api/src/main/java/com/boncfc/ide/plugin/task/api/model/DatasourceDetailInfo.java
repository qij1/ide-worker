package com.boncfc.ide.plugin.task.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasourceDetailInfo {
    int dsId;
    String dsType;
    String dsName;
    String dsConf;
    String className;
    String driverName;
    String driverType;
    String driverVersion;
    String jarDir;
    String jarFileName;
    String clusterName;
    String dependencyJarsDir;
    String isSecurity;
    String hadoopConfDir;
    String metastoreUrl;
    String krbConfPath;
    String krbKeytabsDir;
    String principalName;
    String indexName;
    String dsPasswordAESKey;
    String pluginName;
    String pluginType;
}
