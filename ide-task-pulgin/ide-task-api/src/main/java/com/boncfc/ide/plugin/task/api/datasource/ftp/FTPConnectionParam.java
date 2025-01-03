package com.boncfc.ide.plugin.task.api.datasource.ftp;

import com.boncfc.ide.plugin.task.api.datasource.BaseConnectionParam;
import lombok.Data;

@Data
public class FTPConnectionParam extends BaseConnectionParam {
    String ip;
    String port;
    boolean skipHeader;
}
