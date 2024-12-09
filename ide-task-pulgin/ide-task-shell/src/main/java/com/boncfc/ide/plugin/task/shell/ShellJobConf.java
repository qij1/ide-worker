package com.boncfc.ide.plugin.task.shell;

import com.boncfc.ide.plugin.task.api.model.JobConf;
import lombok.Data;

@Data
public class ShellJobConf implements JobConf {
    String user;
    String shell;
}
