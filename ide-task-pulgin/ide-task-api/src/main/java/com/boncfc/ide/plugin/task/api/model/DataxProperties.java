package com.boncfc.ide.plugin.task.api.model;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class DataxProperties {
    private String executeJobPath;
    private String pythonLauncher;
    private String dataxLauncher;
    private int xms;
    private int xmx;
}
