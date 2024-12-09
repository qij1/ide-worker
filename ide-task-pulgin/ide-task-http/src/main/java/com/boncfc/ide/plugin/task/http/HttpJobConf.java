package com.boncfc.ide.plugin.task.http;

import com.boncfc.ide.plugin.task.api.model.JobConf;
import lombok.Data;

@Data
public class HttpJobConf implements JobConf {
    String requestUrl;
    String requestMethod;
    String requestHeaders;
    String requestBody;
    String responseHeaders;
    boolean ack;
    String ackKey;
    String ackValue;
    String ackValueType;
    String stopRequestUrl;
    String stopRequestMethod;
    String stopRequestHeaders;
}
