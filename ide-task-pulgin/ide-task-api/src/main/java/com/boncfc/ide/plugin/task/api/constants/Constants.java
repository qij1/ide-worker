package com.boncfc.ide.plugin.task.api.constants;

public class Constants {
    public static String YES = "Y";
    public static String NO = "N";

    /**
     * yarn任务的状态：杀死
     */
    public static final String YARN_JOB_KILLED = "KILLED";

    /**
     * http connect time out
     */
    public static final int HTTP_CONNECT_TIMEOUT = 60 * 1000;
    /**
     * http connect request time out
     */
    public static final int HTTP_CONNECTION_REQUEST_TIMEOUT = 60 * 1000;
    /**
     * httpclient soceket time out
     */
    public static final int SOCKET_TIMEOUT = 60 * 1000;

    public static final String APPLICATION_REGEX = "application_\\d+_\\d+";

    public static final String TASK_INSTANCE_ID_MDC_KEY = "jobInst.id";

    public static final String APPLICATION_ID = "APPLICATION_ID";

    public static final String PID = "PID";

    /**
     * common properties path
     */
    public static final String COMMON_PROPERTIES_PATH = "/common.properties";

    public static final String REMOTE_LOGGING_YAML_PATH = "/remote-logging.yaml";

    public static final String TASK_RESOURCE_LIMIT_STATE = "task.resource.limit.state";
    public static final String SUDO_ENABLE = "sudo.enable";

    public static final String EMPTY_STRING = "";


}
