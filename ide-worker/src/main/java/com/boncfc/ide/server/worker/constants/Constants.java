package com.boncfc.ide.server.worker.constants;

import java.time.Duration;

public final class Constants {

    public static final String THREAD_NAME_WORKER_SERVER = "Worker-Server";

    /**
     * SINGLE_SLASH /
     */
    public static final String SINGLE_SLASH = "/";


    /**
     * common properties path
     */
    public static final String COMMON_PROPERTIES_PATH = "/common.properties";

    public static final String REMOTE_LOGGING_YAML_PATH = "/remote-logging.yaml";

    /**
     * sudo enable
     */
    public static final String SUDO_ENABLE = "sudo.enable";


    public static final String WORKFLOW_INSTANCE_ID_MDC_KEY = "workflowInstanceId";

    /**
     * sleep 1000ms
     */
    public static final long SLEEP_TIME_MILLIS = 1_000L;

    /**
     * short sleep 100ms
     */
    public static final long SLEEP_TIME_MILLIS_SHORT = 100L;

    /**
     * COLON :
     */
    public static final String COLON = ":";

    public static final Duration SERVER_CLOSE_WAIT_TIME = Duration.ofSeconds(3);

    public static final int DRY_RUN_FLAG_YES = 1;



}
