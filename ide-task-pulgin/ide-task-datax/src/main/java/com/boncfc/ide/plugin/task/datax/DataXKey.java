package com.boncfc.ide.plugin.task.datax;

public final class DataXKey {
    /**
     * job数据源ID
     */
    public static final String DS_ID = "dsId";
    /**
     * reader 多数据源配置项
     */
    public static final String CONNECTION = "connection";
    /**
     * datax job 配置项
     */
    public static final String JOB = "job";
    public static final String CONTENT = "content";
    public static final String READER = "reader";
    public static final String WRITER = "writer";
    /**
     * datax 参数配置项
     */
    public static final String PARAMETER = "parameter";
    public static final String USERNAME = "username";
    public static final String COLUMN = "column";
    public static final String PASSWORD = "password";
    public static final String JDBCURL = "jdbcUrl";
    public static final String QUERYSQL = "querySql";
    public static final String PATH = "path";
    public static final String ENCODING = "encoding";
    public static final String NULL_FORMAT = "nullFormat";
    public static final String INDEX_NAME = "indexName";
    public static final String INDEX_TYPE = "indexType";
    public static final String SIZE = "size";

    /**
     * datax writer 配置项
     */
    public static final String BATCH_SIZE = "batchSize";
    public static final String TRUNCATE = "truncate";
    public static final String PRE_SQL = "preSql";
    public static final String POST_SQL = "postSql";
    public static final String TABLE = "table";
    public static final String WRITE_MODE = "writeMode";
    /**
     * datax hive 配置项
     */
    public static final String HAVE_KERBEROS = "havekerberos";
    public static final String KRB_CONF_FILEPATH = "krbConfPath";
    public static final String KRB_KEYTAB_FILEPATH =  "krbKeytabPath";
    public static final String KRB_PRINCIPAL = "krbPrincipal";
    public static final String HADOOP_CONFIG_PATH = "hadoopConfigPath";
    public static final String APP_NAME = "appName";
    public static final String FILE_TYPE = "fileType";
    public static final String HADOOP_CONFIG = "hadoopConfig";
    public static final String METASTORE = "metastore";
    public static final String DEFAULT_FS = "defaultFS";
    public static final String DATABASE_NAME = "databaseName";
    public static final String TMP_PATH = "tmpPath";
    public static final String HIVE_PARTITION = "hivePartition";
    public static final String DFS_REPLICATION = "dfsReplication";
    public static final String SET_SQL = "setSql";

    /**
     * datax hdfs 配置项
     */
    public static final String COMPRESS = "compress";

    /**
     * datax ftp 配置项
     */
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String PROTOCOL = "protocol";
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    public static final String SKIP_HEADER = "skipHeader";
    public static final String TIMEOUT = "timeout";
    public static final String CONNECT_PATTERN = "connectPattern";
    public static final String DATE_FORMAT = "dateFormat";
    public static final String ALLOW_CONTROL_CHARACTER = "allowControlCharacter";
    public static final String SPLIT = "split";
    public static final String SPLIT_UNIT = "unit";
    public static final String SPLIT_AVERAGE_UNIT_NUMBER = "averageUnitNumber";
    public static final String SPLIT_MAX_FILE_COUNT = "maxFileCount";

    /**
     * job ftp reader 配置项
     */
    public static final String COL_SPLIT = "colSplit";
    public static final String ROW_SPLIT = "rowSplit";
    public static final String FILE_PATH = "filePath";
    public static final String FILE_NAME = "fileName";
    public static final String HEADER = "header";
    /**
     * job writer 配置项
     */
    public static final String TABLE_NAME = "tableName";
    public static final String NEXT_SQL = "nextSql";
    public static final String HIVEPARTITION = "hivepartition";

    /**
     * datax es配置项
     */
    public static final String ENDPOINT = "endpoint";
    public static final String ACCESS_ID = "accessId";
    public static final String ACCESS_KEY = "accessKey";
    public static final String INDEX = "index";
    public static final String TYPE = "type";
    public static final String CLEAN_UP = "cleanup";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_TYPE = "type";
    public static final String SETTINGS = "settings";

    public static final String QUERY = "query";
    public static final String QUERY_SOURCE = "_source";
    public static final String COLUMN_INCLUDES = "includes";
    public static final String COLUMN_EXCLUDES = "excludes";
    public static final String DISCOVERY = "discovery";

    /**
     * datax kafka配置项
     */
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String TOPIC = "topic";

    /**
     * datax hbase配置项
     */
    public static final String HBASE_CONFIG = "hbaseConfig";
    public static final String KEY_ROOTDIR = "hbase.rootdir";
    public static final String KEY_QUORUM = "hbase.zookeeper.quorum";
    public static final String KEY_PORT = "hbase.zookeeper.property.clientPort";
    public static final String KEY_DISTRIBUTED = "hbase.cluster.distributed";
    public static final String ROOTDIR = "rootdir";
    public static final String QUORUM = "quorum";
    public static final String ENCODE = "encode";
    public static final String OTHER = "other";
    public static final String MAX_VERSION = "maxVersion";
    public static final String MODE = "mode";
    public static final String MODE_MULTI = "multiVersionFixedColumn";
    public static final String RANGE = "range";
    public static final String ROWKEY_COLUMN = "rowkeyColumn";
    public static final String RANGE_START = "startRowkey";
    public static final String RANGE_END = "endRowkey";
    public static final String IS_BINARY = "isBinaryRowkey";
}
