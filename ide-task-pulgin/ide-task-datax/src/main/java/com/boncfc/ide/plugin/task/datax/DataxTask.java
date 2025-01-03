package com.boncfc.ide.plugin.task.datax;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.boncfc.ide.plugin.task.api.*;
import com.boncfc.ide.plugin.task.api.datasource.ConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.DbType;
import com.boncfc.ide.plugin.task.api.datasource.hbase.HBaseXSQLConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.hive.HiveImpalaConnectionParam;
import com.boncfc.ide.plugin.task.api.model.*;
import com.boncfc.ide.plugin.task.api.shell.IShellInterceptorBuilder;
import com.boncfc.ide.plugin.task.api.shell.ShellInterceptorBuilderFactory;
import com.boncfc.ide.plugin.task.api.utils.DataSourceUtils;
import com.boncfc.ide.plugin.task.api.utils.FileUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.boncfc.ide.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;
import static com.boncfc.ide.plugin.task.api.TaskConstants.EXIT_CODE_KILL;
import static com.boncfc.ide.plugin.task.api.constants.DateConstants.YYYYMMDD;
import static com.boncfc.ide.plugin.task.api.constants.DateConstants.YYYYMMDDHHMMSS;
import static com.boncfc.ide.plugin.task.api.datasource.DbType.*;
import static com.boncfc.ide.plugin.task.api.model.DataxPluginType.READER;
import static com.boncfc.ide.plugin.task.api.model.DataxPluginType.WRITER;
import static com.boncfc.ide.plugin.task.datax.DataXKey.*;

@Slf4j
public class DataxTask extends AbstractTask {

    /**
     * jvm parameters
     */
    public static final String JVM_PARAM = "--jvm=\"-Xms%sG -Xmx%sG\" ";

    public static final String CUSTOM_PARAM = " -D%s='%s'";
    private DataxJobConf dataxJobConf;
    private TaskExecutionContext taskExecutionContext;

    private JobDetails jobDetails;

    private JobInstance jobInstance;
    private List<DatasourceDetailInfo> datasourceDetailInfoList;

    private DataxProperties dataxProperties;
    private String taskAppId;

//    private ShellCommandExecutor shellCommandExecutor;


    private final String DATAX_JSON_TEMPLATE_PATH = "/datax/datax_template.json";
    private final String READER_TEMPLATE_PATH = "/datax/reader/%sreader_template.json";
    private final String WRITER_TEMPLATE_PATH = "/datax/writer/%swriter_template.json";

    private JSONObject dataxJobJson;
    private JSONObject readerTemplate;

    private JSONObject writerTemplate;

    private DataxTemplateHandler dataxTemplateHandler;

    /**
     * shell command executor
     */
    private ShellCommandExecutor shellCommandExecutor;

    AtomicBoolean needCrossRegionProcess = new AtomicBoolean(false);

    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    protected DataxTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
        this.dataxJobConf = (DataxJobConf) taskExecutionContext.getJobConf();
        this.datasourceDetailInfoList = taskExecutionContext.getDatasourceDetailInfoList();
        this.jobDetails = taskExecutionContext.getJobDetails();
        this.jobInstance = taskExecutionContext.getJobInstance();
        this.dataxProperties = taskExecutionContext.getDataxProperties();
        this.taskAppId = getTaskAppId(jobInstance.getJiId());
        this.taskExecutionContext.setTaskAppId(this.taskAppId);
        this.dataxTemplateHandler = new DataxTemplateHandler(taskExecutionContext.getJobInstance(),
                jobDetails, taskExecutionContext.getJobInstanceParamsList());
        this.shellCommandExecutor = new ShellCommandExecutor(this::logHandle, taskExecutionContext);

    }



    @Override
    public void init() {
        try {
            loadDataxTemplate(this.datasourceDetailInfoList);
            if (readerTemplate != null && writerTemplate != null) {
                log.info("load reader/writer template successfully");

                for (DataxJobConf.Readers reader : dataxJobConf.getReaders()) {
                    fillReaderFromTemplate(reader);
                }

                for (DataxJobConf.Writers writer : dataxJobConf.getWriters()) {
                    fillWriterFromTemplate(writer);
                }
                this.dataxJobJson.getJSONObject(JOB).getJSONArray(CONTENT)
                        .getJSONObject(0).put(DataXKey.READER, this.readerTemplate);
                this.dataxJobJson.getJSONObject(JOB).getJSONArray(CONTENT)
                        .getJSONObject(0).put(DataXKey.WRITER, this.writerTemplate);
                log.info("fill reader/writer job json successfully");
                log.info(JSONObject.toJSONString(this.dataxJobJson, true));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        try {
            IShellInterceptorBuilder<?, ?> shellActuatorBuilder = ShellInterceptorBuilderFactory.newBuilder()
                    .appendScript(buildCommand(buildDataxJsonFile(), jobInstance.getJiId()));
            TaskResponse commandExecuteResult = shellCommandExecutor.run(shellActuatorBuilder, taskCallBack);
            setExitStatusCode(commandExecuteResult.getExitStatusCode());
            setProcessId(commandExecuteResult.getProcessId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("The current DataX task has been interrupted", e);
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw new TaskException("The current DataX task has been interrupted", e);
        } catch (Exception e){
            log.error("datax task error", e);
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw new TaskException("Execute DataX task failed", e);
        }
    }

    private String buildCommand(String jobConfigFilePath, int jiId) {

        // datax python command
        return dataxProperties.getPythonLauncher() +
                " " +
                dataxProperties.getDataxLauncher() +
                " " +
                loadJvmEnv() +
                " " +
                "--jiId " + jiId +
                " " +
                jobConfigFilePath;
    }

    private String buildDataxJsonFile() throws Exception {
        // generate json
        String dirPath = String.format("%s%s%s", dataxProperties.getExecuteJobPath(),
                File.separator, LocalDateTime.now().format(DateTimeFormatter.ofPattern(YYYYMMDD)));
        this.dataxProperties.setExecuteJobPath(dirPath);

        File dataDir = new File(dirPath);
        // 为了避免多个线程同时检查日期目录时导致线程安全问题
        synchronized (DataxTask.class) {
            if (!dataDir.exists() && !dataDir.mkdirs()) {
                throw new TaskException("创建当天日期目录失败：" + dirPath);
            }
        }
        String fileName = String.format("%s%s%s_job.json", dirPath, File.separator, this.taskAppId);
        FileUtils.writeContent2File(JSONObject.toJSONString(dataxJobJson, true), fileName);
        log.info("已生成DataX任务配置文件: [{}]", fileName);
        return fileName;
    }



    @Override
    public void cancel() throws TaskException {
        this.cancel = true;
        setExitStatusCode(EXIT_CODE_KILL);
        try {
            shellCommandExecutor.cancelApplication();
        } catch (Exception e) {
            throw new TaskException("cancel application error", e);
        }

    }

    private void fillReaderFromTemplate(DataxJobConf.Readers reader) {
        Optional<DatasourceDetailInfo> dsInfoOptional = datasourceDetailInfoList.stream()
                .filter(datasourceDetailInfo -> reader.getDsId() == datasourceDetailInfo.getDsId()).findFirst();
        if (dsInfoOptional.isPresent()) {
            DatasourceDetailInfo dsInfo = dsInfoOptional.get();
            DbType dbType = DbType.valueOf(dsInfo.getDsType());
            ConnectionParam connectionparam = DataSourceUtils.buildConnectionParams(dbType, dsInfo);
            switch (dbType) {
                case MYSQL:
                case ORACLE:
                    dataxTemplateHandler.fillRDMSReaderJobConf(connectionparam, reader, this.readerTemplate);
                    break;
                case HIVE:
                case IMPALA:
                    dataxTemplateHandler.fillHiveImpalaReaderJobConf(connectionparam, reader, this.readerTemplate);
                    break;
                case FTP:
                    dataxTemplateHandler.fillFtpReaderJobConf(connectionparam, reader, this.readerTemplate);
                    break;
                case HBASE:
                    dataxTemplateHandler.fillHBaseReaderJobConf(connectionparam, reader, this.readerTemplate);
                    break;
                case HDFS:
                    dataxTemplateHandler.fillHDFSReaderJobConf(connectionparam, reader, this.readerTemplate);
                    break;
                case VERTICA:
                    dataxTemplateHandler.fillVerticaReaderJobConf(connectionparam, reader, this.readerTemplate);
                    break;
            }

            if(Lists.newArrayList(HIVE, IMPALA, HBASE).contains(dbType)) {
                needCrossRegionProcess.set(true);
            }
        } else {
            throw new TaskException("datax task fillReaderFromTemplate error");
        }
    }


    private void fillWriterFromTemplate(DataxJobConf.Writers writer) {
        Optional<DatasourceDetailInfo> dsInfoOptional = datasourceDetailInfoList.stream()
                .filter(datasourceDetailInfo -> writer.getDsId() == datasourceDetailInfo.getDsId()).findFirst();
        if (dsInfoOptional.isPresent()) {
            DatasourceDetailInfo dsInfo = dsInfoOptional.get();
            DbType dbType = DbType.valueOf(dsInfo.getDsType());
            ConnectionParam connectionparam = DataSourceUtils.buildConnectionParams(dbType, dsInfo);
            if(needCrossRegionProcess.get() && Lists.newArrayList(HIVE, IMPALA, HBASE).contains(dbType)) {
                needCrossRegionProcess.set(true);
            }

            dealWithWriterConnectionParam(dbType, connectionparam);

            switch (dbType) {
                case MYSQL:
                case ORACLE:
                    dataxTemplateHandler.fillRDMSWriterJobConf(connectionparam, writer, this.writerTemplate);
                    break;
                case HIVE:
                    dataxTemplateHandler.fillHiveWriterJobConf(connectionparam, writer, this.writerTemplate);
                    break;
                case IMPALA:
                    dataxTemplateHandler.fillImpalaWriterJobConf(connectionparam, writer, this.writerTemplate);
                    break;
                case FTP:
                    dataxTemplateHandler.fillFtpWriterJobConf(connectionparam, writer, this.writerTemplate);
                    break;
                case HBASE:
                    dataxTemplateHandler.fillHBaseWriterJobConf(connectionparam, writer, this.writerTemplate);
                    break;
                case HDFS:
                    dataxTemplateHandler.fillHDFSWriterJobConf(connectionparam, writer, this.writerTemplate);
                    break;
                case VERTICA:
                    dataxTemplateHandler.fillVerticaWriterJobConf(connectionparam, writer, this.writerTemplate);
                    break;
            }
        }
    }

    private void dealWithWriterConnectionParam(DbType dbType, ConnectionParam connectionparam) {
        JSONObject readerTemplateJSONObject = readerTemplate.getJSONObject(PARAMETER);
        String krbConfPath = readerTemplateJSONObject.getString(KRB_CONF_FILEPATH);
        String krbKeytabPath = readerTemplateJSONObject.getString(KRB_KEYTAB_FILEPATH);
        String krbPrincipal = readerTemplateJSONObject.getString(KRB_PRINCIPAL);
        switch (dbType) {
            case HIVE:
            case IMPALA:
                HiveImpalaConnectionParam hiveImpalaConnectionParam = (HiveImpalaConnectionParam) connectionparam;
                hiveImpalaConnectionParam.setKrbConfPath(krbConfPath);
                hiveImpalaConnectionParam.setKrbKeytabsDir(krbKeytabPath);
                hiveImpalaConnectionParam.setPrincipalName(krbPrincipal);
                break;
            case HBASE:
                HBaseXSQLConnectionParam hBaseXSQLConnectionParam = (HBaseXSQLConnectionParam) connectionparam;
                hBaseXSQLConnectionParam.setKrbConfPath(krbConfPath);
                hBaseXSQLConnectionParam.setKrbKeytabsDir(krbKeytabPath);
                hBaseXSQLConnectionParam.setPrincipalName(krbPrincipal);
                break;
        }
    }


    private void loadDataxTemplate(List<DatasourceDetailInfo> datasourceDetailInfoList) throws IOException {
        try {
            String dataxJsonTemplate = IOUtils.toString(Objects.requireNonNull(DataxTask.class.getResourceAsStream(DATAX_JSON_TEMPLATE_PATH)), StandardCharsets.UTF_8);
            this.dataxJobJson = JSON.parseObject(dataxJsonTemplate);
        } catch (IOException e) {
            throw new IOException("加载datax JSON模板失败", e);
        }

        for (DatasourceDetailInfo dsInfo : datasourceDetailInfoList) {
            String pluginType = dsInfo.getPluginType();
            String pluginName = dsInfo.getPluginName();

            if (pluginType.equals(READER.name())) {
                String templateRefPath = String.format(READER_TEMPLATE_PATH, pluginName);
                try {
                    String template = IOUtils.toString(Objects.requireNonNull(DataxTask.class.getResourceAsStream(templateRefPath)), StandardCharsets.UTF_8);
                    this.readerTemplate = JSON.parseObject(template);
                } catch (IOException e) {
                    throw new IOException(String.format("加载datax %sreader 模板失败", pluginName), e);
                }

            }

            if (pluginType.equals(WRITER.name())) {
                String templateRefPath = String.format(WRITER_TEMPLATE_PATH, pluginName);
                try {
                    String template = IOUtils.toString(Objects.requireNonNull(DataxTask.class.getResourceAsStream(templateRefPath)), StandardCharsets.UTF_8);
                    this.writerTemplate = JSON.parseObject(template);
                } catch (IOException e) {
                    throw new IOException(String.format("加载datax %swriter 模板失败", pluginName), e);
                }
            }
        }

    }

    private String getTaskAppId(int jiId) {
        return String.format("%s_%s", jiId, LocalDateTime.now().format(DateTimeFormatter.ofPattern(YYYYMMDDHHMMSS)));
    }


    public String loadJvmEnv() {
        int xms = Math.max(dataxProperties.getXms(), 1);
        int xmx = Math.max(dataxProperties.getXmx(), 1);
        return String.format(JVM_PARAM, xms, xmx);
    }
}
