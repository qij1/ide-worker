package com.boncfc.ide.plugin.task.datax;

import com.boncfc.ide.plugin.task.api.*;
import com.boncfc.ide.plugin.task.api.datasource.DbType;
import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.boncfc.ide.plugin.task.api.model.DataxPluginType.READER;
import static com.boncfc.ide.plugin.task.api.model.DataxPluginType.WRITER;

public class DataxTask extends AbstractTask {

    private DataxJobConf dataxJobConf;
    private TaskExecutionContext taskExecutionContext;


    private final String DATAX_JSON_TEMPLATE_PATH = "/datax/datax_template.json";
    private final String READER_TEMPLATE_PATH = "/datax/reader/%sreader_template.json";
    private final String WRITER_TEMPLATE_PATH = "/datax/writer/%swriter_template.json";

    private JsonNode dataxJobJson;
    private JsonNode readerTemplate;
    /**
     * constructor
     * @param taskExecutionContext taskExecutionContext
     */
    protected DataxTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
        this.dataxJobConf = (DataxJobConf) taskExecutionContext.getJobConf();
    }

    @Override
    public void init() {
        try {
            parseDataxJson(dataxJobConf, taskExecutionContext.getDatasourceDetailInfoList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void parseDataxJson(DataxJobConf jobConf, List<DatasourceDetailInfo> datasourceDetailInfoList) throws IOException {
        try {
            String dataxJsonTemplate = IOUtils.toString(DataxTask.class.getResourceAsStream(DATAX_JSON_TEMPLATE_PATH), StandardCharsets.UTF_8);
            this.dataxJobJson = JSONUtils.toJsonNode(dataxJsonTemplate);
        } catch (IOException e) {
            throw new IOException("加载datax JSON模板失败", e);
        }

        for(DatasourceDetailInfo dsInfo : datasourceDetailInfoList) {
            String pluginType = dsInfo.getPluginType();
            String pluginName = dsInfo.getPluginName();
            if(pluginType.equals(READER.name())) {
                String templateRefPath = String.format(READER_TEMPLATE_PATH, pluginName);
                try {
                    String template = IOUtils.toString(DataxTask.class.getResourceAsStream(templateRefPath), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new IOException(String.format("加载datax %sreader 模板失败", pluginName, e));
                }

            }

            if(pluginType.equals(WRITER.name())) {
                String templateRefPath = String.format(WRITER_TEMPLATE_PATH, pluginName);
                try {
                    String template = IOUtils.toString(DataxTask.class.getResourceAsStream(templateRefPath), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new IOException(String.format("加载datax %sreader 模板失败", pluginName, e));
                }
            }
        }

    }

    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {

    }

    @Override
    public void cancel() throws TaskException {

    }
}
