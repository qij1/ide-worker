import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.boncfc.ide.plugin.task.datax.DataXKey.PARAMETER;

public class DataxJsonTest {

    private final String DATAX_JSON_TEMPLATE_PATH = "/datax/reader/hive3reader_template.json";

    @Test
    public void testJSON() throws IOException {
        String dataxJsonTemplate = IOUtils.toString(DataxJsonTest.class.getResourceAsStream(DATAX_JSON_TEMPLATE_PATH), StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(dataxJsonTemplate);
        JSONObject param = jsonObject.getJSONObject(PARAMETER);
        param.fluentPut("fileType", "rc");
        System.out.println(jsonObject);
    }
}
