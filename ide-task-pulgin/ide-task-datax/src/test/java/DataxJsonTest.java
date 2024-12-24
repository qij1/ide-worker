import com.boncfc.ide.plugin.task.api.utils.JSONUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DataxJsonTest {

    private final String DATAX_JSON_TEMPLATE_PATH = "/datax/datax_template.json";

    @Test
    public void testJSON() throws IOException {
        String dataxJsonTemplate = IOUtils.toString(DataxJsonTest.class.getResourceAsStream(DATAX_JSON_TEMPLATE_PATH), StandardCharsets.UTF_8);
        JsonNode dataxJobJson = JSONUtils.toJsonNode(dataxJsonTemplate);
        System.out.println(dataxJobJson);
    }
}
