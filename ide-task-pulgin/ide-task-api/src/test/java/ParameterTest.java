import com.boncfc.ide.plugin.task.api.datasource.mysql.MySQLDataSourceProcessor;
import com.boncfc.ide.plugin.task.api.model.JobInstanceParams;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static com.boncfc.ide.plugin.task.api.AbstractTask.rgex;
import static com.boncfc.ide.plugin.task.api.AbstractTask.setSqlParamsMap;

public class ParameterTest {

    @Test
    public void testParam() {
        System.out.println(Double.parseDouble("1"));
        String sql = "/**asdsass" +
                "sddasdsd" +
                "adssdsw*/\n" +
                "    select * from aaaa where ymd = ${ymd} and yest=${yest} and aa = ${aaa};" +
                "insert into ccc select * from bbb";
        JobInstanceParams jobInstanceParams = JobInstanceParams.builder().
                jobParamType("string").paramName("ymd").paramValue("20241203").dataType("string").sortIndex(1).
                build();
        JobInstanceParams jobInstanceParams2 = JobInstanceParams.builder().
                jobParamType("string").paramName("yest").paramValue("20241202").dataType("string").sortIndex(2).
                build();
        JobInstanceParams jobInstanceParams3 = JobInstanceParams.builder().
                jobParamType("string").paramName("aaa").paramValue("11.1").dataType("number").sortIndex(3).
                build();
        List<JobInstanceParams> jobInstanceParamsList = new LinkedList<>();
        jobInstanceParamsList.add(jobInstanceParams2);
        jobInstanceParamsList.add(jobInstanceParams);
        jobInstanceParamsList.add(jobInstanceParams3);
        List<JobInstanceParams> jobInstanceParams11 = jobInstanceParamsList.stream()
                .sorted(Comparator.comparing(JobInstanceParams::getSortIndex))
                .collect(Collectors.toList());

        jobInstanceParams11.get(0).setParamValue("2222222222");
        jobInstanceParams11.get(1).setParamValue("3333333333");
        jobInstanceParams11.get(2).setParamValue("4444444444");

        Map<String, JobInstanceParams> paramsPropsMap = new HashMap<>();
        paramsPropsMap.put("ymd", jobInstanceParams);
        paramsPropsMap.put("yest", jobInstanceParams2);
        paramsPropsMap.put("aaa", jobInstanceParams3);
        List<String> subSqls = new MySQLDataSourceProcessor().splitAndRemoveComment(sql);
        for (String subSql : subSqls) {
            System.out.println(subSql);
//            getSqlAndSqlParamsMap(subSql);
        }
    }

    @Test
    public void testParam2() {
        String sql = "select * from aaaa where ymd = ${ymd} and yest=${yest} and aa = ${aaa}";
        JobInstanceParams jobInstanceParams = JobInstanceParams.builder().
                jobParamType("string").paramName("ymd").paramValue("20241203").dataType("string").
                build();
        JobInstanceParams jobInstanceParams2 = JobInstanceParams.builder().
                jobParamType("string").paramName("yest").paramValue("20241202").dataType("string").
                build();
        JobInstanceParams jobInstanceParams3 = JobInstanceParams.builder().
                jobParamType("string").paramName("aaa").paramValue("11.1").dataType("number").
                build();
        Map<String, JobInstanceParams> paramsPropsMap = new HashMap<>();
        paramsPropsMap.put("ymd", jobInstanceParams);
        paramsPropsMap.put("yest", jobInstanceParams2);
        paramsPropsMap.put("aaa", jobInstanceParams3);
        StringBuilder sqlBuilder = new StringBuilder();
        if (paramsPropsMap == null) {
            sqlBuilder.append(sql);
            System.out.println(sqlBuilder.toString());
        }
        Map<Integer, JobInstanceParams> sqlParamsMap = new HashMap<>();

        setSqlParamsMap(sql, rgex, sqlParamsMap, paramsPropsMap, 1);
        String formatSql = sql.replaceAll(rgex, "?");
        System.out.println(sqlBuilder);
    }
}
