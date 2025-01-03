package com.boncfc.ide.server.worker.mapper;

import com.boncfc.ide.plugin.task.api.model.*;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Mapper
public interface WorkerMapper {
    List<String> getWorkerExecutableJobType(String workerAddress);

    @MapKey("WORKSPACE_ID")
    Map<String, Map<String, String>> getWorkspace();

    String getJobInstId(@Param("jobTypeList") List<String> jobTypeList);

    JobInstance getJobInstanceInfo(String jiId);

    JobDetails getJobDetails(JobInstance jobInstance);

    List<JobInstanceParams> getJobAllParams(JobInstance jobInstance);

    JobInstanceParams getPersetParams(String paramName);

    int updateJobInstance(JobInstance jobInstance);

    int deleteFromJobInstanceQueue(JobInstance jobInstance);

    int deleteJobInstanceParams(String jiId);

    int addJobInstanceParams(@Param("jobInstanceParams") List<JobInstanceParams> jobInstanceParams);

    int addJobInstanceIds(@Param("jobInstanceIdsList") List<JobInstanceIds> jobInstanceIdsList);

    List<DatasourceDetailInfo> getDatasourceDetailInfoList(@Param("dsIds") Set<Integer> dsIds, @Param("pluginType") String pluginType);


}
