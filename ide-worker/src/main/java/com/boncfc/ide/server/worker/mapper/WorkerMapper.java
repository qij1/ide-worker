package com.boncfc.ide.server.worker.mapper;

import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import com.boncfc.ide.plugin.task.api.model.JobDetails;
import com.boncfc.ide.plugin.task.api.model.JobInstance;
import com.boncfc.ide.plugin.task.api.model.JobInstanceParams;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    int addJobInstanceParams(@Param("jobInstanceParams") List<JobInstanceParams> jobInstanceParams);

    List<DatasourceDetailInfo> getDatasourceDetailInfoList(@Param("dsIds")List<Integer> dsIds);


}
