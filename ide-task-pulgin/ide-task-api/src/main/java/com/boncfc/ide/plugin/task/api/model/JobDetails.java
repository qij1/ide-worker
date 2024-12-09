package com.boncfc.ide.plugin.task.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobDetails {

  private String jdId;
  private String jobId;
  private String jobName;
  private String jobType;
  private String jobDesc;
  private String scId;
  private String jobConf;
  private String retryCount;
  private String retryIntervalS;
  private String ignoreError;
  private String jobVersionId;
  private String deleted;
  private String creator;
  private Date createTime;
  private String updater;
  private Date updateTime;

}
