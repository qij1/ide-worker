package com.boncfc.ide.plugin.task.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobInstance {

  private int jiId;
  private int jfiId;
  private int jobId;
  private int jdId;
  private Date batchTime;
  private String batchNo;
  private String jiType;
  private String jobConf;
  private String status;
  private String extendedStatus;
  private String retryCount;
  private String retryIntervalS;
  private String eventDependency;
  private String jobDependency;
  private Date selectedTime;
  private Date takenTime;
  private Date finishTime;
  private String workerIp;
  private String isDryRun;
  private String enabled;
  private Date createTime;
  private Date updateTime;

}
