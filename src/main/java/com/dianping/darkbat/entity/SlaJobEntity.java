package com.dianping.darkbat.entity;

/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-1-22
 * Time: 上午11:46
 * To change this template use File | Settings | File Templates.
 */
public class SlaJobEntity {
    private Integer slaId;
    private Integer slaType;
    private String slaName;
    private Double jobValue;
    private String finishTime;
    private String keyTaskId;
    private Integer keyPreTaskId;
    private String KeyPreTaskName;
    private Integer keyPreStatus;
    private String warnBeginTime;
    private String warnTime;
    private Integer reportStatus;

    public Double getJobValue() {
        return jobValue;
    }

    public void setJobValue(Double jobValue) {
        this.jobValue = jobValue;
    }

    public Integer getSlaType() {
        return slaType;
    }

    public void setSlaType(Integer slaType) {
        this.slaType = slaType;
    }

    public Integer getKeyPreStatus() {
        return keyPreStatus;
    }

    public void setKeyPreStatus(Integer keyPreStatus) {
        this.keyPreStatus = keyPreStatus;
    }


    public Integer getSlaId() {
        return slaId;
    }

    public void setSlaId(Integer slaId) {
        this.slaId = slaId;
    }

    public String getSlaName() {
        return slaName;
    }

    public void setSlaName(String slaName) {
        this.slaName = slaName;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public String getKeyTaskId() {
        return keyTaskId;
    }

    public void setKeyTaskId(String keyTaskId) {
        this.keyTaskId = keyTaskId;
    }

    public Integer getKeyPreTaskId() {
        return keyPreTaskId;
    }

    public void setKeyPreTaskId(Integer keyPreTaskId) {
        this.keyPreTaskId = keyPreTaskId;
    }

    public String getKeyPreTaskName() {
        return KeyPreTaskName;
    }

    public void setKeyPreTaskName(String keyPreTaskName) {
        KeyPreTaskName = keyPreTaskName;
    }

    public String getWarnBeginTime() {
        return warnBeginTime;
    }

    public void setWarnBeginTime(String warnBeginTime) {
        this.warnBeginTime = warnBeginTime;
    }

    public String getWarnTime() {
        return warnTime;
    }

    public void setWarnTime(String warnTime) {
        this.warnTime = warnTime;
    }

    public Integer getReportStatus() {
        return reportStatus;
    }

    public void setReportStatus(Integer reportStatus) {
        this.reportStatus = reportStatus;
    }
}
