package com.dianping.darkbat.entity;

/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-3-5
 * Time: 下午6:08
 * To change this template use File | Settings | File Templates.
 */
public class SlaFailRecordEntity {
    private int slaId;
    private String slaName;

    private int keyTaskId;
    private int keyTaskStatus;
    private String taskName;
    private String owner;
    private int delaySec;
    private double jobValue;

    public int getSlaId() {
        return slaId;
    }

    public String getSlaName() {
        return slaName;
    }

    public void setSlaName(String slaName) {
        this.slaName = slaName;
    }

    public void setSlaId(int slaId) {
        this.slaId = slaId;
    }

    public int getKeyTaskId() {
        return keyTaskId;
    }

    public void setKeyTaskId(int keyTaskId) {
        this.keyTaskId = keyTaskId;
    }

    public int getKeyTaskStatus() {
        return keyTaskStatus;
    }

    public void setKeyTaskStatus(int keyTaskStatus) {
        this.keyTaskStatus = keyTaskStatus;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public int getDelaySec() {
        return delaySec;
    }

    public void setDelaySec(int delaySec) {
        this.delaySec = delaySec;
    }

    public double getJobValue() {
        return jobValue;
    }

    public void setJobValue(double jobValue) {
        this.jobValue = jobValue;
    }
}
