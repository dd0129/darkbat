package com.dianping.darkbat.entity;

/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-3-6
 * Time: 下午4:52
 * To change this template use File | Settings | File Templates.
 */
public class BottleneckTaskEntity {

    private int keyTaskId;
    private String taskName;
    private String owner;
    private int delaySec;
    private double jobValue;
    private int delayCnt;
    private int abnormal;

    public int getKeyTaskId() {
        return keyTaskId;
    }

    public void setKeyTaskId(int keyTaskId) {
        this.keyTaskId = keyTaskId;
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

    public int getDelayCnt() {
        return delayCnt;
    }

    public void setDelayCnt(int delayCnt) {
        this.delayCnt = delayCnt;
    }

    public int getAbnormal() {
        return abnormal;
    }

    public void setAbnormal(int abnormal) {
        this.abnormal = abnormal;
    }


}
