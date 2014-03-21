package com.dianping.darkbat.entity;

public class TaskRelaEntity {

    private String cycleGap;
    private String remark;
    private Integer taskId;
    private Integer taskPreId;
    private String timeStamp;

    public String getCycleGap() {
        return cycleGap;
    }

    public String getRemark() {
        return remark;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public Integer getTaskPreId() {
        return taskPreId;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setCycleGap(String cycleGap) {
        this.cycleGap = cycleGap;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public void setTaskPreId(Integer taskPreId) {
        this.taskPreId = taskPreId;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

}
