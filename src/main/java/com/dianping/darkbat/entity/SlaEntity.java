package com.dianping.darkbat.entity;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-2-12
 * Time: 上午11:14
 * To change this template use File | Settings | File Templates.
 */
public class SlaEntity {

    private Integer slaId;
    private Integer slaType;
    private Integer jobId;
    private String slaName;
    private String warnBeginTime;
    private String warnTime;
    private Integer warnType;
    private String directPreTask;
    private List<Integer> keyPreTasks;
    private String userName;
    private String userEmail;
    private Integer slaStatus;
    public boolean touched = false;
    public Integer getSlaStatus() {
        return slaStatus;
    }

    public void setSlaStatus(Integer slaStatus) {
        this.slaStatus = slaStatus;
    }


    public Integer getJobId() {
        return jobId;
    }

    public void setJobId(Integer jobId) {
        this.jobId = jobId;
    }

    public Integer getSlaId() {
        return slaId;
    }

    public void setSlaId(Integer slaId) {
        this.slaId = slaId;
    }

    public Integer getSlaType() {
        return slaType;
    }

    public void setSlaType(Integer slaType) {
        this.slaType = slaType;
    }

    public String getSlaName() {
        return slaName;
    }

    public void setSlaName(String slaName) {
        this.slaName = slaName;
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

    public Integer getWarnType() {
        return warnType;
    }

    public void setWarnType(Integer warnType) {
        this.warnType = warnType;
    }

    public List<Integer> getKeyPreTasks() {
        return keyPreTasks;
    }

    public void setKeyPreTasks(List<Integer> keyPreTasks) {
        this.keyPreTasks = keyPreTasks;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getDirectPreTask() {
        return directPreTask;

    }

    public void setDirectPreTask(String directPreTask) {
        this.directPreTask = directPreTask;
    }

    public boolean equals(SlaEntity target){
        boolean a = (this.slaType.equals(target.getSlaType()));
        boolean b = (this.jobId.equals(target.getJobId()));
        return a&&b;
    }

    public void refresh(SlaEntity target){
        this.slaName = target.getSlaName();
        if(this.slaStatus != -1)
            this.slaStatus = 1;
        this.keyPreTasks = target.getKeyPreTasks();
        this.touched = true;
        target.touched = true;
    }

}
