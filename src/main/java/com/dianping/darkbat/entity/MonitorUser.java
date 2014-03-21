package com.dianping.darkbat.entity;

public class MonitorUser {

    private long autoId;
    private String userName;
    private String mobileNo;
    private String officeNo;
    private int orderId;
    private String beginDate;
    private String addUser;
    private String addTime;
    private String updateUser;
    private String updateTime;
    
    public long getAutoId() {
        return autoId;
    }
    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }
    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public String getMobileNo() {
        return mobileNo;
    }
    public void setMobileNo(String mobileNo) {
        this.mobileNo = mobileNo;
    }
    public String getOfficeNo() {
        return officeNo;
    }
    public void setOfficeNo(String officeNo) {
        this.officeNo = officeNo;
    }
    public String getBeginDate() {
        return beginDate;
    }
    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }
    public String getAddUser() {
        return addUser;
    }
    public void setAddUser(String addUser) {
        this.addUser = addUser;
    }
    public String getAddTime() {
        return addTime;
    }
    public void setAddTime(String addTime) {
        this.addTime = addTime;
    }
    public String getUpdateUser() {
        return updateUser;
    }
    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }
    public String getUpdateTime() {
        return updateTime;
    }
    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
    public int getOrderId() {
        return orderId;
    }
    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }
}
