package com.dianping.darkbat.entity;

public class MailInfoEntity {

    private Integer mailId;
    private String mailTitle;
    private String sendCycle;
    private String mailContent;
    private String userEmailList;
    private String itemIdList;
    private String itemType;
    private Integer systemId;
    private String sendTime;
    private String timeRange;
    private String addTime;
    private String updateTime;
    private String addUser;
    private String updateUser;
    private Integer isValid;
    private Integer taskId;
    
	public Integer getMailId() {
        return mailId;
    }
    public void setMailId(Integer mailId) {
        this.mailId = mailId;
    }
    public String getMailTitle() {
        return mailTitle;
    }
    public void setMailTitle(String mailTitle) {
        this.mailTitle = mailTitle;
    }
    public String getSendCycle() {
        return sendCycle;
    }
    public void setSendCycle(String sendCycle) {
        this.sendCycle = sendCycle;
    }
    public String getMailContent() {
        return mailContent;
    }
    public void setMailContent(String mailContent) {
        this.mailContent = mailContent;
    }
    public String getUserEmailList() {
        return userEmailList;
    }
    public void setUserEmailList(String userEmailList) {
        this.userEmailList = userEmailList;
    }
    public String getItemIdList() {
        return itemIdList;
    }
    public void setItemIdList(String itemIdList) {
        this.itemIdList = itemIdList;
    }
    public Integer getSystemId() {
        return systemId;
    }
    public String getSendTime() {
        return sendTime;
    }
    public void setSendTime(String sendTime) {
        this.sendTime = sendTime;
    }
    public void setSystemId(Integer systemId) {
        this.systemId = systemId;
    }
    public String getTimeRange() {
        return timeRange;
    }
    public void setTimeRange(String timeRange) {
        this.timeRange = timeRange;
    }
    public String getAddTime() {
        return addTime;
    }
    public void setAddTime(String addTime) {
        this.addTime = addTime;
    }
    public String getUpdateTime() {
        return updateTime;
    }
    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
    public String getAddUser() {
        return addUser;
    }
    public void setAddUser(String addUser) {
        this.addUser = addUser;
    }
    public String getUpdateUser() {
        return updateUser;
    }
    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }
    public Integer getIsValid() {
        return isValid;
    }
    public void setIsValid(Integer isValid) {
        this.isValid = isValid;
    }
    public Integer getTaskId() {
		return taskId;
	}
	public void setTaskId(Integer taskId) {
		this.taskId = taskId;
	}
    public String getItemType() {
        return itemType;
    }
    public void setItemType(String itemType) {
        this.itemType = itemType;
    }
}
