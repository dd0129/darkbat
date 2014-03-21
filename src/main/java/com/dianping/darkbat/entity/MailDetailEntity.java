package com.dianping.darkbat.entity;

public class MailDetailEntity {

    private Integer mailId;
    private Long reportId;
    private String itemType;
    private Integer dataCycle;
    private Integer displayIndex;
    private Integer isHide;

    public Integer getMailId() {
        return mailId;
    }

    public void setMailId(Integer mailId) {
        this.mailId = mailId;
    }

    public Long getReportId() {
        return reportId;
    }

    public void setReportId(Long reportId) {
        this.reportId = reportId;
    }

    public Integer getDataCycle() {
        return dataCycle;
    }

    public void setDataCycle(Integer dataCycle) {
        this.dataCycle = dataCycle;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public Integer getDisplayIndex() {
        return displayIndex;
    }

    public void setDisplayIndex(Integer displayIndex) {
        this.displayIndex = displayIndex;
    }

    public Integer getIsHide() {
        return isHide;
    }

    public void setIsHide(Integer isHide) {
        this.isHide = isHide;
    }

}
