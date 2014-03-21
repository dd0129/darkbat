package com.dianping.darkbat.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.struts2.ServletActionContext;
import org.datanucleus.util.StringUtils;
import org.jsoup.helper.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.MailDetailEntity;
import com.dianping.darkbat.entity.MailInfoEntity;
import com.dianping.darkbat.exception.InvalidInputException;
import com.dianping.darkbat.service.HalleyService;
import com.dianping.darkbat.service.MailInfoService;
import com.dianping.darkbat.service.VenusReportService;
import com.opensymphony.xwork2.Action;

@Repository
public class MailInfoAction {

    @Autowired
    private MailInfoService mailInfoService;
    
    @Autowired
    private VenusReportService venusReportService;
    
    @Autowired
    private HalleyService halleyService;
    
    private JSONObject jsonObject;
    
    public MailInfoEntity getMailInfoEntity() {
        HttpServletRequest req = ServletActionContext.getRequest();
        MailInfoEntity mailInfo = new MailInfoEntity();
        
        try{
            String mailId = CommonUtil.getValueFromReq(req, "mailId");
            String taskId = CommonUtil.getValueFromReq(req, "taskId");
            String systemId = CommonUtil.getValueFromReq(req, "systemId");
            
            mailInfo.setMailId(mailId == null ? null : Integer.parseInt(mailId));
            mailInfo.setTaskId(taskId == null ? null : Integer.parseInt(taskId));
            mailInfo.setSystemId(systemId == null ? null : Integer.parseInt(systemId));

            String itemIdList = CommonUtil.getValueFromReq(req, "itemIdList");
            String userEmailList = CommonUtil.getValueFromReq(req, "userEmailList");
            
            mailInfo.setItemIdList(itemIdList == null ? null : itemIdList.replace("<+>", ","));
            mailInfo.setUserEmailList(userEmailList == null ? null : userEmailList.replace("<+>", ","));
            
            mailInfo.setItemType(CommonUtil.getValueFromReq(req, "itemType"));
            mailInfo.setAddTime(CommonUtil.getValueFromReq(req, "addTime"));
            mailInfo.setAddUser(CommonUtil.getValueFromReq(req, "addUser"));
            mailInfo.setMailContent(CommonUtil.getValueFromReq(req, "mailContent"));
            
            String mailContent = CommonUtil.getValueFromReq(req, "mailContent");
            mailInfo.setMailContent(mailContent == null ? "" : mailContent);
            mailInfo.setMailTitle(CommonUtil.getValueFromReq(req, "mailTitle"));
            mailInfo.setSendCycle(CommonUtil.getValueFromReq(req, "sendCycle"));
            mailInfo.setSendTime(CommonUtil.getValueFromReq(req, "sendTime"));
            mailInfo.setUpdateTime(CommonUtil.getValueFromReq(req, "updateTime"));
            mailInfo.setUpdateUser(CommonUtil.getValueFromReq(req, "updateUser"));
            mailInfo.setTimeRange(CommonUtil.getValueFromReq(req, "timeRange"));
            
            return mailInfo;
        } catch (Exception e) {
            throw new RuntimeException("有非法输入: " + e.getMessage());
        }
    }
    
    protected List<MailDetailEntity> getMailDetail(){
        HttpServletRequest req = ServletActionContext.getRequest();
        
        List<MailDetailEntity> mailDetails = new ArrayList<MailDetailEntity>();
        for (Object json : JSONArray.fromObject(req.getParameter("mailDetail"))) {
            JSONObject jsonMailDetail = JSONObject.fromObject(json);
            MailDetailEntity mailDetail = new MailDetailEntity();
            
            mailDetail.setReportId(jsonMailDetail.getLong("id"));
            mailDetail.setDataCycle(jsonMailDetail.getInt("cycle"));
            mailDetail.setItemType(jsonMailDetail.getString("type").toUpperCase());
            mailDetail.setDisplayIndex(jsonMailDetail.getInt("displayIndex"));
            mailDetail.setIsHide(jsonMailDetail.getInt("isHide"));
            
            mailDetails.add(mailDetail);
        }
        return mailDetails;
    }

    /**
     * 获取所有mail信息
     * @return
     */
    public String getMailList() {
        MailInfoEntity mailInfo = getMailInfoEntity();
        
        List<MailInfoEntity> infoList = mailInfoService.getMailList(mailInfo);
        
        JSONArray jArray = JSONArray.fromObject(infoList);
        jsonObject = CommonUtil.getPubJson(jArray);
        
        return Action.SUCCESS;
    }
    
    /**
     * 获取所有报表信息，包含pretaskid
     * @return
     * @throws InvalidInputException 
     */
    public String getReportListWithPreTaskId() throws InvalidInputException{
        JSONArray jObj = venusReportService.getReportListWithoutDir();
        for(Object oReport:jObj){
            try{
                JSONObject jReport = (JSONObject)oReport;
                List<Integer> preTaskIds = new ArrayList<Integer>();
                for(String sTaskId:mailInfoService.getPreTaskIdsForReports(jReport.getString("reportId"))){
                    preTaskIds.add(Integer.valueOf(sTaskId));
                }
                jReport.accumulate("preTaskIds", JSONArray.fromObject(preTaskIds));
            }catch(Exception e){
                continue;
            }
        }        
        jsonObject = CommonUtil.getPubJson(jObj);
        return Action.SUCCESS;
    }
    
    /**
     * 获取所有页面信息，包含pretaskid
     * @return
     * @throws InvalidInputException 
     */
    public String getPageListWithPreTaskId() throws InvalidInputException{
        JSONArray jObj = venusReportService.getPageListWithoutDir();
        for(Object oPage:jObj){
            JSONObject jPage = (JSONObject)oPage;
            List<Integer> preTaskIds = new ArrayList<Integer>();
            List<MailDetailEntity>  pageDashletConfigs = new ArrayList<MailDetailEntity>();
            System.out.println(jPage.getString("pageId"));
            for(Object oPageItems:jPage.getJSONArray("dashletConfig")){
                for(Object oPageItem:JSONArray.fromObject(oPageItems)){
                    JSONObject jPageItem = (JSONObject)oPageItem;
                    MailDetailEntity pageDashletConfig = new MailDetailEntity();
                    pageDashletConfig.setItemType(jPageItem.getString("type"));
                    pageDashletConfig.setReportId(jPageItem.getLong("id"));
                    pageDashletConfigs.add(pageDashletConfig);
                }
            }
            for(String sTaskId:mailInfoService.getPreTaskIdsForPage(jPage.getString("pageId"),pageDashletConfigs)){
                preTaskIds.add(Integer.valueOf(sTaskId));
            }
            jPage.accumulate("preTaskIds", JSONArray.fromObject(preTaskIds));
        }        
        jsonObject = CommonUtil.getPubJson(jObj);
        return Action.SUCCESS;
    }
    
    public String getMailListContainsReport(){
        HttpServletRequest req = ServletActionContext.getRequest();
        Integer reportId = Integer.parseInt(req.getParameter("reportId"));
        
        List<MailInfoEntity> infoList = mailInfoService.getMailListContainsReport(reportId);
        
        JSONArray jArray = JSONArray.fromObject(infoList);
        jsonObject = CommonUtil.getPubJson(jArray);
        ServletActionContext.getResponse().setHeader("Access-Control-Allow-Origin", "*");//允许跨域访问
        return Action.SUCCESS;
    }
    
    public String deleteMailInfo() {
        MailInfoEntity mailInfo = getMailInfoEntity();
        
        int ret = mailInfoService.deleteMailInfo(mailInfo);
        jsonObject = CommonUtil.getPubJson(ret);
        
        return Action.SUCCESS;
    }
    
    public String addOrUpdateMailInfo() throws InvalidInputException {
        MailInfoEntity mailInfo = getMailInfoEntity();
        Object ret = null;            
                                      
        MailInfoEntity checkInfo = mailInfoService.getExistedMailInfo(mailInfo);
                                      
        if(checkInfo != null) {       
            throw new InvalidInputException("邮件名称已存在，请更换一个名称！");
        }                             
                                       
        if(mailInfo.getMailId() == null) {
            ret = mailInfoService.addMailInfo(mailInfo, getMailDetail());
        } else {     
            ret = mailInfoService.updateMailInfo(mailInfo, getMailDetail());
        }            
                     
        
        jsonObject = CommonUtil.getPubJson(ret);
        
        return Action.SUCCESS;
    }
    
    public String getReportListFromVenus() throws InvalidInputException {
        JSONObject jObj = venusReportService.getReportList();
        jsonObject = CommonUtil.getPubJson(jObj);
        
        return Action.SUCCESS;
    }
    
    /**
     * 获取邮件包含内容，具体到report|table|chart
     * @return
     */
    public String getMailItems() {
        HttpServletRequest req = ServletActionContext.getRequest();
        int mailId = Integer.parseInt(CommonUtil.getValueFromReq(req, "mailId"));

        //page->items
        MailDetailEntity detailInfo = new MailDetailEntity();
        detailInfo.setMailId(mailId);
        
        List<MailDetailEntity> detailList = mailInfoService.getMailDetail(detailInfo);
        List<Long> chartIds = new ArrayList<Long>();
        List<Long> reportIds = new ArrayList<Long>();
        
        for (MailDetailEntity detail : detailList) {
            if (Const.MAIL_ITEM_CHART.equals(detail.getItemType())) {
                chartIds.add(detail.getReportId());
            } else if (Const.MAIL_ITEM_TABLE.equals(detail.getItemType())) {
                reportIds.add(detail.getReportId());
            } else if (Const.MAIL_ITEM_REPORT.equals(detail.getItemType())) {
                reportIds.add(detail.getReportId());
            } else {
                throw new RuntimeException("未识别的元素类型:" + detail.getMailId());
            }
        }
        
        Map<Integer, JSONObject> chartMap = new HashMap<Integer, JSONObject>();
        Map<Integer, JSONObject> reportMap = new HashMap<Integer, JSONObject>();
        
        if (!chartIds.isEmpty()) {
            String strChartIds = StringUtil.join(chartIds, ",");
            JSONArray jArrChart = venusReportService.getChartList(strChartIds);
            chartMap = venusReportService.buildIdToJsonMap(jArrChart, Const.MAIL_ITEM_CHART);
        }

        if (!reportIds.isEmpty()) {
            String strReportIds = StringUtil.join(reportIds, ",");
            JSONArray jArrReport = venusReportService.getReportListByIds(strReportIds);
            reportMap = venusReportService.buildIdToJsonMap(jArrReport, Const.MAIL_ITEM_REPORT);
        }

        JSONArray jArrDetails = JSONArray.fromObject(detailList);
        for (Object obj : jArrDetails) {
            JSONObject jObj = (JSONObject)obj;
            String itemType = jObj.getString("itemType");
            if (Const.MAIL_ITEM_CHART.equals(itemType)) {
                jObj.element(
                        "itemName", 
                        chartMap.get(jObj.getInt("reportId")).getString("chartName"));
            } else if (
                    Const.MAIL_ITEM_REPORT.equals(itemType) ||
                    Const.MAIL_ITEM_TABLE.equals(itemType)
                    ) {
                jObj.element(
                        "itemName", 
                        reportMap.get(jObj.getInt("reportId")).getString("reportName"));
            }
        }
        
        jsonObject = CommonUtil.getPubJson(jArrDetails);
        return Action.SUCCESS;
    }
    
    public String getPageDataCycles() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String jArrStr = CommonUtil.getValueFromReq(req, "dashletConfig");
        JSONArray jArr = JSONArray.fromObject(jArrStr);
        
        if(StringUtils.notEmpty(req.getParameter("mailId"))) {
            MailDetailEntity info = new MailDetailEntity();
            
            info.setMailId(Integer.parseInt(CommonUtil.getValueFromReq(req, "mailId")));
            List<MailDetailEntity> infos = mailInfoService.getMailDetail(info);
            Map<String,Integer> mailDetail = new HashMap<String, Integer>();
            
            for(MailDetailEntity mailDetailInfo : infos){
                String key = mailDetailInfo.getItemType() + ":" +  mailDetailInfo.getReportId();
                mailDetail.put(key, mailDetailInfo.getDataCycle());
            }
            for(Object item : jArr){
                JSONObject configInfo = (JSONObject)item;
                String type = configInfo.getString("type").toUpperCase();
                Long itemId = configInfo.getLong("id");
                String key = type + ":" + itemId;
                
                if(mailDetail.containsKey(key)){
                    configInfo.accumulate("dataCycle", mailDetail.get(key));
                }
            }
        }
        jsonObject = CommonUtil.getPubJson(jArr);
        
        return Action.SUCCESS;
    }
    
    public String getPageListFromVenus() {
        JSONObject jObj = venusReportService.getPageList();
        jsonObject = CommonUtil.getPubJson(jObj);
        
        return Action.SUCCESS;
    }
    
    public String getPageItemListFromVenusByIds() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String pId = CommonUtil.getValueFromReq(req, "pageId");
        
        JSONObject pageInfo = venusReportService.getPageInfo(pId);
        JSONArray jArrItem = JSONArray.fromObject(pageInfo.get("dashletConfig"));
        String strMailId = req.getParameter("mailId");

        //邮件id为空则是新增邮件, 反之则为修改邮件
        //若是修改邮件内容, 则将page内的信息根据配置情况重新排列组合
        if (StringUtils.notEmpty(strMailId)) {
            Map<String, JSONObject> dashletMap = new HashMap<String, JSONObject>();
            List<JSONObject> items = mailInfoService.getFlatObj(jArrItem, new ArrayList<JSONObject>());
            
            for (JSONObject jObj : items) {
                String key = jObj.getString("type").toUpperCase() + ":" + jObj.getString("id");
                dashletMap.put(key, jObj);
            }
            jArrItem = new JSONArray();
            List<MailDetailEntity> deletedEntities = new ArrayList<MailDetailEntity>(); //在北斗平台page里被删除的item
            
            int mailId = Integer.parseInt(strMailId);
            MailDetailEntity info = new MailDetailEntity();
            info.setMailId(mailId);
            for (MailDetailEntity detailEntity : mailInfoService.getMailDetail(info)) {
                String key = detailEntity.getItemType().toUpperCase() + ":" + detailEntity.getReportId();
                JSONObject jObject = dashletMap.get(key);
                
                if (null == jObject) {
                    //如果邮件存在, 而北斗某page不存在该item, 则认为其为北斗平台被删除的item
                    
                    deletedEntities.add(detailEntity);
                } else {
                    jObject.put("isHide", detailEntity.getIsHide());
                    jObject.put("isEdit", true);
                    jObject.put("dataCycle", detailEntity.getDataCycle());
                    jArrItem.add(jObject);
                    
                    dashletMap.remove(key);
                }
            }
            pageInfo.put("deletedItems", deletedEntities);
            
            Iterator<Entry<String, JSONObject>> iterator = dashletMap.entrySet().iterator();
            //如果来自北斗的Page dashletMap还有值, 则认为是北斗新增的item, 在邮件配置需反应
            //默认isHide = 1, dataCycle = 7
            while (iterator.hasNext()) {
                Entry<String, JSONObject> entry = iterator.next();
                JSONObject newItem = entry.getValue();
                
                newItem.put("isHide", 1);
                newItem.put("isEdit", true);
                newItem.put("dataCycle", 7);
                jArrItem.add(newItem);
            }
        }
        
        JSONArray jParamArr = JSONArray.fromObject(pageInfo.get("paramConfig"));
        pageInfo.element("dashletConfig", jArrItem);
        pageInfo.element("paramConfig", jParamArr);
        jsonObject = CommonUtil.getPubJson(pageInfo);
        
        return Action.SUCCESS;
    }

    public String getReportListFromVenusByIds() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String ids = CommonUtil.getValueFromReq(req, "reportIds");
        JSONArray jObj = venusReportService.getReportListByIds(ids);
        
        String strMailId = req.getParameter("mailId");
        if(StringUtils.notEmpty(strMailId)){
            MailDetailEntity info = new MailDetailEntity();
            info.setMailId(Integer.parseInt(CommonUtil.getValueFromReq(req, "mailId")));
            Map<String,Integer> mailDetail = new HashMap<String,Integer>();
            for(MailDetailEntity mailDetailInfo:mailInfoService.getMailDetail(info)){
//                String key = mailDetailInfo.getItemType() + ":" +  mailDetailInfo.getReportId();
//                mailDetail.put(key, mailDetailInfo.getDataCycle());
                mailDetail.put(mailDetailInfo.getReportId().toString(), mailDetailInfo.getDataCycle());
            }
            for(Object report:jObj){
                JSONObject reportInfo = (JSONObject)report;
                reportInfo.put("isEdit", true);
//                String type = reportInfo.getString("itemType");
                Long reportId = reportInfo.getLong("reportId");
//                String key = type + ":" + reportId;
                String key = reportId.toString();
                
                if(mailDetail.containsKey(key)){
                    reportInfo.accumulate("dataCycle", mailDetail.get(key));
                }
            }
        }
        jsonObject = CommonUtil.getPubJson(jObj);
        
        return Action.SUCCESS;
    }
    
    public String getExistMailInfo() throws InvalidInputException {
        MailInfoEntity mailInfo = getMailInfoEntity();
        
        MailInfoEntity gotMailInfo = mailInfoService.getExistedMailInfo(mailInfo);
        if(gotMailInfo == null) {
            jsonObject = CommonUtil.getPubJson("", 500);
        } else {
            jsonObject = CommonUtil.getPubJson(gotMailInfo);
        }
        
        return Action.SUCCESS;
    }
    
    public String updateMailPreTasks() throws InvalidInputException {
        HttpServletRequest req = ServletActionContext.getRequest();
        Integer reportId = Integer.parseInt(req.getParameter("reportId"));
        
        List<MailInfoEntity> infoList = mailInfoService.getMailListContainsReport(reportId);
        
        for (MailInfoEntity info : infoList) {
            mailInfoService.updatePreTasksForReport(info);
        }
        
        jsonObject = CommonUtil.getPubJson(infoList.size());

        return Action.SUCCESS;
    }

    public String sendTestMail() {
        HttpServletRequest req = ServletActionContext.getRequest();
        Integer taskId = Integer.parseInt(req.getParameter("taskId"));
        String recipient = req.getParameter("recipient");
        
        int ret = mailInfoService.sendTestMail(taskId, recipient);

        jsonObject = CommonUtil.getPubJson(ret);

        return Action.SUCCESS;
    }
    
    public String resendMail() {
        HttpServletRequest req = ServletActionContext.getRequest();
        Integer taskId = Integer.parseInt(req.getParameter("taskId"));
        String beginDate = req.getParameter("begin");
        String endDate = req.getParameter("end");
        
        int ret = mailInfoService.resendMail(taskId, beginDate, endDate);
        
        jsonObject = CommonUtil.getPubJson(ret);
        return Action.SUCCESS;
    }
    
    public String handleBDCacheJob() throws InvalidInputException{
        HttpServletRequest req = ServletActionContext.getRequest();
        long reportId = Long.parseLong(req.getParameter("reportId"));
        Integer type = Integer.parseInt(req.getParameter("type"));
        Integer ret = 0;
        if(type == 0)
            ret = mailInfoService.delBDCacheJob(reportId);
        else
            ret = mailInfoService.addOrUpdateBDCacheJob(reportId);
        jsonObject = CommonUtil.getPubJson(ret);
        return Action.SUCCESS;
    }
    
    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }
}
