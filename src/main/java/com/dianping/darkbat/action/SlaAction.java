package com.dianping.darkbat.action;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.common.GlobalResources;
import com.dianping.darkbat.entity.*;
import com.dianping.darkbat.exception.InvalidInputException;
import com.dianping.darkbat.mapper.SlaMapper;
import com.dianping.darkbat.service.MailInfoService;
import com.dianping.darkbat.service.SlaService;
import com.dianping.darkbat.service.VenusReportService;
import com.opensymphony.xwork2.Action;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.struts2.ServletActionContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-1-22
 * Time: 上午11:10
 * To change this template use File | Settings | File Templates.
 */
@Repository("SlaAction")
public class SlaAction {
    @Autowired
    private SlaService slaService;

    @Autowired
    private MailInfoService mailInfoService;

    @Autowired
    private VenusReportService venusReportService;


    private JSONObject jsonObject;

    private List<SlaEntity> slaLists = new ArrayList<SlaEntity>();

    private List<SlaEntity> insertLists = new ArrayList<SlaEntity>();

    private List<SlaEntity> updateLists = new ArrayList<SlaEntity>();

    private List<SlaEntity> currentLists = new ArrayList<SlaEntity>();

    public static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

    private static Logger log = Logger.getLogger(SlaAction.class);

    public String  getSlaJobs() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String date;
        try{
            date = CommonUtil.getValueFromReq(req, "date");
        }catch (Exception e){
            throw new RuntimeException("格式错误");
        }
        if(date == null){
            date = new DateTime().toString("yyyy-MM-dd");
        }
        List<SlaJobEntity> slajobs = slaService.getSlaJobs(date);
        if(slajobs.size() == 0){
            throw new RuntimeException("无活跃SLA任务");
        }
        JSONArray jArray = new JSONArray();
        jArray.addAll(slajobs);
        this.jsonObject = CommonUtil.getPubJson(jArray);
        return Action.SUCCESS;
    }

    public String  getSlaJobFinishTime() {
        HttpServletRequest req = ServletActionContext.getRequest();
        Integer slaId;
        String date;
        try{
           slaId = Integer.valueOf(CommonUtil.getValueFromReq(req, "slaId"));
        }catch (Exception e){
            throw new RuntimeException("SlaId格式错误");
        }

        try{
            date = new DateTime().toString("yyyy-MM-dd");
        }catch (Exception e){
            throw new RuntimeException("格式错误");
        }
        if(date == null){
            date = CommonUtil.getValueFromReq(req, "date");
        }

        SlaJobEntity targetSla =  slaService.getSlaJobFinishTime(slaId,date);
        if (targetSla == null) {
            throw new RuntimeException("无相关SLA任务");
        }

        JSONObject jsonObj = JSONObject.fromObject(targetSla);
        this.jsonObject = CommonUtil.getPubJson(jsonObj);
        return Action.SUCCESS;
    }

    public String  getBottleNecks() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String date;
        try{
            date = CommonUtil.getValueFromReq(req, "date");
        }catch (Exception e){
            throw new RuntimeException("格式错误");
        }
        if(date == null){
            date = new DateTime().toString("yyyy-MM-dd");
        }
        List<BottleneckTaskEntity> bottleNecks = slaService.generateDiagInfo(date);
        if(bottleNecks.size() == 0){
            throw new RuntimeException("无故障分析数据");
        }
        JSONArray jArray = new JSONArray();
        jArray.addAll(bottleNecks);
        this.jsonObject = CommonUtil.getPubJson(jArray);
        return Action.SUCCESS;
    }

    public String  getTaskStatusDetail() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String date;
        try{
            date = CommonUtil.getValueFromReq(req, "date");
        }catch (Exception e){
            throw new RuntimeException("格式错误");
        }
        if(date == null){
            date = new DateTime().toString("yyyy-MM-dd");
        }
        List<Map<String, Object>> taskHis = slaService.getStatusHis(date);
        if(taskHis.size() == 0){
            throw new RuntimeException("无该task详细信息");
        }
        JSONArray jArray = new JSONArray();
        jArray.addAll(taskHis);
        this.jsonObject = CommonUtil.getPubJson(jArray);
        return Action.SUCCESS;
    }


    public void schRun(){
        String localhost = getLocalhostIp();
        if(!localhost.equalsIgnoreCase(Const.SLA_JOB_HOST)){
            return;
        }
        refreshSlaList();
        log.info("SLA batch update completed, updated jobNum: " +this.updateLists.size()+", inserted JobNum: " +this.insertLists.size());
    }

    public void refreshSlaList(){
        slaLists.clear();
        insertLists.clear();
        updateLists.clear();
        currentLists.clear();
        getMailList();
        getReportList();
        getPageListWithPreTaskId();
        currentLists = slaService.getSlas();
        for(int i=0;i<slaLists.size();i++)
            slaCompare(slaLists.get(i));
        for(int i=0;i<slaLists.size();i++){
            if(!slaLists.get(i).touched){
                insertLists.add(slaLists.get(i));
                log.info("inserting new SLA: " + slaLists.get(i).getSlaName()+", Type:" +slaLists.get(i).getSlaType());
            }
        }
        for(int i=0;i<currentLists.size();i++){
            if((!currentLists.get(i).touched)&&currentLists.get(i).getSlaType()!=0){
                currentLists.get(i).setSlaStatus(0);
                updateLists.add(currentLists.get(i));
                log.info(currentLists.get(i).getSlaId()+" will be set as invalid");
            }
        }
        slaService.insertResult(insertLists);
        slaService.updateResult(updateLists);
    }

    private void slaCompare(SlaEntity targetSla) {

        for(int i=0;i<currentLists.size(); i++){
            if((currentLists.get(i).getJobId()==206)&&(targetSla.getJobId()==206))
                System.out.println("test");
            if(currentLists.get(i).equals(targetSla)){
                currentLists.get(i).refresh(targetSla);
                updateLists.add(currentLists.get(i));
                log.info(currentLists.get(i).getSlaId()+" will be synced");
                break;
            }
        }

    }


    public String  buildSlaLists() {
        refreshSlaList();
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("slaListSize",this.slaLists.size());
        jsonObj.put("updateListSize", this.updateLists.size());
        this.jsonObject = CommonUtil.getPubJson(jsonObj);
        return Action.SUCCESS;
    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    private void getMailList() {
        MailInfoEntity mailInfo = new MailInfoEntity();
        List<MailInfoEntity> infoList = mailInfoService.getMailList(mailInfo);
        for(int i = 0; i<infoList.size(); i++){
            MailInfoEntity jReport = infoList.get(i);
            SlaEntity sla = new SlaEntity();
            sla.setSlaName(jReport.getMailTitle());
            sla.setSlaType(2);
            sla.setJobId(jReport.getMailId());
            sla.setWarnBeginTime(Const.SLA_DEFAULT_WARNBEGINTIME);
            DateTime cTime = new DateTime();
            String tDay = cTime.toString("yyyy-MM-dd");

            if((jReport.getSendTime() + ":00").compareTo(Const.SLA_DEFAULT_WARNTIME) >= 0){
                cTime = formatter.parseDateTime(tDay+" "+jReport.getSendTime());
                cTime = cTime.plusMinutes(30);
                sla.setWarnTime(cTime.toString("HH:mm:ss"));
            }
            else
                sla.setWarnTime(Const.SLA_DEFAULT_WARNTIME);
            sla.setUserName(jReport.getAddUser());
            String userEmail = jReport.getAddUser()+"@dianping.com";
            sla.setUserEmail(userEmail);
            List<Integer> preTaskIds = new ArrayList<Integer>();
            preTaskIds.add(jReport.getTaskId());
            sla.setKeyPreTasks(preTaskIds);
            sla.setWarnType(2);
            sla.setSlaStatus(1);
            slaLists.add(sla);
        }

    }

    private void getReportList() {
        JSONArray jObj = venusReportService.getReportListWithoutDir();
        for(Object oReport:jObj){
            JSONObject jReport = (JSONObject)oReport;
            List<Integer> preTaskIds = new ArrayList<Integer>();
            try{
                for(String sTaskId:mailInfoService.getPreTaskIdsForReports(jReport.getString("reportId"))){
                    preTaskIds.add(Integer.valueOf(sTaskId));
                }
            }catch(Exception e){
                continue;
            }
            SlaEntity sla = new SlaEntity();
            sla.setSlaName(jReport.getString("reportName"));
            sla.setSlaType(1);
            sla.setJobId(jReport.getInt("reportId"));
            sla.setWarnBeginTime(Const.SLA_DEFAULT_WARNBEGINTIME);
            sla.setWarnTime(Const.SLA_DEFAULT_WARNTIME);
            sla.setUserName(jReport.getString("addUser"));
            String userEmail = slaService.getUserEmail(jReport.getString("addUser"));
            if (userEmail != null)
                sla.setUserEmail(userEmail);
            sla.setKeyPreTasks(preTaskIds);
            sla.setWarnType(2);
            sla.setSlaStatus(1);
            slaLists.add(sla);
        }
    }

    public static String getLocalhostIp(){
        InetAddress ip = null;
        boolean bFindIP = false;
        Enumeration<NetworkInterface> netInterfaces;
        try {
            netInterfaces = (Enumeration<NetworkInterface>) NetworkInterface.getNetworkInterfaces();
            while (netInterfaces.hasMoreElements())
            {
                if (bFindIP){
                    break;
                }
                NetworkInterface ni = (NetworkInterface) netInterfaces.nextElement();
                // ----------特定情况，可以考虑用ni.getName判断
                // 遍历所有ip
                Enumeration<InetAddress> ips = ni.getInetAddresses();
                while (ips.hasMoreElements()){
                    ip = (InetAddress) ips.nextElement();
                    if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() // 127.开头的都是lookback地址
                            && ip.getHostAddress().indexOf(":") == -1){
                        bFindIP = true;
                        break;
                    }
                }
            }
            if(ip != null)
                return ip.getHostAddress();
        } catch (SocketException e) {
            log.error("getlocalhostiperror:"+e);
        }
        return "";
    }

    public void getPageListWithPreTaskId(){
        JSONArray jObj = venusReportService.getPageListWithoutDir();
        for(Object oPage:jObj){
            try{
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
            try {
                for(String sTaskId:mailInfoService.getPreTaskIdsForPage(jPage.getString("pageId"),pageDashletConfigs)){
                    preTaskIds.add(Integer.valueOf(sTaskId));
                }
            } catch (InvalidInputException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                continue;
            }
            SlaEntity sla = new SlaEntity();
            sla.setSlaName(jPage.getString("pageName"));
            sla.setSlaType(3);
            sla.setJobId(jPage.getInt("pageId"));
            sla.setWarnBeginTime(Const.SLA_DEFAULT_WARNBEGINTIME);
            sla.setWarnTime(Const.SLA_DEFAULT_WARNTIME);
            sla.setUserName(jPage.getString("addUser"));
            String userEmail = slaService.getUserEmail(jPage.getString("addUser"));
            if (userEmail != null)
                sla.setUserEmail(userEmail);
            sla.setKeyPreTasks(preTaskIds);
            sla.setWarnType(2);
            sla.setSlaStatus(1);
            slaLists.add(sla);
            }catch (Exception e){
                continue;
            }
        }

    }
}
