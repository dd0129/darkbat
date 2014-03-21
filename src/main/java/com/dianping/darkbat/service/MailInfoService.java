package com.dianping.darkbat.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.MailDetailEntity;
import com.dianping.darkbat.entity.MailInfoEntity;
import com.dianping.darkbat.entity.TaskEntity;
import com.dianping.darkbat.entity.TaskRelaEntity;
import com.dianping.darkbat.exception.InvalidInputException;
import com.dianping.darkbat.mapper.GPMapper;
import com.dianping.darkbat.mapper.MailInfoMapper;
import com.dianping.darkbat.mapper.REPORTMapper;

@Scope("prototype")
@Service
public class MailInfoService {

    @Autowired
    private TaskService taskService;
    
    @Autowired
    private HalleyService halleyService;
    
    @Autowired
    private MasterdataService masterdataService;

    @Autowired
    private MailInfoMapper mailInfoMapper;
    
    @Autowired
    private GPMapper gpMapper;
    
    @Autowired
    private REPORTMapper reportMapper;

    @Autowired
    private VenusReportService venusReportService;

    public List<MailInfoEntity> getMailList(MailInfoEntity mailInfo) {
        return mailInfoMapper.getMailList(mailInfo);
    }
    
    public List<MailInfoEntity> getMailListContainsReport(Integer reportId) {
        return mailInfoMapper.getMailListContainsReport(reportId);
    }
    
    public MailInfoEntity addMailInfo(MailInfoEntity mailInfo, List<MailDetailEntity> mailDetails) throws InvalidInputException {
        Integer rows = mailInfoMapper.insertMailInfo(mailInfo);
        Integer mailId = mailInfo.getMailId();
        if (!rows.equals(1) ||  mailId == null) {
            throw new InvalidInputException("邮件配置信息新增失败！");
        }

        Integer taskId = addOrUpdateMailJob(mailInfo, mailDetails);
        mailInfo.setTaskId(taskId);
        mailInfoMapper.updateTaskId(mailId, taskId);
        
        MailDetailEntity info = new MailDetailEntity();
        info.setMailId(mailInfo.getMailId());
        mailInfoMapper.delMailDetail(info);
        
        for(MailDetailEntity mailDetail:mailDetails){
            mailDetail.setMailId(mailInfo.getMailId());
//            mailInfoMapper.addMailDetail(mailDetail);
            addOrUpdateMailDetail(mailDetail);
        }
        return mailInfo;
    }
    
    public void addOrUpdateMailDetail(MailDetailEntity mailDetail) {
        MailDetailEntity checkDetail = new MailDetailEntity();
            
        checkDetail.setMailId(mailDetail.getMailId());
        checkDetail.setReportId(mailDetail.getReportId());
        checkDetail.setItemType(mailDetail.getItemType());
            
        if (mailInfoMapper.getMailDetail(checkDetail).isEmpty()) {
            mailInfoMapper.addMailDetail(mailDetail);
        } else {
            mailInfoMapper.setMailDetail(mailDetail);
        }
    }
    
    public Integer deleteMailInfo(MailInfoEntity mailInfo) {
        int ret = taskService.invalidateTask(mailInfo.getTaskId());
        if (ret == -1) {
            throw new RuntimeException("有后继任务，无法使邮件任务失效");
        }

        ret = mailInfoMapper.deleteMailInfo(mailInfo.getMailId());
        if (ret == 0) {
            throw new RuntimeException("邮件配置信息删除失败！");
        }
        
        MailDetailEntity info = new MailDetailEntity();
        info.setMailId(mailInfo.getMailId());
        mailInfoMapper.delMailDetail(info);
        
        return ret;
    }
    
    public MailInfoEntity getExistedMailInfo(MailInfoEntity mailInfo) throws InvalidInputException {
        if(mailInfo.getMailTitle() == null) {
            throw new InvalidInputException("请输入邮件标题");
        }
        
        return mailInfoMapper.getExistedMailInfo(mailInfo.getMailTitle(), mailInfo.getMailId());
    }
    
    public Integer updateMailInfo(MailInfoEntity mailInfo, List<MailDetailEntity> mailDetails) throws InvalidInputException {
        if(mailInfo.getMailId() == null) {
            throw new InvalidInputException("邮件修改必须包含ID信息");
        }
        Integer ret = mailInfoMapper.updateMailInfo(mailInfo);
        if(ret == 0) {
            throw new RuntimeException("邮件配置信息修改失败");
        }
        
        addOrUpdateMailJob(mailInfo, mailDetails);
        
        MailDetailEntity info = new MailDetailEntity();
        info.setMailId(mailInfo.getMailId());
        mailInfoMapper.delMailDetail(info);
        for(MailDetailEntity mailDetail:mailDetails){
            mailDetail.setMailId(mailInfo.getMailId());
//            mailInfoMapper.addMailDeta
            addOrUpdateMailDetail(mailDetail);
        }
        return ret;
    }
    
    public MailInfoEntity getMailInfoByName(MailInfoEntity mailInfo) {
        if(mailInfo.getMailTitle().equals("") || mailInfo.getMailTitle() == null) {
            return null;
        } else {
            return mailInfoMapper.getMailByName(mailInfo);
        }
    }
    
    public void updatePreTasksForPage(MailInfoEntity mailInfo, List<MailDetailEntity> mailDetails) throws InvalidInputException {
        updatePreTasks(mailInfo, mailDetails);
    }
    
    public void updatePreTasksForReport(MailInfoEntity mailInfo) throws InvalidInputException {
        updatePreTasks(mailInfo, null);
    }
    
    private void updatePreTasks(MailInfoEntity mailInfo, List<MailDetailEntity> mailDetails) throws InvalidInputException {
        Integer taskId = mailInfo.getTaskId();
        List<TaskRelaEntity> taskRelaEntitys = generateTaskRelaEntity(mailInfo, mailDetails);
        taskService.updateTaskRela(taskId, taskRelaEntitys);
    }

    // 新增/更新邮件配置任务
    private Integer addOrUpdateMailJob(MailInfoEntity mailInfo, List<MailDetailEntity> mailDetails) throws InvalidInputException {
        TaskEntity taskEntity = generateTaskEntity(mailInfo);
        List<TaskRelaEntity> taskRelaEntitys = generateTaskRelaEntity(mailInfo, mailDetails);
        Integer taskId = taskEntity.getTaskId();
        
        if (CommonUtil.isEmpty(taskId)) {
            taskService.insertTaskAndTaskRela(taskEntity, taskRelaEntitys);
        } else {
            taskService.updateTaskAndTaskRela(taskEntity, taskRelaEntitys);
        }
        
        return taskEntity.getTaskId();
    }

    // prerun atom tasks and then send test email
    public int sendTestMail(Integer taskId, String recipient) {
        TaskEntity taskEntity = taskService.getTaskById(taskId);
        List<TaskRelaEntity> taskRelaEntitys = taskService.getTaskRelaByTaskId(taskId);
        
        Calendar calendar = Calendar.getInstance();
        Date today = new Date();
        calendar.setTime(today);
        calendar.add(Calendar.DATE, 1);
        Date tomorrow = calendar.getTime();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String begin = formatter.format(today);
        String end = formatter.format(tomorrow);

        List<Integer> preTaskIds = new ArrayList<Integer>();
        for (TaskRelaEntity taskRelaEntity : taskRelaEntitys) {
            preTaskIds.add(taskRelaEntity.getTaskPreId());
        }
        
        int ret;
        taskEntity.setPara3("-c -t -u " + recipient);
        taskService.updateTask(taskEntity);
        
        if(taskService.isAllRunSuccessfully(preTaskIds, begin)){
            ret = halleyService.prerunJobWithHignPriority(taskId, begin, end).intValue();
        }else{
            ret = halleyService.prerunChildCascdeJob(taskEntity.getTaskId(), preTaskIds, begin, end);
        }
        
        taskEntity.setPara3("");
        taskService.updateTask(taskEntity);
        
        return ret;
    }
    
    public Integer resendMail(Integer taskId, String begin, String end){
        TaskEntity taskEntity = taskService.getTaskById(taskId);
        taskEntity.setPara3("-c");
        taskService.updateTask(taskEntity);

//        Integer res = halleyService.prerunJobWithHignPriority(taskId,begin,end);
        
        // 直接重跑 by chaos
        List<Integer> taskList = new ArrayList<Integer>();
        taskList.add(taskId);
        Integer res = halleyService.prerunJob(taskList, begin, end);
        
        taskEntity.setPara3("");
        taskService.updateTask(taskEntity);
        return res;
    }

    private TaskEntity generateTaskEntity(MailInfoEntity mailInfo) {
        String taskName = "Mail#" + mailInfo.getMailTitle();
        String tableName = "mail." + mailInfo.getMailId();
        String cycle = mailInfo.getSendCycle();
        String dataSource = "gp57";
        Integer taskId = mailInfo.getTaskId();

        TaskEntity taskEntity = new TaskEntity();

        taskEntity.setTaskId(taskId);
        taskEntity.setTaskName(taskName);
        taskEntity.setTableName(tableName);
        taskEntity.setRemark("");
        taskEntity.setDatabaseSrc(dataSource);

        taskEntity.setTaskObj("sh ${calculate_home}/bin/start_calculate.sh");

        //TODO  修改邮件脚本路径
        String param1 = "/data/deploy/dwarch/bin/atom/mail/main.py";
        String param2 = "-d ${cal_dt} -m " + mailInfo.getMailId();
        String param3 = "";

        taskEntity.setPara1(param1);
        taskEntity.setPara2(param2);
        taskEntity.setPara3(param3);

        taskEntity.setLogHome(Const.LOG_HOME_CALCULATE);
        taskEntity.setLogFile(tableName);

        taskEntity.setTaskGroupId(5);
        taskEntity.setCycle(cycle);
        taskEntity.setPrioLvl(3);
        taskEntity.setIfRecall(1);
        taskEntity.setIfWait(0);
        taskEntity.setIfPre(1);
        taskEntity.setIfVal(1);

        taskEntity.setAddUser(mailInfo.getAddUser());
        taskEntity.setUpdateUser(mailInfo.getUpdateUser());
        taskEntity.setType(2);
        taskEntity.setOffset(cycle + "0");
        taskEntity.setOffsetType("offset");

        String freq = "0 0 9";  // 09:00:00
        String[] sendTime = mailInfo.getSendTime().split(":");
        int hour = Integer.parseInt(sendTime[0]);
        int minute = Integer.parseInt(sendTime[1]);
        if (hour < 24 && minute < 60) {
            freq = "0 " +  minute + " " + hour;
        }
        
        if (cycle.equals("D")) {
            freq += " * * ?";
        } else if (cycle.equals("W")) {
            freq += " ? * MON";
        } else {
            freq += " 1 * ?";
        }
        taskEntity.setFreq(freq);

        taskEntity.setOwner(mailInfo.getAddUser());
        taskEntity.setWaitCode("");
        taskEntity.setRecallCode("1");
        taskEntity.setTimeout(90);
        taskEntity.setRecallInterval(10);
        taskEntity.setRecallLimit(2);
        taskEntity.setSuccessCode("0");

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currTime = formatter.format(new Date());
        taskEntity.setAddTime(currTime);
        taskEntity.setUpdateTime(currTime);

        return taskEntity;
    }

    
    /**
     * 为邮件生成前驱任务
     * @param mailInfo
     * @param mailDetails 如果是报表型邮件，可以忽略之
     * @return
     * @throws InvalidInputException
     */
    private List<TaskRelaEntity> generateTaskRelaEntity(MailInfoEntity mailInfo, List<MailDetailEntity> mailDetails) throws InvalidInputException {
        List<String> preTaskIds = null;
        
        if (Const.MAIL_ITEM_PAGE.equals(mailInfo.getItemType())) {
            preTaskIds = this.getPreTaskIdsForPage(mailInfo.getItemIdList(), mailDetails);
        } else if (Const.MAIL_ITEM_REPORT.equals(mailInfo.getItemType())) {
            preTaskIds = this.getPreTaskIdsForReports(mailInfo.getItemIdList());
        }
        
        List<TaskRelaEntity> taskRelaEntitys = new LinkedList<TaskRelaEntity>();
        String cycleGap = mailInfo.getSendCycle() + "0";

        for (String preTaskId : preTaskIds) {
            if(isSkipTask(preTaskId, mailInfo.getSendCycle()))
                continue;
            TaskRelaEntity taskRelaEntity = new TaskRelaEntity();
            taskRelaEntity.setTaskPreId(Integer.parseInt(preTaskId));
            taskRelaEntity.setCycleGap(cycleGap);
            taskRelaEntity.setRemark("");
            taskRelaEntitys.add(taskRelaEntity);
        }

        return taskRelaEntitys;
    }
    
    private boolean isSkipTask(String preTaskId,String mailTaskCycle){
        //TODO hack code for etl not support day_task dependce week_task
        boolean res = false;
        try{
            String preTaskCycle = taskService.getTaskById(Integer.parseInt(preTaskId)).getCycle();
            @SuppressWarnings("serial")
            Map<String,Integer> cycles = new HashMap<String,Integer>(){{
                put("MI",0);
                put("H",1);
                put("D",2);
                put("W",3);
                put("M",4);
                put("S",5);
                put("Y",6);
            }};
            if(cycles.get(preTaskCycle.toUpperCase()) > cycles.get(mailTaskCycle.toUpperCase()))
                res = true;
        }catch(Exception e){
            res = false;
        }
        return res;
    }

    // 从sql中抽取atom id
    private List<String> getAtomIds(String sqlStr) {
        Set<String> atomIdSet = new HashSet<String>();
        final String atomIdStr = "atom_id = ";

        Pattern pattern = Pattern.compile(atomIdStr + "\\d+");
        Matcher matcher = pattern.matcher(sqlStr);
        
        while (matcher.find()) {
            String atomId = matcher.group(0).substring(atomIdStr.length());
            atomIdSet.add(atomId);
        }

        List<String> atomIdList = new ArrayList<String>(atomIdSet);

        return atomIdList;
    }
    
    private String getParamDefaultValue(String sqlStr,JSONArray paramList){
        String sqlRep = sqlStr;
        for(Object o:paramList){
            JSONObject param = (JSONObject)o;
            String paramType = param.getJSONObject("vnParam").getString("paramType");
            String paramSubtype = param.getJSONObject("vnParam").getString("paramSubtype");
            Integer paramId = param.getJSONObject("vnParam").getInt("paramId");
            String paramValue = null;
            if(paramType.equalsIgnoreCase("CASCADE")){
                paramType = paramSubtype;
            }
            if(paramType.equalsIgnoreCase("CALENDAR") || paramType.equalsIgnoreCase("DATE") || paramType.equalsIgnoreCase("MONTH") || paramType.equalsIgnoreCase("YEAR")){
                paramValue = "3333-01-01 00:00:00";
            }
            if(paramValue == null){
                paramValue = "-9999";
            }
            sqlRep = sqlRep.replaceAll("<@P:"+paramId+".*?>", paramValue);
        }
        return sqlRep.trim();
    }
    
    /**
     * 将嵌套的json数组元素打平
     * @param jArray
     * @param jObjList
     * @return
     */
    public List<JSONObject> getFlatObj(JSONArray jArray, List<JSONObject> jObjList) {
        for (Object obj : jArray) {
            if (obj instanceof JSONArray) {
                JSONArray jArray2 = (JSONArray) obj;
                getFlatObj(jArray2, jObjList);
            } else if (obj instanceof JSONObject) {
                JSONObject jObj = (JSONObject) obj;
                jObjList.add(jObj);
            }
        }
        
        return jObjList;
    }
    
    private Set<String> getTableSchema(String sqlStr,Set<String> tableNames){
        Set<String> tableNamesRes = new HashSet<String>();
        for(String tableName:tableNames){
            Pattern p = Pattern.compile("(\\w+?\\."+tableName+")");
            Matcher m = p.matcher(sqlStr);
            if(m.find()){
                tableNamesRes.add(m.group(1));
            }
            else if(sqlStr.indexOf(tableName) >= 0){
                tableNamesRes.add(tableName);
            }
        }
        return tableNamesRes;
    }
    
    //从sql中抽取tablename
    private List<String> getTableName(String sqlStr,JSONArray paramList,String datasourceId){
        Set<String> tableNames = new HashSet<String>();
        //处理sql中的参数
        String sqlRep = getParamDefaultValue(sqlStr,paramList);
        //explain sql
        List<String> explainRes = null;
        if(datasourceId.equalsIgnoreCase("3")){
            explainRes = reportMapper.explainSql(sqlRep);
        }
        else{
            explainRes = gpMapper.explainSql(sqlRep);
        }
        
        //解析exec plan
        Pattern p = Pattern.compile("Scan\\s+on\\s+([^\\s$\\(]+)\\s+([^\\s$\\(]+)?");
        for(String eachStr:explainRes){
            Matcher m = p.matcher(eachStr);
            if(m.find()){
                if(StringUtils.isNotEmpty(m.group(2))){
                    tableNames.add(m.group(2).trim());
                }
                if(StringUtils.isNotEmpty(m.group(1))){
                    tableNames.add(m.group(1).trim());
                }
            }
        }
        //获取可能带有schema的表
        tableNames = getTableSchema(sqlStr,tableNames);
        return new ArrayList<String>(tableNames);
    }

    /**
     * 从page信息中解析出chart与table ID信息
     * @param details
     * @return
     */
    private Map<String, List<String>> parseMailDetailForPage(List<MailDetailEntity> details) {
        List<String> chartIds = new ArrayList<String>();
        List<String> rptIds = new ArrayList<String>();
        
        Map<String, List<String>> itemIdsMap = new HashMap<String, List<String>>();
        
        for (MailDetailEntity info : details) {
            if (Const.MAIL_ITEM_TABLE.equals(info.getItemType().toUpperCase())) {
                rptIds.add(info.getReportId().toString());
            } else if (Const.MAIL_ITEM_CHART.equals(info.getItemType().toUpperCase())) {
                chartIds.add(info.getReportId().toString());
            }
        }
        
        itemIdsMap.put(Const.MAIL_ITEM_CHART, chartIds);
        itemIdsMap.put(Const.MAIL_ITEM_TABLE, rptIds);
        return itemIdsMap;
     }
    
    /**
     * 为报表获取前驱任务
     * @param reportIds
     * @param atomIds
     * @param tableNames
     * @throws InvalidInputException
     */
    private void getSqlsForReport(List<String> reportIds,Set<String> atomIds, Map<String,Set<String>> tableNames) throws InvalidInputException { 
        String itemType = Const.MAIL_ITEM_REPORT;
        
        getSqls(itemType, reportIds, atomIds, tableNames);
    }
    
    private void getSqls(String itemType, List<String> itemIds,Set<String> atomIds,Map<String,Set<String>> tableNames) throws InvalidInputException {        
        boolean isCharts = Const.MAIL_ITEM_CHART.equals(itemType);
        
        for (String itemId : itemIds) {
            if (!CommonUtil.isEmpty(itemId)) {
                JSONObject jsonObj = null;
                if (isCharts) {
                    jsonObj = venusReportService.getChartInfo(itemId);
                } else {
                    jsonObj  = venusReportService.getReportInfo(itemId);
                }
                String sql = jsonObj.getJSONObject("vnQuery").getString("querySql");
                if (!isCharts) {
                    if(jsonObj.getInt("reportType") == 1) {
                        List<String> tablename = getTableName(sql,jsonObj.getJSONObject("vnQuery").getJSONArray("vnXQueryParamList"),jsonObj.getJSONObject("vnQuery").getJSONObject("vnDatasource").getString("datasourceId"));
                        if(tablename.size() > 0){
                            if(!tableNames.containsKey(jsonObj.getJSONObject("vnQuery").getJSONObject("vnDatasource").getString("datasourceId"))){
                                tableNames.put(jsonObj.getJSONObject("vnQuery").getJSONObject("vnDatasource").getString("datasourceId"), new HashSet<String>());
                            }
                            tableNames.get(jsonObj.getJSONObject("vnQuery").getJSONObject("vnDatasource").getString("datasourceId")).addAll(tablename);
                        }                    
                    } else if (jsonObj.getInt("reportType") == 2) {
                        atomIds.addAll(getAtomIds(sql));
                    }
                }
                else if (isCharts) {
                    //chart默认都是从指标出，默认都是指标报表
                    atomIds.addAll(getAtomIds(sql));
                }
            }
        }
    }

    /**
     * 为报表型邮件生成任务前驱
     * @param reportIdStr
     * @return
     * @throws InvalidInputException
     */
    public List<String> getPreTaskIdsForReports(String reportIdStr) throws InvalidInputException {        
        List<String> reportIds = Arrays.asList(reportIdStr.split(","));
        Set<String> atomIds = new HashSet<String>();
        Map<String,Set<String>> tableNames = new HashMap<String,Set<String>>();
        getSqls(Const.MAIL_ITEM_REPORT, reportIds,atomIds,tableNames);
        
        List<String> preTaskIds = new ArrayList<String>();
        if(!atomIds.isEmpty())
            preTaskIds.addAll(masterdataService.getTaskIds(atomIds));
        if(!tableNames.isEmpty())
            preTaskIds.addAll(masterdataService.getTaskIdsByTableName(tableNames));
        return preTaskIds;
    }
    
    /**
     * 为Page型邮件生成任务前驱
     * @param pageId
     * @param mailDetails
     * @return
     * @throws InvalidInputException
     */
    public List<String> getPreTaskIdsForPage(String pageId, List<MailDetailEntity> mailDetails) throws InvalidInputException {
        Set<String> atomIds = new HashSet<String>();
        Map<String,Set<String>> tableNames = new HashMap<String,Set<String>>();
        Map<String, List<String>> itemIds = this.parseMailDetailForPage(mailDetails);
        
        List<String> chartIds = itemIds.get(Const.MAIL_ITEM_CHART);
        List<String> rptIds = itemIds.get(Const.MAIL_ITEM_TABLE);
        this.getSqls(Const.MAIL_ITEM_CHART, chartIds, atomIds, tableNames);
        this.getSqls(Const.MAIL_ITEM_TABLE, rptIds, atomIds, tableNames);
        
        List<String> preTaskIds = new ArrayList<String>();
        if(!atomIds.isEmpty())
            preTaskIds.addAll(masterdataService.getTaskIds(atomIds));
        if(!tableNames.isEmpty())
            preTaskIds.addAll(masterdataService.getTaskIdsByTableName(tableNames));
        return preTaskIds;
    }
    
    public List<MailDetailEntity> getMailDetail(MailDetailEntity info){
        return mailInfoMapper.getMailDetail(info);
    }
    
    public Integer setMailDetail(MailDetailEntity info){
        return mailInfoMapper.setMailDetail(info);
    }
    
    public Integer addMailDetail(MailDetailEntity info){
        return mailInfoMapper.addMailDetail(info);
    }
    
    public Integer delMailDetail(@Param("info") MailDetailEntity info){
        if(info.getMailId() == null && info.getReportId() == null && info.getDataCycle() == null)
            return 0;
        return mailInfoMapper.delMailDetail(info);
    }
    
    public Integer addOrUpdateBDCacheJob(long reportId) throws InvalidInputException {
        JSONObject jsonObj = venusReportService.getReportInfo(Long.toString(reportId));
        TaskEntity taskEntity = generateTaskEntity(jsonObj);
        List<TaskRelaEntity> taskRelaEntitys = generateTaskRelaEntity(reportId);
        Integer taskId = taskEntity.getTaskId();
        if (CommonUtil.isEmpty(taskId)) {
            taskService.insertTaskAndTaskRela(taskEntity, taskRelaEntitys);
        } else {
            taskService.updateTaskAndTaskRela(taskEntity, taskRelaEntitys);
        }
        return taskEntity.getTaskId();
    }
    
    public Integer delBDCacheJob(long reportId) throws InvalidInputException {
        JSONObject jsonObj = venusReportService.getReportInfo(Long.toString(reportId));
        if(jsonObj.get("taskId") == null || jsonObj.get("taskId").equals(JSONNull.getInstance()))
            return 1;
        int ret = taskService.invalidateTask(jsonObj.getInt("taskId"));
        if (ret == -1) {
            throw new RuntimeException("有后继任务，无法使缓存预跑任务失效");
        }
        if (ret == 0) {
            throw new RuntimeException("无法使缓存预跑任务失效");
        }
        return ret;
    }
    
    private TaskEntity generateTaskEntity(JSONObject reportInfo) {
        TaskEntity taskEntity = new TaskEntity();
        if(reportInfo.get("taskId") != null && !reportInfo.get("taskId").equals(JSONNull.getInstance()))
            taskEntity.setTaskId(reportInfo.getInt("taskId"));
        taskEntity.setTaskName("BDCache#" + reportInfo.getString("reportName"));
        taskEntity.setTableName("BDCache." + reportInfo.getInt("reportId"));
        taskEntity.setRemark("");
        taskEntity.setDatabaseSrc("gp57");

        taskEntity.setTaskObj("sh ${calculate_home}/bin/start_calculate.sh");

        String param1 = "/data/deploy/dwarch/bin/report/report_prehandle.py";
        String param2 = "-r " + reportInfo.getString("reportId");
        String param3 = "";

        taskEntity.setPara1(param1);
        taskEntity.setPara2(param2);
        taskEntity.setPara3(param3);

        taskEntity.setLogHome(Const.LOG_HOME_CALCULATE);
        taskEntity.setLogFile("BDCache." + reportInfo.getString("reportId"));

        taskEntity.setTaskGroupId(5);
        taskEntity.setCycle("D");
        taskEntity.setPrioLvl(3);
        taskEntity.setIfRecall(1);
        taskEntity.setIfWait(0);
        taskEntity.setIfPre(1);
        taskEntity.setIfVal(1);

//        taskEntity.setAddUser(reportInfo.getString("addUser"));
        taskEntity.setAddUser("xiaomeng.chen");
//        taskEntity.setUpdateUser(reportInfo.getString("updateUser"));
        taskEntity.setType(2);
        taskEntity.setOffset("D0");
        taskEntity.setOffsetType("offset");
        taskEntity.setFreq("0 0 6 * * ?");
//        taskEntity.setOwner(reportInfo.getString("reportOwner"));
        taskEntity.setOwner("xiaomeng.chen");
        taskEntity.setWaitCode("");
        taskEntity.setRecallCode("1");
        taskEntity.setTimeout(90);
        taskEntity.setRecallInterval(10);
        taskEntity.setRecallLimit(2);
        taskEntity.setSuccessCode("0");

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currTime = formatter.format(new Date());
        taskEntity.setAddTime(currTime);
        taskEntity.setUpdateTime(currTime);

        return taskEntity;
    }

    private List<TaskRelaEntity> generateTaskRelaEntity(long reportId) throws InvalidInputException {
        List<String> reportIds = new ArrayList<String>();
        reportIds.add(Long.toString(reportId));
        Set<String> atomIds = new HashSet<String>();
        Map<String,Set<String>> tableNames = new HashMap<String,Set<String>>();
        getSqlsForReport(reportIds,atomIds,tableNames);
        List<String> preTaskIds = new ArrayList<String>();
        if(!atomIds.isEmpty())
            preTaskIds.addAll(masterdataService.getTaskIds(atomIds));
        if(!tableNames.isEmpty())
            preTaskIds.addAll(masterdataService.getTaskIdsByTableName(tableNames));

        List<TaskRelaEntity> taskRelaEntitys = new LinkedList<TaskRelaEntity>();
        String cycleGap = "D0";
        for (String preTaskId : preTaskIds) {
            TaskRelaEntity taskRelaEntity = new TaskRelaEntity();
            taskRelaEntity.setTaskPreId(Integer.parseInt(preTaskId));
            taskRelaEntity.setCycleGap(cycleGap);
            taskRelaEntity.setRemark("");
            taskRelaEntitys.add(taskRelaEntity);
        }
        return taskRelaEntitys;
    }
}
