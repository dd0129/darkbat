package com.dianping.darkbat.service;

import com.dianping.darkbat.action.MailInfoAction;
import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.*;
import com.dianping.darkbat.mapper.SlaMapper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-1-22
 * Time: 上午11:22
 * To change this template use File | Settings | File Templates.
 */
@Scope("singleton")
@Service
public class SlaService {

    @Autowired
    private SlaMapper slaMapper;
    private volatile List<BottleneckTaskEntity> delayInfos = new ArrayList<BottleneckTaskEntity>();
    private volatile HashMap<String,List<BottleneckTaskEntity>> hisDelayInfos = new HashMap<String, List<BottleneckTaskEntity>>();
    private volatile List<Map<String, Object>> taskStatusHis = new ArrayList<Map<String, Object>>();
    private volatile HashMap<String,List<Map<String, Object>>> hisTaskStatusHis = new HashMap<String, List<Map<String, Object>>>();

    private DateTime lastUpdateTime = new DateTime();

    public SlaJobEntity getSlaJobFinishTime(Integer slaId, String date){
        return this.slaMapper.getSlaJobById(slaId, date);
    }

    public List<SlaJobEntity> getSlaJobs(String date){
        return this.slaMapper.getSlaJobs(date);
    }

    public List<SlaEntity> getSlas(){
        return this.slaMapper.getSlas();
    }


    public String getUserEmail(String addUser){
        List<String> userEmail = slaMapper.getUserEmail(addUser);
        if(userEmail != null)
            return userEmail.get(0);
        else
            return null;
    }

    public void insertResult(List<SlaEntity> slaLists) {
        for(int i=0; i < slaLists.size(); i++){
            SlaEntity sla = slaLists.get(i);
            slaMapper.insertSlaDim(sla);
            slaMapper.insertSlaUser(sla);
            for(int j=0; j<sla.getKeyPreTasks().size(); j++)
                slaMapper.insertSlaJob(sla,sla.getKeyPreTasks().get(j));
      }

    }
    public void updateResult(List<SlaEntity> slaLists){
        for(int i=0; i < slaLists.size(); i++){
            SlaEntity sla = slaLists.get(i);
            slaMapper.updateSlaDim(sla);
            slaMapper.deleteSlaJob(sla);
            if(sla.getKeyPreTasks()==null)
                continue;
            for(int j=0; j<sla.getKeyPreTasks().size(); j++)
                slaMapper.insertSlaJob(sla,sla.getKeyPreTasks().get(j));
        }

    }

    public List<BottleneckTaskEntity> generateDiagInfo(String date){
        DateTime now = new DateTime();
        if(!date.equals(now.toString("yyyy-MM-dd"))||this.delayInfos.size() ==0 ||now.getDayOfWeek() != this.lastUpdateTime.getDayOfWeek()||(now.getMinuteOfDay()-this.lastUpdateTime.getMinuteOfDay()>5)){
            findKeytasks(date,now);
        }
        if(date.equals(now.toString("yyyy-MM-dd"))){
            return this.delayInfos;
        }
        else{
            return hisDelayInfos.get(date);
        }

    }

    private synchronized void findKeytasks(String date, DateTime now) {
        if(!date.equals(now.toString("yyyy-MM-dd"))&&this.hisDelayInfos.containsKey(date)&&hisTaskStatusHis.containsKey(date))
            if(this.hisDelayInfos.get(date).size()>0&&this.hisTaskStatusHis.get(date).size()>0)
                return;
        List<BottleneckTaskEntity> delayInfos = slaMapper.getdelayinfo(date);
        List<Map<String, Object>> taskStatusHis = new ArrayList<Map<String, Object>>();
        List<Integer> keyTaskIds  = new ArrayList<Integer>();
        for(BottleneckTaskEntity bottleNeck:delayInfos){
            keyTaskIds.add(bottleNeck.getKeyTaskId());
        }
        taskStatusHis =  getTaskStatusHis(date,keyTaskIds);
        if(date.equals(now.toString("yyyy-MM-dd"))){
            this.delayInfos = delayInfos;
            this.lastUpdateTime = new DateTime();
            this.taskStatusHis = taskStatusHis;
        }
        else{
            if(date.compareTo(now.toString(("yyyy-MM-dd")))>0)
                return;
            this.hisDelayInfos.put(date, delayInfos);
            this.hisTaskStatusHis.put(date,taskStatusHis);
        }
    }
    private List<Map<String, Object>> getTaskStatusHis(String date, List<Integer> task_id) {
        return slaMapper.getJobRunHis(date,task_id);
    }
    public List<Map<String, Object>> getStatusHis(String date) {
        DateTime now = new DateTime();
        if(date.equals(now.toString("yyyy-MM-dd"))){
            return this.taskStatusHis;
        }
        else{
            return hisTaskStatusHis.get(date);
        }
    }


}
