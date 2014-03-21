package com.dianping.darkbat.service;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.lib.StringUtil;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.HalleyUtil;
import com.dianping.darkbat.entity.TaskEntity;
import com.dianping.darkbat.entity.TaskRelaEntity;
import com.dianping.darkbat.entity.TaskRelaStatusEntity;
import com.dianping.darkbat.entity.TaskStatusEntity;
import com.dianping.darkbat.exception.BaseRuntimeException;
import com.dianping.darkbat.mapper.TaskMapper;
import com.dianping.darkbat.mapper.TaskRelaMapper;
import com.dianping.darkbat.mapper.TaskStatusMapper;

@Scope("prototype")
@Repository
public class HalleyService {

    @Autowired
    private TaskStatusMapper taskStatusMapper;
    @Autowired
    private TaskMapper taskMapper;
    @Autowired
    private TaskRelaMapper taskRelaMapper;

    private static Log log = LogFactory.getLog(HalleyService.class);

    private static final String[] forceParameters = new String[]{"taskName", "para1", "owner"};

    public Integer batchstopPrerunJob(Integer taskId) {
        return taskStatusMapper.updateTaskStatus(taskId);
    }

    private List<TaskStatusEntity> statusList = new ArrayList<TaskStatusEntity>();
    private List<TaskRelaStatusEntity> statusRelaList = new ArrayList<TaskRelaStatusEntity>();

    private Map<Integer, TaskEntity> taskMap = new HashMap<Integer, TaskEntity>();
    private List<TaskRelaEntity> taskRelaList = new ArrayList<TaskRelaEntity>();

    private List<TaskEntity> nodeList = new ArrayList<TaskEntity>();
    private List<TaskRelaEntity> nodeRelaList = new ArrayList<TaskRelaEntity>();

    /**
     * ******
     *
     * @param taskList
     * @param begin
     * @param end
     * @return
     * @throws Exception
     */
    public Integer prerunJob(List<Integer> taskList, String begin, String end) {
        Date startDate = null;
        Date endDate = null;

        if (taskList.size() == 0 || begin == null || end == null) {
            throw new NullPointerException("parameter(taskList,begin,end) is not null");
        }
        try {
            startDate = HalleyUtil.string2Date(begin);
            endDate = HalleyUtil.string2Date(end);
        } catch (ParseException e) {
            throw new BaseRuntimeException("date format is illegal");
        }

        if (startDate.after(endDate)) {
            throw new BaseRuntimeException("startDate must less than endDate!");
        }
        Date triggerDate = new Date();
        if (startDate.after(triggerDate)) {
            throw new BaseRuntimeException("startDate must less than today!");
        }

        endDate = endDate.after(triggerDate) ? triggerDate : endDate;


        for (Integer taskId : taskList) {
            Date initDate = startDate;
            TaskEntity entity = taskMapper.getTaskById(taskId);
            String freq = entity.getFreq();
            CronExpression ce = null;
            try {
                ce = new CronExpression(freq);
            } catch (ParseException e) {
                throw new BaseRuntimeException("freq is illegal for cronExpression");
            }

            while (true) {
                initDate = ce.getNextValidTimeAfter(initDate);
                if (initDate.after(endDate)) {
                    break;
                }
                TaskStatusEntity statusEntity = this.getInitInstance(entity, initDate, triggerDate);
                taskStatusMapper.insertTaskStatusEntity(statusEntity);
            }
        }
        return 0;
    }

    /**
     * 高优先级预跑任务
     * @param taskId
     * @param begin   begin =< 预跑时间
     * @param end     预跑时间 < end
     * @return
     */
    public Integer prerunJobWithHignPriority(Integer taskId, String begin, String end) {
        List<Integer> boundaryIdList = new ArrayList<Integer>();
        
        return prerunChildCascdeJob(taskId, boundaryIdList, begin, end);
    }
    
    public Integer prerunChildCascdeJob(Integer childId, List<Integer> boundaryIdList, String begin, String end) {
        Date startDate = null;
        Date endDate = null;

        if (begin == null || end == null) {
            throw new NullPointerException("parameter(begin, end) is not null");
        }
        try {
            startDate = HalleyUtil.string2Date(begin);
            endDate = HalleyUtil.string2Date(end);
        } catch (ParseException e) {
            throw new BaseRuntimeException("date format is illegal");
        }

        if (startDate.after(endDate)) {
            throw new BaseRuntimeException("startDate must less than endDate!");
        }
        Date triggerDate = new Date();
        if (startDate.after(triggerDate)) {
            throw new BaseRuntimeException("startDate must less than today!");
        }

        endDate = endDate.after(triggerDate) ? triggerDate : endDate;

        Date initDate = startDate;
        CronExpression ce = null;

        TaskEntity entity = taskMapper.getTaskById(childId);
        try {
            ce = new CronExpression(entity.getFreq());
        } catch (ParseException e) {
            throw new BaseRuntimeException("freq is illegal for cronExpression");
        }

        if (null == boundaryIdList || boundaryIdList.isEmpty()) {
            while (true) {
                initDate = ce.getNextValidTimeAfter(initDate);
                if (initDate.after(endDate)) {
                    break;
                }
                TaskStatusEntity statusEntity = this.makeImportantInstance(entity, initDate, triggerDate);
                this.statusList.add(statusEntity);
            }
            if (!this.statusList.isEmpty()) {
                this.taskStatusMapper.insertTaskStatusEntityList(this.statusList);
                return 0;
            }
        } else {
            List<TaskEntity> taskList = this.taskMapper.getTask();
            List<TaskRelaEntity> taskRelaList = this.taskRelaMapper.getTaskRela();
            
            this.findTreesFromChildNode(childId, taskList, taskRelaList);
            for (Integer taskId : boundaryIdList) {
                this.findBoundaryTree(taskId);
            }
            
            while (true) {
                initDate = ce.getNextValidTimeAfter(initDate);
                if (initDate.after(endDate)) {
                    break;
                }
                this.makeTreeStatus(initDate, triggerDate);
            }
            
            if (!this.statusList.isEmpty() & !this.statusRelaList.isEmpty()) {
                this.taskStatusMapper.insertTaskStatusEntityList(this.statusList);
                this.taskStatusMapper.insertTaskRelaStatusEntityList(this.statusRelaList);
                return 0;
            }
        }
        
        return -1;
    }

    private void findTreesFromChildNode(Integer taskId, List<TaskEntity> taskList, List<TaskRelaEntity> relaList) {
        for (TaskEntity entity : taskList) {
            if (String.valueOf(entity.getTaskId()).equals(String.valueOf(taskId))) {
                for (TaskEntity entity1 : this.taskMap.values()) {
                    if (String.valueOf(entity1.getTaskId()).equals(String.valueOf(taskId))) {
                        return;
                    }
                }
                this.taskMap.put(taskId, entity);
                for (TaskRelaEntity relaEntity : relaList) {
                    if (String.valueOf(relaEntity.getTaskId()).equals(String.valueOf(taskId))) {
                        this.taskRelaList.add(relaEntity);
                        this.findTreesFromChildNode(relaEntity.getTaskPreId(), taskList, relaList);
                    }
                }
            }
        }
    }

    private void findBoundaryTree(Integer taskId) {
        for (TaskEntity entity : this.nodeList) {
            if (String.valueOf(entity.getTaskId()).equals(String.valueOf(taskId))) {
                return;
            }
        }
        this.nodeList.add(this.taskMap.get(taskId));
        for (TaskRelaEntity relaEntity : this.taskRelaList) {
            if (String.valueOf(relaEntity.getTaskPreId()).equals(String.valueOf(taskId))) {
                this.nodeRelaList.add(relaEntity);
                this.findBoundaryTree(relaEntity.getTaskId());
            }
        }
    }

    public Integer temporaryRunJob(Map<String, String> paras) {
        //force parameter valid
        for (String para : forceParameters) {
            if (paras.get(para) == null) {
                throw new NullPointerException(para + " is null");
            }
        }
        TaskStatusEntity statusEntity = this.getTemporaryInstance(paras);
        taskStatusMapper.insertTaskStatusEntity(statusEntity);
        return 0;
    }

    private void makeTreeStatus(Date initDate, Date triggerDate) {
        for (TaskEntity entity1 : this.nodeList) {
            TaskStatusEntity statusEntity = this.makeImportantInstance(entity1, initDate, triggerDate);
            this.statusList.add(statusEntity);
        }
        for (TaskRelaEntity relaEntity : nodeRelaList) {
            TaskRelaStatusEntity relaStatusEntity = new TaskRelaStatusEntity();
            relaStatusEntity.setTaskId(relaEntity.getTaskId());

            TaskEntity task = this.taskMap.get(relaEntity.getTaskId());
            String taskInstanceId = "imp_" + HalleyUtil.generateInstanceId(String.valueOf(relaEntity.getTaskId()), String.valueOf(task.getCycle()), initDate) + "_" + HalleyUtil.Date2String(triggerDate);
            TaskEntity fatherTask = this.taskMap.get(relaEntity.getTaskId());
            String fatherTaskInstanceId = "imp_" + HalleyUtil.generateInstanceId(String.valueOf(relaEntity.getTaskPreId()), String.valueOf(fatherTask.getCycle()), initDate) + "_" + HalleyUtil.Date2String(triggerDate);
            relaStatusEntity.setTaskStatusId(taskInstanceId);
            relaStatusEntity.setPreId(relaEntity.getTaskPreId());
            relaStatusEntity.setPreStsId(fatherTaskInstanceId);
            this.statusRelaList.add(relaStatusEntity);
        }
    }

    private TaskStatusEntity makeImportantInstance(TaskEntity entity, Date initDate, Date triggerDate) {
        TaskStatusEntity result = new TaskStatusEntity();
        String instanceId = "imp_" + HalleyUtil.generateInstanceId(String.valueOf(entity.getTaskId()), String.valueOf(entity.getCycle()), initDate) + "_" + HalleyUtil.Date2String(triggerDate);
        result.setTask_status_id(instanceId);
        result.setTask_id(entity.getTaskId());
        result.setTask_group_id(entity.getTaskGroupId());
        result.setTask_name(entity.getTaskName());
        result.setTask_obj(HalleyUtil.dirReplace(entity.getTaskObj()));
        result.setPara1(StringUtil.isEmpty(entity.getPara1()) ? null : HalleyUtil.CaldtReplace(entity.getPara1(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        result.setPara1(StringUtil.isEmpty(entity.getPara1()) ? null : HalleyUtil.CaldtReplace(entity.getPara1(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        result.setPara2(StringUtil.isEmpty(entity.getPara2()) ? null : HalleyUtil.CaldtReplace(entity.getPara2(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        result.setPara3(StringUtil.isEmpty(entity.getPara3()) ? null : HalleyUtil.CaldtReplace(entity.getPara3(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        String logPath = HalleyUtil.dirReplace(entity.getLogHome()) + File.separator + entity.getLogFile() + "." + HalleyUtil.getDay8();
        result.setLog_path(logPath);
        result.setCycle(entity.getCycle());
        result.setTime_id(HalleyUtil.getDay10(initDate));
        result.setStatus(0);
        result.setPrio_lvl(5);
        result.setRun_num(0);
        result.setType(entity.getType());
        result.setTable_name(entity.getTableName());
        String baseCaldt = HalleyUtil.getLastDay10(initDate);
        String caldt = null;
        try {
            caldt = HalleyUtil.getCalDt(baseCaldt, entity.getOffsetType(), entity.getOffset());
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            caldt = null;
        }

        result.setCal_dt(caldt);
        result.setDatabase_src(entity.getDatabaseSrc());
        result.setIf_pre(entity.getIfPre());
        result.setIf_wait(entity.getIfWait());
        result.setIf_recall(0);
        result.setSts_desc("INIT");
        result.setRecall_num(0);
        result.setOwner(entity.getOwner());
        Calendar c = Calendar.getInstance();
        c.setTime(triggerDate);
        result.setTrigger_time(c.getTimeInMillis());
        result.setRecall_code(entity.getRecallCode());
        result.setSuccess_code(entity.getSuccessCode());
        result.setWait_code(entity.getWaitCode());
        result.setJob_code(-1);
        result.setFreq(entity.getFreq());
        result.setTimeout(entity.getTimeout());
        result.setRecall_interval(entity.getRecallInterval());
        result.setRecall_limit(entity.getRecallLimit());
        result.setRunning_prio(0);
        return result;
    }

    private TaskStatusEntity getTemporaryInstance(Map<String, String> paras) {
        TaskStatusEntity result = new TaskStatusEntity();
        Date triggerDate = new Date();
        String instanceId = "tmp_" + "_" + HalleyUtil.Date2String(triggerDate);
        result.setTask_status_id(instanceId);
        result.setTask_id(99999);
        result.setTask_group_id(2);
        result.setTask_name(paras.get("taskName"));
        result.setTask_obj("sh /data/deploy/dwarch/conf/ETL/bin/start_calculate.sh");
        result.setPara1(paras.get("para1"));
        result.setPara2(paras.get("para2"));
        result.setPara3(paras.get("para3"));
        result.setLog_path("/data/deploy/dwarch/log/ETL/rpt/" + paras.get("taskName").trim() + "." + HalleyUtil.getDay8());
        result.setCycle("D");
        result.setTime_id(HalleyUtil.getDay10(triggerDate));
        result.setStatus(0);
        result.setPrio_lvl(5);
        result.setRun_num(0);
        result.setType(2);
        result.setTable_name(paras.get("taskName"));
        String baseCaldt = HalleyUtil.getLastDay10(triggerDate);
        String caldt = null;
        try {
            caldt = HalleyUtil.getCalDt(baseCaldt, "offset", "D0");
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            caldt = null;
        }

        result.setCal_dt(caldt);
        result.setDatabase_src("gp57");
        result.setIf_pre(0);
        result.setIf_wait(0);
        result.setIf_recall(0);
        result.setSts_desc("INIT");
        result.setRecall_num(0);
        result.setOwner(paras.get("owner"));
        Calendar c = Calendar.getInstance();
        c.setTime(triggerDate);
        result.setTrigger_time(c.getTimeInMillis());
        result.setRecall_code(null);
        result.setSuccess_code("0");
        result.setWait_code(null);
        result.setJob_code(-1);
        result.setFreq(null);
        result.setTimeout(paras.get("timeout") == null ? 30 : Integer.valueOf(paras.get("timeout")));
        result.setRecall_interval(10);
        result.setRecall_limit(0);
        return result;
    }


    private TaskStatusEntity getInitInstance(TaskEntity entity, Date initDate, Date triggerDate) {
        TaskStatusEntity result = new TaskStatusEntity();
        String instanceId = "pre_" + HalleyUtil.generateInstanceId(String.valueOf(entity.getTaskId()), String.valueOf(entity.getCycle()), initDate) + "_" + HalleyUtil.Date2String(triggerDate);
        result.setTask_status_id(instanceId);
        result.setTask_id(entity.getTaskId());
        result.setTask_group_id(entity.getTaskGroupId());
        result.setTask_name(entity.getTaskName());
        result.setTask_obj(HalleyUtil.dirReplace(entity.getTaskObj()));
        result.setPara1(StringUtil.isEmpty(entity.getPara1()) ? null : HalleyUtil.CaldtReplace(entity.getPara1(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        result.setPara1(StringUtil.isEmpty(entity.getPara1()) ? null : HalleyUtil.CaldtReplace(entity.getPara1(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        result.setPara2(StringUtil.isEmpty(entity.getPara2()) ? null : HalleyUtil.CaldtReplace(entity.getPara2(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        result.setPara3(StringUtil.isEmpty(entity.getPara3()) ? null : HalleyUtil.CaldtReplace(entity.getPara3(), entity.getOffsetType(), entity.getOffset(), initDate).replace("${task_id}", String.valueOf(entity.getTaskId())));
        String logPath = HalleyUtil.dirReplace(entity.getLogHome()) + File.separator + entity.getLogFile() + "." +instanceId + "."+ HalleyUtil.getDay8();
        result.setLog_path(logPath);
        result.setCycle(entity.getCycle());
        result.setTime_id(HalleyUtil.getDay10(initDate));
        result.setStatus(0);
        result.setPrio_lvl(5);
        result.setRun_num(0);
        result.setType(entity.getType());
        result.setTable_name(entity.getTableName());
        String baseCaldt = HalleyUtil.getLastDay10(initDate);
        String caldt = null;
        try {
            caldt = HalleyUtil.getCalDt(baseCaldt, entity.getOffsetType(), entity.getOffset());
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            caldt = null;
        }

        result.setCal_dt(caldt);
        result.setDatabase_src(entity.getDatabaseSrc());
        result.setIf_pre(0);
        result.setIf_wait(entity.getIfWait());
        result.setIf_recall(0);
        result.setSts_desc("INIT");
        result.setRecall_num(0);
        result.setOwner(entity.getOwner());
        Calendar c = Calendar.getInstance();
        c.setTime(triggerDate);
        result.setTrigger_time(c.getTimeInMillis());
        result.setRecall_code(entity.getRecallCode());
        result.setSuccess_code(entity.getSuccessCode());
        result.setWait_code(entity.getWaitCode());
        result.setJob_code(-1);
        result.setFreq(entity.getFreq());
        result.setTimeout(entity.getTimeout());
        result.setRecall_interval(entity.getRecallInterval());
        result.setRecall_limit(entity.getRecallLimit());
        result.setRunning_prio(-1);

        return result;
    }

}
