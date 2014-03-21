package com.dianping.darkbat.entity;

import java.util.List;

public class TaskStatusEntity {

    private Integer task_group_id;
    private String task_obj;
    private String para1;
    private String para2;
    private String para3;
    public Integer prio_lvl;
    private Integer type;
    private String table_name;
    private String cal_dt;
    private String database_src;
    private Integer if_pre;
    private Integer if_wait;
    private Integer if_recall;
    private String sts_desc;
    private Long trigger_time;
    private String recall_code;
    private String success_code;
    private String wait_code;
    private Integer job_code;



    private String freq;
    private Integer timeout;
    private Integer recall_interval;
    private Integer recall_limit;
    private String time_stamp;
    private Integer running_prio;
    
    private String cycle;
    private String end_time;
    private String log_path;
    private String owner;
    private Integer recall_num;
    private Integer run_num;
    private String start_time;
    private Integer status;
    private String task_name;
    private String task_status_id;
    private Integer task_id;
   
    private List<String> parents;
    private List<String> children;

    private boolean isTarget;

	private String time_id;

    private String priority;
    private Integer onlyself;



    public Long getTrigger_time() {
        return trigger_time;
    }

    public void setTrigger_time(Long trigger_time) {
        this.trigger_time = trigger_time;
    }
    public List<String> getChildren() {
        return children;
    }

    public String getCycle() {
        return cycle;
    }

    public Integer getTask_id() {
		return task_id;
	}

	public void setTask_id(Integer task_id) {
		this.task_id = task_id;
	}

	public String getEnd_time() {
        return end_time;
    }

    public String getLog_path() {
        return log_path;
    }

    public String getOwner() {
        return owner;
    }

    public List<String> getParents() {
        return parents;
    }

    public Integer getRecall_num() {
        return recall_num;
    }

    public Integer getRun_num() {
        return run_num;
    }

    public String getStart_time() {
        return start_time;
    }

    public Integer getStatus() {
        return status;
    }

    public String getTask_name() {
        return task_name;
    }

    public String getTask_status_id() {
        return task_status_id;
    }

    public boolean isTarget() {
        return isTarget;
    }

    public void setChildren(List<String> children) {
        this.children = children;
    }

    public void setCycle(String cycle) {
        this.cycle = cycle;
    }

    public void setEnd_time(String endTime) {
        end_time = endTime;
    }

    public void setLog_path(String logPath) {
        log_path = logPath;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setParents(List<String> parents) {
        this.parents = parents;
    }

    public void setRecall_num(Integer recallNum) {
        recall_num = recallNum;
    }

    public void setRun_num(Integer runNum) {
        run_num = runNum;
    }

    public void setStart_time(String startTime) {
        start_time = startTime;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public void setTarget(boolean isTarget) {
        this.isTarget = isTarget;
    }

    public void setTask_name(String taskName) {
        task_name = taskName;
    }

    public void setTask_status_id(String taskStatusId) {
        task_status_id = taskStatusId;
    }

    public String getTime_id() {
		return time_id;
	}

	public void setTime_id(String time_id) {
		this.time_id = time_id;
	}

    public Integer getTask_group_id() {
        return task_group_id;
    }

    public void setTask_group_id(Integer task_group_id) {
        this.task_group_id = task_group_id;
    }

    public String getPara1() {
        return para1;
    }

    public void setPara1(String para1) {
        this.para1 = para1;
    }

    public String getPara2() {
        return para2;
    }

    public void setPara2(String para2) {
        this.para2 = para2;
    }

    public String getPara3() {
        return para3;
    }

    public void setPara3(String para3) {
        this.para3 = para3;
    }

    public Integer getPrio_lvl() {
        return prio_lvl;
    }

    public void setPrio_lvl(Integer prio_lvl) {
        this.prio_lvl = prio_lvl;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getCal_dt() {
        return cal_dt;
    }

    public void setCal_dt(String cal_dt) {
        this.cal_dt = cal_dt;
    }

    public String getDatabase_src() {
        return database_src;
    }

    public void setDatabase_src(String database_src) {
        this.database_src = database_src;
    }

    public Integer getIf_pre() {
        return if_pre;
    }

    public void setIf_pre(Integer if_pre) {
        this.if_pre = if_pre;
    }

    public Integer getIf_wait() {
        return if_wait;
    }

    public void setIf_wait(Integer if_wait) {
        this.if_wait = if_wait;
    }

    public Integer getIf_recall() {
        return if_recall;
    }

    public void setIf_recall(Integer if_recall) {
        this.if_recall = if_recall;
    }

    public String getSts_desc() {
        return sts_desc;
    }

    public void setSts_desc(String sts_desc) {
        this.sts_desc = sts_desc;
    }

    public String getRecall_code() {
        return recall_code;
    }

    public void setRecall_code(String recall_code) {
        this.recall_code = recall_code;
    }

    public String getSuccess_code() {
        return success_code;
    }

    public void setSuccess_code(String success_code) {
        this.success_code = success_code;
    }

    public String getWait_code() {
        return wait_code;
    }

    public void setWait_code(String wait_code) {
        this.wait_code = wait_code;
    }

    public Integer getJob_code() {
        return job_code;
    }

    public void setJob_code(Integer job_code) {
        this.job_code = job_code;
    }

    public String getFreq() {
        return freq;
    }

    public void setFreq(String freq) {
        this.freq = freq;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getRecall_interval() {
        return recall_interval;
    }

    public void setRecall_interval(Integer recall_interval) {
        this.recall_interval = recall_interval;
    }

    public Integer getRecall_limit() {
        return recall_limit;
    }

    public void setRecall_limit(Integer recall_limit) {
        this.recall_limit = recall_limit;
    }

    public String getTime_stamp() {
        return time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = time_stamp;
    }

    public String getTask_obj() {
        return task_obj;
    }

    public void setTask_obj(String task_obj) {
        this.task_obj = task_obj;
    }

    public Integer getRunning_prio() {
        return running_prio;
    }

    public void setRunning_prio(Integer running_prio) {
        this.running_prio = running_prio;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public Integer getOnlyself() {
        return onlyself;
    }

    public void setOnlyself(Integer onlyself) {
        this.onlyself = onlyself;
    }
}
