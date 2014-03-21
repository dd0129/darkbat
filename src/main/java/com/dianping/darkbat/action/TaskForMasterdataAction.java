package com.dianping.darkbat.action;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.struts2.ServletActionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.service.TaskService;
import com.opensymphony.xwork2.Action;

@Repository
public class TaskForMasterdataAction {

	@Autowired
	private TaskService taskService;
	
	private JSONObject jsonObject;

	public String updateTaskPrioLvlForMasterdata(){
		HttpServletRequest req = ServletActionContext.getRequest();
		JSONArray taskIdsJson = JSONArray.fromObject(req.getParameter("ids"));
		Integer lvl = Integer.parseInt(req.getParameter("lvl"));
		List<Integer> taskIds = new ArrayList<Integer>();
		for(Object taskIdJson:taskIdsJson){
			taskIds.add((Integer) taskIdJson);
		}
		jsonObject = CommonUtil.getPubJson(taskService.updateTaskPrioLvlForMasterdata(taskIds, lvl));
		return Action.SUCCESS;
	}
	
	public String getPrerunTaskStatusEntityByTaskId(){
        HttpServletRequest req = ServletActionContext.getRequest();
        String taskId = req.getParameter("taskId");
        JSONArray jsonArray = taskService.getPrerunTaskStatusEntityByTaskId(taskId);
        jsonObject=CommonUtil.getPubJson( jsonArray);
        ServletActionContext.getResponse().setHeader("Access-Control-Allow-Origin", "*");//允许跨域访问
        return Action.SUCCESS;
	}
	
    /**
     * 返回传入Id中所有有在跑任务的Id
     * */
    public String getAllRunningTaskIds() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String taskIdStr = req.getParameter("taskIds");
        List<String> taskIds = new ArrayList<String>();
        for (String taskId : taskIdStr.split(",")) {
            taskIds.add(taskId);
        }
        String runningTaskIds = taskService.getAllRunningTaskIds(taskIds);
        jsonObject = CommonUtil.getPubJson(runningTaskIds);
        ServletActionContext.getResponse().setHeader("Access-Control-Allow-Origin", "*");// 允许跨域访问
        return Action.SUCCESS;
    }
    
    // 返回传入Id中所有挂起的任务id, by chaos
    public String getSuspendTaskIds(){
        HttpServletRequest req = ServletActionContext.getRequest();
        String taskIdStr = req.getParameter("taskIds");
        List<String> taskIds = new ArrayList<String>();
        for (String taskId : taskIdStr.split(",")) {
            taskIds.add(taskId);
        }
        String suspendingTaskIds = taskService.getSuspendTaskIds(taskIds);
        jsonObject = CommonUtil.getPubJson(suspendingTaskIds);
        ServletActionContext.getResponse().setHeader("Access-Control-Allow-Origin", "*");// 允许跨域访问
        return Action.SUCCESS;
    }
	
	public JSONObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}
	
}
