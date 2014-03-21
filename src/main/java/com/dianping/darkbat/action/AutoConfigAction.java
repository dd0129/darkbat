package com.dianping.darkbat.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.struts2.ServletActionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.entity.TaskEntity;
import com.dianping.darkbat.service.AutoConfigService;
import com.dianping.darkbat.service.TaskService;
import com.opensymphony.xwork2.Action;

@Repository
public class AutoConfigAction {

	@Autowired
	private AutoConfigService autoConfigService;
	
	@Autowired
	private TaskService taskService;

	public String getDolInfo() throws Exception{
 		HttpServletRequest req = ServletActionContext.getRequest();
		String dolName = req.getParameter("dolName");
		String group = req.getParameter("group");
        String product = req.getParameter("product");
		
		Random rand = new Random(System.currentTimeMillis());
		Integer taskId = Math.abs(rand.nextInt());
		autoConfigService.preExcutor(dolName, taskId, group, product);
		JSONObject jsonObj = new JSONObject();
		jsonObj.accumulate("tableName", JSONArray.fromObject(autoConfigService.getTaskTargetTableList(taskId)));
		List<TaskEntity> taskEntitys = new ArrayList<TaskEntity>();
		for(Integer taskIdVar:autoConfigService.getTaskParentTaskIdList(taskId)){
			taskEntitys.add(taskService.getTaskById(taskIdVar));
		}
		jsonObj.accumulate("parentInfo", JSONArray.fromObject(taskEntitys));
		jsonObject = CommonUtil.getPubJson(jsonObj);
		return Action.SUCCESS;
	}
	
	private JSONObject jsonObject;

	public JSONObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}

}