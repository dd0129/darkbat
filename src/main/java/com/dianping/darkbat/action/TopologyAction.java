package com.dianping.darkbat.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.struts2.ServletActionContext;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.TaskChildToParentEntity;
import com.dianping.darkbat.entity.TaskStatusEntity;
import com.dianping.darkbat.service.TopologyService;
import com.opensymphony.xwork2.Action;

@Repository
public class TopologyAction {
	@Autowired
	private TopologyService topologyService;
	
	private JSONObject jsonObject;
	
	/**
	 * 获取直接的父任务子任务
	 * @return
	 */
	public String getSimplePostAndPre() {
		HttpServletRequest req = ServletActionContext.getRequest();
		
		String taskStatusID = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID));
		String date = CommonUtil.parseStr(req.getParameter("date"));
		if(null == date)
			date = new DateTime().toString("yyyy-MM-dd");
		
		List<TaskChildToParentEntity> childToParentEntities = topologyService.getChildToParent(date);
		
		Map<String, Integer> idToStatusMap = new HashMap<String, Integer>();
		Map<String, String> idToTextMap = new HashMap<String, String>();
		
		for(TaskChildToParentEntity entity : childToParentEntities) {
			if(null == idToStatusMap.get(entity.getChildId())) {
				idToStatusMap.put(entity.getChildId(), entity.getChildStatus());
				idToTextMap.put(entity.getChildId(), entity.getChildText());
			}
			if(null == idToStatusMap.get(entity.getParentId())) {
				idToStatusMap.put(entity.getParentId(), entity.getParentStatus());
				idToTextMap.put(entity.getParentId(), entity.getParentText());
			}
			
		}
		
		Map<String, List<String>> idToChildrenMap = topologyService.getTaskIdToChildMap(childToParentEntities);
		Map<String, List<String>> idToParentMap = topologyService.getTaskIdToParentMap(childToParentEntities);
		
		JSONArray jArray = new JSONArray();
		JSONObject jObject = new JSONObject();
		jObject.element("id", taskStatusID)
			.element("status", idToStatusMap.get(taskStatusID))
			.element("text", idToTextMap.get(taskStatusID))
			.element("children", (idToChildrenMap.get(taskStatusID) == null ? "[]" : idToChildrenMap.get(taskStatusID)))
			.element("parents", (idToParentMap.get(taskStatusID) == null ? "[]" : idToParentMap.get(taskStatusID)))
			.element("isTarget", 1);
		jArray.add(jObject);
		
		List<String> cIdList = idToChildrenMap.get(taskStatusID);
		List<String> pIdList = idToParentMap.get(taskStatusID);
		
		if(null != cIdList) {
			/**
			 * 配置一层直接的孩子节点
			 */
			for(String cid: idToChildrenMap.get(taskStatusID)) {
				jObject = new JSONObject();
				jObject.element("id", cid)
					.element("status", idToStatusMap.get(cid))
					.element("text", idToTextMap.get(cid))
					.element("children", "[]")
					.element("parents", "[\"" + taskStatusID + "\"]")
					.element("isTarget", 0);
				jArray.add(jObject);
			}
		}
		if(null != pIdList) {
			/**
			 * 配置一层直接的父亲节点
			 */
			for(String pid: idToParentMap.get(taskStatusID)) {
				jObject = new JSONObject();
				jObject.element("id", pid)
					.element("status", idToStatusMap.get(pid))
					.element("text", idToTextMap.get(pid))
					.element("children", "[\"" + taskStatusID + "\"]")
					.element("parents", "[]")
					.element("isTarget", 0);
				jArray.add(jObject);
			}
		}
		
		setJsonObject(CommonUtil.getPubJson(jArray));
		
		return Action.SUCCESS;
	}
	
	/**
	 * 获取节点所有孩子与父亲节点
	 * @return
	 */
	public String getAllPreAndPost() {
		HttpServletRequest req = ServletActionContext.getRequest();
		
		String taskStatusID = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID));
		String date = CommonUtil.parseStr(req.getParameter("date"));
		if(null == date)
			date = new DateTime().toString("yyyy-MM-dd");
		
		List<TaskChildToParentEntity> childToParentEntities = topologyService.getChildToParent(date);
		
		Map<String, Integer> idToStatusMap = new HashMap<String, Integer>();
		Map<String, String> idToTextMap = new HashMap<String, String>();
		
		for(TaskChildToParentEntity entity : childToParentEntities) {
			if(null == idToStatusMap.get(entity.getChildId())) {
				idToStatusMap.put(entity.getChildId(), entity.getChildStatus());
				idToTextMap.put(entity.getChildId(), entity.getChildText());
			}
			if(null == idToStatusMap.get(entity.getParentId())) {
				idToStatusMap.put(entity.getParentId(), entity.getParentStatus());
				idToTextMap.put(entity.getParentId(), entity.getParentText());
			}
		}
		
		Map<String, List<String>> idToChildrenMap = topologyService.getTaskIdToChildMap(childToParentEntities);
		Map<String, List<String>> idToParentMap = topologyService.getTaskIdToParentMap(childToParentEntities);
		
		List<TaskStatusEntity> allEntities = new ArrayList<TaskStatusEntity>();
		allEntities.addAll(topologyService.getTaskContextEntities(idToParentMap, idToChildrenMap, taskStatusID, Const.TASK_RELA_PRE));
		allEntities.addAll(topologyService.getTaskContextEntities(idToParentMap, idToChildrenMap, taskStatusID, Const.TASK_RELA_POST));
		
		JSONArray jArray = new JSONArray();
		JSONObject jObject = new JSONObject();
		jObject.element("id", taskStatusID)
			.element("status", idToStatusMap.get(taskStatusID))
			.element("text", idToTextMap.get(taskStatusID))
			.element("children", (idToChildrenMap.get(taskStatusID) == null ? "[]" : idToChildrenMap.get(taskStatusID)))
			.element("parents", (idToParentMap.get(taskStatusID) == null ? "[]" : idToParentMap.get(taskStatusID)))
			.element("isTarget", 1);
		jArray.add(jObject);
		
		for(TaskStatusEntity tSEntity: allEntities) {
			String tSEntityId = tSEntity.getTask_status_id();
			
			jObject = new JSONObject();
			jObject.element("id", tSEntityId)
				.element("status", idToStatusMap.get(tSEntityId))
				.element("text", idToTextMap.get(tSEntityId))
				.element("children", (tSEntity.getChildren() == null ? "[]" : tSEntity.getChildren()))
				.element("parents", (tSEntity.getParents() == null ? "[]" : tSEntity.getParents()))
				.element("isTarget", 0);
			
			jArray.add(jObject);
		}
		
		setJsonObject(CommonUtil.getPubJson(jArray));
		
		return Action.SUCCESS;
	}

    /**
     * 获取节点所有孩子与父亲节点
     * @return
     */
    public String getTimecostLongPath() {
        HttpServletRequest req = ServletActionContext.getRequest();

        String taskStatusID = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID));
        String date = CommonUtil.parseStr(req.getParameter("date"));
        if(null == date)
            date = new DateTime().toString("yyyy-MM-dd");

        List<TaskChildToParentEntity> childToParentEntities = topologyService.getChildToParent(date);

        Map<String, Integer> idToStatusMap = new HashMap<String, Integer>();
        Map<String, String> idToTextMap = new HashMap<String, String>();

        for(TaskChildToParentEntity entity : childToParentEntities) {
            if(null == idToStatusMap.get(entity.getChildId())) {
                idToStatusMap.put(entity.getChildId(), entity.getChildStatus());
                idToTextMap.put(entity.getChildId(), entity.getChildText());
            }
            if(null == idToStatusMap.get(entity.getParentId())) {
                idToStatusMap.put(entity.getParentId(), entity.getParentStatus());
                idToTextMap.put(entity.getParentId(), entity.getParentText());
            }
        }

        Map<String, List<String>> idToParentMap = topologyService.getTaskIdToParentLongPathMap(childToParentEntities);
        Map<String, List<String>> idToChildrenMap = topologyService.getTaskIdToChildMap(childToParentEntities);

        List<TaskStatusEntity> allEntities = new ArrayList<TaskStatusEntity>();
        allEntities.addAll(topologyService.getTaskContextEntities(idToParentMap, idToChildrenMap, taskStatusID, Const.TASK_RELA_PRE));
        //allEntities.addAll(topologyService.getTaskContextEntities(idToParentMap, idToChildrenMap, taskStatusID, Const.TASK_RELA_POST));

        JSONArray jArray = new JSONArray();
        JSONObject jObject = new JSONObject();
        jObject.element("id", taskStatusID)
                .element("status", idToStatusMap.get(taskStatusID))
                .element("text", idToTextMap.get(taskStatusID))
                .element("children", ("[]"))
                .element("parents", (idToParentMap.get(taskStatusID) == null ? "[]" : idToParentMap.get(taskStatusID)))
                .element("isTarget", 1);
        jArray.add(jObject);

        for(TaskStatusEntity tSEntity: allEntities) {
            String tSEntityId = tSEntity.getTask_status_id();

            jObject = new JSONObject();
            jObject.element("id", tSEntityId)
                    .element("status", idToStatusMap.get(tSEntityId))
                    .element("text", idToTextMap.get(tSEntityId))
                    .element("children", (tSEntity.getChildren() == null ? "[]" : tSEntity.getChildren()))
                    .element("parents", (tSEntity.getParents() == null ? "[]" : tSEntity.getParents()))
                    .element("isTarget", 0);

            jArray.add(jObject);
        }

        setJsonObject(CommonUtil.getPubJson(jArray));

        return Action.SUCCESS;
    }

	/**
	 * 重跑任务
	 * @return
	 */
	public String rerunTask() {
		HttpServletRequest req = ServletActionContext.getRequest();
		String taskStatusId = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID));
		
		topologyService.rerunTaskByID(taskStatusId);
		
		setJsonObject(CommonUtil.getPubJson(null));
		
		return Action.SUCCESS;
	}

    /**
	 * 提高优先级
	 * @return
	 */
	public String raisePriority() {
		HttpServletRequest req = ServletActionContext.getRequest();
		String taskStatusId = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID));
        if(StringUtils.isBlank(taskStatusId)){
            throw new NullPointerException("taskstatusId is null");
        }

		topologyService.raisePriorityByID(taskStatusId);

		setJsonObject(CommonUtil.getPubJson(null));

		return Action.SUCCESS;
	}
	
	public String rerunMultiJobs() {
		HttpServletRequest req = ServletActionContext.getRequest();
		String idsString = CommonUtil.parseStr(req.getParameter("ids"));
		String startDate = CommonUtil.parseStr(req.getParameter("startDate"));
		String endDate = CommonUtil.parseStr(req.getParameter("endDate"));
		
		Integer result = topologyService.rerunMultiJobs(startDate, endDate, idsString);
		
		setJsonObject(CommonUtil.getPubJson(result));
		
		return Action.SUCCESS;
	}
	
   /**
	 * 获取所有孩子节点
	 * @return
	 */
	public String getAllChildren() {
		HttpServletRequest req = ServletActionContext.getRequest();
		
		String taskStatusID = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID));
		String date = CommonUtil.parseStr(req.getParameter("date"));
		if(null == date)
			date = new DateTime().toString("yyyy-MM-dd");
		
		List<TaskChildToParentEntity> childToParentEntities = topologyService.getChildToParent(date);
		
		Map<String, String> idToNameMap = new HashMap<String, String>();
		Map<String, Integer> idToStatusMap = new HashMap<String, Integer>();
		
		for(TaskChildToParentEntity entity : childToParentEntities) {
			if(null == idToNameMap.get(entity.getChildId())) {
				idToNameMap.put(entity.getChildId(), entity.getChildTaskName());
				idToStatusMap.put(entity.getChildId(), entity.getChildStatus());
			}
			if(null == idToNameMap.get(entity.getParentId())) {
				idToNameMap.put(entity.getParentId(), entity.getParentTaskName());
				idToStatusMap.put(entity.getParentId(), entity.getParentStatus());
			}
		}
		
		Map<String, List<String>> idToChildrenMap = topologyService.getTaskIdToChildMap(childToParentEntities);
		
		JSONArray jArray = new JSONArray();
		
		if (topologyService.isEditable(idToStatusMap.get(taskStatusID))) {
		    
		    JSONObject jObj = new JSONObject();
		    jObj.element("id", taskStatusID)
		        .element("pId", 0)
		        .element("name", idToNameMap.get(taskStatusID));
     
		    jArray.add(jObj);
		}
		
		List<String> idList = new ArrayList<String>();
		idList.add(taskStatusID);
		
		while (idList.size() > 0) {
		    String currentId = idList.remove(0);

		    if (topologyService.isEditable(idToStatusMap.get(currentId))) {
		        
		        List<String> cList = idToChildrenMap.get(currentId);
		        if (cList != null) {
		            for (String childId : cList) {
		                if (topologyService.isEditable(idToStatusMap.get(childId))) {
		                    idList.add(childId);
		                    
		                    JSONObject jObj = new JSONObject();
		                    
		                    jObj.element("id", childId)
		                        .element("pId", currentId)
		                        .element("name", idToNameMap.get(childId));
		                    
		                    jArray.add(jObj);
		                }
		            }
		        }
		    }
		    
		}
		jsonObject = CommonUtil.getPubJson(jArray);
		
		return Action.SUCCESS;
	}
	
	public String modifyTaskStatus() {
		HttpServletRequest req = ServletActionContext.getRequest();
		String taskStatusId = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID));
		String allowStatusList = req.getParameter("allowStatus");
		Integer status = CommonUtil.parseInt(req.getParameter("status"));
		
		
		int result = topologyService.modifyTaskByID(allowStatusList, taskStatusId, status);
		setJsonObject(CommonUtil.getPubJson(result));
		
		return Action.SUCCESS;
	}
	
	public JSONObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}

}
