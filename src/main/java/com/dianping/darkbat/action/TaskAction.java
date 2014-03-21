package com.dianping.darkbat.action;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.struts2.ServletActionContext;
import org.joda.time.DateTime;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.MonitorUser;
import com.dianping.darkbat.entity.TaskEntity;
import com.dianping.darkbat.entity.TaskRelaEntity;
import com.dianping.darkbat.entity.TaskStatusEntity;
import com.dianping.darkbat.service.AutoConfigService;
import com.dianping.darkbat.service.MonitorUserService;
import com.dianping.darkbat.service.TaskService;
import com.opensymphony.xwork2.Action;

@Repository
public class TaskAction {

    @Autowired
    private MonitorUserService monitorUserService;
    
	@Autowired
	private TaskService taskService;

	@Autowired
	private AutoConfigService autoConfigService;

	private JSONObject jsonObject;

	public String reloadMonitorUser() {
	    monitorUserService.reload();
	    
	    return Action.SUCCESS;
	}
	
	public String getCurrentMonitorUser() {
	    MonitorUser currentUser = monitorUserService.getCurrentUser();
	    
	    JSONObject jObj = JSONObject.fromObject(currentUser);
	    jsonObject = CommonUtil.getPubJson(jObj);
	    
	    return Action.SUCCESS;
	}
	
	public String setLoginUserToken() {
		HttpServletRequest req = ServletActionContext.getRequest();
		String token = req.getParameter("token");
		req.getSession().setAttribute("token", token);
		jsonObject = CommonUtil.getPubJson(jsonObject);
		return Action.SUCCESS;
	}

	/**
	 * 检查任务名称是否有效
	 * 
	 * @return
	 */
	public String checkTaskName() {
		HttpServletRequest req = ServletActionContext.getRequest();
		String taskName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_NAME));
		String tableName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_TABLE_NAME));

		CommonUtil.validateParam(taskName, "任务名称");

		taskName = taskName.toLowerCase();
		boolean isExist = taskService.isTaskNameDuplicated(taskName);

		if (!isExist) {
			List<String> tableNames = new ArrayList<String>();
			tableNames.addAll(Arrays.asList(tableName.split(";")));
			for (int i = 0; i < tableNames.size(); ++i) {
				tableNames.set(
						i,
						tableNames.get(i).substring(
								tableNames.get(i).indexOf(".") + 1));
			}
			List<String> notExistTableNames = autoConfigService
					.getNotExistTableList(tableNames);
			if (notExistTableNames.size() > 0) {
				jsonObject = new JSONObject();
				jsonObject.put("notExistTable",
						JSONArray.fromObject(notExistTableNames));
				jsonObject = CommonUtil.getPubJson(jsonObject, 201);
			}
		} else {
			jsonObject = new JSONObject();
			jsonObject.put("isExist", isExist ? 1 : 0);
			jsonObject = CommonUtil.getPubJson(jsonObject);
		}

		return Action.SUCCESS;
	}

	/**
	 * 删除任务，使任务失效
	 * 
	 * @return
	 */
	public String deleteTask() {
		HttpServletRequest req = ServletActionContext.getRequest();
		Integer taskId = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_ID));

		CommonUtil.validateParam(taskId, "任务ID");

		int status = taskService.invalidateTask(taskId);
		switch (status) {
		case -1:
			throw new RuntimeException("该任务有后继任务，请先删除后继任务");
		case 1:
			jsonObject = CommonUtil.getPubJson("1");
			return Action.SUCCESS;
		default:
			throw new RuntimeException("删除失败");
		}
	}

	/**
	 * 查看任务详细信息
	 * 
	 * @return
	 */
	public String getTaskDetail() {
		HttpServletRequest req = ServletActionContext.getRequest();

		Integer taskId = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_ID));
		CommonUtil.validateParam(taskId, "任务ID");

		TaskEntity taskEntity = taskService.getTaskById(taskId);

		if (taskEntity == null) {
			throw new RuntimeException("该条件的结果不存在");
		} else {
			JSONObject jsonObjectTmp = JSONObject.fromObject(taskEntity);
			List<TaskRelaEntity> taskRelaList = taskService
					.getTaskRelaByTaskId(taskId);
			List<TaskEntity> preTaskList = taskService
					.getPreTaskInfoByTaskId(taskId);

			JSONArray jsonArray = new JSONArray();
			for (TaskEntity preTaskEntity : preTaskList) {
				JSONObject jsonObjectChild = new JSONObject();
				jsonObjectChild.element(Const.FLD_DEP_TASK_ID,
						preTaskEntity.getTaskId());
				jsonObjectChild.element(Const.FLD_DEP_TASK_GROUP_ID,
						preTaskEntity.getTaskGroupId());
				jsonObjectChild.element(Const.FLD_DEP_TASK_NAME,
						preTaskEntity.getTaskName());
				for (TaskRelaEntity taskRela : taskRelaList) {
					if (taskRela.getTaskPreId().equals(
							preTaskEntity.getTaskId())) {
						// cyclegap去除周期字母
						jsonObjectChild.element(Const.FLD_DEP_CYCLE_GAP,
								taskRela.getCycleGap().substring(1));
						// jsonObjectChild.element(Const.FLD_DEP_REMARK,
						// taskRela.getRemark());
						break;
					}
				}
				jsonArray.add(jsonObjectChild);
			}
			jsonObjectTmp.element(Const.FLD_PRE_DEPENDS, jsonArray);
			jsonObject = CommonUtil.getPubJson(jsonObjectTmp);
		}

		return Action.SUCCESS;
	}

	/**
	 * 获取所有任务基本信息
	 * 
	 * @return
	 */
	public String getAllTaskBasicInfo() {
		Map<String, List<TaskEntity>> map = new HashMap<String, List<TaskEntity>>();
		for (TaskEntity entity : taskService.getAllTaskBasicInfo()) {
			if (!map.containsKey(String.valueOf(entity.getTaskGroupId()))) {
				map.put(String.valueOf(entity.getTaskGroupId()),
						new ArrayList<TaskEntity>());
			}
			map.get(String.valueOf(entity.getTaskGroupId())).add(entity);
		}
		jsonObject = CommonUtil.getPubJson(JSONObject.fromObject(map));
		return Action.SUCCESS;
	}

	/**
	 * 拓扑-获取单个任务信息
	 * 
	 * @return
	 */
	public String getTaskStatus() {
		HttpServletRequest req = ServletActionContext.getRequest();

		List<String> taskStatusId = req
				.getParameter(Const.FLD_TASK_RELA_STATUS_ID) == null ? null
				: Arrays.asList(req.getParameter(Const.FLD_TASK_RELA_STATUS_ID)
						.split("\\<\\+\\>"));
		JSONArray jsonArray = taskService
				.getTaskStatusInfoJsonObject(taskStatusId);
		if (jsonArray == null) {
			throw new RuntimeException("系统出错");
		} else {
			jsonObject = CommonUtil.getPubJson(jsonArray);
		}
		return Action.SUCCESS;
	}

	/**
	 * 查询任务
	 * 
	 * @return
	 */
	public String searchTask() {
		HttpServletRequest req = ServletActionContext.getRequest();

		Integer taskId = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_ID));
		String taskName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_NAME));
		Integer taskGroupId = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_GROUP_ID));
		String cycle = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_CYCLE));
		String databaseSrc = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_DATABASE_SRC));
		Integer ifVal = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_IF_VAL));
		String owner = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_OWNER));

		TaskEntity taskEntity = new TaskEntity();
		taskEntity.setTaskId(taskId);
		taskEntity.setTaskName(taskName);
		taskEntity.setTaskGroupId(taskGroupId);
		taskEntity.setCycle(cycle);
		taskEntity.setDatabaseSrc(databaseSrc);
		taskEntity.setIfVal(ifVal);
		taskEntity.setOwner(owner);

		Integer limit = CommonUtil.parseInt(req
				.getParameter(Const.PARAM_PAGE_SIZE));
		Integer offset = CommonUtil.parseInt(req
				.getParameter(Const.PARAM_PAGE_NUM));
		if (null != offset) {
			offset = (offset - 1) * limit;
		}
		String sort = CommonUtil.parseStr(req
				.getParameter(Const.PARAM_PAGE_SORT));

		JSONObject obj = new JSONObject();
		obj.element("count", taskService.searchTaskCount(taskEntity));
		JSONArray jsonArray = new JSONArray();
		for (TaskEntity entity : taskService.searchTask(taskEntity, limit,
				offset, sort)) {
			JSONObject jsonObjectTmp = JSONObject.fromObject(entity);
			jsonArray.add(jsonObjectTmp);
		}
		obj.elementOpt("tasks", jsonArray);
		jsonObject = CommonUtil.getPubJson(obj);
		return Action.SUCCESS;
	}

	/**
	 * 拓扑-搜索任务
	 * 
	 * @return
	 */
	public String searchTaskRelaStatus() {
		HttpServletRequest req = ServletActionContext.getRequest();

		String startDate = CommonUtil.parseStr(req.getParameter("startDate"));
		if (null == startDate) {
			startDate = new DateTime().toString("yyyy-MM-dd");
		}

		String endDate = CommonUtil.parseStr(req.getParameter("endDate"));
		if (null == endDate) {
			endDate = new DateTime().toString("yyyy-MM-dd");
		}

		TaskStatusEntity taskStatusEntity = new TaskStatusEntity();
		taskStatusEntity.setTask_status_id(CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_RELA_STATUS_ID)));
		taskStatusEntity.setTask_id(CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_ID)));
		taskStatusEntity.setTask_name(CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_NAME)));
		taskStatusEntity.setCycle(CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_CYCLE)));
		taskStatusEntity.setStatus(CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_STATUS)));
		taskStatusEntity.setOwner(CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_OWNER)));

		taskStatusEntity.setPrio_lvl(CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_PRIO_LVL)));
		taskStatusEntity.setOnlyself(CommonUtil.parseInt(req
				.getParameter(Const.FLD_ONLYSELF)));

		JSONArray jsonArray = null;

		if (startDate.equals(endDate)) {
			jsonArray = taskService.getTaskStatusJsonObject(startDate, null,
					taskStatusEntity);
		} else if (taskStatusEntity.getTask_name().matches(
				"^\\d+(\\s*,\\s*\\d+)*$")) {
			// if time interval is used and taskId is provided
			jsonArray = getAllIsolateTasks(startDate, endDate, taskStatusEntity);
		}

		if (jsonArray == null) {
			throw new RuntimeException("系统出错");
		} else {
			jsonObject = CommonUtil.getPubJson(jsonArray);
		}
		return Action.SUCCESS;
	}

	private JSONArray getAllIsolateTasks(String startDate, String endDate,
			TaskStatusEntity taskStatusEntity) {
		List<TaskStatusEntity> taskStatusList = taskService
				.getTaskStatusWithTimeInterval(startDate, endDate,
						taskStatusEntity);
		JSONArray jsonArray = new JSONArray();

		for (TaskStatusEntity taskStatus : taskStatusList) {
			String taskStatusID = taskStatus.getTask_status_id();

			String cycle = taskStatus.getCycle();
			Integer hour = new DateTime(taskStatus.getTrigger_time())
					.getHourOfDay();

			String text = cycle.equals("H") ? taskStatus.getTask_id() + "."
					+ taskStatus.getTime_id() + "." + hour : taskStatus
					.getTask_id() + "." + taskStatus.getTime_id();

			JSONObject jObject = new JSONObject();

			jObject.element("id", taskStatusID)
					.element("status", taskStatus.getStatus())
					.element("text", text).element("children", "[]")
					.element("parents", "[]").element("isTarget", 0);
			jsonArray.add(jObject);
		}

		return jsonArray;
	}

	/**
	 * 新增任务
	 * 
	 * @return
	 */
	public String addOrModifyTask() {
		HttpServletRequest req = ServletActionContext.getRequest();

		Integer taskId = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_ID));
		String taskName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_NAME));
		String cycle = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_CYCLE));
		String databaseSrc = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_DATABASE_SRC));
		String freq = CommonUtil
				.parseStr(req.getParameter(Const.FLD_TASK_FREQ));
		Integer ifRecall = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_IF_RECALL));
		Integer ifVal = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_IF_VAL));
		Integer ifWait = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_IF_WAIT));
		String offset = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_OFFSET));
		String offsetType = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_OFFSET_TYPE));
		String owner = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_OWNER));
		String para1 = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_PARA1));
		String para2 = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_PARA2));
		String para3 = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_PARA3));
		Integer recallLimit = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_RECALL_LIMIT));
		Integer recallInterval = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_RECALL_INTERVAL));
		Integer prioLvl = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_PRIO_LVL));
		String recallCode = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_RECALL_CODE));
		String remark = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_REMARK));
		String successCode = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_SUCCESS_CODE));
		Integer taskGroupId = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_GROUP_ID));
		Integer type = taskGroupId.equals(1) ? 1 : 2;
		String tableName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_TABLE_NAME));
		Integer timeout = CommonUtil.parseInt(req
				.getParameter(Const.FLD_TASK_TIMEOUT));
		String addUser = CommonUtil.parseStr(
				req.getParameter(Const.FLD_TASK_ADD_USER)).replace(
				"@dianping.com", "");
		String updateUser = CommonUtil.parseStr(
				req.getParameter(Const.FLD_TASK_UPDATE_USER)).replace(
				"@dianping.com", "");

		CommonUtil.validateParam(cycle, "周期");
		cycle = cycle.toUpperCase();
		// CommonUtil.validateParamRange(databaseSrc, "数据源", new
		// Object[]{Const.DATABASE_TYPE_WORMHOLE,Const.DATABASE_TYPE_HIVE,Const.DATABASE_TYPE_GP57,Const.DATABASE_TYPE_GP59});
		CommonUtil.validateParam(freq, "执行频率");
		CommonUtil
				.validateParamRange(ifRecall, "出错重新执行", new Object[] { 0, 1 });
		CommonUtil.validateParamRange(ifVal, "是否生效", new Object[] {
				Const.IF_VAL_NO, Const.IF_VAL_YES, Const.IF_VAL_UNUSE });
		CommonUtil.validateParamRange(ifWait, "依赖重新执行", new Object[] { 0, 1 });
		CommonUtil.validateParamRange(timeout, "超时时间", new Object[] { 90, 120,
				150 });
		if (offsetType.equals(Const.OFFSET_OFFSET)) {
			CommonUtil.validateParamRange(offset, "偏移量", new Object[] { "D0",
					"D1", "D2", "D3", "D4", "D5", "D6", "M0", "M1", "M2", "M3",
					"M4", "M5", "M6" });
		}
		CommonUtil.validateParamRange(offsetType, "偏移类型", new Object[] {
				Const.OFFSET_OFFSET, Const.OFFSET_APPOINT });
		CommonUtil.validateParam(owner, "开发者");
		owner = owner.toLowerCase();
		CommonUtil.validateParamRange(prioLvl, "运行级别",
				new Object[] { Const.PRIO_LVL_HIGH, Const.PRIO_LVL_MEDIUM,
						Const.PRIO_LVL_LOW });
		CommonUtil.validateParam(successCode, "成功code");
		CommonUtil.validateParamRange(taskGroupId, "任务组ID", new Object[] {
				Const.TASK_GROUP_WORMHOLE, Const.TASK_GROUP_MID_DIM,
				Const.TASK_GROUP_DM, Const.TASK_GROUP_RPT,
				Const.TASK_GROUP_MAIL, Const.TASK_GROUP_DW,
				Const.TASK_GROUP_DQ, Const.TASK_GROUP_ATOM });
		CommonUtil.validateParam(tableName, "结果表名");
		tableName = tableName.toLowerCase();

		if (!CronExpression.isValidExpression(freq)) {
			throw new RuntimeException(freq + "不是正确的CronExpression表达式");
		}

		// 前端不显示执行程序选择框，根据类型判断
		String taskObj = null;
		if (taskGroupId.equals(Const.TASK_GROUP_WORMHOLE)) {
			taskObj = Const.COMMAND_WORMHOLE;
		} else {
			if (databaseSrc.equalsIgnoreCase(Const.DATABASE_TYPE_HIVE)) {
				taskObj = Const.COMMAND_HIVE;
			} else if (databaseSrc.equalsIgnoreCase(Const.DATABASE_TYPE_GP57)
					|| databaseSrc.equalsIgnoreCase(Const.DATABASE_TYPE_GP59)) {
				taskObj = Const.COMMAND_GP;
			}
		}
		String waitCode = CommonUtil.parseStr(req
				.getParameter(Const.FLD_TASK_WAIT_CODE));

		if (taskGroupId != 1) {
			int cntDot = 0;
			for (int i = 0; i < tableName.length(); ++i) {
				if (tableName.charAt(i) == '.') {
					++cntDot;
				}
			}
			if (cntDot != 1) {
				throw new RuntimeException("结果表名格式错误，应为数据库.表或模式.表");
			}
		}

		if (ifRecall == 0) {
			recallCode = null;
		}
		if (ifWait == 0) {
			waitCode = null;
		}

		// if (ifRecall == 1 && (recallCode == null || recallCode.isEmpty())) {
		// throw new RuntimeException("重新执行code不能为空");
		// }
		if (ifWait == 1 && (waitCode == null || waitCode.isEmpty())) {
			throw new RuntimeException("依赖执行code不能为空");
		}
		if (taskId == null && taskService.isTaskNameDuplicated(taskName)) {
			throw new RuntimeException("任务名称已存在");
		}

		TaskEntity taskEntity = new TaskEntity();
		taskEntity.setTaskId(taskId);
		taskEntity.setCycle(cycle);
		taskEntity.setDatabaseSrc(databaseSrc);
		taskEntity.setFreq(freq);
		taskEntity.setIfRecall(ifRecall);
		taskEntity.setIfVal(ifVal);
		taskEntity.setIfWait(ifWait);
		taskEntity.setOffset(offset);
		taskEntity.setOffsetType(offsetType);
		taskEntity.setOwner(owner);
		taskEntity.setPara1(para1);
		taskEntity.setPara2(para2);
		taskEntity.setPara3(para3);
		taskEntity.setRecallLimit(recallLimit);
		taskEntity.setRecallInterval(recallInterval);
		taskEntity.setPrioLvl(prioLvl);
		taskEntity.setRecallCode(recallCode);
		taskEntity.setRemark(remark);
		taskEntity.setSuccessCode(successCode);
		taskEntity.setTableName(tableName);
		taskEntity.setTaskGroupId(taskGroupId);
		taskEntity.setTaskName(taskName);
		taskEntity.setTaskObj(taskObj);
		taskEntity.setTimeout(timeout);
		taskEntity.setType(type);
		taskEntity.setWaitCode(waitCode);
		taskEntity.setAddUser(addUser);
		taskEntity.setUpdateUser(updateUser);

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currTime = formatter.format(new Date());
		taskEntity.setAddTime(currTime);
		taskEntity.setUpdateTime(currTime);

		if (type == 1) {
			String taskGroupName = null;
			switch (taskGroupId) {
			case 1:
				taskGroupName = Const.TASK_GROUP_1;
				break;
			case 2:
				taskGroupName = Const.TASK_GROUP_2;
				break;
			case 3:
				taskGroupName = Const.TASK_GROUP_3;
				break;
			case 4:
				taskGroupName = Const.TASK_GROUP_4;
				break;
			case 5:
				taskGroupName = Const.TASK_GROUP_5;
				break;
			case 6:
				taskGroupName = Const.TASK_GROUP_6;
				break;
			case 7:
				taskGroupName = Const.TASK_GROUP_7;
				break;
			case 8:
				taskGroupName = Const.TASK_GROUP_8;
				break;
			default:
				throw new RuntimeException("任务组不正确");
			}
			taskEntity
					.setLogHome(Const.LOG_HOME_WORMHOLE + "/" + taskGroupName);
			taskEntity.setLogFile(tableName);
		} else if (type == 2) {
			taskEntity.setLogHome(Const.LOG_HOME_CALCULATE);
			taskEntity.setLogFile(tableName);
		}

		List<TaskRelaEntity> taskRelaEntitys = new LinkedList<TaskRelaEntity>();
		String depTaskIds = CommonUtil.parseStr(req
				.getParameter(Const.FLD_DEP_TASK_ID + "s"));
		String depCycleGaps = CommonUtil.parseStr(req
				.getParameter(Const.FLD_DEP_CYCLE_GAP + "s"));
		if (depTaskIds != null && depCycleGaps != null) {
			taskEntity.setIfPre(1);
			String[] depTaskIdArr = depTaskIds.split("&");
			String[] depCycleGapArr = depCycleGaps.split("&");
			if (depTaskIdArr.length == depCycleGapArr.length) {
				for (int i = 0; i < depTaskIdArr.length; ++i) {
					TaskRelaEntity taskRelaEntity = new TaskRelaEntity();
					Integer taskPreId = Integer.parseInt(depTaskIdArr[i]
							.substring(depTaskIdArr[i].indexOf("=") + 1));
					taskRelaEntity.setTaskPreId(taskPreId);
					taskRelaEntity.setCycleGap(taskService.getTaskById(
							taskPreId).getCycle()
							+ depCycleGapArr[i].substring(depCycleGapArr[i]
									.indexOf("=") + 1));
					taskRelaEntity.setRemark("");
					taskRelaEntitys.add(taskRelaEntity);
				}
			}
		} else {
			taskEntity.setIfPre(0);
		}

		if (taskId == null) {
			taskService.insertTaskAndTaskRela(taskEntity, taskRelaEntitys);
		} else {
			taskService.updateTaskAndTaskRela(taskEntity, taskRelaEntitys);
			if (taskEntity.getIfVal() != 1) {
				taskService.updateTaskTableStatus(taskId, "N");
			} else {
				taskService.updateTaskTableStatus(taskId, "Y");
			}
		}

		// add for cannan
		List<String> taskTargetTableList = new ArrayList<String>();
		if (tableName != null && tableName.length() > 0) {
			for (String tableTarget : tableName.split(";")) {
				taskTargetTableList.add(tableTarget.substring(tableTarget
						.indexOf(".") + 1));
			}
		}
		if (taskRelaEntitys != null && taskRelaEntitys.size() > 0) {
			List<Integer> parentTaskIdList = new ArrayList<Integer>();
			for (TaskRelaEntity taskRela : taskRelaEntitys) {
				parentTaskIdList.add(taskRela.getTaskPreId());
			}
			autoConfigService.insertDataMap(parentTaskIdList,
					taskTargetTableList);
		}
		if (taskId == null) {
			autoConfigService.insertDataTaskMap(taskEntity.getTaskId(),
					taskTargetTableList);
		}else{
			
		}
		jsonObject = CommonUtil.getPubJson(taskEntity.getTaskId());
		return Action.SUCCESS;
	}

	/**
	 * 获取任务日志
	 * 
	 * @return
	 */
	public String getTaskLogContent() {
		HttpServletRequest req = ServletActionContext.getRequest();
		String logPath = CommonUtil.parseStr(req.getParameter("logPath"));
		String logContent = taskService.getTaskLogContent(logPath);
		jsonObject = CommonUtil.getPubJson(logContent, logContent != null ? 200
				: 500);

		return Action.SUCCESS;
	}

	public JSONObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}
}
