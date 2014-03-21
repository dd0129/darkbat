package com.dianping.darkbat.service;

import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.TaskEntity;
import com.dianping.darkbat.entity.TaskRelaEntity;
import com.dianping.darkbat.entity.TaskStatusEntity;
import com.dianping.darkbat.mapper.TaskMapper;
import com.dianping.darkbat.mapper.TaskRelaMapper;
import com.dianping.darkbat.mapper.TaskStatusMapper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.api.Task;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

@Scope("singleton")
@Repository
public class TaskService {

    private static Log log = LogFactory.getLog(TaskService.class);
	@Autowired
	private TaskMapper taskMapper;

	@Autowired
	private TaskRelaMapper taskRelaMapper;

	@Autowired
	private TaskStatusMapper taskStatusMapper;

	/**
	 * 检查是否任务名称已存在
	 * @param  taskName 任务名称
	 * @return <tt>true</tt> 已存在<br>
	 *		 <tt>false</tt> 不存在
	 */
	public boolean isTaskNameDuplicated(String taskName) {
		List<TaskEntity> list = taskMapper.getTaskByName(taskName);
		if (null == list || list.isEmpty()) {
			return false;
		} else {
			return true;
		}
	}
	
	/**
	 * 获取所有任务基本信息
	 * 
	 * @return 所有任务基本信息
	 */
	public List<TaskEntity> getAllTaskBasicInfo() {
		List<TaskEntity> ret = taskMapper.getAllTaskBasicInfo();
		Collections.sort(ret, new Comparator<TaskEntity>() {
			public int compare(TaskEntity entity1, TaskEntity entity2) {
				return entity1.getTaskName().compareTo(entity2.getTaskName());
			}
		});
		return ret;
	}
	
	/**
	 * 由于taskid的自动生成规则的问题，在同一时刻只能插入一条记录，用来保证获取的taskid不会重复
	 * 
	 * @param taskEntity 任务信息
	 */
	public synchronized void insertTask(TaskEntity taskEntity){
		taskEntity.setTaskId(generateNewTaskId(taskEntity));
		taskMapper.insertTask(taskEntity);
	}

	/**
	 * 更新任务基本信息
	 * 
	 * @param taskEntity 任务信息
	 */
	public synchronized void updateTask(TaskEntity taskEntity){
		taskMapper.updateTask(taskEntity);
	}
	
	public synchronized void updateTaskTableStatus(int taskID,String status){
		taskMapper.updateTaskTableStatus(taskID, status);
	}

	/**
	 * 插入任务信息
	 * 
	 * @param taskEntity
	 * @param taskRelaEntitys
	 */
	public void insertTaskAndTaskRela(TaskEntity taskEntity, List<TaskRelaEntity> taskRelaEntitys){
		insertTask(taskEntity);
		for (TaskRelaEntity taskRelaEntity : taskRelaEntitys){
			taskRelaEntity.setTaskId(taskEntity.getTaskId());
			taskRelaMapper.insertTaskRela(taskRelaEntity);
		}
	}

	/**
	 * 更新任务信息
	 * 
	 * @param taskEntity
	 * @param taskRelaEntitys
	 */
	public void updateTaskAndTaskRela(TaskEntity taskEntity, List<TaskRelaEntity> taskRelaEntitys){
		updateTask(taskEntity);
		updateTaskRela(taskEntity.getTaskId(), taskRelaEntitys);
	}

    /************
     * 更新任务依赖信息
     * @param taskId
     * @param taskRelaEntitys
     */
    public void updateTaskRela(Integer taskId, List<TaskRelaEntity> taskRelaEntitys) {
        taskRelaMapper.deleteTaskRela(taskId);
        for (TaskRelaEntity taskRelaEntity : taskRelaEntitys) {
            taskRelaEntity.setTaskId(taskId);
            taskRelaMapper.insertTaskRela(taskRelaEntity);
        }
    }

    /*************
     * 生成任务id
     * @param taskEntity
     * @return
     */
	public int generateNewTaskId(TaskEntity taskEntity) {
		int taskGroupId = taskEntity.getTaskGroupId();
		String databaseSrc = taskEntity.getDatabaseSrc();
		int iDatabaseSrc = 0;
		if (databaseSrc.equalsIgnoreCase(Const.DATABASE_TYPE_HIVE)) {
			iDatabaseSrc = 1;
		} else if (databaseSrc.equalsIgnoreCase(Const.DATABASE_TYPE_GP57)) {
			iDatabaseSrc = 2;
		} else if (databaseSrc.equalsIgnoreCase(Const.DATABASE_TYPE_GP59)) {
			iDatabaseSrc = 3;
		}
		List<Integer> taskIds = new LinkedList<Integer>();
		for (Integer taskId : taskMapper.getTaskId()) {
			if (taskGroupId == 1 && taskId < 100000) {
				taskIds.add(taskId);
			} else if (taskId / 100000 == taskGroupId && taskId % 10 == iDatabaseSrc) {
				taskIds.add(taskId / 10);
			}
		}
		int groupId = -1;
		for (int i = taskGroupId * 10000 + 1; i < (taskGroupId + 1) * 10000; ++i) {
			if (!taskIds.contains(i)) {
				groupId = i;
				break;
			}
		}
		if (taskGroupId != 1) {
			groupId = groupId * 10 + iDatabaseSrc;
		}
		return groupId;
	}

	/**
	 * 是否有关联任务
	 * 
	 * @param  taskId 任务ID
	 * @return <tt>true</tt> 有关联任务<br>
	 *		 <tt>false</tt> 无关联任务
	 */
	public boolean isPreTask(Integer taskId) {
		return !taskRelaMapper.getTaskRelaByTaskPreId(taskId).isEmpty();
	}

	/**
	 * 使任务失效时需检测是否有后继任务,若有后继任务，则无法使其失效
	 * 
	 * @param  taskId 任务ID
	 * @return <tt>-1</tt> 有后继任务，无法使其失效<br>
	 *		 <tt>1</tt> 置失效成功<br>
	 *		 <tt>0</tt> 置失效失败
	 */
	public int invalidateTask(Integer taskId) {
		if (!isPreTask(taskId)) {
			taskMapper.invalidateTask(taskId);
			taskRelaMapper.deleteTaskRela(taskId);
			return 1;
		} else {
			return -1;
		}
	}

	/**
	 * 获取所有满足条件的任务数量
	 * 
	 * @param taskEntity
	 * @return 所有满足条件的任务数量
	 */
	public int searchTaskCount(TaskEntity taskEntity) {
		return taskMapper.searchTaskCount(taskEntity);
	}

	/**
	 * 获取所有满足条件的任务
	 * 
	 * @param task
	 * @param limit
	 * @param offset
	 * @param sort
	 * @return
	 */
	public List<TaskEntity> searchTask(TaskEntity task, Integer limit, Integer offset, String sort) {
		return taskMapper.searchTask(task, limit, offset, sort);
	}

	/**
	 * 获取当前任务所依赖的任务的信息
	 * 
	 * @param taskId
	 * @return
	 */
	public List<TaskRelaEntity> getTaskRelaByTaskId(Integer taskId) {
		return taskRelaMapper.getTaskRelaByTaskId(taskId);
	}
	
	/**
	 * 获取当前任务所依赖的任务的信息
	 * 
	 * @param taskId
	 * @return
	 */
	public List<Integer> getTaskRelaIdByTaskId(Integer taskId) {
		return taskRelaMapper.getTaskRelaIdByTaskId(taskId);
	}

	/**
	 * 获取依赖当前任务的任务
	 * 
	 * @param taskPreId
	 * @return
	 */
	public List<TaskRelaEntity> getTaskRelaByTaskPreId(Integer taskPreId) {
		return taskRelaMapper.getTaskRelaByTaskPreId(taskPreId);
	}

	/**
	 * 获取所有依赖任务的信息
	 * 
	 * @param taskId 任务ID
	 * @return
	 */
	public List<TaskEntity> getPreTaskInfoByTaskId(Integer taskId) {
		return taskRelaMapper.getPreTaskInfoByTaskId(taskId);
	}

	public JSONArray getTaskStatusJsonObject(String date, List<String> taskStatusId, TaskStatusEntity taskStatusEntity) {
		List<TaskStatusEntity> taskStatus = null;
		if (taskStatusEntity != null) {
			List<TaskStatusEntity> taskStatus1 = getTaskStatus(date, null,taskStatusEntity);
			List<TaskStatusEntity> taskStatus2 = taskStatusMapper.getTaskStatusEntityByParam(date, taskStatusEntity);
			taskStatus = new LinkedList<TaskStatusEntity>();
			boolean isFirst = true;
			
			while(taskStatus2 != null && taskStatus2.size() > 0){
				List<TaskStatusEntity> taskStatus3 = new LinkedList<TaskStatusEntity>();
				for(TaskStatusEntity var:taskStatus1){
					for(TaskStatusEntity var1:taskStatus2){
						if(var.getTask_status_id().equals(var1.getTask_status_id())){
							if(isFirst)
								var.setTarget(true);
							taskStatus.add(var);
						}
						else if(var.getParents() != null && var.getParents().contains(var1.getTask_status_id())){
							boolean isExist = false;
							for(TaskStatusEntity var2:taskStatus2){
								if(var2.getTask_status_id().equals(var.getTask_status_id())){
									isExist = true;
									break;
								}
							}
							for(TaskStatusEntity var3:taskStatus){
								if(var3.getTask_status_id().equals(var.getTask_status_id())){
									isExist = true;
									break;
								}
							}
							if(!isExist){
								taskStatus3.add(var);
							}
						}
					}
				}
				taskStatus2 = taskStatus3;
				isFirst = false;
			}
			for(TaskStatusEntity var:taskStatus){
				if(var.getParents() != null){
					List<String> parents = new LinkedList<String>();
					for(TaskStatusEntity var1:taskStatus){
						if(var.getParents().contains(var1.getTask_status_id())){
							parents.add(var1.getTask_status_id());
						}
					}
					var.setParents(parents);
				}
			}
		} else {
			taskStatus = getTaskStatus(date, taskStatusId,null);
		}
		
		if(taskStatus == null){
			return null;
		} else {
			JSONArray jsonArray = new JSONArray();
			for(TaskStatusEntity var:taskStatus){
				JSONObject jsonObject = new JSONObject();

				jsonObject.element("id", var.getTask_status_id());
				jsonObject.element("status", var.getStatus()==null?"":var.getStatus());
                if(var.getCycle().equals("H")){
                    Long triggerTime = var.getTrigger_time();
                    DateTime jodaTime = new DateTime(triggerTime);
                    jsonObject.element("text", var.getTask_status_id().substring(0, var.getTask_status_id().length()-10).concat(".")
                            .concat(String.valueOf(jodaTime.getHourOfDay())));
                }else{
                    jsonObject.element("text", var.getTask_status_id().substring(0, var.getTask_status_id().length()-10));
                }

                if(var.getChildren() != null && var.getChildren().size() > 0){
                    StringBuffer children = new StringBuffer();
                    children.append("[");
                    for(String var2:var.getChildren())
                        children.append("\"").append(var2).append("\"").append(",");
                    children.replace(children.length()-1, children.length(), "]");
                    jsonObject.element("children", children.toString());
                } else {
                    jsonObject.element("children", "[]");
                }

				if(var.getParents() != null && var.getParents().size() > 0){
					StringBuffer parents = new StringBuffer();
					parents.append("[");
					for(String var1:var.getParents())
						parents.append("\"").append(var1).append("\"").append(",");
					parents.replace(parents.length()-1, parents.length(), "]");
					jsonObject.element("parents", parents.toString());
				} else {
					jsonObject.element("parents", "[]");
				}

                jsonObject.element("isTarget", var.isTarget()?1:0);

				jsonArray.add(jsonObject);
			}
			
			return jsonArray;
		}
	}

	/**
	 * 根据任务ID查找任务
	 * 
	 * @param taskId 任务ID
	 * @return
	 */
	public TaskEntity getTaskById(Integer taskId) {
		return taskMapper.getTaskById(taskId);
	}

	public List<TaskStatusEntity> getTaskStatus(String date, List<String> taskStatusId,TaskStatusEntity taskStatusEntity) {
		List<TaskStatusEntity> taskStatus = null;
		List<Map<String,Object>> taskStatusRela =null;
		
		if (date != null) {
			taskStatus = taskStatusMapper.getTaskStatusEntityByDate(date,taskStatusEntity);
		} else if(taskStatusId != null && taskStatusId.size() > 0) {
			taskStatus = taskStatusMapper.getTaskStatusEntityByTaskStatusId(taskStatusId);
		}
		
		if(taskStatus != null && taskStatus.size() > 0){
			if(date != null){
				List<String> paramTaskStatusId = new LinkedList<String>();
				for(TaskStatusEntity var:taskStatus){
					paramTaskStatusId.add(var.getTask_status_id());
				}
				if(paramTaskStatusId.size() > 0)
					taskStatusRela = taskStatusMapper.getTaskRelaStatusByTaskStatusId(paramTaskStatusId);
			} else if(taskStatusId != null && taskStatusId.size() > 0){
				taskStatusRela = taskStatusMapper.getTaskRelaStatusByTaskStatusId(taskStatusId);
			}
		}
		
		if(taskStatusRela != null && taskStatusRela.size() > 0){
			List<String> paramTaskStatusId = new LinkedList<String>();
			for(Map<String,Object> var:taskStatusRela){
				boolean isExist = false;
				for(TaskStatusEntity var1:taskStatus){
					if(var1.getTask_status_id().equals((String)var.get("task_status_id"))){
						if(var1.getParents() == null)
							var1.setParents(new LinkedList<String>());
						var1.getParents().add((String)var.get("pre_sts_id"));
					} else if(var1.getTask_status_id().equals((String) var.get("pre_sts_id"))){
						isExist = true;
						if(var1.getChildren() == null)
							var1.setChildren(new LinkedList<String>());
						var1.getChildren().add((String)var.get("task_status_id"));
					}
				}
				if(!isExist)
					paramTaskStatusId.add((String)var.get("pre_sts_id"));
			}
			if(paramTaskStatusId != null && paramTaskStatusId.size() > 0)
				taskStatus.addAll(taskStatusMapper.getTaskStatusEntityByTaskStatusId(paramTaskStatusId));
		}
		
		return taskStatus;
	}
	
	public JSONArray getPrerunTaskStatusEntityByTaskId(String taskId){
	    List<TaskStatusEntity> taskStatus= taskStatusMapper.getPrerunTaskStatusEntityByTaskId(taskId);
	    if (taskStatus == null || taskStatus.isEmpty()) {
            return null;
        } else {
            JSONArray jsonArray = new JSONArray();
            for (TaskStatusEntity var : taskStatus) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.element("task_name", var.getTask_name());
                jsonObject.element("task_status_id", var.getTask_status_id());
                jsonObject.element("task_id", var.getTask_id());
                jsonObject.element("status", var.getStatus());
                jsonObject.element("sts_desc", var.getSts_desc());
                jsonObject.element("cal_dt", var.getCal_dt());
                jsonArray.add(jsonObject);
            }
            return jsonArray;
        }
	}
	
	public JSONArray getTaskStatusInfoJsonObject(List<String> taskStatusId) {
		List<TaskStatusEntity> taskStatus = getTaskStatusInfo(taskStatusId);
		if (taskStatus == null || taskStatus.isEmpty()) {
			return null;
		} else {
			JSONArray jsonArray = new JSONArray();
			for (TaskStatusEntity var : taskStatus) {
				JSONObject jsonObject = new JSONObject();
				jsonObject.element("task_name", var.getTask_name());
				jsonObject.element("task_status_id", var.getTask_status_id());
                jsonObject.element("task_id", var.getTask_id());
				jsonObject.element("status", var.getStatus());
				jsonObject.element("log_path", var.getLog_path());
				jsonObject.element("cycle", var.getCycle());
				jsonObject.element("owner", var.getOwner());
				jsonObject.element("recall_num", var.getRecall_num());
				jsonObject.element("run_num", var.getRun_num());
				jsonObject.element("start_time", var.getStart_time());
				jsonObject.element("end_time", var.getEnd_time());
				jsonObject.element("time_id", var.getTime_id());
                jsonObject.element("prio_lvl", var.getPrio_lvl()<=1?"高":var.getPrio_lvl()==2?"中":"低");
				jsonArray.add(jsonObject);
			}
			return jsonArray;
		}
	}
	
	public List<TaskStatusEntity> getTaskStatusInfo(List<String> taskStatusId) {
		List<TaskStatusEntity> taskStatus = null;
		if (taskStatusId != null && taskStatusId.size() > 0) {
			taskStatus = taskStatusMapper.getTaskStatusEntityInfoByTaskStatusId(taskStatusId);
		}
		return taskStatus;
	}
	
	public Integer updateTaskPrioLvlForMasterdata(List<Integer> taskIds,Integer lvl){
		Integer res = 0;
		
		List<Integer> newTaskIds = new LinkedList<Integer>();
		List<Integer> allTaskIds = new LinkedList<Integer>();
		newTaskIds.addAll(taskIds);
		while(newTaskIds.size() > 0){
			Integer taskId = newTaskIds.remove(0);
			allTaskIds.add(taskId);
			List<Integer> resTaskIds = getTaskRelaIdByTaskId(taskId);
			for(Integer resTaskId:resTaskIds){
				if(allTaskIds.contains(resTaskId)){
					continue;
				}
				if(newTaskIds.contains(resTaskId)){
					continue;
				}
				newTaskIds.add(resTaskId);
			}
		}
		
		for(Integer taskId:allTaskIds){
			TaskEntity taskEntity = getTaskById(taskId);
			if(taskEntity.getPrioLvl() > lvl){
				taskEntity.setPrioLvl(lvl);
				updateTask(taskEntity);
				++res;
			}
		}
		
		return res;
	}
	
	public List<TaskStatusEntity> getTaskStatusWithTimeInterval(String startDate, String endDate, TaskStatusEntity taskStatusEntity) {
		String[] taskIdsArray = taskStatusEntity.getTask_name().split(",");
		return taskStatusMapper.getTaskStatusWithTimeInterval(startDate, endDate, Arrays.asList(taskIdsArray), taskStatusEntity);
	}

	public String getTaskLogContent(String logPath) {
		final String logDir = "/data/deploy/dwarch/log";
		final String logUrl = "http://10.1.6.151";
		logPath = logPath.replace(logDir,  logUrl);
		
		try {
			URL url = new URL(logPath);
			URLConnection urlconn = url.openConnection();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
					urlconn.getInputStream(), "UTF-8"));
			StringBuffer stringBuffer = new StringBuffer();
			String line = bufferedReader.readLine();
			while (line != null) {
				stringBuffer.append(line + "<br>");
				line = bufferedReader.readLine();
			}
			return stringBuffer.toString();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return null;
		}
	}

    public String getAllRunningTaskIds(List<String> taskIds) {
        String runningTaskIds = "";
        List<TaskStatusEntity> taskStatusEntities = taskStatusMapper
                .getAllRunningTaskIds(taskIds);
        for (TaskStatusEntity taskStatus : taskStatusEntities) {
            runningTaskIds += taskStatus.getTask_id() + ",";
        }
        if (runningTaskIds.length() != 0) {
            return runningTaskIds.substring(0, runningTaskIds.length() - 1);
        } else {
            return runningTaskIds;
        }
    }
    
    /**
     * 获取挂起任务的id(字符串)
     * 
     * @param taskIds
     * @return 挂起的taskid字符串
     */
    public String getSuspendTaskIds(List<String> taskIds) {
        String suspendingTaskIds = "";
        List<TaskStatusEntity> taskStatusEntities = taskStatusMapper
                .getSuspendTaskIds(taskIds);
        for (TaskStatusEntity taskStatus : taskStatusEntities) {
            suspendingTaskIds += taskStatus.getTask_id() + ",";
        }
        if (suspendingTaskIds.length() != 0) {
            return suspendingTaskIds.substring(0, suspendingTaskIds.length() - 1);
        } else {
            return suspendingTaskIds;
        }
    }
    
    /**
     * 判断任务是否都已成功跑完
     * 
     * @param taskIds
     * @return 
     */
    public boolean isAllRunSuccessfully(List<Integer> taskIds, String date) {
        List<TaskStatusEntity> taskStatusEntities = taskStatusMapper.getSuccessTaskIds(taskIds, date);
        if (taskIds.size() == taskStatusEntities.size())
            return true;
        return false;
    }
}
