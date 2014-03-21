package com.dianping.darkbat.service;

import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.TaskChildToParentEntity;
import com.dianping.darkbat.entity.TaskRelaStatusEntity;
import com.dianping.darkbat.entity.TaskStatusEntity;
import com.dianping.darkbat.exception.EndlessLoopError;
import com.dianping.darkbat.mapper.TaskRelaStatusMapper;
import com.dianping.darkbat.mapper.TaskStatusMapper;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import java.util.*;

@Scope("singleton")
@Repository
public class TopologyService {
	@Autowired
	private TaskRelaStatusMapper taskRelaStatusMapper;
	@Autowired
	private TaskStatusMapper taskStatusMapper;
	
	/**
	 * 获取单个任务所有前驱或后驱的JsonArray
	 * @param taskRelaMap				任务ID与任务上下文IDMap
	 * @param taskOppositRelaMap		相反的任务ID与任务上下文IDMap
	 * @param taskStatusID				任务ID
	 * @param type						前驱；后驱
	 * @return
	 */
	public JSONArray getAllTaskRelaStatusJsonArray(Map<String, List<String>> taskRelaMap, Map<String, List<String>> taskOppositRelaMap, String taskStatusID, String type) {
		JSONArray jsonArray = new JSONArray();
				
		List<String> preOrPostIdList = taskRelaMap.get(taskStatusID);
		while(null != preOrPostIdList && 0 < preOrPostIdList.size()) {
			Set<String> idSet = new HashSet<String>();
			
			for(String id: preOrPostIdList) {
				JSONObject jObject = new JSONObject();
				jObject.element("id", id)
					   .element("text", getTaskText(id))
					   .element("isTarget", 0);
				
				List<String> reList = taskRelaMap.get(id) == null ? new ArrayList<String>() : taskRelaMap.get(id);
				
				if(type.equals(Const.TASK_RELA_PRE)) {
					jObject.element("children", taskOppositRelaMap.get(id))
						.element("parents", reList);
				} else if(type.equals(Const.TASK_RELA_POST)) {
					jObject.element("children", reList)
						.element("parents", taskOppositRelaMap.get(id));
				}

				jsonArray.add(jObject);

				idSet.addAll(reList);
			}
			preOrPostIdList = new ArrayList<String>();
			preOrPostIdList.addAll(idSet);	
		}
		return jsonArray;
	}

	/**
	 * 任务是否可被重跑
	 * @param status	任务状态值
	 * @return
	 */
	public boolean isEditable(Integer status) {
		return status == Const.TASK_STATUS_FAIL 
				|| status == Const.TASK_STATUS_SUCCESS 
				|| status == Const.TASK_STATUS_SUSPEND 
				|| status == Const.TASK_STATUS_WAIT;
	}
	
	public Integer rerunTaskByID(String taskStatusId) {
		return taskStatusMapper.rerunTaskByID(taskStatusId);
	}

    public Integer raisePriorityByID(String taskStatusId) {
		return taskStatusMapper.raisePriorityByID(taskStatusId);
	}
	
	/**
	 * 获取实例任务的依赖关系
	 * @param taskStatusId
	 * @param preStatusId
	 * @return
	 */
	public List<TaskRelaStatusEntity> getTaskRelaStatus(String taskStatusId, String preStatusId) {
			return taskRelaStatusMapper.getTaskRelaStatus(taskStatusId, preStatusId);
	}
	
	/**
	 * 获取任务ID与任务状态的对应Map
	 * @param taskStatusEntities	任务状态实体列表
	 * @return
	 */
	public Map<String, TaskStatusEntity> getTaskStatusMap(List<TaskStatusEntity> taskStatusEntities) {
		Map<String, TaskStatusEntity> taskStatusMap = new HashMap<String, TaskStatusEntity>();
		
		for(TaskStatusEntity entity: taskStatusEntities) {
			taskStatusMap.put(entity.getTask_status_id(), entity);
		}
		
		return taskStatusMap;
	}
	
	/**
	 * 获取拓扑图上节点显示值
	 * @param taskStatusId
	 * @return
	 */
	public String getTaskText(String taskStatusId) {
		return taskStatusId.substring(0, taskStatusId.length() - 10);
	}
	

	/**
	 * 修改任务状态
	 * @param statusAllowList
	 * @param taskStatusId
	 * @param status
	 * @return
	 */
	public Integer modifyTaskByID(String statusAllowList, String taskStatusId, Integer status) {
		return taskStatusMapper.modifyTaskByID(statusAllowList, taskStatusId, status);
	}
	
	/**
	 * 批量任务重跑
	 * @param idsString			需要被重跑的任务列表字符串
	 * @return
	 */
	public Integer rerunMultiJobs(String startDate, String endDate, String idsString) {
		return taskStatusMapper.rerunMultiJobs(startDate, endDate, idsString);
	}
	
	/**
	 * 获取某天的所有父辈依赖
	 * @param taskRelaStatusEntities
	 * @param date
	 * @return
	 */
	public Map<String, List<String>> getPreTaskRelaMap(List<TaskRelaStatusEntity> taskRelaStatusEntities, String date) {
		Map<String, List<String>> taskPreMap = new HashMap<String, List<String>>();
		
		for(TaskRelaStatusEntity entity: taskRelaStatusEntities) {
			//构造任务与父任务的Map
			List<String> preIdList = taskPreMap.get(entity.getTaskStatusId());
			if(null == preIdList) {
				preIdList = new ArrayList<String>();
				preIdList.add(entity.getPreStsId());
				taskPreMap.put(entity.getTaskStatusId(), preIdList);
			} else {
				preIdList.add(entity.getPreStsId());
			}
		}
		
		return taskPreMap;
	}
	
	/**
	 * 构造任务id至父亲节点列表Map
	 * @param childToParentEntities		对应关系
	 * @return
	 */
	public Map<String, List<String>> getTaskIdToParentMap(List<TaskChildToParentEntity> childToParentEntities) {
		Map<String, List<String>> idToParentMap = new HashMap<String, List<String>>();

		for(TaskChildToParentEntity entity: childToParentEntities) {
			String cId = entity.getChildId();
			List<String> pList = idToParentMap.get(cId);
			
			if(null == pList) {
				pList = new ArrayList<String>();
				idToParentMap.put(cId, pList);
			}
			
			if (null != entity.getParentId() ) {
				pList.add(entity.getParentId());
			}
		}
		return idToParentMap;
	}

    /**
     * 构造任务id至父亲节点列表Map
     * @param childToParentEntities		对应关系
     * @return
     */
    public Map<String, List<String>> getTaskIdToParentLongPathMap(List<TaskChildToParentEntity> childToParentEntities) {
        Map<String, List<String>> idToParentMap = new HashMap<String, List<String>>();
        Map<String,Long> lastEndTimeMap = new HashMap<String, Long>();

        for(TaskChildToParentEntity entity: childToParentEntities) {
            String cId = entity.getChildId();
            List<String> pList = idToParentMap.get(cId);

            if(null == pList) {
                pList = new ArrayList<String>();
                idToParentMap.put(cId, pList);
            }

            if(entity.getParentEndTime()!=null && (lastEndTimeMap.size() == 0 || lastEndTimeMap.get(cId) == null)){
                pList.add(entity.getParentId());
                lastEndTimeMap.put(cId,entity.getParentEndTime());
            }

            if (null != entity.getParentId() && null != entity.getParentEndTime() && lastEndTimeMap.get(cId)<entity.getParentEndTime()) {
                pList.clear();
                pList.add(entity.getParentId());
                lastEndTimeMap.remove(cId);
                lastEndTimeMap.put(cId,entity.getParentEndTime());
            }
        }

        return idToParentMap;
    }
	
	/**
	 * 构造任务id至直接孩子节点列表Map
	 * @param childToParentEntities		对应关系
	 * @return
	 */
	public Map<String, List<String>> getTaskIdToChildMap(List<TaskChildToParentEntity> childToParentEntities) {
		Map<String, List<String>> idToChildMap = new HashMap<String, List<String>>();
		
		for(TaskChildToParentEntity entity: childToParentEntities) {
			String pId = entity.getParentId();
			if (null != pId) {
				List<String> cList = idToChildMap.get(pId);

				if (null == cList) {
					cList = new ArrayList<String>();
					idToChildMap.put(pId, cList);
				}
				cList.add(entity.getChildId());
			}
		}
		
		return idToChildMap;
	}
	
	/**
	 * 根据节点ID获取其所有的孩子或父亲节点
	 * 
	 * @param idToParentMap		节点id与直接父亲对应Map
	 * @param idToChildMap		节点id与直接孩子对应Map
	 * @param taskStatusID
	 * @param type				遍历父亲还是孩子
	 * @return
	 */
	public List<TaskStatusEntity> getTaskContextEntities(Map<String, List<String>> idToParentMap, Map<String, List<String>> idToChildMap, String taskStatusID, String type) {
		List<TaskStatusEntity> taskStatusEntities = new ArrayList<TaskStatusEntity>();
		
		List<String> idList = new ArrayList<String>();
		idList.add(taskStatusID);
		
		int MAX_LOOP_COUNT = 100000000, counter = 0;
		while(idList.size() > 0) {
		    counter++;
		    if (counter >= MAX_LOOP_COUNT) {
		        throw new EndlessLoopError("查询失败, 可能包含循环依赖的任务!");
		    }
		    String currentId = idList.remove(0);
			List<String> contextIdList;
			
			//如果是后继遍历，获取节点的孩子列表；如果是前驱遍历，获取节点的父亲列表
			if(type.equals(Const.TASK_RELA_PRE))
				contextIdList = idToParentMap.get(currentId);
			else
				contextIdList = idToChildMap.get(currentId);
			
			if(null != contextIdList) {
				idList.addAll(contextIdList);
				
				for(String id: contextIdList) {
					TaskStatusEntity entity = new TaskStatusEntity();
					
					entity.setTask_status_id(id);
					entity.setChildren(idToChildMap.get(id));
					entity.setParents(idToParentMap.get(id));
					
					taskStatusEntities.add(entity);
				}
			}
		}
		
		return taskStatusEntities;
	}
	
	/**
	 * 获取任务子任务与父任务对应关系
	 * @param date
	 * @return
	 */
	public List<TaskChildToParentEntity> getChildToParent(String date) {
		return taskRelaStatusMapper.getChildToParent(date);
	}
	
	/**
	 * 获取某天的所有的孩子依赖
	 * @param taskRelaStatusEntities
	 * @param date
	 * @return
	 */
	public Map<String, List<String>> getPostTaskRelaMap(List<TaskRelaStatusEntity> taskRelaStatusEntities, String date) {
		Map<String, List<String>> taskPostMap = new HashMap<String, List<String>>();
		
		for(TaskRelaStatusEntity entity: taskRelaStatusEntities) {
			
			//构造任务与子任务的Map
			List<String> postIdList = taskPostMap.get(entity.getPreStsId());
			if(null == postIdList) {
				postIdList = new ArrayList<String>();
				postIdList.add(entity.getTaskStatusId());
				taskPostMap.put(entity.getPreStsId(), postIdList);
			} else {
				postIdList.add(entity.getTaskStatusId());
			}
		}
		
		return taskPostMap;
	}


}
