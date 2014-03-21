package com.dianping.darkbat.mapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.dianping.darkbat.entity.TaskRelaStatusEntity;
import org.apache.ibatis.annotations.Param;

import com.dianping.darkbat.entity.TaskStatusEntity;

public interface TaskStatusMapper {

    List<TaskStatusEntity> getTaskStatusEntityByDate(@Param("date") String date,@Param("taskStatusEntity")TaskStatusEntity taskStatusEntity);
    
    List<TaskStatusEntity> getTaskStatusEntityByTaskStatusId(List<String> taskStatusId);

    List<TaskStatusEntity> getPrerunTaskStatusEntityByTaskId(String taskId);

    List<TaskStatusEntity> getTaskStatusEntityInfoByTaskStatusId(List<String> taskStatusId);

    List<TaskStatusEntity> getTaskStatusEntityByParam(@Param("date") String date, @Param("taskStatusEntity") TaskStatusEntity taskStatusEntity);

    List<TaskStatusEntity> getTaskStatusWithTimeInterval(@Param("startDate") String startDate, @Param("endDate") String endDate,
    		 @Param("taskIdsList") List<String> taskIdsList, @Param("taskStatusEntity") TaskStatusEntity taskStatusEntity);
    
    List<Map<String, Object>> getTaskRelaStatusByTaskStatusId(List<String> taskStatusId);
    
    List<TaskStatusEntity> getAllRunningTaskIds(List<String> taskIds);
    
    // by chaos
    List<TaskStatusEntity> getSuspendTaskIds(List<String> taskIds);
    List<TaskStatusEntity> getSuccessTaskIds(@Param("list") List<Integer> taskIds, @Param("date") String date);

    Integer rerunTaskByID(@Param("taskStatusId") String taskStatusId);

    Integer raisePriorityByID(@Param("taskStatusId") String taskStatusId);

    Integer updateTaskStatus(@Param("taskId") Integer taskId);
    
    /**
     * 更改实例任务状态
     * @param statusAllowList	允许变更状态的原状态任务列表
     * @param taskStatusId		任务ID
     * @param status			更改后的状态
     * @return
     */
    Integer modifyTaskByID(@Param("statusAllowList") String statusAllowList, @Param("taskStatusId") String taskStatusId, @Param("status") Integer status);

    Integer insertTaskStatusEntity(TaskStatusEntity entity);

    Integer insertTaskStatusEntityList(List<TaskStatusEntity> entityList);

    Integer insertTaskRelaStatusEntityList(List<TaskRelaStatusEntity> relaList);
    
    /**
     * 批量任务重跑
     * @param idsString			需要被重跑的任务列表字符串
     * @return
     */
    Integer rerunMultiJobs(@Param("startDate") String startDate, @Param("endDate") String endDate, @Param("idsString") String idsString);
}
