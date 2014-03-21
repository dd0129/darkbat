package com.dianping.darkbat.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.TaskEntity;

public interface TaskMapper {

    /**
     * 获取所有任务
     * 
     * @return  所有任务
     */
    List<TaskEntity> getTask();

    /**
     * 获取所有任务ID
     * 
     * @return  所有任务ID
     */
    List<Integer> getTaskId();

    /**
     * 根据任务ID获取任务信息
     * 
     * @param taskId
     * @return	任务信息
     */
    TaskEntity getTaskById(Integer taskId);

    /**
     * 添加任务
     * 
     * @param taskEntity
     */
    void insertTask(TaskEntity taskEntity);

    /**
     * 根据任务名称精确搜索
     * 
     * @param taskName 任务名称
     * @return
     */
    List<TaskEntity> getTaskByName(@Param("taskName") String taskName);

    /**
     * 获取所有任务基本信息
     * 
     * @return
     */
    List<TaskEntity> getAllTaskBasicInfo();

    /**
     * 查询任务
     * 
     * @param task
     * @param limit
     * @param offset
     * @param sort
     * @return
     */
    List<TaskEntity> searchTask(@Param("task") TaskEntity task, @Param("limit") Integer limit, @Param("offset") Integer offset, @Param("sort") String sort);

    /**
     * 查询任务count
     * 
     * @param task
     * @return
     */
    int searchTaskCount(@Param("task") TaskEntity task);
    
    List<String> getParentTableList(Integer taskId);
    
    List<String> getTargetTableList(Integer taskId);
    
    List<Integer> getParentTaskIdList(List<String> taskParentTableList);
    
    List<String> getTaskParentTableList(List<Integer> taskParentTaskIdList);
    
    void insertDataTaskMap(@Param("tableId") Integer tableId, @Param("taskId") Integer taskId);
    
    void insertDataMap(@Param("tableId") Integer tableId, @Param("parentTableId") Integer parentTableId);
    
    Integer getTableIdFromName(@Param("tableName") String tableName);

    Integer updateTask(@Param("task") TaskEntity task);
    
    void updateTaskTableStatus(@Param("taskId") int taskId, @Param("status") String status);
    
    Integer invalidateTask(Integer taskId);

    String searchRuleByDB(String databaseName);
    
    List<ColumnEntity> getColumnList( @Param("tableName") String tableName);
}
