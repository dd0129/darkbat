package com.dianping.darkbat.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.dianping.darkbat.entity.TaskEntity;
import com.dianping.darkbat.entity.TaskRelaEntity;

public interface TaskRelaMapper {

    List<TaskRelaEntity> getTaskRela();

    List<TaskRelaEntity> getTaskRelaByTaskId(Integer taskId);
    
    List<Integer> getTaskRelaIdByTaskId(Integer taskId);

    List<TaskRelaEntity> getTaskRelaByParam(@Param("taskRela") TaskRelaEntity taskRelaEntity, @Param("limit") Integer limit, @Param("offset") Integer offset,
            @Param("sort") String sort);

    List<TaskRelaEntity> getTaskRelaByTaskPreId(Integer taskPreId);

    void insertTaskRela(TaskRelaEntity taskRelaEntity);

    List<TaskEntity> getPreTaskInfoByTaskId(Integer taskId);

    void deleteTaskRela(Integer taskId);
}
