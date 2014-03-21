package com.dianping.darkbat.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.dianping.darkbat.entity.TaskChildToParentEntity;
import com.dianping.darkbat.entity.TaskRelaStatusEntity;

public interface TaskRelaStatusMapper {
	List<TaskRelaStatusEntity> getTaskRelaStatus(@Param("taskStatusId") String taskStatusId, @Param("preStatusId") String preStatusId);

	List<TaskChildToParentEntity> getChildToParent(@Param("date") String date);
}
