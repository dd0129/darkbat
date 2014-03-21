package com.dianping.darkbat.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionInfo;
import ch.ethz.ssh2.Session;

import com.dianping.darkbat.common.GlobalResources;
import com.dianping.darkbat.entity.TaskRelaEntity;
import com.dianping.darkbat.mapper.TaskMapper;
import com.dianping.darkbat.mapper.TaskRelaMapper;

@Scope("singleton")
@Repository
public class AutoConfigService {

	private static Logger log = Logger.getLogger(AutoConfigService.class);

	public void preExcutor(String dolName, int taskId,String group,String product) throws Exception {
		
		String para = "";
		if (StringUtils.isNotBlank(group)) {
			para += " -g " + group;
			if (StringUtils.isNotBlank(product)) {
				para += " -p " + product;
			}
		}
		
		String command = (StringUtils.isNotBlank(para)) ? GlobalResources.COMMAND_SUN + para + " -o "  : GlobalResources.COMMAND_CANAAN + " -p ";
		String conaanCommand = "/usr/bin/ssh -o ConnectTimeout=3 -o ConnectionAttempts=5 -o PasswordAuthentication=no -o StrictHostKeyChecking=no -p "
				+ GlobalResources.DEPLOY_PORT
				+" -i " 
				+GlobalResources.IDENTITY_FILE
				+ " "
				+ GlobalResources.DEPLOY_USER
				+ "@"
				+ GlobalResources.DEPLOY_IP
				+ " "
				+ command
				+ " -dol "
				+ dolName
				+ " -d 3000-12-31 -tid "
				+ Integer.toString(taskId);
		System.out.println(conaanCommand);
//		String[] command = new String[] { "sh", " -c ", conaanCommand};

		Process p = Runtime.getRuntime().exec(conaanCommand);
		BufferedReader br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		String res = null;
		StringBuffer resStr = new StringBuffer();
		while ((res = br.readLine()) != null) {
			System.out.println(res);
			resStr.append(res);
		}
		Integer is = p.waitFor();
		if (is == null || is != 0) {
			throw new RuntimeException(resStr.toString());
		}
		br.close();
		System.out.println(is);
	}

	@Autowired
	private TaskMapper taskMapper;

	@Autowired
	private TaskRelaMapper taskRelaMapper;

	public List<String> getSqlParentTableList(int taskId) {
		return taskMapper.getParentTableList(taskId);

	}

	public List<String> getSqlTargetTableList(int taskId) {
		return taskMapper.getTargetTableList(taskId);

	}

	public List<Integer> getTaskParentTaskIdList(int taskId) {
		List<String> sqlParentTableList = getSqlParentTableList(taskId);
		List<String> sqlTargetTableList = getSqlTargetTableList(taskId);
		Iterator<String> sqlParentTableIt = sqlParentTableList.iterator();
		Iterator<String> sqlTargetTableIt = null;
		List<String> taskParentTableList = new ArrayList<String>();
		String parentTableName = "";
		String targetTableName = "";
		boolean isParenttable;

		while (sqlParentTableIt.hasNext()) {
			isParenttable = true;
			parentTableName = (String) sqlParentTableIt.next().split("/")[1];
			sqlTargetTableIt = sqlTargetTableList.iterator();
			while (sqlTargetTableIt.hasNext()) {
				targetTableName = sqlTargetTableIt.next().split("/")[1];
				if (parentTableName.equalsIgnoreCase(targetTableName)) {
					isParenttable = false;
				}
			}
			if (isParenttable) {
				taskParentTableList.add(parentTableName);
			}
		}
		log.error("taskParentTableList == null:" + (taskParentTableList == null));
		log.error("taskParentTableList.isEmpty():" + (taskParentTableList.isEmpty()));
		log.error("taskParentTableList.size():" + (taskParentTableList.size()));
		return taskMapper.getParentTaskIdList(taskParentTableList);
	}

	public List<String> getTaskTargetTableList(int taskId) {
		List<String> sqlParentTableList = getSqlParentTableList(taskId);
		List<String> sqlTargetTableList = getSqlTargetTableList(taskId);
		Iterator<String> sqlParentTableIt = null;
		Iterator<String> sqlTargetTableIt = sqlTargetTableList.iterator();
		List<String> taskTargetTableList = new ArrayList<String>();
		String parentTableName = "";
		String targetTableName = "";
		String parentTable = "";
		String targetTable = "";
		int parentTableLine = 0;
		int targetTableLine = 0;
		boolean isTargettable = true;

		while (sqlTargetTableIt.hasNext()) {
			isTargettable = true;
			targetTable = sqlTargetTableIt.next();
			targetTableName = (String) targetTable.split("/")[1];
			targetTableLine = Integer.parseInt(targetTable.split("/")[0]);
			sqlParentTableIt = sqlParentTableList.iterator();
			while (sqlParentTableIt.hasNext()) {
				parentTable = sqlParentTableIt.next();
				parentTableName = parentTable.split("/")[1];
				parentTableLine = Integer.parseInt(parentTable.split("/")[0]);
				if (parentTableName.equalsIgnoreCase(targetTableName) && parentTableLine > targetTableLine) {
					isTargettable = false;
				}
			}
			if (isTargettable) {
				taskTargetTableList.add(targetTableName);
			}
		}
		return taskTargetTableList;
	}

	public void insertTaskRelation(String cycleGap, String remark, int taskId, List<Integer> parentTaskIdList) {
		Iterator<Integer> parentTaskIdIterator = parentTaskIdList.iterator();
		TaskRelaEntity taskRelaEntity = new TaskRelaEntity();
		taskRelaEntity.setCycleGap(cycleGap);
		taskRelaEntity.setRemark(remark);
		taskRelaEntity.setTaskId(taskId);
		while (parentTaskIdIterator.hasNext()) {
			taskRelaEntity.setTaskPreId(parentTaskIdIterator.next());
			taskRelaMapper.insertTaskRela(taskRelaEntity);
		}

	}

	public void insertDataTaskMap(Integer taskId, List<String> taskTargetTableList) {
		// Iterator<String> taskTargetTableIterator = taskTargetTableList.iterator();
		// int tableId = 0;
		// while (taskTargetTableIterator.hasNext()) {
		// tableId = taskMapper.getTableIdFromName(taskTargetTableIterator.next());
		// taskMapper.insertDataTaskMap(tableId, taskId);
		// }
		if (taskTargetTableList != null) {
			for (String taskTargetTable : taskTargetTableList) {
				Integer tableId = taskMapper.getTableIdFromName(taskTargetTable);
				if (tableId != null && taskId != null)
					taskMapper.insertDataTaskMap(tableId, taskId);
			}
		}
	}

	public void insertDataMap(List<Integer> parentTaskIdList, List<String> taskTargetTableList) {
		// Iterator<String> taskTargetTableIterator = null;
		// List<String> taskParentTableList = taskMapper.getTaskParentTableList(parentTaskIdList);
		// Iterator<String> taskParentTableIterator = taskParentTableList.iterator();
		// int tableId = 0, parentTableId = 0;
		// while (taskParentTableIterator.hasNext()) {
		// parentTableId = taskMapper.getTableIdFromName(taskParentTableIterator.next());
		// taskTargetTableIterator = taskTargetTableList.iterator();
		// while (taskTargetTableIterator.hasNext()) {
		// tableId = taskMapper.getTableIdFromName(taskTargetTableIterator.next());
		// taskMapper.insertDataMap(tableId, parentTableId);
		// }
		// }
		List<String> taskParentTableList = taskMapper.getTaskParentTableList(parentTaskIdList);
		if (taskParentTableList != null) {
			for (String taskParentTable : taskParentTableList) {
				Integer parentTableId = taskMapper.getTableIdFromName(taskParentTable);
				if (taskTargetTableList != null) {
					for (String taskTargetTable : taskTargetTableList) {
						Integer tableId = taskMapper.getTableIdFromName(taskTargetTable);
						if (parentTableId != null && tableId != null)
							taskMapper.insertDataMap(tableId, parentTableId);
					}
				}
			}
		}
	}

	public List<String> getNotExistTableList(List<String> taskTargetTableList) {
		List<String> notExistTableList = new ArrayList<String>();
		Iterator<String> taskTargetTableIterator = taskTargetTableList.iterator();
		String tableName = "";
		while (taskTargetTableIterator.hasNext()) {
			tableName = taskTargetTableIterator.next();
			if (taskMapper.getTableIdFromName(tableName) == null)
				notExistTableList.add(tableName);
		}
		return notExistTableList;

	}
}
