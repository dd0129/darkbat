package com.dianping.darkbat.action;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.struts2.ServletActionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.TableEntity;
import com.dianping.darkbat.service.AutoETLService;
import com.dianping.darkbat.service.SalesforceMetaDataService;
import com.opensymphony.xwork2.Action;

@Repository
public class AutoETLAction {

	private static Logger log = Logger.getLogger(AutoETLAction.class);

	@Autowired
	private AutoETLService autoETLService;
	
	@Autowired
	private SalesforceMetaDataService salesforceService;


	private JSONObject jsonObject;



	/**
	 * 根据数据源类型获取所有数据库列表
	 * 
	 * 
	 * @return
	 * @throws Exception
	 */
	public String getAllDatabase() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();

		String datasourceType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATASOURCE_TYPE));
		
		List<String> list = null;
		if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
			list = autoETLService.getAllHiveDatabase();
		} else if (Const.DATASOURCE_TYPE_MYSQL.equals(datasourceType)) {
			list = autoETLService.getAllMySQLDatabase();
		} else {
			throw new RuntimeException("Unknown source datasource type: " + datasourceType);
		}
		jsonObject = CommonUtil.getPubJson(list);
		return Action.SUCCESS;
	}

	/**
	 * 指定数据源类型、数据库名和表名，进行模糊搜索表结果
	 * 
	 * @return
	 * @throws Exception
	 */
	public String searchTable() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();

		String datasourceType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATASOURCE_TYPE));
		String databaseName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATABASE_NAME));
		String tableName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TABLE_NAME));
		if (null != tableName) {
			// search时不区分大小写
			tableName = tableName.toLowerCase();
		}

		Integer limit = CommonUtil.parseInt(req.getParameter(Const.PARAM_PAGE_SIZE));
		Integer offset = CommonUtil.parseInt(req.getParameter(Const.PARAM_PAGE_NUM));
		if (null != offset) {
			offset = (offset - 1) * limit;
		}
		
		TableEntity tableEntity = new TableEntity();
		tableEntity.setDatabaseName(databaseName);
		tableEntity.setTableName(tableName);

		List<TableEntity> tableList = null;
		int tableCount = 0;
		if (Const.DATASOURCE_TYPE_MYSQL.equals(datasourceType)) {
			tableList = autoETLService.searchMySQLTable(tableEntity, limit, offset);
			tableCount = autoETLService.searchMySQLTableCount(tableEntity);
		} else if (Const.DATASOURCE_TYPE_GP.equals(datasourceType)) {
			tableList = autoETLService.searchGPTable(tableEntity, limit, offset);
			tableCount = autoETLService.searchGPTableCount(tableEntity);
		} else if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
			tableList = autoETLService.searchHiveTable(tableEntity, limit, offset);
			tableCount = autoETLService.searchHiveTableCount(tableEntity);
		} else if (Const.DATASOURCE_TYPE_SQLSERVER.equals(datasourceType)) {
			tableList = autoETLService.searchSQLServerTable(tableEntity, limit, offset);
			tableCount = autoETLService.searchSQLServerTableCount(tableEntity);
		}
		//add by hongdi for salesforce
		else if (Const.DATASOURCE_TYPE_SALESFORCE.equals(datasourceType)) {
			tableList = salesforceService.searchSalesForceTable(tableEntity, limit, offset);
			tableCount = salesforceService.searchSalesForceTableCount(tableEntity);
		} 
		else {
			throw new RuntimeException("Unknown source datasource type: " + datasourceType);
		}
		
		Map<String, String> onScheduleMap = autoETLService.searchOnSchedule(tableList);

		JSONObject obj = new JSONObject();
		obj.element("count", tableCount);
		JSONArray jsonArray = new JSONArray();
		for (TableEntity entity : tableList) {
			String tblName = entity.getTableName().toLowerCase();
			JSONObject jsonObjectTmp = JSONObject.fromObject(entity);
			String onScheduleVal = "";
			String onScheduleVal0 = onScheduleMap.get(tblName);
			String onScheduleVal1 = onScheduleMap.get("dpods_" + tblName);
			String onScheduleVal2 = onScheduleMap.get(tblName.substring(tblName.indexOf("_") + 1));
			if (onScheduleVal0 != null) {
				onScheduleVal += onScheduleVal0;
			}
			if (onScheduleVal1 != null) {
				onScheduleVal += onScheduleVal1;
			}
			if (onScheduleVal2 != null) {
				onScheduleVal += onScheduleVal2;
			}
			jsonObjectTmp.put("onSchedule", onScheduleVal);
			jsonArray.add(jsonObjectTmp);
		}
		obj.elementOpt("tables", jsonArray);
		jsonObject = CommonUtil.getPubJson(obj);

		return Action.SUCCESS;
	}

	public String getAllColumn() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();

		String datasourceType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATASOURCE_TYPE));
		String databaseName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATABASE_NAME));
		String schemaName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_SCHEMA_NAME));
		String tableName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TABLE_NAME));

		List<ColumnEntity> columnList = null;

		if (Const.DATASOURCE_TYPE_MYSQL.equals(datasourceType)) {
			columnList = autoETLService.getMySQLColumn(databaseName, tableName);
		} else {
			TableEntity tableEntity = new TableEntity();
			tableEntity.setDatasourceType(datasourceType);
			tableEntity.setDatabaseName(databaseName);
			tableEntity.setSchemaName(schemaName);
			tableEntity.setTableName(tableName);
			if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
				columnList = autoETLService.getHiveColumn(tableEntity);
			} else if (Const.DATASOURCE_TYPE_SQLSERVER.equals(datasourceType)) {
				columnList = autoETLService.getSQLServerColumn(tableEntity);
			} 
			//add by hongdi for salesforce
			else if (Const.DATASOURCE_TYPE_SALESFORCE.equals(datasourceType)) {
					columnList = salesforceService.getSalesforceColumn(tableEntity);
			}
			else {
				throw new RuntimeException("Unknown source datasource type: " + datasourceType);
			}
		}
		
		JSONObject obj = new JSONObject();
		obj.element("count", columnList.size());
		JSONArray jsonArray = new JSONArray();
		for (ColumnEntity columnEntity : columnList) {
			JSONObject jsonObjectTmp = JSONObject.fromObject(columnEntity);
			jsonArray.add(jsonObjectTmp);
		}
		obj.elementOpt("columns", jsonArray);
		jsonObject = CommonUtil.getPubJson(obj);
		return Action.SUCCESS;
	}

	public String generateDDL() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();
		
		String datasourceType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATASOURCE_TYPE));
		String databaseName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATABASE_NAME));
		String schemaName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_SCHEMA_NAME));
		String tableName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TABLE_NAME));
		
		
		String targetIsActiveSchedule = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_IS_ACTIVE_SCHEDULE));
		String targetDatasourceType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_DATASOURCE_TYPE));
		String targetSchemaTable = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_SCHEMA_TABLE));
		if (null != targetSchemaTable) {
			// 目标表保证全部小写
			targetSchemaTable = targetSchemaTable.toLowerCase();
		}
		String targetTableType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_TABLE_TYPE));
		String targetSegmentColumn = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_SEGMENT_COLUMN));
		if (null != targetSegmentColumn) {
			// 目标表字段保证全部小写
			targetSegmentColumn = targetSegmentColumn.toLowerCase();
		}

		String targetSchemaName = generateTargetSchemaName(targetIsActiveSchedule, targetDatasourceType, targetSchemaTable, targetTableType);
		
		log.info(
				"generate ddl request, " + 
				"datasourceType [" + datasourceType + "], " +
				"databaseName [" + databaseName + "], " +
				"schemaName [" + schemaName + "], " +
				"tableName [" + tableName + "], " + 
				"targetIsActiveSchedule [" + targetIsActiveSchedule + "], " + 
				"targetDatasourceType [" + targetDatasourceType + "], " +
				"targetSchemaName [" + targetSchemaName + "], " +
				"targetSchemaTable [" + targetSchemaTable + "], " + 
				"targetTableType [" + targetTableType + "], " + 
				"targetSegmentColumn [" + targetSegmentColumn + "]");

		String ddl = autoETLService.generateDDL(datasourceType, databaseName, schemaName, tableName, targetDatasourceType, targetSchemaName, targetSchemaTable, targetSegmentColumn);
		
		// 为了前台能有换行效果
		jsonObject = CommonUtil.getPubJson(ddl.replaceAll(";", ";\n"));
		
		return Action.SUCCESS;
	}
	
	public String createTable() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();
		
		String owner = CommonUtil.parseStr(req.getParameter(Const.FLD_TASK_OWNER));
		String datasourceType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATASOURCE_TYPE));
		String databaseName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATABASE_NAME));
		String schemaName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_SCHEMA_NAME));
		String tableName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TABLE_NAME));
        String writeType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_WRITE_TYPE));
		
		String targetIsActiveSchedule = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_IS_ACTIVE_SCHEDULE));
		String targetDatasourceType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_DATASOURCE_TYPE));
		String targetSchemaTable = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_SCHEMA_TABLE));
		if (null != targetSchemaTable) {
			// 目标表保证全部小写
			targetSchemaTable = targetSchemaTable.toLowerCase();
		}
		String targetTableType = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_TABLE_TYPE));
		String targetSegmentColumn = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TARGET_SEGMENT_COLUMN));
		if (null != targetSegmentColumn) {
			// 目标表字段保证全部小写
			targetSegmentColumn = targetSegmentColumn.toLowerCase();
		}
		
		String targetSchemaName = generateTargetSchemaName(targetIsActiveSchedule, targetDatasourceType, targetSchemaTable, targetTableType);
		
		log.info(
				"generate ddl request, " + 
				"datasourceType [" + datasourceType + "], " +
				"databaseName [" + databaseName + "], " +
				"tableName [" + tableName + "], " + 
				"targetIsActiveSchedule [" + targetIsActiveSchedule + "], " + 
				"targetDatasourceType [" + targetDatasourceType + "], " +
				"targetSchemaName [" + targetSchemaName + "], " +
				"targetSchemaTable [" + targetSchemaTable + "], " + 
				"targetTableType [" + targetTableType + "], " + 
				"targetSegmentColumn [" + targetSegmentColumn + "]");

//		boolean res = autoETLService.createTable2(
//				owner, targetIsActiveSchedule, targetTableType,
//				datasourceType, databaseName, schemaName, tableName,
//				targetDatasourceType, targetSchemaName, targetSchemaTable, targetSegmentColumn,writeType);
        boolean res = autoETLService.createTable2(
                owner, targetIsActiveSchedule, targetTableType,
                datasourceType, databaseName, schemaName, tableName,
                targetDatasourceType, targetSchemaName, targetSchemaTable, targetSegmentColumn);
		jsonObject = CommonUtil.getPubJson(res);
		return Action.SUCCESS;
	}


    public String checkDBRule() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();

		String databaseName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_DATABASE_NAME));
		String schemaName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_SCHEMA_NAME));
		String tableName = CommonUtil.parseStr(req.getParameter(Const.FLD_AUTOETL_TABLE_NAME));

//		boolean res = autoETLService.createTable2(
//				owner, targetIsActiveSchedule, targetTableType,
//				datasourceType, databaseName, schemaName, tableName,
//				targetDatasourceType, targetSchemaName, targetSchemaTable, targetSegmentColumn,writeType);

		//jsonObject = CommonUtil.getPubJson(res);
        Integer ret = autoETLService.checkDBRule(databaseName,tableName,schemaName);
        jsonObject = CommonUtil.getPubJson(ret);
		return Action.SUCCESS;
	}

	private String generateTargetSchemaName(String targetIsActiveSchedule, String targetDatasourceType, String targetSchemaTable, String targetTableType) {
		String targetSchemaName = "";
		// 不上调度
		if ("0".equals(targetIsActiveSchedule)) {
			targetSchemaName = "dptmp";
		}
		// 上调度
		else if ("1".equals(targetIsActiveSchedule)) {
			if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
				// 拉链表
				if (Const.TABLE_TYPE_LALIANBIAO.equals(targetTableType)) {
					targetSchemaName = "dpods";
				}
				// 历史快照表 & 全量镜像表 & 日志表
				else if (
						Const.TABLE_TYPE_LISHIKUAIZHAOBIAO.equals(targetTableType) || 
						Const.TABLE_TYPE_QUANLIANGJINGXIANGBIAO.equals(targetTableType) || 
						Const.TABLE_TYPE_RIZHIBIAO.equals(targetTableType)
				) {
					targetSchemaName = "dpods";
				} else if(Const.TABLE_TYPE_WEIDUBIAO.equals(targetTableType)){
					targetSchemaName = "dpdim";
				}
				else {
					throw new RuntimeException("Unknown target table type: " + targetTableType);
				}
			} else if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)) {
				if (targetSchemaTable.indexOf(".") != -1) {
					targetSchemaName = targetSchemaTable.substring(0, targetSchemaTable.indexOf("."));
				} else {
					throw new RuntimeException("GP's target table name must contains schema name: " + targetSchemaTable);
				}
				if (targetSchemaName.equals("dpdim")) {
				    targetSchemaName = "dpdim";
				}
				if (targetSchemaName.isEmpty() || (
						!targetSchemaName.equals("dpdm") &&
						!targetSchemaName.equals("dpdw") &&
						!targetSchemaName.equals("dpfinance") &&
						!targetSchemaName.equals("dpmid") && 
						!targetSchemaName.equals("dpods") && 
						!targetSchemaName.equals("dpodssec") && 
						!targetSchemaName.equals("dpdim") && 
						!targetSchemaName.equals("dprpt"))) {
					targetSchemaName = "dptmp";
				}
			} else if (Const.DATASOURCE_TYPE_GPREPORT.equals(targetDatasourceType)){
                targetSchemaName = "bi";
            } else if (Const.DATASOURCE_TYPE_GPANALYSIS.equals(targetDatasourceType)){
                targetSchemaName = "bi";
            } else {
				throw new RuntimeException("Unknown target datasource type: " + targetDatasourceType);
			}
		} else {
			throw new RuntimeException("Unknown target is active schedule parameter: " + targetIsActiveSchedule);
		}
		return targetSchemaName;
	}

	public JSONObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}

}