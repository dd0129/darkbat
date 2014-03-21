package com.dianping.darkbat.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Resource;

import com.dianping.darkbat.action.AutoBuildTabAction;
import com.dianping.darkbat.common.GlobalResources;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.HiveTableEntity;
import com.dianping.darkbat.entity.TableInfo;
import com.dianping.darkbat.mapper.TabbuildMapper;
import com.dianping.darkbat.mapper.TaskMapper;

@Scope("singleton")
@Service
public class AutoBuildTabService {

	private static Logger log = Logger.getLogger(AutoBuildTabService.class);

	@Resource(name = "HiveMetaMapper")
	private TabbuildMapper tabbuildMapper;

	@Resource(name = "HivePredeployMetaMapper")
	private TabbuildMapper predeployTabbuildMapper;
	@Autowired
	private TaskMapper taskMapper;

	private List<ColumnEntity> getColumnList(String storageType,
			String databaseName, String tableName) {
		if (storageType.equals(Const.HIVE_ENV_PREDEPLOY)) {
			List<ColumnEntity> columnEntityListP = predeployTabbuildMapper
					.getColumnList(databaseName, tableName);
			List<ColumnEntity> columnEntityListM = taskMapper
					.getColumnList(tableName);
			List<ColumnEntity> columnEntityList = new ArrayList<ColumnEntity>();
			Iterator<ColumnEntity> columnEntityInterP = columnEntityListP
					.iterator();
			Iterator<ColumnEntity> columnEntityInterM = columnEntityListM
					.iterator(); 		
			while (columnEntityInterP.hasNext()) {
				ColumnEntity columnEntityP = columnEntityInterP.next();
				columnEntityInterM = columnEntityListM
						.iterator();
				while (columnEntityInterM.hasNext()) {
					ColumnEntity columnEntityM = columnEntityInterM.next();
					if (columnEntityP.getColumnName().equalsIgnoreCase(
							columnEntityM.getColumnName())
							&& columnEntityM.getColumnComment() != null
							&& columnEntityP.getColumnComment() == null) {
						columnEntityP.setColumnComment(columnEntityM
								.getColumnComment());
					}
				}
				columnEntityList.add(columnEntityP);
			}
			return columnEntityList;

		} else {
			return tabbuildMapper.getColumnList(databaseName, tableName);
		}

	}

	private List<ColumnEntity> getPartitionKey(String storageType, String databaseName, String tableName) {
		if (storageType.equals(Const.HIVE_ENV_PREDEPLOY)) {
			return predeployTabbuildMapper.getPartitionKey(databaseName, tableName);
		} else {
			return tabbuildMapper.getPartitionKey(databaseName, tableName);
		}

	}

	private TableInfo getTableInfo(String storageType, String databaseName, String tableName) {
		if (storageType.equals(Const.HIVE_ENV_PREDEPLOY)) {
			return predeployTabbuildMapper.getTableInfo(databaseName, tableName);
		} else {
			return tabbuildMapper.getTableInfo(databaseName, tableName);
		}
	}

	public boolean ifTableExist(String storageType, String databaseName, String tableName) {
		if (storageType.equals(Const.HIVE_ENV_PREDEPLOY)) {
			return (predeployTabbuildMapper.ifTableExist(databaseName, tableName) == 1);
		} else {
			return (tabbuildMapper.ifTableExist(databaseName, tableName) == 1);
		}
	}

	private String lpad(String c, int length, String content) {
		String str = "";
		String cs = "";
		if (content.length() > length) {
			str = content;
		} else {
			for (int i = 0; i < length - content.length(); i++) {
				cs = cs + c;
			}
		}
		str = cs + content;
		return str;
	}

	public HiveTableEntity getTableEntity(String storageType, String databaseName, String tableName) {
		HiveTableEntity tableEntity = new HiveTableEntity();
		tableEntity.setColumnEntity(getColumnList(storageType, databaseName, tableName));
		tableEntity.setPartitionKey(getPartitionKey(storageType, databaseName, tableName));
		tableEntity.setTableInfo(getTableInfo(storageType, databaseName, tableName));
		tableEntity.setTableName(tableName);
		return tableEntity;
	}

	public String getCreateDdl(HiveTableEntity tableEntity, String tableComment, List<String> colComment,
			List<String> parKeyComment, boolean hasComment) {
		String hiveDdl = "CREATE ";
		ColumnEntity columnEntity = new ColumnEntity();
		ColumnEntity partitionColumnEntity = new ColumnEntity();
		int columnKey = 0;
		if (tableEntity.getTableInfo().getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
			hiveDdl += "EXTERNAL ";
		}
		hiveDdl = hiveDdl + "TABLE " + tableEntity.getTableName() + " ( \n";
		Iterator<ColumnEntity> col = tableEntity.getColumnEntity().iterator();
		String columnComment = "";
		while (col.hasNext()) {
			columnEntity = col.next();
			hiveDdl = hiveDdl + "    " + columnEntity.getColumnName() + " " + columnEntity.getColumnType();
			columnKey = Integer.parseInt(columnEntity.getColumnKey());
			columnComment = colComment.get(columnKey - 1);
			if (hasComment && !(columnComment == null || columnComment.equals(""))) {
				hiveDdl = hiveDdl + " COMMENT \"" + columnComment + "\", \n";
			} else {
				hiveDdl = hiveDdl + ", \n";
			}
		}
		hiveDdl = hiveDdl.substring(0, hiveDdl.length() - 3) + " \n) \n";
		if (!(tableComment == null || tableComment.equals("")) && hasComment) {
			hiveDdl = hiveDdl + "COMMENT \"" + tableComment + "\" \n";
		}

		if (!tableEntity.getPartitionKey().isEmpty()) {
			Iterator<ColumnEntity> partitionKey = tableEntity.getPartitionKey().iterator();
			hiveDdl = hiveDdl + "PARTITIONED BY ( \n";
			while (partitionKey.hasNext()) {
				partitionColumnEntity = partitionKey.next();
				columnKey = Integer.parseInt(partitionColumnEntity.getColumnKey());
				columnComment = parKeyComment.get(columnKey - 1);
				hiveDdl = hiveDdl + "    " + partitionColumnEntity.getColumnName() + " "
						+ partitionColumnEntity.getColumnType();
				if (hasComment && !(columnComment == null || columnComment.equals(""))) {
					hiveDdl = hiveDdl + " COMMENT \"" + columnComment + "\", \n";
				} else {
					hiveDdl = hiveDdl + ", \n";
				}
			}
			hiveDdl = hiveDdl.substring(0, hiveDdl.length() - 3) + " \n) \n";
		}

		if (tableEntity.getTableInfo().getTableBucketsNum() != -1) {
			hiveDdl = hiveDdl + "CLUSTERED BY (" + tableEntity.getTableInfo().getTableBucketscol() + ") \n";
			String sortCol = tableEntity.getTableInfo().getTableSortcol();
			if (!(sortCol == null || sortCol.equals(""))) {
				hiveDdl = hiveDdl + "SORTED BY (" + sortCol + ") \n";
			}
			hiveDdl = hiveDdl + "INTO " + tableEntity.getTableInfo().getTableBucketsNum() + " BUCKETS \n";
		}

		hiveDdl = hiveDdl + "ROW FORMAT \n";
		String tableFielddlim = tableEntity.getTableInfo().getTableFielddlim();
		String tableLinedelim = tableEntity.getTableInfo().getTableLinedelim();
		String tableColelctiondelim = tableEntity.getTableInfo().getTableColelctiondelim();
		String mapKeydelim = tableEntity.getTableInfo().getMapKeydelim();
		String serializationFormat = tableEntity.getTableInfo().getSerializationFormat();

		hiveDdl += ("   SERDE \"" + tableEntity.getTableInfo().getTableSerde() + "\" \n");
		hiveDdl += ("	WITH SERDEPROPERTIES (");
		if (!(tableFielddlim == null || tableFielddlim.equals(""))) {
			if (Integer.parseInt(tableFielddlim) >= 0 && Integer.parseInt(tableFielddlim) <= 9) {
				hiveDdl = hiveDdl + "'field.delim' = '\\" + lpad("0", 3, tableFielddlim) + "',";
			} else {
				hiveDdl = hiveDdl + "'field.delim' = '" + String.valueOf((char) Integer.parseInt(tableFielddlim))
						+ "',";
			}

		} else {
			hiveDdl = hiveDdl + "'field.delim' = '\\005',";
		}
		// if (!(tableLinedelim == null || tableLinedelim.equals(""))) {
		// hiveDdl = hiveDdl + "    LINES TERMINATED BY '\\" + lpad("0", 3, tableLinedelim) + "' \n";
		// }
		if (!(tableColelctiondelim == null || tableColelctiondelim.equals(""))) {
			if (Integer.parseInt(tableColelctiondelim) >= 0 && Integer.parseInt(tableColelctiondelim) <= 9) {
				hiveDdl = hiveDdl + "'colelction.delim' = '\\" + lpad("0", 3, tableColelctiondelim) + "',";
			} else {
				hiveDdl = hiveDdl + "'colelction.delim' = '"
						+ String.valueOf((char) Integer.parseInt(tableColelctiondelim)) + "',";
			}

		}
		if (!(mapKeydelim == null || mapKeydelim.equals(""))) {
			if (Integer.parseInt(mapKeydelim) >= 0 && Integer.parseInt(mapKeydelim) <= 9) {
				hiveDdl = hiveDdl + "'mapkey.delim' = '\\" + lpad("0", 3, mapKeydelim) + "',";
			} else {
				hiveDdl = hiveDdl + "'mapkey.delim' = '" + String.valueOf((char) Integer.parseInt(mapKeydelim)) + "',";
			}

		}

		if (!(serializationFormat == null || serializationFormat.equals(""))) {
			if (Integer.parseInt(serializationFormat) >= 0 && Integer.parseInt(serializationFormat) <= 9) {
				hiveDdl = hiveDdl + "'serialization.format' = '\\" + lpad("0", 3, serializationFormat) + "',";
			} else {
				hiveDdl = hiveDdl + "'serialization.format' = '"
						+ String.valueOf((char) Integer.parseInt(serializationFormat)) + "',";
			}

		}

		hiveDdl = hiveDdl.substring(0, hiveDdl.length() - 1) + ")\n";

		hiveDdl = hiveDdl + "STORED AS INPUTFORMAT \"" + tableEntity.getTableInfo().getTableInputformat()
				+ "\" OUTPUTFORMAT \"" + tableEntity.getTableInfo().getTableOutputformat() + "\" \n";
		if (tableEntity.getTableInfo().getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
			hiveDdl = hiveDdl + "LOCATION \"" + tableEntity.getTableInfo().getTableLocation() + "\" \n";
		}
		hiveDdl += ";";
        if(Integer.parseInt(tableLinedelim) == 0){
            hiveDdl += ("ALTER TABLE " + tableEntity.getTableName() + " SET SERDEPROPERTIES('serialization.null.format' = \"\");");
        }
		return hiveDdl;
	}

	public String getAlterTableDdl(String databaseName, String tableName, String tableComment, List<String> colComment,
			List<String> parKeyComment, boolean hasComment) {
		String alterTableDdl = "";
		String addColumnDdl = "";
		String changeTmpColumnDdl = "";
		String changeColumnDdl = "";
		ColumnEntity[] onlineColumnList = getColumnList(Const.HIVE_ENV_ONLINE, databaseName, tableName).toArray(
				new ColumnEntity[0]);
		ColumnEntity[] predeployColumnList = getColumnList(Const.HIVE_ENV_PREDEPLOY, databaseName, tableName).toArray(
				new ColumnEntity[0]);
		boolean isAddColumn = true;
		boolean isDropColumn = true;
		ColumnEntity pcolumnEntity = new ColumnEntity();
		ColumnEntity ocolumnEntity = new ColumnEntity();

		for (int pidx = 0; pidx < predeployColumnList.length; pidx++) {
			isAddColumn = true;
			pcolumnEntity = predeployColumnList[pidx];
			for (int oidx = 0; oidx < onlineColumnList.length; oidx++) {
				ocolumnEntity = onlineColumnList[oidx];
				if (Integer.parseInt(pcolumnEntity.getColumnKey()) == Integer.parseInt(ocolumnEntity.getColumnKey())) {
					isAddColumn = false;
					break;
				}
			}
			if (isAddColumn) {
				addColumnDdl = addColumnDdl + "ALTER TABLE " + tableName + " ADD COLUMNS ("
						+ pcolumnEntity.getColumnName() + " " + pcolumnEntity.getColumnType() + ");\n";
			} else {
				if (!pcolumnEntity.getColumnName().equals(ocolumnEntity.getColumnName())
						|| !pcolumnEntity.getColumnType().equals(ocolumnEntity.getColumnType())) {
					changeTmpColumnDdl = changeTmpColumnDdl + "ALTER TABLE " + tableName + " CHANGE "
							+ ocolumnEntity.getColumnName() + " " + pcolumnEntity.getColumnName() + "_tmp "
							+ pcolumnEntity.getColumnType() + ";\n";
					changeColumnDdl = changeColumnDdl + "ALTER TABLE " + tableName + " CHANGE "
							+ pcolumnEntity.getColumnName() + "_tmp " + pcolumnEntity.getColumnName() + " "
							+ pcolumnEntity.getColumnType() + ";\n";
				}
			}
		}
		
		for (int oidx = 0; oidx < onlineColumnList.length; oidx++) {
			isDropColumn = true;
			ocolumnEntity = onlineColumnList[oidx];
			for (int pidx = 0; pidx < predeployColumnList.length; pidx++) {
				pcolumnEntity = predeployColumnList[pidx];
				if (Integer.parseInt(pcolumnEntity.getColumnKey()) == Integer.parseInt(ocolumnEntity.getColumnKey())) {
					isDropColumn = false;
					break;
				}
			}
			if (isDropColumn) {
				changeTmpColumnDdl = changeTmpColumnDdl + "ALTER TABLE " + tableName + " CHANGE "
				+ ocolumnEntity.getColumnName() + " " + ocolumnEntity.getColumnName() + "_bak "
				+ ocolumnEntity.getColumnType() + ";\n";
			}
		}
		alterTableDdl = changeTmpColumnDdl + changeColumnDdl + addColumnDdl;
		return alterTableDdl;
	}
	/**
	 * 推送表授权信息
	 * @param online 线上组帐号
	 * @param offline 线下组帐号
	 * @param tableIDs 授权表的ID
	 */
	public String pushACLinfo(ArrayList<String> online,ArrayList<String> offline,ArrayList<Integer> tableIDs,String mail,String token) throws Exception{
		JSONObject push_info = new JSONObject();
		push_info.put("code",200);
		JSONArray msg = new JSONArray();
		JSONArray table = new JSONArray();
		for (int tableID : tableIDs) {
			JSONObject t = new JSONObject();
			t.put("table_id",tableID);
			t.put("priv",1);
			table.add(t);
		}
		for (String group : online) {
			if (group != null) {
				JSONObject o = new JSONObject();
				o.put("type",6);
				o.put("user_type",0);
				o.put("usage_type",0);
				o.put("user_name",group);
				o.put("table",table);
				msg.add(o);
			}
		}

		for (String group : offline) {
			if (group != null) {
				JSONObject o = new JSONObject();
				o.put("type",6);
				o.put("user_type",0);
				o.put("usage_type",1);
				o.put("user_name",group);
				o.put("table",table);
				msg.add(o);
			}
		}
		push_info.put("msg",msg);
		push_info.put("mail_list",mail);
		String url = GlobalResources.PLUTO_URL + "pushPrivsForDatatool?access_token="+token + "&push_info=" + push_info.toString();
		log.info("push acl info :" + url);

		String context = null;
		//授权操作默认重试三次
		for (int i = 0;i < 3;i++) {
			try {
				context = Jsoup.connect(url).timeout(3000).execute().body();
			} catch (IOException e) {
				log.info("push acl 网络错误。重试次数:"+ i +"error:" + e);
			}
			JSONObject object = JSONObject.fromObject(context);
			if (context != null && context != "" && object.has("code"))
				return context;
		}
		throw new Exception("授权请求发送失败:网络错误");
	}

	/**
	 * 获取对应的组帐号列表
	 * @param online 是否为线上
	 * @param loginID
	 * @return
	 */
	public String[] getACLgroup(boolean online,int loginID) throws Exception{
		String[] groups = new String[10];
		int type = online ? 0 : 1;

		String url = GlobalResources.PLUTO_URL + "getUserInfoForEmployee?login_id="+loginID+"&usage_type="+type;
		String context = null;
		try {
			context = Jsoup.connect(url).timeout(60000).execute().body();
		} catch (IOException e) {
			throw new Exception("网络错误导致获取组帐号失败");
		}
		//包装线上组帐号list
		if(context != null){
			JSONObject jsonObj = JSONObject.fromObject(context);
			JSONArray onlineList = jsonObj.getJSONObject("msg").getJSONArray("infos");
			groups = new String[onlineList.size()];
			for (int i = 0;i < onlineList.size();i++) {
				groups[i] = onlineList.getJSONObject(i).get("user_name").toString();
			}
		}
		return groups;
	}

}
