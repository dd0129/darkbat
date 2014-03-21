package com.dianping.darkbat.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import com.google.common.base.Strings;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.struts2.ServletActionContext;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.common.GlobalResources;
import com.dianping.darkbat.entity.HiveTableEntity;
import com.dianping.darkbat.entity.LoginUserInfo;
import com.dianping.darkbat.entity.TableInfo;
import com.dianping.darkbat.service.AclService;
import com.dianping.darkbat.service.AutoBuildTabService;
import com.dianping.darkbat.service.AutoConfigService;
import com.dianping.darkbat.service.AutoETLService;
import com.opensymphony.xwork2.Action;

public class AutoBuildTabAction {

	private static Logger log = Logger.getLogger(AutoBuildTabAction.class);

	@Autowired
	private AutoBuildTabService autoBuildTabService;

	@Autowired
	private AutoETLService autoETLService;

	@Autowired
	private AutoConfigService autoConfigService;

	private JSONObject jsonObject;

	public String generateTabInfo() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();

		String databaseName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_DATASOURCE_NAME));
		String tableName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_TABLE_NAME));

		CommonUtil.validateParam(databaseName, "数据库名");
		CommonUtil.validateParam(tableName, "表名");

		boolean isExist = autoBuildTabService.ifTableExist(
				Const.HIVE_ENV_PREDEPLOY, databaseName, tableName);
		if (isExist) {
			HiveTableEntity hiveTableEntity = autoBuildTabService
					.getTableEntity(Const.HIVE_ENV_PREDEPLOY, databaseName,
							tableName);
			jsonObject = CommonUtil.getPubJson(hiveTableEntity);
		} else {
			jsonObject = new JSONObject();
			jsonObject.put("code", Const.RET_CODE_201);
			jsonObject.element("msg", "Cannot find hive table [" + tableName
					+ "] in predeploy env");
			// throw new RuntimeException("Cannot find hive table [" + tableName
			// + "] in predeploy env");
		}
		return Action.SUCCESS;
	}

	public String generateAutoBuildTabDDL() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();

		String databaseName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_DATASOURCE_NAME));
		String tableName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_TABLE_NAME));
		String owner = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_OWNER));
		String storageCycle = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_STORAGE_CYCLE));
		String tableComment = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_TABLE_COMMENT));
		String columnComment = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_COLUMN_COMMENT));
		String location = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_LOCATION));

		if (columnComment == null)
			columnComment = "";
		int columnSize = CommonUtil.parseInt(CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_COLUMN_SIZE)));
		String[] columnCommentArr = columnComment.split("\\(~.~\\)",
				columnSize + 1);
		for (int i = 0; i < columnCommentArr.length; ++i) {
			columnCommentArr[i] = columnCommentArr[i].replaceAll("\\(~.~\\)",
					"");
		}
		List<String> columnCommentList = new ArrayList<String>();
		for (int i = 0; i < columnCommentArr.length - 1; ++i) {
			columnCommentList.add(columnCommentArr[i]);
		}
		String partitionColumnComment = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_COLUMN_COMMENT));
		if (partitionColumnComment == null)
			partitionColumnComment = "";
		int partitionColumnSize = CommonUtil.parseInt(CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_COLUMN_SIZE)));
		String[] partitionColumnCommentArr = partitionColumnComment.split(
				"\\(~.~\\)", partitionColumnSize + 1);
		for (int i = 0; i < partitionColumnCommentArr.length; ++i) {
			partitionColumnCommentArr[i] = partitionColumnCommentArr[i]
					.replaceAll("\\(~.~\\)", "");
		}
		List<String> partitionColumnCommentList = new ArrayList<String>();
		for (int i = 0; i < partitionColumnCommentArr.length - 1; ++i) {
			partitionColumnCommentList.add(partitionColumnCommentArr[i]);
		}

		CommonUtil.validateParam(databaseName, "数据库名");
		CommonUtil.validateParam(tableName, "表名");
		CommonUtil.validateParam(owner, "开发者");
		CommonUtil.validateParam(storageCycle, "存储周期");

		boolean isExistPreDeploy, isExistOnline;
		String sql = "";

		isExistPreDeploy = autoBuildTabService.ifTableExist(
				Const.HIVE_ENV_PREDEPLOY, databaseName, tableName);
		if (isExistPreDeploy) {
			HiveTableEntity hiveTableEntity = autoBuildTabService
					.getTableEntity(Const.HIVE_ENV_PREDEPLOY, databaseName,
							tableName);
			isExistOnline = autoBuildTabService.ifTableExist(
					Const.HIVE_ENV_ONLINE, databaseName, tableName);
			if (isExistOnline) {
				sql = autoBuildTabService.getAlterTableDdl(databaseName,
						tableName, tableComment, columnCommentList,
						partitionColumnCommentList, true);
			} else {
				if (hiveTableEntity.getTableInfo().getTableType()
						.equalsIgnoreCase("EXTERNAL_TABLE")) {
					CommonUtil.validateParam(location, "路径");
					hiveTableEntity.getTableInfo().setTableLocation(location);
				}
				
				sql = autoBuildTabService.getCreateDdl(hiveTableEntity,
						tableComment, columnCommentList,
						partitionColumnCommentList, true);
			}
		} else {
			throw new RuntimeException("Cannot find hive table [" + tableName
					+ "] in predeploy env");
		}
		jsonObject = CommonUtil.getPubJson(sql);
		return Action.SUCCESS;
	}

	public String createTableAutoBuildTab() throws Exception {
		HttpServletRequest req = ServletActionContext.getRequest();

		String databaseName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_DATASOURCE_NAME));
		String tableName = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_TABLE_NAME));
		String owner = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_OWNER));
		String storageCycle = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_STORAGE_CYCLE));
		String tableComment = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_TABLE_COMMENT));
		String columnComment = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_COLUMN_COMMENT));
		String onlineGroup = CommonUtil
				.parseStr(req
						.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_AUTH_ONLINE_GROUP));
		String offlineGroup = CommonUtil
				.parseStr(req
						.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_AUTH_OFFLINE_GROUP));
		String location = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_LOCATION));
		String mail = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_MAIL));

		Strings.nullToEmpty(columnComment);
		Strings.nullToEmpty(mail);
		int columnSize = CommonUtil.parseInt(CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_COLUMN_SIZE)));
		String[] columnCommentArr = columnComment
				.split("\\(~.~\\)", columnSize);
		for (int i = 0; i < columnCommentArr.length; ++i) {
			columnCommentArr[i] = columnCommentArr[i].replaceAll("\\(~.~\\)",
					"");
		}
		String partitionColumnComment = CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_COLUMN_COMMENT));
		if (partitionColumnComment == null)
			partitionColumnComment = "";
		int partitionColumnSize = CommonUtil.parseInt(CommonUtil.parseStr(req
				.getParameter(Const.FLD_AUTOBUILDTAB_PARTITION_COLUMN_SIZE)));
		String[] partitionColumnCommentArr = partitionColumnComment.split(
				"\\(~.~\\)", partitionColumnSize);
		for (int i = 0; i < partitionColumnCommentArr.length; ++i) {
			partitionColumnCommentArr[i] = partitionColumnCommentArr[i]
					.replaceAll("\\(~.~\\)", "");
		}

		CommonUtil.validateParam(databaseName, "数据库名");
		CommonUtil.validateParam(tableName, "表名");
		CommonUtil.validateParam(owner, "开发者");
		CommonUtil.validateParam(storageCycle, "存储周期");

		boolean isExistPreDeploy, isExistOnline;
		String sql = "";
		int tableId = 0;

		isExistPreDeploy = autoBuildTabService.ifTableExist(
				Const.HIVE_ENV_PREDEPLOY, databaseName, tableName);
		if (isExistPreDeploy) {
			HiveTableEntity hiveTableEntity = autoBuildTabService
					.getTableEntity(Const.HIVE_ENV_PREDEPLOY, databaseName,
							tableName);
			isExistOnline = autoBuildTabService.ifTableExist(
					Const.HIVE_ENV_ONLINE, databaseName, tableName);
			if (hiveTableEntity.getTableInfo().getTableType()
					.equalsIgnoreCase("EXTERNAL_TABLE")) {
				CommonUtil.validateParam(location, "路径");
				hiveTableEntity.getTableInfo().setTableLocation(location);
			}

			sql = autoBuildTabService.getCreateDdl(hiveTableEntity,
					tableComment, Arrays.asList(columnCommentArr),
					Arrays.asList(partitionColumnCommentArr), true);
			log.info("getCreateDdl: " + sql);
			// 坑必须标记 这里建表和记录元信息 非原子操作。
			// 如果建表失败，那么主数据的表信息仍然存在一直到第二天
			String ret = collectMetadata(sql, owner, tableComment);
			log.info("master data json:" + ret);
			JSONObject resJson = JSONObject.fromObject(ret);
			if (resJson != null) {
				if ((resJson.has("code") && resJson.getInt("code") == 500)
						|| (resJson.has("msg") && !resJson.getJSONObject("msg")
								.has("tableid"))) {
					log.error("call masterdata error:" + resJson.toString());
					throw new RuntimeException(resJson.getString("msg"));
				}
			}
			// 获取主数据生成的tableID
			// TODO 如果tableid不存在，那么不推送授权信息
			tableId = resJson.getJSONObject("msg").getInt("tableid");

			if (isExistOnline) {
				sql = autoBuildTabService.getAlterTableDdl(databaseName,
						tableName, tableComment,
						Arrays.asList(columnCommentArr),
						Arrays.asList(partitionColumnCommentArr), false);
				log.info("isExistOnline true sql: " + sql);
			} else {
				if (hiveTableEntity.getTableInfo().getTableType()
						.equalsIgnoreCase("EXTERNAL_TABLE")) {
					CommonUtil.validateParam(location, "路径");
					TableInfo tableInfo = hiveTableEntity.getTableInfo();
					tableInfo.setTableLocation(location);
					hiveTableEntity.setTableInfo(tableInfo);
				}
				sql = autoBuildTabService.getCreateDdl(hiveTableEntity,
						tableComment, Arrays.asList(columnCommentArr),
						Arrays.asList(partitionColumnCommentArr), false);
				log.info("isExistOnline false sql: " + sql);
				ArrayList<String> group1 = new ArrayList<String>();
				if(!Strings.isNullOrEmpty(onlineGroup))
					group1.add(onlineGroup);
				ArrayList<String> group2 = new ArrayList<String>();
				if (!Strings.isNullOrEmpty(offlineGroup))
					group2.add(offlineGroup);
				ArrayList<Integer> tables = new ArrayList<Integer>();
				if (tableId > 0)
					tables.add(tableId);

				if ((group1.size() > 0 || group2.size() > 0)
						&& tables.size() > 0) {
					String pushResult = autoBuildTabService.pushACLinfo(group1,
							group2, tables, mail, GlobalResources.TOKEN);
					JSONObject object = JSONObject.fromObject(pushResult);
					if (object.getInt("code") != 200)
						throw new RuntimeException("授权请求发送失败"
								+ object.toString());
					log.info("ACL push authorization info:" + object.toString());
				}
			}
			autoETLService.createTableHive(sql);
			// 授权

		} else {
			throw new RuntimeException("Cannot find hive table [" + tableName
					+ "] in predeploy env");
		}

		jsonObject = CommonUtil.getPubJson(sql);
		return Action.SUCCESS;
	}

	/**
	 * 获取对应组帐号列表 add by xiong.chen
	 * 
	 * @return
	 */
	public String getGroupList() throws Exception {
		// 利用token获取loginID
		HttpServletRequest request = ServletActionContext.getRequest();
		HttpSession session = request.getSession();
		String token = "";
		if (session != null)
			token = session.getAttribute("token").toString();
		if (token != null && token.length() > 0) {
			LoginUserInfo userInfo = AclService.getLoginUserInfo(token);
			int loginID = userInfo.getLogin_id();
			jsonObject = new JSONObject();
			// 根据loginID获取对应的线上线下组帐号
			String[] online_group = autoBuildTabService.getACLgroup(true,
					loginID);
			jsonObject.element("online", online_group);

			String[] offline_group = autoBuildTabService.getACLgroup(false,
					loginID);
			jsonObject.element("offline", offline_group);
			jsonObject.element("code", 200);
			jsonObject.element("msg", "");
		} else {
			jsonObject = CommonUtil.getPubJson("无法获取相关信息", 500);
		}
		return Action.SUCCESS;
	}

	private String collectMetadata(String ddl, String owner, String description)
			throws IOException {
		String url = GlobalResources.MASTERDATA_URL + "collectMetadata";
		String ret = null;
		Connection conn = Jsoup.connect(url);
		ret = conn
				.data("ddl", ddl)
				.data("owner", owner)
				.data("description",
						CommonUtil.isEmpty(description) ? "" : description)
				.timeout(60000).method(Method.POST).execute().body();
		return ret;
	}

	public JSONObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}
}
