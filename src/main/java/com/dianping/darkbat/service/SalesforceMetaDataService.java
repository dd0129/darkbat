package com.dianping.darkbat.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.TableEntity;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

@Scope("singleton")
@Repository
public class SalesforceMetaDataService {

    private static Log log = LogFactory.getLog(SalesforceMetaDataService.class);

	/**
	 * @param args
	 */	

	PartnerConnection connection;
	String authEndPoint;
	
	
	/*************
	 * 得到salesforce table的集合，这里完全是调用salesforce接口，没有走mybaties
	 * 
	 * @param tableEntity
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<TableEntity> searchSalesForceTable(TableEntity tableEntity,
			Integer limit, Integer offset) {
		List<TableEntity> ret = new ArrayList<TableEntity>();

		String regex = StringUtils.lowerCase(tableEntity.getTableName());
		
		this.login();
		DescribeGlobalResult dgr;
		try {
			dgr = connection.describeGlobal();
		} catch (ConnectionException e) {		
			log.error(e.getMessage(), e);
			throw new RuntimeException("SalesForce connect fail,"+e.getMessage());
		}		

		List<TableEntity> all = new ArrayList<TableEntity>();

		if (StringUtils.isEmpty(regex)) {
			for (int i = 0; i < dgr.getSobjects().length; i++) {
				String tableName = dgr.getSobjects()[i].getName();
				TableEntity entity = new TableEntity();
				entity.setDatabaseName("default");
				entity.setDatasourceType(Const.DATASOURCE_TYPE_SALESFORCE);
				entity.setSchemaName("default");
				entity.setTableName(tableName);
				all.add(entity);
			}
		} else {
			for (int i = 0; i < dgr.getSobjects().length; i++) {
				String tableName = dgr.getSobjects()[i].getName();
				if (tableName.toLowerCase().contains(regex)) {
					TableEntity entity = new TableEntity();
					entity.setDatabaseName("default");
					entity.setDatasourceType(Const.DATASOURCE_TYPE_SALESFORCE);
					entity.setSchemaName("default");
					entity.setTableName(tableName);
					all.add(entity);
				}
			}
		}
		for (int i = offset; i < offset + limit && i < all.size(); ++i) {
			ret.add(all.get(i));
		}			
		this.logout();
		
		return ret;
	}
	
	
	/*************
	 * 这里获取count就是获取list.size
	 * @param tableEntity
	 * @return
	 */
	public Integer searchSalesForceTableCount(TableEntity tableEntity) {
		String regex = StringUtils.lowerCase(tableEntity.getTableName());
		Integer ret = 0;		
		this.login();
		DescribeGlobalResult dgr = null;
		try {
			dgr = connection.describeGlobal();
		} catch (ConnectionException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException("SalesForce connect fail,"+e.getMessage());			
		}			
		
		if(StringUtils.isEmpty(regex)){
			ret = dgr.getSobjects().length;
		}else{
			for (int i = 0; i < dgr.getSobjects().length; i++) {
				String tableName = dgr.getSobjects()[i].getName();
				if (tableName.toLowerCase().contains(regex)) {
					ret++;
				}
			}
		}			
		
		this.logout();
		return ret;
		
	}
	
	public List<ColumnEntity> getSalesforceColumn(TableEntity tableEntity) {
		List<ColumnEntity> ret = new ArrayList<ColumnEntity>();

		this.login();
		DescribeSObjectResult dsr;
		try {
			dsr = connection.describeSObject(tableEntity.getTableName().trim());
		} catch (ConnectionException e) {			
			log.error(e.getMessage(), e);
			throw new RuntimeException("SalesForce connect fail,"+e.getMessage());
		}

		for (int i = 0; i < dsr.getFields().length; i++) {
			Field field = dsr.getFields()[i];
			ColumnEntity entity = new ColumnEntity();
			entity.setColumnComment(field.getLabel());
			entity.setColumnName(field.getName());
			entity.setColumnType(field.getType().name());
			if(entity.getColumnType().toLowerCase().equals("id")){
				entity.setColumnKey("key");
			}
			
			ret.add(entity);
		}
		this.logout();

		return ret;
	}
	
	
	List<Map<String,String>> fromSalesforce(String tableName) {			
		List<Map<String,String>> ret = new ArrayList<Map<String,String>>();
		this.login();
		DescribeSObjectResult dsr;
		try {
			dsr = connection.describeSObject(tableName);
		} catch (ConnectionException e) {			
			log.error(e.getMessage(), e);
			throw new RuntimeException("SalesForce connect fail,"+e.getMessage());
		}

		for (int i = 0; i < dsr.getFields().length; i++) {			
			Field field = dsr.getFields()[i];
			Map<String,String> tmp = new HashMap<String,String>();
			tmp.put("column_name", field.getName());
			tmp.put("data_type", field.getType().name());
			tmp.put("character_maximum_length", String.valueOf(field.getLength()));
			
			ret.add(tmp);
		}
		this.logout();

		return ret;
	}
	
	

	private void logout() {
		try {
			connection.logout();
			System.out.println("Logged out.");
		} catch (ConnectionException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException("SalesForce logout fail,"+e.getMessage());
		}
	}

	private void login() {		
		//authEndPoint = Const.SALESFORCE_URL;
		//String username = "zheng.he@dianping.com.dev";
		//String password = "hezheng238LDluBeE2KS9cCLSIuWfa801Y";
		
//		String username = Const.SALESFORCE_USER;
//		String password = Const.SALESFORCE_PASSWORD;
		
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(Const.SALESFORCE_USER);
		config.setPassword(Const.SALESFORCE_PASSWORD);

		System.out.println("AuthEndPoint: " + Const.SALESFORCE_URL);
		config.setAuthEndpoint(Const.SALESFORCE_URL);

		try {
			connection = new PartnerConnection(config);
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage(), e);
			throw new RuntimeException("Saleforce login fail,"+e.getMessage());
		}

	}

}
