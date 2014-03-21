package com.dianping.darkbat.common;

import java.util.HashMap;
import java.util.Map;

public class Const {
	

	/**
     * 唐弘頔
     */
    public static final String IP_HONGDI_TANG = "192.168.32.53";

    // ETL_TASK_CFG的字段
    public static final String FLD_TASK_ID              = "taskId",
                               FLD_TASK_NAME            = "taskName",
                               FLD_TASK_TABLE_NAME      = "tableName",
                               FLD_TASK_REMARK          = "remark",
                               FLD_TASK_DATABASE_SRC    = "databaseSrc",
                               FLD_TASK_OBJECT          = "taskObj",
                               FLD_TASK_PARA1           = "para1",
                               FLD_TASK_PARA2           = "para2",
                               FLD_TASK_PARA3           = "para3",
                               FLD_TASK_RECALL_LIMIT    = "recallLimit",
                               FLD_TASK_RECALL_INTERVAL = "recallInterval",
                               FLD_TASK_GROUP_ID        = "taskGroupId",
                               FLD_TASK_CYCLE           = "cycle",
                               FLD_TASK_PRIO_LVL        = "prioLvl",
                               FLD_TASK_IF_RECALL       = "ifRecall",
                               FLD_TASK_TIMEOUT         = "timeout",
                    		   FLD_TASK_IF_PRE          = "ifPre",
                               FLD_TASK_IF_WAIT         = "ifWait",
                               FLD_TASK_IF_VAL          = "ifVal",
                               FLD_TASK_TYPE            = "type",
                               FLD_TASK_OFFSET          = "offset",
                               FLD_TASK_OFFSET_TYPE     = "offsetType",
                               FLD_TASK_FREQ            = "freq",
                               FLD_TASK_OWNER           = "owner",
                               FLD_TASK_WAIT_CODE       = "waitCode",
                               FLD_TASK_RECALL_CODE     = "recallCode",
                               FLD_TASK_SUCCESS_CODE    = "successCode",
                               FLD_TASK_STATUS          = "status",
                               FLD_TASK_ADD_USER        = "addUser",
                               FLD_TASK_UPDATE_USER     = "updateUser",
                               FLD_ONLYSELF             = "onlyself";
    
    public static final String FLD_AUTOETL_DATASOURCE_TYPE = "datasourceType",
                               FLD_AUTOETL_DATABASE_NAME   = "databaseName",
                    		   FLD_AUTOETL_SCHEMA_NAME     = "schemaName",
                               FLD_AUTOETL_TABLE_NAME      = "tableName",
                    		   FLD_AUTOETL_TARGET_IS_ACTIVE_SCHEDULE = "targetIsActiveSchedule",
                               FLD_AUTOETL_TARGET_DATASOURCE_TYPE    = "targetDatasourceType",
                               FLD_AUTOETL_TARGET_SCHEMA_TABLE       = "targetSchemaTable",
                               FLD_AUTOETL_TARGET_TABLE_TYPE         = "targetTableType",
                               FLD_AUTOETL_TARGET_SEGMENT_COLUMN     = "targetSegmentColumn",
                               FLD_AUTOETL_WRITE_TYPE     = "targetWriteType";
    
    public static final String FLD_AUTOBUILDTAB_DATASOURCE_NAME = "databaseName",
                               FLD_AUTOBUILDTAB_TABLE_NAME      = "tableName",
                               FLD_AUTOBUILDTAB_OWNER           = "owner",
                               FLD_AUTOBUILDTAB_STORAGE_CYCLE   = "storageCycle",
                    		   FLD_AUTOBUILDTAB_TABLE_COMMENT   = "tableComment",
                    		   FLD_AUTOBUILDTAB_COLUMN_COMMENT  = "columnComment",
            				   FLD_AUTOBUILDTAB_COLUMN_SIZE     = "columnSize",
            				   FLD_AUTOBUILDTAB_PARTITION_COLUMN_COMMENT = "partitionColumnComment",
    						   FLD_AUTOBUILDTAB_PARTITION_COLUMN_SIZE    = "partitionColumnSize",
							   FLD_AUTOBUILDTAB_PARTITION_AUTH_ONLINE_GROUP = "auth_online",
							   FLD_AUTOBUILDTAB_PARTITION_AUTH_OFFLINE_GROUP = "auth_offline",
   							   FLD_AUTOBUILDTAB_PARTITION_LOCATION = "location",
							   FLD_AUTOBUILDTAB_MAIL            = "mail"
									   ;

    // 关联任务
    public static final String FLD_PRE_DEPENDS          = "preDepends",
                               FLD_DEP_TASK_ID          = "depTaskId",
                    		   FLD_DEP_TASK_GROUP_ID    = "depTaskGroupId",
                               FLD_DEP_TASK_NAME        = "depTaskName",
                               FLD_DEP_CYCLE_GAP        = "depCycleGap",
                               FLD_DEP_REMARK           = "depRemark";
    
    public static final String FLD_TASK_RELA_STATUS_ID = "taskStatusId";

    public static final String HIVE_ENV_PREDEPLOY = "predeploy",
    		                   HIVE_ENV_ONLINE    = "online";

    /**
     * 翻页参数 - 排序字段
     */
    public static final String PARAM_PAGE_SORT  = "pageSort";

    /**
     * 翻页参数 - 第几页
     */
    public static final String PARAM_PAGE_NUM   = "pageNo";

    /**
     * 翻页参数 - 每页行数
     */
    public static final String PARAM_PAGE_SIZE  = "pageSize";

    /**
     * 0: unknown
     */
    public static final String TASK_GROUP_0 = "unknown";

    /**
     * 0: unknown
     */
    public static final int TASK_GROUP_UNKNOWN = 0;

    /**
     * 1: wormhole
     */
    public static final String TASK_GROUP_1 = "wormhole";

    /**
     * 1: wormhole
     */
    public static final int TASK_GROUP_WORMHOLE = 1;

    /**
     * 2: mid-dim
     */
    public static final String TASK_GROUP_2 = "mid-dim";

    /**
     * 2: mid-dim
     */
    public static final int TASK_GROUP_MID_DIM = 2;

    /**
     * 3: dm
     */
    public static final String TASK_GROUP_3 = "dm";

    /**
     * 3: dm
     */
    public static final int TASK_GROUP_DM = 3;

    /**
     * 4: rpt
     */
    public static final String TASK_GROUP_4 = "rpt";

    /**
     * 4: rpt
     */
    public static final int TASK_GROUP_RPT = 4;

    /**
     * 5: mail
     */
    public static final String TASK_GROUP_5 = "mail";

    /**
     * 5: mail
     */
    public static final int TASK_GROUP_MAIL = 5;

    /**
     * 6: dw
     */
    public static final String TASK_GROUP_6 = "dw";

    /**
     * 6: dw
     */
    public static final int TASK_GROUP_DW = 6;

    public static final String TASK_GROUP_7 = "DQ";

    public static final int TASK_GROUP_DQ = 7;

    public static final String TASK_GROUP_8 = "atom";

    public static final int TASK_GROUP_ATOM = 8;

    /**
     * 运行级别 - 高
     */
    public static final int PRIO_LVL_HIGH = 1;

    /**
     * 运行级别 - 中
     */
    public static final int PRIO_LVL_MEDIUM = 2;

    /**
     * 运行级别 - 低
     */
    public static final int PRIO_LVL_LOW = 3;

    /**
     * 200
     */
    public static final int RET_CODE_200 = 200;

    /**
     * 201
     */
    public static final int RET_CODE_201 = 201;

    /**
     * 202
     */
    public static final int RET_CODE_202 = 202;

    /**
     * 500
     */
    public static final int RET_CODE_500 = 500;

    /**
     * 数据源类型 - HIVE
     */
    public static final String DATABASE_TYPE_WORMHOLE = "wormhole";
    
    /**
     * 数据源类型 - HIVE
     */
    public static final String DATABASE_TYPE_HIVE = "hive";

    /**
     * 数据源类型 - GP57
     */
    public static final String DATABASE_TYPE_GP57 = "gp57";

    /**
     * 数据源类型 - GP59
     */
    public static final String DATABASE_TYPE_GP59 = "gp59";

    /**
     * 生效 - 否
     */
    public static final int IF_VAL_NO = 0;

    /**
     * 生效 - 是
     */
    public static final int IF_VAL_YES = 1;

    /**
     * 生效 - 失效
     */
    public static final int IF_VAL_UNUSE = 2;
    
    
    /**
     * 偏移类型 - 相对
     */
    public static final String OFFSET_OFFSET = "offset";

    /**
     * 偏移类型 - 指定日期
     */
    public static final String OFFSET_APPOINT = "appoint";

    /**
     * 执行程序 - WORMHOLE
     */
    public static final String COMMAND_WORMHOLE = "ssh -o ConnectTimeout=3 -o ConnectionAttempts=5 -o PasswordAuthentication=no -o StrictHostKeyChecking=no -p 58422 deploy@10.1.6.49 sh /data/deploy/dwarch/conf/ETL/bin/start_autoetl.sh";
    /**
     * 执行程序 - HIVE
     */
    public static final String COMMAND_HIVE = "ssh -o ConnectTimeout=3 -o ConnectionAttempts=5 -o PasswordAuthentication=no -o StrictHostKeyChecking=no -p 58422 deploy@10.1.6.49 sh /data/deploy/dwarch/conf/ETL/bin/start_shellTask.sh";

    /**
     * 执行程序 - GP
     */
    public static final String COMMAND_GP   = "sh ${calculate_home}/bin/start_calculate.sh";


    /***
     * halley的目录变量
     */
    public static final String wormhole_home = "/data/deploy/dwarch/conf/ETL",
                               wormhole_job_home = "/data/deploy/dwarch/conf/ETL/job",
                               wormhole_log_home="/data/deploy/dwarch/log/ETL",
                               calculate_home="/data/deploy/dwarch/conf/ETL",
                               calculate_job_home="/home/dwdev/work",
                               calculate_log_home="/data/deploy/dwarch/log/calculate";

   public static final Map<String,String> HALLEY_DIRS= new HashMap<String,String>();

   static{
       HALLEY_DIRS.put("${wormhole_home}",wormhole_home);
       HALLEY_DIRS.put("${wormhole_job_home}",wormhole_job_home);
       HALLEY_DIRS.put("${wormhole_log_home}",wormhole_log_home);
       HALLEY_DIRS.put("${calculate_home}",calculate_home);
       HALLEY_DIRS.put("${calculate_job_home}",calculate_job_home);
       HALLEY_DIRS.put("${calculate_log_home}",calculate_log_home);
   }

    /**
     * 日志目录 - WORMHOLE
     */
    public static final String LOG_HOME_WORMHOLE  = "${wormhole_log_home}";

    /**
     * 日志目录 - 其他
     */
    public static final String LOG_HOME_CALCULATE = "${calculate_log_home}";
    
    /**
     * AutoETL数据源连接配置
     */
    public static final String DBCONF_PATH = "/autoetl/dbconf.properties";

    /**
     * AutoETL字段配置
     */
    public static final String MAPPING_PATH = "/autoetl/mapping.properties";

    public static final String DATASOURCE_TYPE_MYSQL     = "mysql",
    						   DATASOURCE_TYPE_GP        = "greenplum",
    						   DATASOURCE_TYPE_HIVE      = "hive",
							   DATASOURCE_TYPE_HIVEMETA  = "hivemeta",
    						   DATASOURCE_TYPE_SQLSERVER = "sqlserver",
    						   DATASOURCE_TYPE_SALESFORCE = "salesforce",
                               DATASOURCE_TYPE_POSTGRESQL = "postgresql",
                               DATASOURCE_TYPE_GPREPORT = "gpreport",
                               DATASOURCE_TYPE_GPANALYSIS = "gpanalysis";

	public static final String TABLE_TYPE_LALIANBIAO = "1",
							   TABLE_TYPE_LISHIKUAIZHAOBIAO = "2",
							   TABLE_TYPE_QUANLIANGJINGXIANGBIAO = "3",
							   TABLE_TYPE_RIZHIBIAO = "4",
	                           TABLE_TYPE_WEIDUBIAO = "5";
	
	
	/**
     * deploy服务器连接配置
     */
//	public static final String 	DEPLOY_IP = "10.1.6.151";
//	public static final Integer DEPLOY_PORT = 58422;
//	public static final String  DEPLOY_USER = "deploy";
//	public static final String  DEPLOY_PASSWD = "zxcvbnm";
//	public static final String COMMAND_CANAAN   = "sh /data/deploy/canaan/bin/dwexec.sh";
//	public static final String 	DEPLOY_IP = "192.168.26.172";
//	public static final Integer DEPLOY_PORT = 22;
//	public static final String  DEPLOY_USER = "test";
//	public static final String  DEPLOY_PASSWD = "test";
//	public static final String COMMAND_CANAAN   = "sh /home/cccyf/canaan/bin/dwexec.sh";

	/**
	 * 任务前驱后继字段
	 */
    public static final String TASK_RELA_PRE  	= "taskRelaPre",
    						   TASK_RELA_POST 	= "taskRelaPost";
    
    
    
    /**
     * 任务运行实例状态
     */
    public static final Integer TASK_STATUS_UNKOWN	   = -3,
    							TASK_STATUS_UNSUCCESS  = -2,
    							TASK_STATUS_FAIL 	   = -1,
    							TASK_STATUS_INIT       = 0,
    							TASK_STATUS_SUCCESS    = 1,
    							TASK_STATUS_RUNNING    = 2,
    							TASK_STATUS_SUSPEND    = 3,
    							TASK_STATUS_INIT_ERROR = 4,
    							TASK_STATUS_WAIT	   = 5,
    							TASK_STATUS_READY      = 6,
    							TASK_STATUS_TIMEOUT    = 7;
    
    public static final String SALESFORCE_URL = "https://cs5.salesforce.com/services/Soap/u/27";
    
    /**
     * 邮件配置相关常量
     */
    public static final int DATE_CYCLE_MTD = 0;
    
    public static final String  MAIL_INFO_CYCLE_DAY   = "D",
                                MAIL_INFO_CYCLE_WEEK  = "W",
                                MAIL_INFO_CYCLE_MONTH = "M";
    
    public static final String MAIL_ITEM_CHART  = "CHART",
                               MAIL_ITEM_REPORT = "REPORT",
                               MAIL_ITEM_TABLE  = "TABLE", 
                               MAIL_ITEM_PAGE   = "PAGE";
    
  //String username = "zheng.he@dianping.com.dev";
  		//String password = "hezheng238LDluBeE2KS9cCLSIuWfa801Y";	
//	public static final String SALESFORCE_USER = "zheng.he@dianping.com.dev";	
//	public static final String SALESFORCE_PASSWORD = "hezheng238LDluBeE2KS9cCLSIuWfa801Y";
	
	public static final String SALESFORCE_USER = "zheng.he@dianping.com.dev";	
	public static final String SALESFORCE_PASSWORD = "crm@BA@DPc5";
	
    /**
     * 邮件服务admin_token
     */
    public static final String VENUS_ADMIN_TOKEN_MAIL_SERVICE = "dmVudXMgYWRtaW4gLSBtYWlsIHNlcnZpY2U";

    public static final String SLA_DEFAULT_WARNTIME = "09:00:00";

    public static final String SLA_DEFAULT_WARNBEGINTIME = "08:30:00";

    public static final String SLA_JOB_HOST = "10.1.6.153";

}
