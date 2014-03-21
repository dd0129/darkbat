package com.dianping.darkbat.service;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dianping.darkbat.mapper.TaskMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.DSInfo;
import com.dianping.darkbat.entity.TableEntity;
import com.dianping.darkbat.mapper.GPMapper;
import com.dianping.darkbat.mapper.HiveMapper;
import com.dianping.darkbat.mapper.MySQLMapper;

@Scope("singleton")
@Repository
public class AutoETLService {

    private static Logger log = Logger.getLogger(AutoETLService.class);

    @Autowired
    private TaskService taskService;

    @Autowired
    private SalesforceMetaDataService salesforceService;

    @Autowired
    private MySQLMapper mySQLMapper;

    @Autowired
    private TaskMapper taskMapper;

    @Autowired
    private GPMapper gpMapper;

    @Autowired
    private HiveMapper hiveMapper;

    /**
     * 建半年的分区
     */
    private static int PARTITION_CNT = 180;

    static {
        Configuration conf = new Configuration();
        // Kerberos Authentication
        UserGroupInformation.setConfiguration(conf);
        try {
            SecurityUtil.login(conf, "test.hadoop.keytab.file",
                    "test.hadoop.principal");
        } catch (IOException e) {
            log.error("hive login failed");
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 获取数据源信息
     * 
     * @param dsName
     * @param dbName
     * @return
     */
    private DSInfo getDSInfo(String dsName, String dbName) {
        Properties prop = new Properties();
        InputStream in = this.getClass().getResourceAsStream(Const.DBCONF_PATH);
        try {
            prop.load(in);
        } catch (IOException e) {
            log.error(e.getMessage(),e);
            throw new RuntimeException("Read properties file error ["
                    + Const.DBCONF_PATH + "] " + e.getMessage());
        }
        DSInfo dsInfo = new DSInfo();
        dsInfo.setType(dsName);
        if (Const.DATASOURCE_TYPE_HIVEMETA.equals(dsName)) {
            dsInfo.setDbname(prop.getProperty(dsName + ".dbname"));
            dsInfo.setIp(prop.getProperty(dsName + ".ip"));
            dsInfo.setPort(prop.getProperty(dsName + ".port"));
            dsInfo.setUsername(prop.getProperty(dsName + ".username"));
            dsInfo.setPassword(prop.getProperty(dsName + ".password"));
        } else {
            dsInfo.setDbname(prop
                    .getProperty(dsName + "_" + dbName + ".dbname"));
            dsInfo.setIp(prop.getProperty(dsName + "_" + dbName + ".ip"));
            dsInfo.setPort(prop.getProperty(dsName + "_" + dbName + ".port"));
            dsInfo.setUsername(prop.getProperty(dsName + "_" + dbName
                    + ".username"));
            dsInfo.setPassword(prop.getProperty(dsName + "_" + dbName
                    + ".password"));
        }
        return dsInfo;
    }

    /**
     * 获取GP数据源信息
     * 
     * @param dsName
     * @param dbName
     * @param schemaName
     * @return
     */
    private DSInfo getGPDSInfo(String dsName, String dbName, String schemaName) {
        Properties prop = new Properties();
        InputStream in = this.getClass().getResourceAsStream(Const.DBCONF_PATH);
        try {
            prop.load(in);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Read properties file error ["
                    + Const.DBCONF_PATH + "] " + e.getMessage());
        }
        DSInfo dsInfo = new DSInfo();
        dsInfo.setType(dsName);
        dsInfo.setDbname(prop.getProperty(dsName + "_" + dbName + "_"
                + schemaName + ".dbname"));
        dsInfo.setIp(prop.getProperty(dsName + "_" + dbName + "_" + schemaName
                + ".ip"));
        dsInfo.setPort(prop.getProperty(dsName + "_" + dbName + "_"
                + schemaName + ".port"));
        dsInfo.setUsername(prop.getProperty(dsName + "_" + dbName + "_"
                + schemaName + ".username"));
        dsInfo.setPassword(prop.getProperty(dsName + "_" + dbName + "_"
                + schemaName + ".password"));
        return dsInfo;
    }

    /**
     * 获取数据源信息(realDbName为实际数据库名)
     * 
     * @param dsName
     * @param dbName
     * @param realDbName
     * @return
     */
    private DSInfo getDSInfo(String dsName, String dbName, String realDbName) {
        Properties prop = new Properties();
        InputStream in = this.getClass().getResourceAsStream(Const.DBCONF_PATH);
        try {
            prop.load(in);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Read properties file error ["
                    + Const.DBCONF_PATH + "] " + e.getMessage());
        }
        DSInfo dsInfo = new DSInfo();
        dsInfo.setType(dsName);
        dsInfo.setDbname(realDbName);
        dsInfo.setIp(prop.getProperty(dsName + "_" + dbName + ".ip"));
        dsInfo.setPort(prop.getProperty(dsName + "_" + dbName + ".port"));
        dsInfo.setUsername(prop
                .getProperty(dsName + "_" + dbName + ".username"));
        dsInfo.setPassword(prop
                .getProperty(dsName + "_" + dbName + ".password"));
        return dsInfo;
    }

    /**
     * 获取JDBC连接
     * 
     * @param dsInfo
     * @return
     */
    private Connection getConnection(DSInfo dsInfo) {
        String url = "";
        String type = dsInfo.getType();
        String ip = dsInfo.getIp();
        String port = dsInfo.getPort();
        String dbName = dsInfo.getDbname();
        String username = dsInfo.getUsername();
        String password = dsInfo.getPassword();
        try {
            if (Const.DATASOURCE_TYPE_MYSQL.equals(type)) {
                url = "jdbc:mysql://" + ip + ":" + port + "/" + dbName
                        + "?useUnicode=true&characterEncoding=utf-8";
                Class.forName("com.mysql.jdbc.Driver");
            } else if (Const.DATASOURCE_TYPE_SQLSERVER.equals(type)) {
                url = "jdbc:sqlserver://" + ip + ":" + port + ";DatabaseName="
                        + dbName;
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } else if (Const.DATASOURCE_TYPE_GP.equals(type)) {
                url = "jdbc:postgresql://" + ip + ":" + port + "/" + dbName
                        + "?useUnicode=true&characterEncoding=utf-8";
                Class.forName("org.postgresql.Driver");
            } else if (Const.DATASOURCE_TYPE_GPANALYSIS.equals(type)) {
                url = "jdbc:postgresql://" + ip + ":" + port + "/" + dbName
                        + "?useUnicode=true&characterEncoding=utf-8";
                Class.forName("org.postgresql.Driver");
            } else if ("gpreport58".equals(type)) {
                url = "jdbc:postgresql://" + ip + ":" + port + "/" + dbName
                        + "?useUnicode=true&characterEncoding=utf-8";
                Class.forName("org.postgresql.Driver");
            } else if ("gpreport59".equals(type)) {
                url = "jdbc:postgresql://" + ip + ":" + port + "/" + dbName
                        + "?useUnicode=true&characterEncoding=utf-8";
                Class.forName("org.postgresql.Driver");
            } else if (Const.DATASOURCE_TYPE_HIVEMETA.equals(type)) {
                url = "jdbc:mysql://" + ip + ":" + port + "/" + dbName
                        + "?useUnicode=true&characterEncoding=utf-8";
                Class.forName("com.mysql.jdbc.Driver");
            } else if (Const.DATASOURCE_TYPE_HIVE.equals(type)) {
                url = "jdbc:hive2://" + ip + ":" + port + "/" + dbName + ";principal=hadoop/10.1.1.161@DIANPING.COM";// +
                                                                        // "?useUnicode=true&characterEncoding=utf-8";
                Class.forName("org.apache.hive.jdbc.HiveDriver");
            } else {
                throw new RuntimeException(
                        "unsupport from " + null == type ? "null" : type);
            }
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(
                    "Class.forName(jdbc driver) cannot resolve driver class, type ["
                            + type + "] " + e.getMessage());
        }

        Connection ret = null;
        try {
            ret = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            if (Const.DATASOURCE_TYPE_HIVE.equals(type)) {
                Configuration conf = new Configuration();
                // Kerberos Authentication
                UserGroupInformation.setConfiguration(conf);
                try {
                    SecurityUtil.login(conf, "test.hadoop.keytab.file",
                            "test.hadoop.principal");
                }
                catch (IOException ex) {
                    log.error("hive login failed");
                    ex.printStackTrace();
                    throw new RuntimeException(
                            "hive login failed, JDBC Driver Manager cannot get connection, url ["
                                    + url + "], username [" + username
                                    + "], password [ ... ] " + e.getMessage());
                }
                try {
                    ret = DriverManager.getConnection(url, username, password);
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    throw new RuntimeException(
                            "JDBC Driver Manager cannot get connection, url ["
                                    + url + "], username [" + username
                                    + "], password [ ... ] " + e1.getMessage());
                }
                return ret;
            }
            log.error(e.getMessage(), e);
            throw new RuntimeException(
                    "JDBC Driver Manager cannot get connection, url [" + url
                            + "], username [" + username
                            + "], password [ ... ] " + e.getMessage());
        }
        return ret;
    }

    /**
     * 生成mysql源表的ResultSet
     * 
     * @param dsName
     * @param dbName
     * @param tableName
     * @return
     */
    private Object[] fromMysql(String dsName, String dbName, String tableName) {
        DSInfo dsInfo = getDSInfo(dsName, dbName);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;
        try {
            conn = getConnection(dsInfo);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            sql = " SELECT" + " column_name,data_type,character_maximum_length"
                    + " FROM information_schema.columns" + " WHERE"
                    + " table_schema='" + dsInfo.getDbname()
                    + "' AND table_name ='" + tableName + "'"
                    + " ORDER BY ordinal_position";
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            throw new RuntimeException("cannot get resultset from mysql, sql ["
                    + sql + "] " + e.getMessage());
        }
        return new Object[] { conn, stmt, rs };
    }

    /**
     * 生成hive源表的ResultSet
     * 
     * @param dsName
     * @param dbName
     * @param tableName
     * @return
     */
    private Object[] fromHive(String dsName, String dbName, String tableName) {
        DSInfo dsInfo = getDSInfo(dsName, dbName);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;
        try {
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            sql = " SELECT * FROM ("
                    + " SELECT"
                    + " a.column_name,a.type_name AS data_type,'' AS character_maximum_length"
                    + " FROM COLUMNS_V2 a"
                    + " JOIN SDS c ON a.cd_id=c.cd_id"
                    + " JOIN TBLS b ON c.sd_id=b.sd_id"
                    + " JOIN DBS d ON b.db_id=d.db_id"
                    + " WHERE"
                    + " b.tbl_name='"
                    + tableName
                    + "' AND d.name='"
                    + dbName
                    + "'"
                    + " ORDER BY integer_idx"
                    + " ) z"
                    + " UNION ALL"
                    + " SELECT"
                    + " a.PKEY_NAME AS column_name,a.PKEY_TYPE AS data_type,'' AS character_maximum_length"
                    + " FROM PARTITION_KEYS a"
                    + " JOIN TBLS b ON (a.TBL_ID = b.TBL_ID)"
                    + " JOIN DBS c ON(b.DB_ID = c.DB_ID)" + " WHERE"
                    + " b.TBL_NAME='" + tableName + "' AND c.NAME='" + dbName
                    + "'";
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            throw new RuntimeException("cannot get resultset from hive, sql ["
                    + sql + "] " + e.getMessage());
        }
        return new Object[] { conn, stmt, rs };
    }

    /**
     * 生成sqlserver源表的ResultSet
     * 
     * @param dsName
     * @param dbName
     * @param tableName
     * @return
     */
    private Object[] fromSqlserver(String dsName, String dbName,
            String schemaName, String tableName) {
        DSInfo dsInfo = getDSInfo(dsName, dbName);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;
        try {
            conn = getConnection(dsInfo);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            sql = " SELECT"
                    + " b.name AS column_name,c.name AS data_type,b.length AS character_maximum_length"
                    + " FROM sysobjects a" + " JOIN syscolumns b ON a.id=b.id"
                    + " JOIN systypes c ON b.xtype=c.xtype" + " WHERE"
                    + " a.name='" + tableName + "'"
                    + " AND SCHEMA_NAME(a.uid)='" + schemaName + "'"
                    + " AND c.name<>'sysname'" + " ORDER BY b.colorder";
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            throw new RuntimeException(
                    "cannot get resultset from sqlserver, sql [" + sql + "] "
                            + e.getMessage());
        }
        return new Object[] { conn, stmt, rs };
    }

    /**
     * 根据源表的ResultSet生成gp建表语句
     * 
     * @param rs
     * @param datasourceType
     * @param targetSchemaName
     * @param targetSchemaTable
     * @param targetSegmentColumn
     * @return
     */
    private String toGp(ResultSet rs, String datasourceType,
            String targetSchemaName, String targetSchemaTable,
            String targetSegmentColumn) {
        String targetDatasourceType = "gp";
        Properties prop = new Properties();
        InputStream in = this.getClass()
                .getResourceAsStream(Const.MAPPING_PATH);
        try {
            prop.load(in);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Read properties file error ["
                    + Const.MAPPING_PATH + "] " + e.getMessage());
        }
        String sqls = "CREATE TABLE " + targetSchemaTable + "(\n";
        try {
            while (rs.next()) {
                String dataType = rs.getString("data_type");
                if (CommonUtil.isEmpty(dataType)) {
                    throw new RuntimeException(
                            "cannot get data type, null or empty");
                }
                dataType = dataType.trim();
                String columnName = rs.getString("column_name");
                if (CommonUtil.isEmpty(columnName)) {
                    throw new RuntimeException(
                            "cannot get column name, null or empty");
                }
                // 目标列名全部小写
                columnName = columnName.toLowerCase().trim();
                String type = "";
                if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)
                        && dataType.startsWith("array<")) {
                    type = "text";
                } else {
                    try {
                        type = prop.getProperty(
                                (Const.DATASOURCE_TYPE_HIVE
                                        .equals(datasourceType) ? "hivemeta"
                                        : datasourceType)
                                        + "_"
                                        + targetDatasourceType
                                        + "."
                                        + dataType).trim();
                    } catch (Exception e) {
                        type = "text";
                    }
                }
                if (CommonUtil.isEmpty(type)) {
                    throw new RuntimeException(
                            "cannot find column mapping, properties file ["
                                    + Const.MAPPING_PATH + "], data_type ["
                                    + dataType + "], from [" + datasourceType
                                    + "], to [" + targetDatasourceType + "]");
                }
                if ("varchar".equals(type) || "char".equals(type)) {
                    String characterMaximumLength = rs
                            .getString("character_maximum_length");
                    if (null == characterMaximumLength
                            || characterMaximumLength.isEmpty()) {
                        throw new RuntimeException(
                                "cannot get character maximum length, null or empty");
                    }
                    characterMaximumLength = characterMaximumLength.trim();
                    sqls += "\t\"" + columnName + "\" " + type + "("
                            + characterMaximumLength + "),\n";
                } else {
                    sqls += "\t\"" + columnName + "\" " + type + ",\n";
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("toGp error " + e.getMessage());
        }
        if (!sqls.contains("\t\"dw_add_ts\"")) {
            sqls += "\t\"dw_add_ts\" timestamp DEFAULT current_timestamp,\n";
        }
        sqls = sqls.substring(0, sqls.length() - 2) + "\n";
        if (null == targetSegmentColumn || targetSegmentColumn.isEmpty()) {
            sqls += ") WITH (APPENDONLY=true, COMPRESSLEVEL=6, ORIENTATION=column, COMPRESSTYPE=zlib, OIDS=false) DISTRIBUTED RANDOMLY;";
        } else {
            sqls += ") WITH (APPENDONLY=true, COMPRESSLEVEL=6, ORIENTATION=column, COMPRESSTYPE=zlib, OIDS=false) DISTRIBUTED RANDOMLY \n"
                    + "PARTITION BY RANGE(\"" + targetSegmentColumn + "\")(";
            DateTime bt = new DateTime();
            bt = bt.minusMonths(1);
            bt = bt.minusDays(1);
            DateTime et = new DateTime();
            et = et.minusDays(1);
            sqls += "PARTITION p" + bt.toString("yyyyMMdd") + " START ('"
                    + bt.toString("yyyyMMdd") + "'::date) END ('"
                    + et.toString("yyyyMMdd") + "'::date));";
            DateTime curr = new DateTime().minusDays(1);
            DateTime next = new DateTime();
            for (int i = 0; i < PARTITION_CNT; ++i) {
                sqls += "ALTER TABLE "
                        + targetSchemaTable
                        + " ADD PARTITION p"
                        + curr.toString("yyyyMMdd")
                        + " START ('"
                        + curr.toString("yyyyMMdd")
                        + "'::date)"
                        + " END ('"
                        + next.toString("yyyyMMdd")
                        + "'::date)"
                        + " WITH (APPENDONLY=true, COMPRESSLEVEL=6, ORIENTATION=column, COMPRESSTYPE=zlib, OIDS=FALSE);";
                curr = curr.plusDays(1);
                next = next.plusDays(1);
            }
            sqls += "ALTER TABLE " + targetSchemaTable
                    + " ADD DEFAULT PARTITION other WITH (appendonly=false);";
        }
        return sqls;
    }

    /**
     * 根据源表的ResultSet生成gp建表语句
     * 
     * @param rs
     * @param datasourceType
     * @param targetSchemaName
     * @param targetSchemaTable
     * @param targetSegmentColumn
     * @return
     */
    private String toPostgresql2(ResultSet rs, String datasourceType,
            String targetSchemaName, String targetSchemaTable,
            String targetSegmentColumn) {
        String targetDatasourceType = "gp";
        Properties prop = new Properties();
        InputStream in = this.getClass()
                .getResourceAsStream(Const.MAPPING_PATH);
        try {
            prop.load(in);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Read properties file error ["
                    + Const.MAPPING_PATH + "] " + e.getMessage());
        }
        String sqls = "CREATE TABLE " + targetSchemaTable + "(\n";
        try {
            while (rs.next()) {
                String dataType = rs.getString("data_type");
                if (CommonUtil.isEmpty(dataType)) {
                    throw new RuntimeException(
                            "cannot get data type, null or empty");
                }
                dataType = dataType.trim();
                String columnName = rs.getString("column_name");
                if (CommonUtil.isEmpty(columnName)) {
                    throw new RuntimeException(
                            "cannot get column name, null or empty");
                }
                // 目标列名全部小写
                columnName = columnName.toLowerCase().trim();
                String type = "";
                if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)
                        && dataType.startsWith("array<")) {
                    type = "text";
                } else {
                    type = prop
                            .getProperty(
                                    (Const.DATASOURCE_TYPE_HIVE
                                            .equals(datasourceType) ? "hivemeta"
                                            : datasourceType)
                                            + "_"
                                            + targetDatasourceType
                                            + "."
                                            + dataType).trim();
                }
                if (CommonUtil.isEmpty(type)) {
                    throw new RuntimeException(
                            "cannot find column mapping, properties file ["
                                    + Const.MAPPING_PATH + "], data_type ["
                                    + dataType + "], from [" + datasourceType
                                    + "], to [" + targetDatasourceType + "]");
                }
                if ("varchar".equals(type) || "char".equals(type)) {
                    String characterMaximumLength = rs
                            .getString("character_maximum_length");
                    if (null == characterMaximumLength
                            || characterMaximumLength.isEmpty()) {
                        throw new RuntimeException(
                                "cannot get character maximum length, null or empty");
                    }
                    characterMaximumLength = characterMaximumLength.trim();
                    sqls += "\t\"" + columnName + "\" " + type + "("
                            + characterMaximumLength + "),\n";
                } else {
                    sqls += "\t\"" + columnName + "\" " + type + ",\n";
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("toGp error " + e.getMessage());
        }
        if (!sqls.contains("\t\"dw_add_ts\"")) {
            sqls += "\t\"dw_add_ts\" timestamp DEFAULT current_timestamp,\n";
        }
        sqls = sqls.substring(0, sqls.length() - 2) + "\n";
        if (null == targetSegmentColumn || targetSegmentColumn.isEmpty()) {
            sqls += ") ;";
        } else {
            sqls += ") ;" + "\n";
            DateTime bt = new DateTime();
            bt = bt.minusMonths(1);
            bt = bt.minusDays(1);
            DateTime et = new DateTime();
            et = et.minusDays(1);
            // sqls += "PARTITION p" + bt.toString("yyyyMMdd") + " START ('" +
            // bt.toString("yyyyMMdd") + "'::date) END ('" +
            // et.toString("yyyyMMdd") + "'::date));";
            DateTime curr = new DateTime().minusDays(1);
            DateTime next = new DateTime();
            for (int i = 0; i < PARTITION_CNT; ++i) {
                sqls += "CREATE TABLE " + targetSchemaTable + "_"
                        + curr.toString("yyyyMMdd") + "(check ( "
                        + targetSegmentColumn + " >= DATE '"
                        + curr.toString("yyyy-MM-dd") + "'" + " and "
                        + targetSegmentColumn + " < DATE '"
                        + next.toString("yyyy-MM-dd") + "')) INHERITS("
                        + targetSchemaTable + "); \n";
                curr = curr.plusDays(1);
                next = next.plusDays(1);
            }
        }
        return sqls;
    }

    /**
     * 根据源表的ResultSet生成hive建表语句
     * 
     * @param rs
     * @param datasourceType
     * @param targetSchemaName
     * @param targetSchemaTable
     * @param targetSegmentColumn
     * @return
     */
    private String toHive(ResultSet rs, String datasourceType,
            String targetSchemaName, String targetSchemaTable,
            String targetSegmentColumn) {
        String targetDatasourceType = Const.DATASOURCE_TYPE_HIVE;
        Properties prop = new Properties();
        InputStream in = this.getClass()
                .getResourceAsStream(Const.MAPPING_PATH);
        try {
            prop.load(in);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Read properties file error ["
                    + Const.MAPPING_PATH + "] " + e.getMessage());
        }
        String sql = "CREATE TABLE " + targetSchemaTable + "(\n";
        try {
            while (rs.next()) {
                String dataType = rs.getString("data_type");
                if (CommonUtil.isEmpty(dataType)) {
                    throw new RuntimeException(
                            "cannot get data type, null or empty");
                }
                dataType = dataType.trim();
                String columnName = rs.getString("column_name");
                if (CommonUtil.isEmpty(columnName)) {
                    throw new RuntimeException(
                            "cannot get column name, null or empty");
                }
                // 目标列名全部小写
                columnName = columnName.toLowerCase().trim();
                String type = prop.getProperty(
                        datasourceType + "_" + targetDatasourceType + "."
                                + dataType).trim();
                if (CommonUtil.isEmpty(type)) {
                    throw new RuntimeException(
                            "cannot find column mapping, properties file ["
                                    + Const.MAPPING_PATH + "], data_type ["
                                    + dataType + "], from [" + datasourceType
                                    + "], to [" + targetDatasourceType + "]");
                }
                sql += "\t`" + columnName + "` " + type + ",\n";
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("toHive error " + e.getMessage());
        }
        if (null == targetSegmentColumn || targetSegmentColumn.isEmpty()) {
            sql += "\t`dw_add_ts` string\n) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\005'"
                    + " STORED AS INPUTFORMAT \"com.hadoop.mapred.DeprecatedLzoTextInputFormat\" OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\";";
        } else {
            sql += "\t`dw_add_ts` string\n) PARTITIONED BY ("
                    + targetSegmentColumn
                    + " string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\005'"
                    + " STORED AS INPUTFORMAT \"com.hadoop.mapred.DeprecatedLzoTextInputFormat\" OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\";";
        }
        return sql;
    }

    @SuppressWarnings("unchecked")
    private String toHive2(List<Map<String, String>> rs, String datasourceType,
            String targetSchemaName, String targetSchemaTable,
            String targetSegmentColumn) {
        String targetDatasourceType = Const.DATASOURCE_TYPE_HIVE;
        Properties prop = new Properties();
        InputStream in = this.getClass()
                .getResourceAsStream(Const.MAPPING_PATH);
        try {
            prop.load(in);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Read properties file error ["
                    + Const.MAPPING_PATH + "] " + e.getMessage());
        }
        String sql = "CREATE TABLE " + targetSchemaTable + "(\n";

        // List<Map<String,String>> rs = (List<Map<String,String>>)obj;
        for (Map<String, String> map : rs) {
            String dataType = map.get("data_type");
            if (CommonUtil.isEmpty(dataType)) {
                throw new RuntimeException(
                        "cannot get data type, null or empty");
            }
            dataType = dataType.trim();
            String columnName = map.get("column_name");
            if (CommonUtil.isEmpty(columnName)) {
                throw new RuntimeException(
                        "cannot get column name, null or empty");
            }
            // 目标列名全部小写
            columnName = columnName.toLowerCase().trim();
            String type = prop.getProperty(
                    datasourceType + "_" + targetDatasourceType + "."
                            + dataType).trim();
            if (CommonUtil.isEmpty(type)) {
                throw new RuntimeException(
                        "cannot find column mapping, properties file ["
                                + Const.MAPPING_PATH + "], data_type ["
                                + dataType + "], from [" + datasourceType
                                + "], to [" + targetDatasourceType + "]");
            }
            sql += "\t`" + columnName + "` " + type + ",\n";
        }

        if (null == targetSegmentColumn || targetSegmentColumn.isEmpty()) {
            // sql +=
            // "\t`dw_add_ts` timestamp\n) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\005'"
            // +
            // " STORED AS INPUTFORMAT \"com.hadoop.mapred.DeprecatedLzoTextInputFormat\" OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\";";
            sql = sql.substring(0, sql.length() - 2)
                    + "\n) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\005'"
                    + " STORED AS INPUTFORMAT \"com.hadoop.mapred.DeprecatedLzoTextInputFormat\" OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\";";
        } else {
            // sql += "\t`dw_add_ts` timestamp\n) PARTITIONED BY (" +
            // targetSegmentColumn +
            // " string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\005'" +
            // " STORED AS INPUTFORMAT \"com.hadoop.mapred.DeprecatedLzoTextInputFormat\" OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\";";
            sql = sql.substring(0, sql.length() - 2)
                    + "\n) PARTITIONED BY ("
                    + targetSegmentColumn
                    + " string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\005'"
                    + " STORED AS INPUTFORMAT \"com.hadoop.mapred.DeprecatedLzoTextInputFormat\" OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\";";
        }
        return sql;
    }   

    /**
     * action的建表请求到这里
     * 
     * @param owner
     * @param targetIsActiveSchedule
     * @param targetTableType
     * @param datasourceType
     * @param databaseName
     * @param tableName
     * @param targetDatasourceType
     * @param targetSchemaName
     * @param targetSchemaTable
     * @param targetSegmentColumn
     * @return
     */
    public synchronized boolean createTable2(String owner,
            String targetIsActiveSchedule, String targetTableType,
            String datasourceType, String databaseName, String schemaName,
            String tableName, String targetDatasourceType,
            String targetSchemaName, String targetSchemaTable,
            String targetSegmentColumn) {

        String schemaTable = databaseName + "." + tableName;

        String sourceAllColumn = "";
        List<String[]> columnList = new ArrayList<String[]>();
        boolean ok = true;
        String sqls = null;

        if (Const.DATASOURCE_TYPE_SALESFORCE.equals(datasourceType)) {
            List<Map<String, String>> tmpRs1 = salesforceService
                    .fromSalesforce(tableName);

            if (tmpRs1 != null) {
                int idx = 0;
                // while (tmpRs.next()) {
                for (Map<String, String> tmp : tmpRs1) {
                    // String dataType = tmpRs.getString("data_type");
                    String dataType = tmp.get("data_type");
                    if (CommonUtil.isEmpty(dataType)) {
                        throw new RuntimeException(
                                "cannot get data type, null or empty");
                    }
                    dataType = dataType.trim();
                    // String columnName = tmpRs.getString("column_name");
                    String columnName = tmp.get("column_name");
                    if (CommonUtil.isEmpty(columnName)) {
                        throw new RuntimeException(
                                "cannot get column name, null or empty");
                    }
                    // 目标列名全部小写
                    columnName = columnName.toLowerCase().trim();
                    sourceAllColumn += columnName + ",";
                    String characterMaximumLength = tmp
                            .get("character_maximum_length");
                    columnList.add(new String[] { "" + (++idx), columnName,
                            dataType, characterMaximumLength });
                }
                if (!sourceAllColumn.isEmpty()) {
                    sourceAllColumn = sourceAllColumn.substring(0,
                            sourceAllColumn.length() - 1);
                }

                sqls = generateDDLFromResultSet2(tmpRs1, datasourceType,
                        databaseName, tableName, targetDatasourceType,
                        targetSchemaName, targetSchemaTable,
                        targetSegmentColumn);
            }
        } else {
            // step 0: generate result set
            Object[] arr = generateResultSet(datasourceType, databaseName,
                    schemaName, tableName);

            ResultSet tmpRs = (ResultSet) arr[2];
            try {
                if (tmpRs != null) {
                    int idx = 0;
                    while (tmpRs.next()) {
                        String dataType = tmpRs.getString("data_type");
                        if (CommonUtil.isEmpty(dataType)) {
                            throw new RuntimeException(
                                    "cannot get data type, null or empty");
                        }
                        dataType = dataType.trim();
                        String columnName = tmpRs.getString("column_name");
                        if (CommonUtil.isEmpty(columnName)) {
                            throw new RuntimeException(
                                    "cannot get column name, null or empty");
                        }
                        // 目标列名全部小写
                        columnName = columnName.toLowerCase().trim();
                        sourceAllColumn += columnName + ",";
                        String characterMaximumLength = tmpRs
                                .getString("character_maximum_length");
                        columnList.add(new String[] { "" + (++idx), columnName,
                                dataType, characterMaximumLength });
                    }
                    if (!sourceAllColumn.isEmpty()) {
                        sourceAllColumn = sourceAllColumn.substring(0,
                                sourceAllColumn.length() - 1);
                    }
                }
                tmpRs.beforeFirst();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException("iterate resultset error "
                        + e.getMessage());
            }

            // step 1: generate ddl
            sqls = generateDDLFromResultSet((ResultSet) arr[2], datasourceType,
                    databaseName, tableName, targetDatasourceType,
                    targetSchemaName, targetSchemaTable, targetSegmentColumn);

            // close rs,stmt,conn

            if (arr != null) {
                if (arr.length > 0 && arr[0] != null) {
                    try {
                        ((Connection) arr[0]).close();
                    } catch (SQLException e1) {
                        ok = false;
                        e1.printStackTrace();
                    }
                }
                if (arr.length > 1 && arr[1] != null) {
                    try {
                        ((Statement) arr[1]).close();
                    } catch (SQLException e1) {
                        ok = false;
                        e1.printStackTrace();
                    }
                }
                if (arr.length > 2 && arr[2] != null) {
                    try {
                        ((ResultSet) arr[2]).close();
                    } catch (SQLException e1) {
                        ok = false;
                        e1.printStackTrace();
                    }
                }
            }
            if (!ok) {
                throw new RuntimeException("close rs,stmt,conn error");
            }
        }

        // step 2: create hive/gp table
        if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
            createTableHive(sqls);
        } else if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)) {
            createTableGp57(targetSchemaName, sqls);
        } else if (Const.DATASOURCE_TYPE_GPREPORT.equals(targetDatasourceType)) {
            createTableGPReport58(targetSchemaName, sqls);
            createTableGPReport59(targetSchemaName, sqls);
        } else if (Const.DATASOURCE_TYPE_GPANALYSIS
                .equals(targetDatasourceType)) {
            createTableGPAnalysis(targetSchemaName, sqls);
        } else {
            throw new RuntimeException("unsupport to [" + targetDatasourceType
                    + "]");
        }

        // ///////////////////////////////////

        DSInfo dsInfo3 = null;
        Connection conn3 = null;
        PreparedStatement stmt4, stmt5, stmt6, stmt7, stmt8, stmt9, stmt10, stmt11, stmt12;
        stmt4 = stmt5 = stmt6 = stmt7 = stmt8 = stmt9 = stmt10 = stmt11 = stmt12 = null;

        PreparedStatement stmt14, stmt15, stmt16, stmt17, stmt18, stmt19, stmt20, stmt21;
        stmt14 = stmt15 = stmt16 = stmt17 = stmt18 = stmt19 = stmt20 = stmt21 = null;

        ResultSet rs4 = null;
        ResultSet rs14 = null;

        long ctm = System.currentTimeMillis();
        Timestamp ts = new Timestamp(ctm);
        SimpleDateFormat formatter1 = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");
        String currTime1 = formatter1.format(new Date(ctm));
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String currTime2 = formatter2.format(new Date(ctm));
        SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy-MM-dd");
        String currTime3 = formatter3.format(new Date(ctm));
        String currTime4 = formatter3
                .format(new Date(ctm - 24 * 60 * 60 * 1000));
        String pSql = "";
        try {
            dsInfo3 = getDSInfo(Const.DATASOURCE_TYPE_MYSQL, "darkbat");
            conn3 = getConnection(dsInfo3);
            conn3.setAutoCommit(false);

            if ("0".equals(targetIsActiveSchedule)) {
                // step 3: insert etl_task_status
                pSql = " INSERT INTO etl_task_status"
                        + " ("
                        + "`task_status_id`,`task_id`,`task_name`,`task_group_id`,`database_src`,"
                        + "`task_obj`,`para1`,`para2`,`para3`,`log_path`,"
                        + "`cycle`,`time_id`,`status`,`sts_desc`,`if_wait`,"
                        + "`if_recall`,`if_pre`,`prio_lvl`,`recall_num`,`run_num`,"
                        + "`start_time`,`end_time`,`time_stamp`,`type`,`table_name`,"
                        + "`cal_dt`,`freq`,`owner`,`trigger_time`,`wait_code`,"
                        + "`recall_code`,`success_code`,`job_code`,`running_prio`,`timeout`"
                        + " )" + " VALUES" + " (" + "?,?,?,?,?," + "?,?,?,?,?,"
                        + "?,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?,"
                        + "?,?,?,?,?," + "?,?,?,?,?" + " )";
                stmt4 = conn3.prepareStatement(pSql);
                stmt4.setString(1, "tmp_" + currTime2);
                stmt4.setInt(2, 99999);
                stmt4.setString(3, "tmp_load##" + targetSchemaTable);
                stmt4.setInt(4, 1);
                stmt4.setString(5, datasourceType + "_" + databaseName);
                stmt4.setString(6,
                        "sh /data/deploy/dwarch/conf/ETL/bin/start_tmp_autoetl.sh");
                String tmpType = "";
                if ("hive".equals(datasourceType)
                        && "greenplum".equals(targetDatasourceType)) {
                    tmpType = "1";
                } else if ("mysql".equals(datasourceType)
                        && "greenplum".equals(targetDatasourceType)) {
                    tmpType = "2";
                } else if ("mysql".equals(datasourceType)
                        && "hive".equals(targetDatasourceType)) {
                    tmpType = "3";
                } else if ("sqlserver".equals(datasourceType)
                        && "greenplum".equals(targetDatasourceType)) {
                    tmpType = "4";
                } else if ("sqlserver".equals(datasourceType)
                        && "hive".equals(targetDatasourceType)) {
                    tmpType = "5";
                } else if ("salesforce".equals(datasourceType)
                        && "greenplum".equals(targetDatasourceType)) {
                    tmpType = "6";
                } else if ("salesforce".equals(datasourceType)
                        && "hive".equals(targetDatasourceType)) {
                    tmpType = "7";
                }

                String tmpSourceTable = "";
                if ("hive".equals(datasourceType)) {
                    tmpSourceTable = databaseName + "." + tableName;
                } else if ("mysql".equals(datasourceType)) {
                    tmpSourceTable = tableName;
                } else if ("sqlserver".equals(datasourceType)) {
                    // TODO add schema name
                    tmpSourceTable = "dbo." + tableName;
                }
                String tmpTargetSchemaTable = "";
                if ("hive".equals(targetDatasourceType)) {
                    tmpTargetSchemaTable = "bi." + targetSchemaTable;
                } else if ("greenplum".equals(targetDatasourceType)) {
                    tmpTargetSchemaTable = targetSchemaTable;
                }
                stmt4.setString(7, tmpType + " " + datasourceType + "_"
                        + databaseName + " " + tmpSourceTable + " "
                        + tmpTargetSchemaTable);
                if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)) {
                    stmt4.setString(8, sourceAllColumn.isEmpty() ? "" : ("\""
                            + sourceAllColumn.replaceAll(",", "\",\"") + "\""));
                } else {
                    stmt4.setString(8, sourceAllColumn.isEmpty() ? "" : ("`"
                            + sourceAllColumn.replaceAll(",", "`,`") + "`"));
                }
                stmt4.setString(9, null);
                stmt4.setString(10, "/data/deploy/dwarch/log/ETL/wormhole/tmp/"
                        + targetSchemaTable + ".log");
                stmt4.setString(11, "D");
                stmt4.setString(12, currTime3);
                stmt4.setInt(13, 0);
                stmt4.setString(14, "INIT");
                stmt4.setInt(15, 0);
                stmt4.setInt(16, 0);
                stmt4.setInt(17, 0);
                stmt4.setInt(18, 4);
                stmt4.setInt(19, 0);
                stmt4.setInt(20, 0);
                stmt4.setTimestamp(21, null);
                stmt4.setTimestamp(22, null);
                stmt4.setTimestamp(23, ts);
                stmt4.setInt(24, 1);
                stmt4.setString(25, targetSchemaTable);
                stmt4.setString(26, currTime4);
                stmt4.setString(27, null);
                stmt4.setString(28, owner);
                stmt4.setLong(29, ctm);
                stmt4.setString(30, null);
                stmt4.setString(31, null);
                stmt4.setString(32, "0");
                stmt4.setInt(33, 0);
                stmt4.setInt(34, 0);
                stmt4.setInt(35, 60);
                stmt4.executeUpdate();
            } else if ("1".equals(targetIsActiveSchedule)) {
                // step 3: get task id
                int taskId = 0;
                int taskId1 = 0;
                pSql = " SELECT max(task_id)+1" + " FROM etl_task_cfg"
                        + " WHERE task_group_id=? and type=? and task_id<?";
                stmt4 = conn3.prepareStatement(pSql);
                stmt4.setInt(1, 1);
                stmt4.setInt(2, 1);
                stmt4.setInt(3, 20000);
                rs4 = stmt4.executeQuery();
                while (rs4.next()) {
                    taskId = rs4.getInt(1);
                }

                // step 4: insert etl_task_cfg
                pSql = " INSERT INTO etl_task_cfg"
                        + " ("
                        + "`add_time`,`add_user`,`cycle`,`database_src`,`freq`,"
                        + "`if_pre`,`if_recall`,`if_val`,`if_wait`,`log_file`,"
                        + "`log_home`,`offset`,`offset_type`,`owner`,`para1`,"
                        + "`para2`,`para3`,`prio_lvl`,`recall_code`,`remark`,"
                        + "`success_code`,`table_name`,`task_group_id`,`task_id`,`task_name`,"
                        + "`task_obj`,`timeout`,`type`,`update_time`,`update_user`,"
                        + "`wait_code`" + " )" + " VALUES" + " ("
                        + "?,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?,"
                        + "?,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?," + "?"
                        + " )";
                stmt5 = conn3.prepareStatement(pSql);
                stmt5.setTimestamp(1, ts);
                stmt5.setString(2, owner);
                stmt5.setString(3, "D");
                if (Const.DATABASE_TYPE_HIVE.equals(datasourceType)
                        && (Const.DATASOURCE_TYPE_GP
                                .equals(targetDatasourceType))) {
                    stmt5.setString(4, "greenplum");
                } else if (Const.DATASOURCE_TYPE_SALESFORCE
                        .equals(datasourceType)) {
                    stmt5.setString(4, "salesforce");
                } else if (Const.DATASOURCE_TYPE_GPREPORT
                        .equals(targetDatasourceType)) {
                    stmt5.setString(4, "gpreport58");
                } else if (Const.DATASOURCE_TYPE_GPANALYSIS
                        .equals(targetDatasourceType)) {
                    stmt5.setString(4, "gpanalysis");
                } else {
                    stmt5.setString(4, datasourceType + "_" + databaseName);
                }
                stmt5.setString(5, "0 5 0 * * ?");
                if(Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)){
                    stmt5.setInt(6, 1);
                }else{
                    stmt5.setInt(6, 0);
                }
                stmt5.setInt(7, 1);
                stmt5.setInt(8, 1);
                stmt5.setInt(9, 0);
                stmt5.setString(10, targetSchemaTable);
                stmt5.setString(11, "${wormhole_log_home}/wormhole");
                stmt5.setString(12, "D0");
                stmt5.setString(13, "offset");
                stmt5.setString(14, owner);
                stmt5.setString(
                        15,
                        "\""
                                + taskId
                                + " "
                                + datasourceType
                                + " "
                                + (Const.DATASOURCE_TYPE_HIVE
                                        .equals(targetDatasourceType) ? "hdfs"
                                        : Const.DATASOURCE_TYPE_GPANALYSIS
                                                .equals(targetDatasourceType)
                                                || Const.DATASOURCE_TYPE_GPREPORT
                                                        .equals(targetDatasourceType) ? "greenplum"
                                                : targetDatasourceType) + "\"");
                if (Const.DATASOURCE_TYPE_GPREPORT.equals(targetDatasourceType)) {
                    stmt5.setString(16,
                            "\"/data/deploy/dwarch/conf/ETL/job/auto_etl/"
                                    + targetSchemaTable + "58.xml\"");
                } else if (Const.DATASOURCE_TYPE_GPANALYSIS
                        .equals(targetDatasourceType)) {
                    stmt5.setString(16,
                            "\"/data/deploy/dwarch/conf/ETL/job/auto_etl/"
                                    + targetSchemaTable + "62.xml\"");
                } else {
                    stmt5.setString(16,
                            "\"/data/deploy/dwarch/conf/ETL/job/auto_etl/"
                                    + targetSchemaTable + ".xml\"");
                }

                if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)
                        && Const.TABLE_TYPE_RIZHIBIAO.equals(targetTableType)) {
                    if (CommonUtil.isEmpty(targetSegmentColumn)) {
                        throw new RuntimeException(
                                "target segment column is null or empty");
                    }
                    stmt5.setString(17,
                            "\" @{cal_dt}=${cal_dt} @{ncal_dt}=${ncal_dt}  @{dw57_table}="
                                    + targetSchemaTable + " \"");
                } else if (Const.DATASOURCE_TYPE_GPREPORT
                        .equals(targetDatasourceType)
                        && Const.TABLE_TYPE_RIZHIBIAO.equals(targetTableType)) {
                    if (CommonUtil.isEmpty(targetSegmentColumn)) {
                        throw new RuntimeException(
                                "target segment column is null or empty");
                    }
                    stmt5.setString(17,
                            "\" @{cal_dt}=${cal_dt8} @{ncal_dt}=${cal_dt}  @{gp_table}="
                                    + targetSchemaTable + " \"");
                } else if (Const.DATASOURCE_TYPE_GPANALYSIS
                        .equals(targetDatasourceType)
                        && Const.TABLE_TYPE_RIZHIBIAO.equals(targetTableType)) {
                    if (CommonUtil.isEmpty(targetSegmentColumn)) {
                        throw new RuntimeException(
                                "target segment column is null or empty");
                    }
                    stmt5.setString(17,
                            "\" @{cal_dt}=${cal_dt8} @{ncal_dt}=${cal_dt}  @{analysis_table}="
                                    + targetSchemaTable + " \"");
                } else if (Const.DATASOURCE_TYPE_HIVE
                        .equals(targetDatasourceType)
                        && (Const.TABLE_TYPE_LISHIKUAIZHAOBIAO
                                .equals(targetTableType) || Const.TABLE_TYPE_RIZHIBIAO
                                .equals(targetTableType))) {
                    if (CommonUtil.isEmpty(targetSegmentColumn)) {
                        throw new RuntimeException(
                                "target segment column is null or empty");
                    }
                    stmt5.setString(17,
                            "\" @{cal_dt}=${cal_dt} @{ncal_dt}=${ncal_dt} \" \"${task_id}\" \"${cal_dt}\"");
                } else {
                    stmt5.setString(17,
                            "\" @{cal_dt}=${cal_dt} @{ncal_dt}=${ncal_dt} \" \"${task_id}\" \"${cal_dt}\"");
                }
                stmt5.setInt(18, 3);
                stmt5.setString(19, null);
                stmt5.setString(20, " ");
                stmt5.setString(21, "0");
                stmt5.setString(22, targetSchemaTable);
                stmt5.setInt(23, 1);
                stmt5.setInt(24, taskId);
                String tmpDsType = datasourceType;
                if (Const.DATASOURCE_TYPE_GP.equals(datasourceType)) {
                    tmpDsType = "gp";
                }
                String tmpTgtDsType = targetDatasourceType;
                if (Const.DATASOURCE_TYPE_GP.equals(tmpTgtDsType)) {
                    tmpTgtDsType = "gp";
                } else if (Const.DATASOURCE_TYPE_HIVE.equals(tmpTgtDsType)) {
                    tmpTgtDsType = "hdfs";
                }
                if (!Const.DATASOURCE_TYPE_GPREPORT
                        .equals(targetDatasourceType)) {
                    stmt5.setString(25, tmpDsType + "2" + tmpTgtDsType + "##"
                            + targetSchemaTable);
                } else {
                    stmt5.setString(25, tmpDsType + "2" + tmpTgtDsType + "58##"
                            + targetSchemaTable);
                }

                stmt5.setString(
                        26,
                        "ssh -o ConnectTimeout=3 -o ConnectionAttempts=5 -o PasswordAuthentication=no -o StrictHostKeyChecking=no -p 58422 deploy@10.1.6.49 sh /data/deploy/dwarch/conf/ETL/bin/start_autoetl.sh");
                stmt5.setInt(27, 90);
                stmt5.setInt(28, 1);
                stmt5.setTimestamp(29, ts);
                stmt5.setString(30, owner);
                stmt5.setString(31, null);
                stmt5.executeUpdate();

                if (Const.DATASOURCE_TYPE_GPREPORT.equals(targetDatasourceType)) {
                    // step 3: get task id
                    taskId1 = taskId + 1;
                    // step 4: insert etl_task_cfg
                    pSql = " INSERT INTO etl_task_cfg"
                            + " ("
                            + "`add_time`,`add_user`,`cycle`,`database_src`,`freq`,"
                            + "`if_pre`,`if_recall`,`if_val`,`if_wait`,`log_file`,"
                            + "`log_home`,`offset`,`offset_type`,`owner`,`para1`,"
                            + "`para2`,`para3`,`prio_lvl`,`recall_code`,`remark`,"
                            + "`success_code`,`table_name`,`task_group_id`,`task_id`,`task_name`,"
                            + "`task_obj`,`timeout`,`type`,`update_time`,`update_user`,"
                            + "`wait_code`" + " )" + " VALUES" + " ("
                            + "?,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?,"
                            + "?,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?," + "?"
                            + " )";
                    stmt15 = conn3.prepareStatement(pSql);
                    stmt15.setTimestamp(1, ts);
                    stmt15.setString(2, owner);
                    stmt15.setString(3, "D");
                    stmt15.setString(4, "gpreport59");
                    stmt15.setString(5, "0 5 0 * * ?");
                    if(Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)){
                        stmt15.setInt(6, 1);
                    }else{
                        stmt15.setInt(6, 0);
                    }
                    stmt15.setInt(7, 1);
                    stmt15.setInt(8, 1);
                    stmt15.setInt(9, 0);
                    stmt15.setString(10, targetSchemaTable);
                    stmt15.setString(11, "${wormhole_log_home}/wormhole");
                    stmt15.setString(12, "D0");
                    stmt15.setString(13, "offset");
                    stmt15.setString(14, owner);
                    stmt15.setString(
                            15,
                            "\""
                                    + taskId1
                                    + " "
                                    + datasourceType
                                    + " "
                                    + (Const.DATASOURCE_TYPE_HIVE
                                            .equals(targetDatasourceType) ? "hdfs"
                                            : Const.DATASOURCE_TYPE_GPREPORT
                                                    .equals(targetDatasourceType) ? "greenplum"
                                                    : targetDatasourceType)
                                    + "\"");
                    if (!Const.DATASOURCE_TYPE_GPREPORT
                            .equals(targetDatasourceType)) {
                        stmt15.setString(16,
                                "\"/data/deploy/dwarch/conf/ETL/job/auto_etl/"
                                        + targetSchemaTable + ".xml\"");
                    } else {
                        stmt15.setString(16,
                                "\"/data/deploy/dwarch/conf/ETL/job/auto_etl/"
                                        + targetSchemaTable + "59.xml\"");
                    }
                    if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)
                            && Const.TABLE_TYPE_RIZHIBIAO
                                    .equals(targetTableType)) {
                        if (CommonUtil.isEmpty(targetSegmentColumn)) {
                            throw new RuntimeException(
                                    "target segment column is null or empty");
                        }
                        stmt15.setString(17,
                                "\" @{cal_dt}=${cal_dt} @{ncal_dt}=${ncal_dt}  @{dw57_table}="
                                        + targetSchemaTable + " \"");
                    } else if (Const.DATASOURCE_TYPE_GPREPORT
                            .equals(targetDatasourceType)
                            && Const.TABLE_TYPE_RIZHIBIAO
                                    .equals(targetTableType)) {
                        if (CommonUtil.isEmpty(targetSegmentColumn)) {
                            throw new RuntimeException(
                                    "target segment column is null or empty");
                        }
                        stmt15.setString(17,
                                "\" @{cal_dt}=${cal_dt8} @{ncal_dt}=${cal_dt}  @{gp_table}="
                                        + targetSchemaTable + " \"");
                    } else if (Const.DATASOURCE_TYPE_HIVE
                            .equals(targetDatasourceType)
                            && (Const.TABLE_TYPE_LISHIKUAIZHAOBIAO
                                    .equals(targetTableType) || Const.TABLE_TYPE_RIZHIBIAO
                                    .equals(targetTableType))) {
                        if (CommonUtil.isEmpty(targetSegmentColumn)) {
                            throw new RuntimeException(
                                    "target segment column is null or empty");
                        }
                        stmt15.setString(17,
                                "\" @{cal_dt}=${cal_dt} @{ncal_dt}=${ncal_dt} \"");
                    } else {
                        stmt15.setString(17,
                                "\" @{cal_dt}=${cal_dt} @{ncal_dt}=${ncal_dt} \"");
                    }
                    stmt15.setInt(18, 3);
                    stmt15.setString(19, null);
                    stmt15.setString(20, " ");
                    stmt15.setString(21, "0");
                    stmt15.setString(22, targetSchemaTable);
                    stmt15.setInt(23, 1);
                    stmt15.setInt(24, taskId1);
                    tmpDsType = datasourceType;
                    if (Const.DATASOURCE_TYPE_GP.equals(datasourceType)) {
                        tmpDsType = "gp";
                    }
                    tmpTgtDsType = targetDatasourceType;
                    if (Const.DATASOURCE_TYPE_GP.equals(tmpTgtDsType)) {
                        tmpTgtDsType = "gp";
                    } else if (Const.DATASOURCE_TYPE_HIVE.equals(tmpTgtDsType)) {
                        tmpTgtDsType = "hdfs";
                    }
                    if (!Const.DATASOURCE_TYPE_GPREPORT
                            .equals(targetDatasourceType)) {
                        stmt15.setString(25, tmpDsType + "2" + tmpTgtDsType
                                + "##" + targetSchemaTable);
                    } else {
                        stmt15.setString(25, tmpDsType + "2" + tmpTgtDsType
                                + "59##" + targetSchemaTable);
                    }

                    stmt15.setString(
                            26,
                            "ssh -o ConnectTimeout=3 -o ConnectionAttempts=5 -o PasswordAuthentication=no -o StrictHostKeyChecking=no -p 58422 deploy@10.1.6.49 sh /data/deploy/dwarch/conf/ETL/bin/start_autoetl.sh");
                    stmt15.setInt(27, 90);
                    stmt15.setInt(28, 1);
                    stmt15.setTimestamp(29, ts);
                    stmt15.setString(30, owner);
                    stmt15.setString(31, null);
                    stmt15.executeUpdate();
                }

                // step 5: insert reader
                if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
                    pSql = " INSERT INTO etl_hive_reader_cfg"
                            + " ("
                            + "`task_id`,`id`,`plugin`,`path`,`username`,`password`,`sql`,"
                            + "`mode`,`datadir`,`reducenumber`,`concurrency`"
                            + " )" + " VALUES" + " (" + "?,?,?,?,?,"
                            + "?,?,?,?,?,?" + " )";
                    stmt6 = conn3.prepareStatement(pSql);
                    stmt6.setInt(1, taskId);
                    stmt6.setString(2, schemaTable);
                    stmt6.setString(3, "hivereader");
                    stmt6.setString(4, "jdbc:hive://10.1.1.161:10000/"
                            + databaseName);
                    stmt6.setString(5, null);
                    stmt6.setString(6, null);

                    List<String> taskParentTableList = new ArrayList<String>();
                    taskParentTableList.add(tableName);
                    List<Integer> parentTaskIdList = taskMapper
                            .getParentTaskIdList(taskParentTableList);
                    if (parentTaskIdList.size() > 0) {
                        int taskPreId = parentTaskIdList.get(0);
                        String pSql1 = " INSERT INTO etl_taskrela_cfg" + " ("
                                + "`task_id`,`task_pre_id`,`cycle_gap`" + " )"
                                + " VALUES" + " (" + "?,?,?" + " )";
                        stmt12 = conn3.prepareStatement(pSql1);
                        stmt12.setInt(1, taskId);
                        stmt12.setInt(2, taskPreId);
                        stmt12.setString(3, "D0");
                        stmt12.executeUpdate();
                    }

                    if (Const.TABLE_TYPE_WEIDUBIAO.equals(targetTableType)) {
                        stmt6.setString(
                                7,
                                "select " + "`"
                                        + sourceAllColumn.replace(",", "`,`")
                                        + "`" + " from " + schemaTable
                                        + " where hp_cal_dt='${cal_dt}'");
                    } else if (targetSegmentColumn == null
                            || targetSegmentColumn.trim().equals("")) {
                        stmt6.setString(
                                7,
                                "select " + "`"
                                        + sourceAllColumn.replace(",", "`,`")
                                        + "`" + " from " + schemaTable);
                    } else {
                        stmt6.setString(
                                7,
                                "select " + "`"
                                        + sourceAllColumn.replace(",", "`,`")
                                        + "`" + " from " + schemaTable
                                        + " where " + targetSegmentColumn
                                        + "='${ncal_dt}'");
                    }
                    stmt6.setString(8, "READ_FROM_HDFS");
                    stmt6.setString(9, "hdfs://10.2.6.102/tmp/");
                    stmt6.setInt(10, -1);
                    stmt6.setInt(11, 10);
                    stmt6.executeUpdate();
                    if (Const.DATASOURCE_TYPE_GPREPORT
                            .equals(targetDatasourceType)) {
                        stmt16 = conn3.prepareStatement(pSql);
                        stmt16.setInt(1, taskId1);
                        stmt16.setString(2, schemaTable);
                        stmt16.setString(3, "hivereader");
                        stmt16.setString(4, "jdbc:hive://10.1.1.161:10000/"
                                + databaseName);
                        stmt16.setString(5, null);
                        stmt16.setString(6, null);

                        if (Const.TABLE_TYPE_WEIDUBIAO.equals(targetTableType)) {
                            stmt16.setString(7, "select " + "`"
                                    + sourceAllColumn.replace(",", "`,`") + "`"
                                    + " from " + schemaTable
                                    + " where hp_cal_dt='${cal_dt}'");
                        } else if (targetSegmentColumn == null
                                || targetSegmentColumn.trim().equals("")) {
                            stmt16.setString(7, "select " + "`"
                                    + sourceAllColumn.replace(",", "`,`") + "`"
                                    + " from " + schemaTable);
                        } else {
                            stmt16.setString(7, "select " + "`"
                                    + sourceAllColumn.replace(",", "`,`") + "`"
                                    + " from " + schemaTable + " where "
                                    + targetSegmentColumn + "='${ncal_dt}'");
                        }

                        stmt16.setString(8, "READ_FROM_HDFS");
                        stmt16.setString(9, "hdfs://10.2.6.102/tmp/");
                        stmt16.setInt(10, -1);
                        stmt16.setInt(11, 10);
                        stmt16.executeUpdate();
                        
                        if (parentTaskIdList.size() > 0) {
                            int taskPreId = parentTaskIdList.get(0);
                            String pSql1 = " INSERT INTO etl_taskrela_cfg" + " ("
                                    + "`task_id`,`task_pre_id`,`cycle_gap`" + " )"
                                    + " VALUES" + " (" + "?,?,?" + " )";
                            stmt12 = conn3.prepareStatement(pSql1);
                            stmt12.setInt(1, taskId1);
                            stmt12.setInt(2, taskPreId);
                            stmt12.setString(3, "D0");
                            stmt12.executeUpdate();
                        }
                    }
                } else if (Const.DATASOURCE_TYPE_MYSQL.equals(datasourceType)) {
                    pSql = " INSERT INTO etl_mysql_reader_cfg"
                            + " ("
                            + "`task_id`,`id`,`plugin`,`connectprops`,`ip`,"
                            + "`port`,`dbname`,`username`,`password`,`encoding`,"
                            + "`params`,`precheck`,`sql`,`needsplit`,`concurrency`,"
                            + "`blocksize`,`tablename`,`autoinckey`,`columns`,`where`,"
                            + "`countsql`" + " )" + " VALUES" + " ("
                            + "?,?,?,?,?," + "null,?,?,?,?," + "?,?,?,?,?,"
                            + "?,?,?,?,?," + "?" + " )";
                    stmt6 = conn3.prepareStatement(pSql);
                    stmt6.setInt(1, taskId);
                    stmt6.setString(2, schemaTable);
                    stmt6.setString(3, "mysqlreader");
                    stmt6.setString(4, "mysql_" + databaseName);
                    stmt6.setString(5, null);
                    stmt6.setString(6, null);
                    stmt6.setString(7, null);
                    stmt6.setString(8, null);
                    stmt6.setString(9, "UTF-8");
                    stmt6.setString(10, null);
                    stmt6.setString(11, null);

                    String tmpStr = "select ";
                    for (String[] tmp : columnList) {
                        tmpStr += "`" + tmp[1] + "`,";
                    }
                    if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
                        tmpStr += "now() as dw_add_ts,";
                    }
                    tmpStr = tmpStr.substring(0, tmpStr.length() - 1)
                            + " from " + tableName;
                    if (targetTableType.equals("4")) {
                        if (CommonUtil.isEmpty(targetSegmentColumn)) {
                            throw new RuntimeException(
                                    "target segment column is null or empty");
                        }
                        tmpStr += " where " + targetSegmentColumn
                                + ">='${cal_dt}' and " + targetSegmentColumn
                                + "<'${ncal_dt}'";
                    }
                    stmt6.setString(12, tmpStr);
                    stmt6.setString(13, "false");
                    stmt6.setInt(14, 10);
                    stmt6.setInt(15, 10000);
                    stmt6.setString(16, null);
                    stmt6.setString(17, null);
                    stmt6.setString(18, null);
                    stmt6.setString(19, null);
                    stmt6.setString(20, null);
                    stmt6.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_SQLSERVER
                        .equals(datasourceType)) {
                    pSql = " INSERT INTO etl_sqlserver_reader_cfg"
                            + " ("
                            + "`task_id`,`id`,`plugin`,`connectprops`,`ip`,"
                            + "`port`,`dbname`,`username`,`password`,`encoding`,"
                            + "`params`,`precheck`,`sql`,`needsplit`,`concurrency`,"
                            + "`blocksize`,`tablename`,`autoinckey`,`columns`,`where`,"
                            + "`countsql`" + " )" + " VALUES" + " ("
                            + "?,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?,"
                            + "?,?,?,?,?," + "?" + " )";
                    stmt6 = conn3.prepareStatement(pSql);
                    stmt6.setInt(1, taskId);
                    stmt6.setString(2, schemaName + "." + tableName);
                    stmt6.setString(3, "sqlserverreader");
                    stmt6.setString(4, "sqlserver_" + databaseName);
                    stmt6.setString(5, null);
                    stmt6.setInt(6, 0);
                    stmt6.setString(7, null);
                    stmt6.setString(8, null);
                    stmt6.setString(9, null);
                    stmt6.setString(10, "UTF-8");
                    stmt6.setString(11, null);
                    stmt6.setString(12, null);

                    String tmpStr = "select ";
                    for (String[] tmp : columnList) {
                        tmpStr += tmp[1] + ",";
                    }
                    tmpStr += "getdate() as dw_add_ts,";
                    tmpStr = tmpStr.substring(0, tmpStr.length() - 1)
                            + " from " + schemaName + "." + tableName;
                    if (targetTableType.equals("4")) {
                        tmpStr += " where chosen_partition_column>='${cal_dt}' and chosen_partition_column<'${ncal_dt}'";
                    }
                    stmt6.setString(13, tmpStr);
                    stmt6.setString(14, "false");
                    stmt6.setInt(15, 10);
                    stmt6.setInt(16, 10000);
                    stmt6.setString(17, null);
                    stmt6.setString(18, null);
                    stmt6.setString(19, null);
                    stmt6.setString(20, null);
                    stmt6.setString(21, null);
                    stmt6.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_SALESFORCE
                        .equals(datasourceType)) {
                    pSql = " INSERT INTO etl_salesforce_reader_cfg" + " ("
                            + "`task_id`,`id`,`plugin`,`username`,`password`,"
                            + "`entity`,`extractionSOQL`,`encryptionKeyFile`"
                            + " )" + " VALUES" + " (" + "?,?,?,?,?," + "?,?,?"
                            + " )";
                    stmt6 = conn3.prepareStatement(pSql);
                    stmt6.setInt(1, taskId);
                    stmt6.setString(2, tableName);
                    stmt6.setString(3, "salesforcereader");
                    stmt6.setString(4, "chao.li@dianping.com");
                    stmt6.setString(
                            5,
                            "91dc9526a0d5a60ddf6ba2f18a269096824eea0d2609540cfe10fbc1505caa40cf4725a38241d6e1");
                    stmt6.setString(6, tableName);
                    String tmpStr = "select ";
                    for (String[] tmp : columnList) {
                        // tmpStr += "`" + tmp[1] + "`,";
                        tmpStr += tmp[1] + ",";
                    }
                    // if
                    // (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType))
                    // {
                    // tmpStr += "now() as dw_add_ts,";
                    // }
                    tmpStr = tmpStr.substring(0, tmpStr.length() - 1)
                            + " from " + tableName;
                    if (targetTableType.equals("4")) {
                        if (CommonUtil.isEmpty(targetSegmentColumn)) {
                            throw new RuntimeException(
                                    "target segment column is null or empty");
                        }
                        tmpStr += " where " + targetSegmentColumn
                                + ">='${cal_dt}' and " + targetSegmentColumn
                                + "<'${ncal_dt}'";
                    }
                    stmt6.setString(7, tmpStr);
                    stmt6.setString(8,
                            "/data/deploy/dwarch/conf/ETL/conf/lycenway.key");

                    stmt6.executeUpdate();
                }

                // step 6: insert writer
                if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
                    pSql = " INSERT INTO etl_hdfs_writer_cfg"
                            + " ("
                            + "`task_id`,`plugin`,`dir`,`prefix_filename`,`field_split`,"
                            + "`line_split`,`encoding`,`nullchar`,`replace_char`,`codec_class`,"
                            + "`buffer_size`,`file_type`,`concurrency`,`hive_table_add_partition_switch`,`hive_table_add_partition_condition`"
                            + " )" + " VALUES" + " (" + "?,?,?,?,?,"
                            + "?,?,?,?,?," + "?,?,?,?,?" + " )";
                    stmt7 = conn3.prepareStatement(pSql);
                    stmt7.setInt(1, taskId);
                    stmt7.setString(2, "hdfswriter");
                    if (targetTableType.equals("1")) {
                        stmt7.setString(3,
                                "hdfs://10.2.6.102/user/hive/warehouse/bi.db/"
                                        + targetSchemaTable);
                    } else if (targetTableType.equals("2")) {
                        stmt7.setString(3,
                                "hdfs://10.2.6.102/user/hive/warehouse/bi.db/"
                                        + targetSchemaTable
                                        + "/hp_statdate=${cal_dt}");
                    } else if (targetTableType.equals("4")) {
                        stmt7.setString(3,
                                "hdfs://10.2.6.102/user/hive/warehouse/bi.db/"
                                        + targetSchemaTable
                                        + "/hp_statdate=${cal_dt}");
                    } else if (targetTableType.equals("5")) {
                        stmt7.setString(3,
                                "hdfs://10.2.6.102/user/hive/warehouse/bi.db/"
                                        + targetSchemaTable
                                        + "/hp_cal_dt=${cal_dt}");
                    }
                    stmt7.setString(4, targetSchemaTable);
                    stmt7.setString(5, "\\005");
                    stmt7.setString(6, "\\n");
                    stmt7.setString(7, "UTF-8");
                    stmt7.setString(8, null);
                    stmt7.setString(9, null);
                    stmt7.setString(10, "com.hadoop.compression.lzo.LzopCodec");
                    stmt7.setInt(11, 4096);
                    stmt7.setString(12, "TXT_COMP");
                    stmt7.setInt(13, 1);
                    stmt7.setString(14, "false");
                    stmt7.setString(15, null);
                    stmt7.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_GP
                        .equals(targetDatasourceType)) {
                    pSql = " INSERT INTO etl_greenplum_writer_cfg"
                            + " ("
                            + "`task_id`,`id`,`plugin`,`connectprops`,`ip`,"
                            + "`port`,`dbname`,`username`,`password`,`priority`,"
                            + "`encoding`,`params`,`tablename`,`columns`,`pre`,"
                            + "`post`,`countsql`,`rollback`,`logerrortable`,`failedlinesthreshold`"
                            + " )" + " VALUES" + " (" + "?,?,?,?,?,"
                            + "null,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?"
                            + " )";
                    stmt7 = conn3.prepareStatement(pSql);
                    stmt7.setInt(1, taskId);
                    stmt7.setString(2, "gp_dianpingdw57");
                    stmt7.setString(3, "greenplumwriter");
                    stmt7.setString(4, "gp_dianpingdw57_" + targetSchemaName);
                    stmt7.setString(5, null);
                    stmt7.setString(6, null);
                    stmt7.setString(7, null);
                    stmt7.setString(8, null);
                    stmt7.setInt(9, 1);
                    stmt7.setString(10, "UTF-8");
                    stmt7.setString(11, null);

                    stmt7.setString(13, sourceAllColumn.isEmpty() ? "" : ("\""
                            + sourceAllColumn.replaceAll(",", "\",\"") + "\""));
                    if (Const.TABLE_TYPE_QUANLIANGJINGXIANGBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, targetSchemaTable);
                        stmt7.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt7.setString(17, "truncate table "
                                + targetSchemaTable);
                    } else if (Const.TABLE_TYPE_RIZHIBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, "${dw57_partition_name}");
                        stmt7.setString(14,
                                "truncate table ${dw57_partition_name}");
                        stmt7.setString(17,
                                "truncate table ${dw57_partition_name}");
                    } else if (Const.TABLE_TYPE_WEIDUBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, targetSchemaTable);
                        stmt7.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt7.setString(17, "truncate table "
                                + targetSchemaTable);
                    }
                    stmt7.setString(15, null);
                    stmt7.setString(16, null);
                    stmt7.setString(18, null);
                    stmt7.setInt(19, 0);
                    stmt7.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_GPANALYSIS
                        .equals(targetDatasourceType)) {
                    pSql = " INSERT INTO etl_greenplum_writer_cfg"
                            + " ("
                            + "`task_id`,`id`,`plugin`,`connectprops`,`ip`,"
                            + "`port`,`dbname`,`username`,`password`,`priority`,"
                            + "`encoding`,`params`,`tablename`,`columns`,`pre`,"
                            + "`post`,`countsql`,`rollback`,`logerrortable`,`failedlinesthreshold`"
                            + " )" + " VALUES" + " (" + "?,?,?,?,?,"
                            + "null,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?"
                            + " )";
                    stmt7 = conn3.prepareStatement(pSql);
                    stmt7.setInt(1, taskId);
                    stmt7.setString(2, "gpanalysis");
                    stmt7.setString(3, "greenplumwriter");
                    stmt7.setString(4, "gpanalysis_dianpingdw_"
                            + targetSchemaName);
                    stmt7.setString(5, null);
                    stmt7.setString(6, null);
                    stmt7.setString(7, null);
                    stmt7.setString(8, null);
                    stmt7.setInt(9, 1);
                    stmt7.setString(10, "UTF-8");
                    stmt7.setString(11, null);

                    stmt7.setString(13, sourceAllColumn.isEmpty() ? "" : ("\""
                            + sourceAllColumn.replaceAll(",", "\",\"") + "\""));
                    if (Const.TABLE_TYPE_QUANLIANGJINGXIANGBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, targetSchemaTable);
                        stmt7.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt7.setString(17, "truncate table "
                                + targetSchemaTable);
                    } else if (Const.TABLE_TYPE_RIZHIBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, "${analysis_partition_name}");
                        stmt7.setString(14,
                                "truncate table ${analysis_partition_name}");
                        stmt7.setString(17,
                                "truncate table ${analysis_partition_name}");
                    } else if (Const.TABLE_TYPE_WEIDUBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, targetSchemaTable);
                        stmt7.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt7.setString(17, "truncate table "
                                + targetSchemaTable);
                    }
                    stmt7.setString(15, null);
                    stmt7.setString(16, null);
                    stmt7.setString(18, null);
                    stmt7.setInt(19, 0);
                    stmt7.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_GPREPORT
                        .equals(targetDatasourceType)) {
                    pSql = " INSERT INTO etl_greenplum_writer_cfg"
                            + " ("
                            + "`task_id`,`id`,`plugin`,`connectprops`,`ip`,"
                            + "`port`,`dbname`,`username`,`password`,`priority`,"
                            + "`encoding`,`params`,`tablename`,`columns`,`pre`,"
                            + "`post`,`countsql`,`rollback`,`logerrortable`,`failedlinesthreshold`"
                            + " )" + " VALUES" + " (" + "?,?,?,?,?,"
                            + "null,?,?,?,?," + "?,?,?,?,?," + "?,?,?,?,?"
                            + " )";
                    stmt7 = conn3.prepareStatement(pSql);
                    stmt7.setInt(1, taskId);
                    stmt7.setString(2, "gp_report");
                    stmt7.setString(3, "greenplumwriter");
                    stmt7.setString(4, "gpreport58_dianpingdw_bi");
                    stmt7.setString(5, null);
                    stmt7.setString(6, null);
                    stmt7.setString(7, null);
                    stmt7.setString(8, null);
                    stmt7.setInt(9, 1);
                    stmt7.setString(10, "UTF-8");
                    stmt7.setString(11, null);

                    stmt7.setString(13, sourceAllColumn.isEmpty() ? "" : ("\""
                            + sourceAllColumn.replaceAll(",", "\",\"") + "\""));
                    if (Const.TABLE_TYPE_QUANLIANGJINGXIANGBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, targetSchemaTable);
                        stmt7.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt7.setString(17, "truncate table "
                                + targetSchemaTable);
                    } else if (Const.TABLE_TYPE_RIZHIBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, "${gp_partition_name}");
                        stmt7.setString(14,
                                "truncate table ${gp_partition_name}");
                        stmt7.setString(17,
                                "truncate table ${gp_partition_name}");
                    } else if (Const.TABLE_TYPE_WEIDUBIAO
                            .equals(targetTableType)) {
                        stmt7.setString(12, targetSchemaTable);
                        stmt7.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt7.setString(17, "truncate table "
                                + targetSchemaTable);
                    }
                    stmt7.setString(15, null);
                    stmt7.setString(16, null);
                    stmt7.setString(18, null);
                    stmt7.setInt(19, 0);
                    stmt7.executeUpdate();

                    // 双写
                    stmt17 = conn3.prepareStatement(pSql);
                    stmt17.setInt(1, taskId1);
                    stmt17.setString(2, "gp_report");
                    stmt17.setString(3, "greenplumwriter");
                    stmt17.setString(4, "gpreport59_dianpingdw_bi");
                    stmt17.setString(5, null);
                    stmt17.setString(6, null);
                    stmt17.setString(7, null);
                    stmt17.setString(8, null);
                    stmt17.setInt(9, 1);
                    stmt17.setString(10, "UTF-8");
                    stmt17.setString(11, null);

                    stmt17.setString(13, sourceAllColumn.isEmpty() ? "" : ("\""
                            + sourceAllColumn.replaceAll(",", "\",\"") + "\""));
                    if (Const.TABLE_TYPE_QUANLIANGJINGXIANGBIAO
                            .equals(targetTableType)) {
                        stmt17.setString(12, targetSchemaTable);
                        stmt17.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt17.setString(17, "truncate table "
                                + targetSchemaTable);
                    } else if (Const.TABLE_TYPE_RIZHIBIAO
                            .equals(targetTableType)) {
                        stmt17.setString(12, "${gp_partition_name}");
                        stmt17.setString(14,
                                "truncate table ${gp_partition_name}");
                        stmt17.setString(17,
                                "truncate table ${gp_partition_name}");
                    } else if (Const.TABLE_TYPE_WEIDUBIAO
                            .equals(targetTableType)) {
                        stmt17.setString(12, targetSchemaTable);
                        stmt17.setString(14, "truncate table "
                                + targetSchemaTable);
                        stmt17.setString(17, "truncate table "
                                + targetSchemaTable);
                    }
                    stmt17.setString(15, null);
                    stmt17.setString(16, null);
                    stmt17.setString(18, null);
                    stmt17.setInt(19, 0);
                    stmt17.executeUpdate();
                }

                // step 7: insert masterdata mc_table_info
                List<String> conf = new ArrayList<String>();
                String targetTableName = targetSchemaTable;
                if (targetTableName.indexOf("load_") == 0) {
                    targetTableName = "dpods_" + targetTableName.substring(5);
                }
                targetTableName = targetTableName.substring(targetTableName
                        .indexOf(".") + 1);
                conf.add(targetSchemaName);
                conf.add(targetTableName);
                conf.add(Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType) ? "gp"
                        : targetDatasourceType);

                String tmpSchemaName = null;
                if (Const.DATASOURCE_TYPE_GPREPORT.equals(targetDatasourceType)
                        || Const.DATASOURCE_TYPE_GPANALYSIS
                                .equals(targetDatasourceType)) {
                    if (tableName.indexOf("dpdm_") == 0) {
                        tmpSchemaName = "dpdm";
                    } else if (tableName.indexOf("dpdw_") == 0) {
                        tmpSchemaName = "dpdw";
                    } else if (tableName.indexOf("dpfinance_") == 0) {
                        tmpSchemaName = "dpfinance";
                    } else if (tableName.indexOf("dpmid_") == 0) {
                        tmpSchemaName = "dpmid";
                    } else if (tableName.indexOf("dpods_") == 0) {
                        tmpSchemaName = "dpods";
                    } else if (tableName.indexOf("dpodssec_") == 0) {
                        tmpSchemaName = "dpodssec";
                    } else if (tableName.indexOf("dpdim_") == 0) {
                        tmpSchemaName = "dpdim";
                    } else if (tableName.indexOf("dprpt_") == 0) {
                        tmpSchemaName = "dprpt";
                    } else {
                        throw new RuntimeException("非法的数据模式");
                    }
                } else {
                    tmpSchemaName = targetSchemaName;
                }

                String tableIdStr = tmpSchemaName
                        + targetTableName
                        + (Const.DATASOURCE_TYPE_GP
                                .equals(targetDatasourceType) ? "gp"
                                : targetDatasourceType);

                pSql = " INSERT INTO mc_table_info"
                        + " ("
                        + "`table_id`,`schema_name`,`table_name`,`storage_type`,`table_owner`,"
                        + "`table_desc`,`table_size`,`refresh_cycle`,`refresh_type`,`table_access_level`,"
                        + "`table_access_desc`,`add_time`,`update_time`" + " )"
                        + " VALUES ("
                        + "hex(sha1(?))%(1024*1024*1024-1),?,?,?,?,"
                        + "?,?,?,?,?," + "?,?,?" + " )";
                stmt8 = conn3.prepareStatement(pSql);
                stmt8.setString(1, tableIdStr);
                stmt8.setString(2, tmpSchemaName);
                stmt8.setString(3, targetTableName);
                stmt8.setString(
                        4,
                        Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType) ? "gp"
                                : Const.DATASOURCE_TYPE_GPREPORT
                                        .equals(targetDatasourceType) ? "gp_report"
                                        : Const.DATASOURCE_TYPE_GPANALYSIS
                                                .equals(targetDatasourceType) ? "gp_analysis"
                                                : targetDatasourceType);
                stmt8.setString(5, owner);
                stmt8.setString(6, "");
                stmt8.setLong(7, 0L);
                stmt8.setString(8, "D");
                stmt8.setString(9,
                        (targetTableType.equals("1") || targetTableType
                                .equals("4")) ? "append" : "full");
                stmt8.setInt(10, 2);
                stmt8.setString(11, "");
                stmt8.setTimestamp(12, ts);
                stmt8.setTimestamp(13, ts);
                stmt8.executeUpdate();

                // step 8: insert masterdata mc_column_info
                Properties prop = new Properties();
                InputStream in = this.getClass().getResourceAsStream(
                        Const.MAPPING_PATH);
                try {
                    prop.load(in);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException("Read properties file error ["
                            + Const.MAPPING_PATH + "] " + e.getMessage());
                }

                pSql = " INSERT INTO mc_column_info"
                        + " ("
                        + "`column_id`,`table_id`,`column_name`,`column_type`,`column_desc`,"
                        + "`default_value`,`column_access_level`,`column_access_desc`,`column_rn`,`add_time`,"
                        + "`update_time`"
                        + " )"
                        + " VALUES ("
                        + "concat(hex(sha1(?))%(1024*1024*1024-1),?),hex(sha1(?))%(1024*1024*1024-1),?,?,?,"
                        + "?,?,?,?,?," + "?" + " )";
                for (String[] tmp : columnList) {
                    int columnIdx = Integer.parseInt(tmp[0]);
                    String columnName = tmp[1];
                    String dataType = tmp[2];
                    String characterMaximumLength = tmp[3];

                    conf = new ArrayList<String>();
                    conf.add(targetSchemaName);
                    conf.add(targetTableName);
                    conf.add(columnName);
                    conf.add(Const.DATASOURCE_TYPE_GP
                            .equals(targetDatasourceType) ? "gp"
                            : targetDatasourceType);
                    stmt9 = conn3.prepareStatement(pSql);
                    stmt9.setString(1, tableIdStr);
                    stmt9.setInt(2, columnIdx);
                    stmt9.setString(3, tableIdStr);
                    stmt9.setString(4, columnName);

                    String columnType = null;
                    String tmpDBType = targetDatasourceType;
                    if (Const.DATASOURCE_TYPE_GPREPORT
                            .equals(targetDatasourceType)
                            || Const.DATASOURCE_TYPE_GPANALYSIS
                                    .equals(targetDatasourceType)) {
                        tmpDBType = Const.DATASOURCE_TYPE_GP;
                    }

                    try {
                        columnType = prop.getProperty(
                                (Const.DATASOURCE_TYPE_HIVE
                                        .equals(datasourceType) ? "hivemeta"
                                        : datasourceType)
                                        + "_"
                                        + (Const.DATASOURCE_TYPE_GP
                                                .equals(tmpDBType) ? "gp"
                                                : tmpDBType) + "." + dataType)
                                .trim();
                    } catch (NullPointerException e) {
                        log.info("没有发现指定字段类型");
                        log.debug(e.getMessage());
                        columnType = null;
                    }

                    if (Const.DATASOURCE_TYPE_GP.equals(tmpDBType)
                            && columnType == null) {
                        columnType = "text";
                    } else if (columnType == null) {
                        columnType = "unknow";
                    }

                    if (Const.DATASOURCE_TYPE_GP.equals(tmpDBType)
                            && ("varchar".equals(columnType) || "char"
                                    .equals(columnType))) {
                        columnType += "(" + characterMaximumLength + ")";
                    }
                    stmt9.setString(5, columnType);
                    stmt9.setString(6, "");
                    stmt9.setString(7, "");
                    stmt9.setInt(8, 2);
                    stmt9.setString(9, "");
                    stmt9.setInt(10, columnIdx);
                    stmt9.setTimestamp(11, ts);
                    stmt9.setTimestamp(12, ts);
                    stmt9.executeUpdate();
                }

                // step 9: insert masterdata mc_data_task_map
                pSql = " INSERT INTO mc_data_task_map"
                        + " ("
                        + "`table_id`,`database_name`,`task_id`,`add_time`,`update_time`"
                        + " )" + " VALUES ("
                        + "hex(sha1(?))%(1024*1024*1024-1),?,?,?,?" + " )";
                stmt10 = conn3.prepareStatement(pSql);
                if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
                    stmt10.setString(1, tableIdStr);
                    stmt10.setString(2, "hdfs");
                    stmt10.setInt(3, taskId);
                    stmt10.setTimestamp(4, ts);
                    stmt10.setTimestamp(5, ts);
                    stmt10.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_GP
                        .equals(targetDatasourceType)) {
                    stmt10.setString(1, tableIdStr);
                    stmt10.setString(2, "dw57");
                    stmt10.setInt(3, taskId);
                    stmt10.setTimestamp(4, ts);
                    stmt10.setTimestamp(5, ts);
                    stmt10.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_GPANALYSIS
                        .equals(targetDatasourceType)) {
                    stmt10.setString(1, tableIdStr);
                    stmt10.setString(2, "gp_analysis");
                    stmt10.setInt(3, taskId);
                    stmt10.setTimestamp(4, ts);
                    stmt10.setTimestamp(5, ts);
                    stmt10.executeUpdate();
                } else if (Const.DATASOURCE_TYPE_GPREPORT
                        .equals(targetDatasourceType)) {
                    stmt10.setString(1, tableIdStr);
                    stmt10.setString(2, "gp_report");
                    stmt10.setInt(3, taskId);
                    stmt10.setTimestamp(4, ts);
                    stmt10.setTimestamp(5, ts);
                    stmt10.executeUpdate();

                    stmt20 = conn3.prepareStatement(pSql);
                    stmt20.setString(1, tableIdStr);
                    stmt20.setString(2, "gp_report");
                    stmt20.setInt(3, taskId1);
                    stmt20.setTimestamp(4, ts);
                    stmt20.setTimestamp(5, ts);
                    stmt20.executeUpdate();
                }

                // step 10: insert masterdata mc_data_map
                pSql = " INSERT INTO mc_data_map"
                        + " ("
                        + "`id`,`parent_id`,`map_type`,`add_time`,`update_time`"
                        + " )"
                        + " SELECT hex(sha1(?))%(1024*1024*1024-1), datasource_id,0,?,?"
                        + " FROM mc_datasource_info"
                        + " WHERE datasource_name=?";
                stmt11 = conn3.prepareStatement(pSql);
                stmt11.setString(1, tableIdStr);
                stmt11.setTimestamp(2, ts);
                stmt11.setTimestamp(3, ts);
                tmpDsType = datasourceType;
                if (Const.DATASOURCE_TYPE_GP.equals(datasourceType)) {
                    tmpDsType = "gp";
                } else if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
                    tmpDsType = "hdfs";
                }
                stmt11.setString(4,
                        tmpDsType + "_" + databaseName.toLowerCase());
                stmt11.executeUpdate();
            } else {
                throw new RuntimeException("isActiveSchedule must be 0 or 1, ["
                        + targetIsActiveSchedule + "]");
            }
            conn3.commit();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(
                    "insert schedule/masterdata table fail, pSql [ " + pSql
                            + " ] " + e.getMessage());
        } finally {
            ok = true;

            if (null != rs4) {
                try {
                    rs4.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }

            if (null != stmt4) {
                try {
                    stmt4.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (null != stmt5) {
                try {
                    stmt5.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (null != stmt6) {
                try {
                    stmt6.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (null != stmt7) {
                try {
                    stmt7.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (null != stmt8) {
                try {
                    stmt8.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (null != stmt9) {
                try {
                    stmt9.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (null != stmt10) {
                try {
                    stmt10.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (null != stmt11) {
                try {
                    stmt11.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }

            if (null != conn3) {
                try {
                    conn3.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }

            if (!ok) {
                throw new RuntimeException(
                        "close rs,stmt,conn after insert schedule/masterdata table fail");
            }
        }
        return true;
    }

    /**
     * 实际执行gp57目标表建表语句
     * 
     * @param targetSchemaName
     * @param sqls
     */
    private void createTableGp57(String targetSchemaName, String sqls) {
        DSInfo dsInfo = null;
        Connection conn = null;
        Statement stmt = null;
        String[] sqlArr = sqls.split(";");
        try {
            dsInfo = getGPDSInfo(Const.DATASOURCE_TYPE_GP, "dianpingdw57",
                    targetSchemaName);
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            for (String sql : sqlArr) {
                if (!sql.isEmpty() && !"\n".equals(sql)) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            for (String sql : sqlArr) {
                log.error(sql);
            }
            throw new RuntimeException("create gp57 table fail, sqls [ " + sqls
                    + " ] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("stmt,conn close error");
            }
        }
    }

    /**
     * 实际执行gp57目标表建表语句
     * 
     * @param targetSchemaName
     * @param sqls
     */
    private void createTableGPAnalysis(String targetSchemaName, String sqls) {
        DSInfo dsInfo = null;
        Connection conn = null;
        Statement stmt = null;
        String[] sqlArr = sqls.split(";");
        try {
            dsInfo = getGPDSInfo(Const.DATASOURCE_TYPE_GPANALYSIS,
                    "dianpingdw", targetSchemaName);
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            for (String sql : sqlArr) {
                if (!sql.isEmpty() && !"\n".equals(sql)) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            for (String sql : sqlArr) {
                log.error(sql);
            }
            throw new RuntimeException("create gp57 table fail, sqls [ " + sqls
                    + " ] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("stmt,conn close error");
            }
        }
    }

    /**
     * 实际执行gp57目标表建表语句
     * 
     * @param targetSchemaName
     * @param sqls
     */
    private void createTablePostgresql(String targetSchemaName, String sqls) {
        DSInfo dsInfo = null;
        Connection conn = null;
        Statement stmt = null;
        String[] sqlArr = sqls.split(";");
        try {
            dsInfo = getGPDSInfo(Const.DATASOURCE_TYPE_POSTGRESQL,
                    "dianpingdw", targetSchemaName);
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            for (String sql : sqlArr) {
                if (!sql.isEmpty() && !"\n".equals(sql)) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            for (String sql : sqlArr) {
                log.error(sql);
            }
            throw new RuntimeException("create postgresql table fail, sqls [ "
                    + sqls + " ] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("stmt,conn close error");
            }
        }
    }

    /**
     * 实际执行gp57目标表建表语句
     * 
     * @param targetSchemaName
     * @param sqls
     */
    private void createTableGPReport58(String targetSchemaName, String sqls) {
        DSInfo dsInfo = null;
        Connection conn = null;
        Statement stmt = null;
        String[] sqlArr = sqls.split(";");
        try {
            dsInfo = getGPDSInfo("gpreport58", "dianpingdw", "bi");
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            for (String sql : sqlArr) {
                if (!sql.isEmpty() && !"\n".equals(sql)) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            for (String sql : sqlArr) {
                log.error(sql);
            }
            throw new RuntimeException("create postgresql table fail, sqls [ "
                    + sqls + " ] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("stmt,conn close error");
            }
        }
    }

    /**
     * 实际执行gp57目标表建表语句
     * 
     * @param targetSchemaName
     * @param sqls
     */
    private void createTableGPReport59(String targetSchemaName, String sqls) {
        DSInfo dsInfo = null;
        Connection conn = null;
        Statement stmt = null;
        String[] sqlArr = sqls.split(";");
        try {
            dsInfo = getGPDSInfo("gpreport59", "dianpingdw", "bi");
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            for (String sql : sqlArr) {
                if (!sql.isEmpty() && !"\n".equals(sql)) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            for (String sql : sqlArr) {
                log.error(sql);
            }
            throw new RuntimeException("create postgresql table fail, sqls [ "
                    + sqls + " ] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("stmt,conn close error");
            }
        }
    }

    // /**
    // * 实际执行gp59目标表建表语句
    // *
    // * @param targetSchemaName
    // * @param sqls
    // */
    // private void createTableGp59(String targetSchemaName, String sqls) {
    // DSInfo dsInfo = null;
    // Connection conn = null;
    // Statement stmt = null;
    // String[] sqlArr = sqls.split(";");
    // try {
    // dsInfo = getGPDSInfo(Const.DATASOURCE_TYPE_GP, "dianpingdw59",
    // targetSchemaName);
    // conn = getConnection(dsInfo);
    // stmt = conn.createStatement();
    // for (String sql : sqlArr) {
    // if (!sql.isEmpty() && !"\n".equals(sql)) {
    // stmt.execute(sql);
    // }
    // }
    // } catch (SQLException e) {
    // log.error(e.getMessage(),e)
    // for (String sql : sqlArr) {
    // log.error(sql);
    // }
    // throw new RuntimeException("create gp59 table fail, sqls [ " + sqls +
    // " ] " + e.getMessage());
    // } finally {
    // boolean ok = true;
    // if (stmt != null) {
    // try {
    // stmt.close();
    // } catch (SQLException e) {
    // ok = false;
    // log.error(e.getMessage(),e)
    // }
    // }
    // if (conn != null) {
    // try {
    // conn.close();
    // } catch (SQLException e) {
    // ok = false;
    // log.error(e.getMessage(),e)
    // }
    // }
    // if (!ok) {
    // throw new RuntimeException("stmt,conn close error");
    // }
    // }
    // }

    /**
     * 实际执行hive目标表建表语句
     * 
     * @param sqls
     */
    public void createTableHive(String sqls) {
        DSInfo dsInfo = null;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String[] sqlArr = sqls.split(";");
        try {
            dsInfo = getDSInfo(Const.DATASOURCE_TYPE_HIVE, "bi");
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            stmt.execute("use bi");
            for (String sql : sqlArr) {
                if (!sql.isEmpty() && !"\n".equals(sql)) {
                    System.out.println(sql);
                    stmt.execute(sql);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("use bi");
            for (String sql : sqlArr) {
                log.error(sql);
            }
            throw new RuntimeException("create hive table fail, sqls [ " + sqls
                    + " ] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("rs,stmt,conn close error");
            }
        }
    }

    /**
     * action传来的生成ddl请求
     * 
     * @param datasourceType
     * @param databaseName
     * @param tableName
     * @param targetDatasourceType
     * @param targetSchemaName
     * @param targetSchemaTable
     * @param targetSegmentColumn
     * @return
     */
    public String generateDDL(String datasourceType, String databaseName,
            String schemaName, String tableName, String targetDatasourceType,
            String targetSchemaName, String targetSchemaTable,
            String targetSegmentColumn) {
        if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
            datasourceType = Const.DATASOURCE_TYPE_HIVEMETA;
        }

        // add salesforce logic
        if (Const.DATASOURCE_TYPE_SALESFORCE.equals(datasourceType)) {
            List<Map<String, String>> list = salesforceService
                    .fromSalesforce(tableName);

            String ddl = generateDDLFromResultSet2(list, datasourceType,
                    databaseName, tableName, targetDatasourceType,
                    targetSchemaName, targetSchemaTable, targetSegmentColumn);
            return ddl;
        }

        Object[] arr = generateResultSet(datasourceType, databaseName,
                schemaName, tableName);

        String ddl = generateDDLFromResultSet((ResultSet) arr[2],
                datasourceType, databaseName, tableName, targetDatasourceType,
                targetSchemaName, targetSchemaTable, targetSegmentColumn);

        boolean ok = true;
        if (arr != null) {
            if (arr.length > 0 && arr[0] != null) {
                try {
                    ((Connection) arr[0]).close();
                } catch (SQLException e1) {
                    ok = false;
                    e1.printStackTrace();
                }
            }
            if (arr.length > 1 && arr[1] != null) {
                try {
                    ((Statement) arr[1]).close();
                } catch (SQLException e1) {
                    ok = false;
                    e1.printStackTrace();
                }
            }
            if (arr.length > 2 && arr[2] != null) {
                try {
                    ((ResultSet) arr[2]).close();
                } catch (SQLException e1) {
                    ok = false;
                    e1.printStackTrace();
                }
            }
        }
        if (!ok) {
            throw new RuntimeException("close rs,stmt,conn error");
        }
        return ddl;
    }

    /**
     * 生成ddl，同时供两个生成ddl和建表使用
     * 
     * @param rs
     * @param datasourceType
     * @param targetDatasourceType
     * @param databaseName
     * @param targetSchemaName
     * @param tableName
     * @param targetSegmentColumn
     * @return
     */
    private String generateDDLFromResultSet(ResultSet rs,
            String datasourceType, String databaseName, String tableName,
            String targetDatasourceType, String targetSchemaName,
            String targetSchemaTable, String targetSegmentColumn) {
        if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
            datasourceType = Const.DATASOURCE_TYPE_HIVEMETA;
        }

        targetSegmentColumn = targetSegmentColumn == null ? null
                : targetSegmentColumn.toLowerCase();
        String sqls = "";
        if (Const.DATASOURCE_TYPE_MYSQL.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to mysql");
        } else if (Const.DATASOURCE_TYPE_SQLSERVER.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to sqlserver");
        } else if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)) {
            sqls = toGp(rs, datasourceType, targetSchemaName,
                    targetSchemaTable, targetSegmentColumn);
        } else if (Const.DATASOURCE_TYPE_GPANALYSIS
                .equals(targetDatasourceType)) {
            sqls = toGp(rs, datasourceType, targetSchemaName,
                    targetSchemaTable, targetSegmentColumn);
        } else if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
            String segmentCol = null;
            if (targetSegmentColumn == null) {
                segmentCol = null;
            } else if (targetSegmentColumn.equals("hp_cal_dt")) {
                segmentCol = targetSegmentColumn;
            } else {
                segmentCol = null != targetSegmentColumn ? "hp_statdate" : null;
            }
            sqls = toHive(rs, datasourceType, targetSchemaName,
                    targetSchemaTable, segmentCol);
        } else if (Const.DATASOURCE_TYPE_GPREPORT.equals(targetDatasourceType)) {
            // sqls = toPostgresql(rs, datasourceType, targetSchemaName,
            // targetSchemaTable, targetSegmentColumn);
            sqls = toGp(rs, datasourceType, targetSchemaName,
                    targetSchemaTable, targetSegmentColumn);
        } else {
            throw new RuntimeException(
                    "unsupport to " + null == targetDatasourceType ? "null"
                            : targetDatasourceType);
        }
        return sqls;
    }

    public Integer checkDBRule(String databaseName, String tableName,
            String schemaName) {
        String rule = this.taskMapper.searchRuleByDB(databaseName);
        Pattern p = Pattern.compile(rule);
        Matcher m = p.matcher(tableName.trim().toLowerCase());
        return m.find() ? 1 : 0;
    }

    /**
     * 生成ddl，同时供两个生成ddl和建表使用
     * 
     * @param rs
     * @param datasourceType
     * @param targetDatasourceType
     * @param databaseName
     * @param targetSchemaName
     * @param tableName
     * @param targetSegmentColumn
     * @return
     */
    private String generateDDLFromResultSet2(List<Map<String, String>> rs,
            String datasourceType, String databaseName, String tableName,
            String targetDatasourceType, String targetSchemaName,
            String targetSchemaTable, String targetSegmentColumn) {
        if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
            datasourceType = Const.DATASOURCE_TYPE_HIVEMETA;
        }

        targetSegmentColumn = targetSegmentColumn == null ? null
                : targetSegmentColumn.toLowerCase();
        String sqls = "";
        if (Const.DATASOURCE_TYPE_MYSQL.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to mysql");
        } else if (Const.DATASOURCE_TYPE_SQLSERVER.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to sqlserver");
        } else if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to greenplum");
            // sqls = toGp2(rs, datasourceType, targetSchemaName,
            // targetSchemaTable, targetSegmentColumn);
        } else if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
            sqls = toHive2(rs, datasourceType, targetSchemaName,
                    targetSchemaTable,
                    null != targetSegmentColumn ? "hp_statdate" : null);
        } else {
            throw new RuntimeException(
                    "unsupport to " + null == targetDatasourceType ? "null"
                            : targetDatasourceType);
        }
        return sqls;
    }

    /**
     * 生成ddl，同时供两个生成ddl和建表使用
     * 
     * @param rs
     * @param datasourceType
     * @param targetDatasourceType
     * @param databaseName
     * @param targetSchemaName
     * @param tableName
     * @param targetSegmentColumn
     * @return
     */
    private List<String> generateDDLFromResultSetForSalesForce(
            List<Map<String, String>> rs, String datasourceType,
            String databaseName, String tableName, String targetDatasourceType,
            String targetSchemaName, String targetSchemaTable,
            String targetSegmentColumn) {
        if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
            datasourceType = Const.DATASOURCE_TYPE_HIVEMETA;
        }

        targetSegmentColumn = targetSegmentColumn == null ? null
                : targetSegmentColumn.toLowerCase();
        String sqls = "";
        List<String> sqlList = new ArrayList<String>();
        if (Const.DATASOURCE_TYPE_MYSQL.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to mysql");
        } else if (Const.DATASOURCE_TYPE_SQLSERVER.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to sqlserver");
        } else if (Const.DATASOURCE_TYPE_GP.equals(targetDatasourceType)) {
            throw new RuntimeException("unsupport to greenplum");
            // sqls = toGp2(rs, datasourceType, targetSchemaName,
            // targetSchemaTable, targetSegmentColumn);
        } else if (Const.DATASOURCE_TYPE_HIVE.equals(targetDatasourceType)) {
            sqlList.add(toHive2(rs, datasourceType, targetSchemaName,
                    targetSchemaTable,
                    null != targetSegmentColumn ? "hp_statdate" : null));
        } else {
            throw new RuntimeException(
                    "unsupport to " + null == targetDatasourceType ? "null"
                            : targetDatasourceType);
        }
        return sqlList;
    }

    /**
     * 生成结果集，返回数组为了关闭连接对象，防止泄露内存
     * 
     * @param datasourceType
     * @param databaseName
     * @param tableName
     * @return
     */
    private Object[] generateResultSet(String datasourceType,
            String databaseName, String schemaName, String tableName) {
        if (Const.DATASOURCE_TYPE_HIVE.equals(datasourceType)) {
            datasourceType = Const.DATASOURCE_TYPE_HIVEMETA;
        }

        if (Const.DATASOURCE_TYPE_MYSQL.equals(datasourceType)) {
            return fromMysql(datasourceType, databaseName, tableName);
        } else if (Const.DATASOURCE_TYPE_SQLSERVER.equals(datasourceType)) {
            return fromSqlserver(datasourceType, databaseName, schemaName,
                    tableName);
        } else if (Const.DATASOURCE_TYPE_GP.equals(datasourceType)) {
            throw new RuntimeException("unsupport from gp");
        } else if (Const.DATASOURCE_TYPE_HIVEMETA.equals(datasourceType)) {
            return fromHive(datasourceType, databaseName, tableName);
        } else {
            throw new RuntimeException(
                    "unsupport from " + null == datasourceType ? "null"
                            : datasourceType);
        }
    }

    public List<String> getAllMySQLDatabase() {
        return mySQLMapper.getAllDatabase();
    }

    public List<String> getAllHiveDatabase() {
        return hiveMapper.getAllDatabase();
    }

    public List<TableEntity> searchMySQLTable(TableEntity tableEntity,
            Integer limit, Integer offset) {
        return mySQLMapper.searchTable(tableEntity, limit, offset);
    }

    public Integer searchMySQLTableCount(TableEntity tableEntity) {
        return mySQLMapper.searchTableCount(tableEntity);
    }

    public List<TableEntity> searchGPTable(TableEntity tableEntity,
            Integer limit, Integer offset) {
        return gpMapper.searchTable(tableEntity, limit, offset);
    }

    public Integer searchGPTableCount(TableEntity tableEntity) {
        return gpMapper.searchTableCount(tableEntity);
    }

    public List<TableEntity> searchHiveTable(TableEntity tableEntity,
            Integer limit, Integer offset) {
        return hiveMapper.searchTable(tableEntity, limit, offset);
    }

    public Integer searchHiveTableCount(TableEntity tableEntity) {
        return hiveMapper.searchTableCount(tableEntity);
    }

    /**
     * 由于sqlserver mybatis不支持limit，此处做了个坑爹的伪limit
     * 
     * @param tableEntity
     * @param limit
     * @param offset
     * @return
     */
    public List<TableEntity> searchSQLServerTable(TableEntity tableEntity,
            Integer limit, Integer offset) {
        String databaseName = tableEntity.getDatabaseName();
        String tableName = tableEntity.getTableName();

        List<TableEntity> ret = new ArrayList<TableEntity>();
        List<TableEntity> all = new ArrayList<TableEntity>();
        DSInfo dsInfo = null;

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;

        try {
            dsInfo = getDSInfo(Const.DATASOURCE_TYPE_SQLSERVER, databaseName);
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();

            sql = " SELECT" + " SCHEMA_NAME(uid) as schemaname,name"
                    + " FROM sysobjects" + " WHERE xtype='U'";
            if (null != tableName && !tableName.isEmpty()) {
                sql += " AND LOWER(name) LIKE '%" + tableName + "%'";
            }
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                TableEntity tblEntity = new TableEntity();
                tblEntity.setDatasourceType(Const.DATASOURCE_TYPE_SQLSERVER);
                tblEntity.setDatabaseName(databaseName);
                tblEntity.setSchemaName(rs.getString("schemaname"));
                tblEntity.setTableName(rs.getString("name"));
                tblEntity.setTableRows("");
                all.add(tblEntity);
            }
            for (int i = offset; i < offset + limit && i < all.size(); ++i) {
                ret.add(all.get(i));
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("search SQLServer table fail, sql ["
                    + sql + "] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("rs,stmt,conn close error");
            }
        }
        return ret;
    }

    public Integer searchSQLServerTableCount(TableEntity tableEntity) {
        String databaseName = tableEntity.getDatabaseName();
        String tableName = tableEntity.getTableName();

        DSInfo dsInfo = getDSInfo(Const.DATASOURCE_TYPE_SQLSERVER, databaseName);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;
        int ret = 0;

        try {
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();

            sql = " SELECT" + " count(*) AS cnt" + " FROM sysobjects"
                    + " WHERE xtype='U'";
            if (null != tableName && !tableName.isEmpty()) {
                sql += " AND LOWER(name) LIKE '%" + tableName + "%'";
            }
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                ret = rs.getInt("cnt");
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(
                    "search SQLServer table count fail, sql [" + sql + "] "
                            + e.getMessage());
        } finally {
            boolean ok = true;
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("rs,stmt,conn close error");
            }
        }
        return ret;
    }

    /**
     * 只有mysql需要到toolsdba取列详情信息
     * 
     * @param databaseName
     * @param tableName
     * @return
     */
    public List<ColumnEntity> getMySQLColumn(String databaseName,
            String tableName) {
        List<ColumnEntity> ret = new ArrayList<ColumnEntity>();
        DSInfo dsInfo;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;

        try {
            dsInfo = getDSInfo(Const.DATASOURCE_TYPE_MYSQL, "toolsdba",
                    databaseName);
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            sql = " SELECT" + " column_name," + " column_type,"
                    + " column_key," + " column_comment"
                    + " FROM information_schema.columns" + " WHERE"
                    + " table_schema='" + dsInfo.getDbname()
                    + "' AND table_name='" + tableName + "'"
                    + " ORDER BY ordinal_position";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                ColumnEntity columnEntity = new ColumnEntity();
                columnEntity.setColumnName(rs.getString("column_name"));
                columnEntity.setColumnType(rs.getString("column_type"));
                columnEntity.setColumnKey(rs.getString("column_key"));
                columnEntity.setColumnComment(rs.getString("column_comment"));
                ret.add(columnEntity);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("get MySQL column fail, sql [" + sql
                    + "] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("rs,stmt,conn close error");
            }
        }
        return ret;
    }

    public List<ColumnEntity> getGPColumn(TableEntity tableEntity) {
        return gpMapper.getAllColumn(tableEntity);
    }

    public List<ColumnEntity> getHiveColumn(TableEntity tableEntity) {
        return hiveMapper.getAllColumn(tableEntity);
    }

    /**
     * sqlserver没用mybatis
     * 
     * @param tableEntity
     * @return
     */
    public List<ColumnEntity> getSQLServerColumn(TableEntity tableEntity) {
        String databaseName = tableEntity.getDatabaseName();
        String schemaName = tableEntity.getSchemaName();
        String tableName = tableEntity.getTableName();

        List<ColumnEntity> ret = new ArrayList<ColumnEntity>();
        DSInfo dsInfo;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;

        try {
            dsInfo = getDSInfo(Const.DATASOURCE_TYPE_SQLSERVER, databaseName);
            conn = getConnection(dsInfo);
            stmt = conn.createStatement();
            sql = " SELECT"
                    + " b.name AS column_name,"
                    + " c.name+'('+rtrim(cast(b.length as char))+')' AS column_type,"
                    + " '' AS column_key," + " '' AS column_comment"
                    + " FROM sysobjects a" + " JOIN syscolumns b ON a.id=b.id"
                    + " JOIN systypes c ON b.xtype=c.xtype" + " WHERE"
                    + " a.name='" + tableName + "' AND SCHEMA_NAME(a.uid)='"
                    + schemaName + "' AND c.name<>'sysname'"
                    + " ORDER BY b.colorder";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                ColumnEntity columnEntity = new ColumnEntity();
                columnEntity.setColumnName(rs.getString("column_name"));
                columnEntity.setColumnType(rs.getString("column_type"));
                columnEntity.setColumnKey(rs.getString("column_key"));
                columnEntity.setColumnComment(rs.getString("column_comment"));
                ret.add(columnEntity);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("get SQLServer column fail, sql [" + sql
                    + "] " + e.getMessage());
        } finally {
            boolean ok = true;
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    ok = false;
                    log.error(e.getMessage(), e);
                }
            }
            if (!ok) {
                throw new RuntimeException("rs,stmt,conn close error");
            }
        }
        return ret;
    }

    public Map<String, String> searchOnSchedule(List<TableEntity> tableList) {
        Map<String, String> ret = new HashMap<String, String>();
        if (null != tableList && !tableList.isEmpty()) {
            String sourceStorageType = tableList.get(0).getDatasourceType();
            if ("greenplum".equals(sourceStorageType)) {
                sourceStorageType = "gp";
            }
            DSInfo dsInfo = null;
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            String sql = "";
            try {
                dsInfo = getDSInfo(Const.DATASOURCE_TYPE_MYSQL, "darkbat");
                conn = getConnection(dsInfo);
                stmt = conn.createStatement();
                sql = " SELECT" + " table_name, storage_type" + " FROM"
                        + " mc_table_info" + " WHERE" + " storage_type<>'"
                        + sourceStorageType + "' AND LOWER(table_name) IN (";
                for (TableEntity tableEntity : tableList) {
                    String tblName = tableEntity.getTableName().toLowerCase();
                    sql += "'" + tblName + "',";
                    sql += "'dpods_" + tblName + "',";
                    if ("hive".equals(sourceStorageType)) {
                        sql += "'"
                                + tblName.substring(tblName.indexOf("_") + 1)
                                + "',";
                    }
                }
                sql = sql.substring(0, sql.length() - 1) + ")";
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String storageType = rs.getString(2);
                    if (ret.containsKey(tableName)) {
                        ret.put(tableName, ret.get(tableName) + ","
                                + storageType);
                    } else {
                        ret.put(tableName, storageType);
                    }
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException("search on schedule fail, sql ["
                        + sql + "] " + e.getMessage());
            }
        }
        return ret;
    }

}