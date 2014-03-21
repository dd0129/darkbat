package com.dianping.darkbat.common;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GlobalResources {
	private static Log LOG = LogFactory.getLog(GlobalResources.class);
	
	public static final String URL = "darkbat.properties";
	public static String HALLEY_URL;
	public static String PLUTO_URL;
	public static String MASTERDATA_URL;
	public static String VENUS_URL;
	public static String DEPLOY_IP;
	public static Integer DEPLOY_PORT;
	public static String DEPLOY_USER;
	public static String DEPLOY_PASSWD;
	public static String COMMAND_CANAAN;
	public static String IDENTITY_FILE;
	public static String COMMAND_SUN;
	public static String TOKEN;
	
	static{
		Properties properties = new Properties();
		try {
			properties.load(GlobalResources.class.getClassLoader().getResourceAsStream(URL));			
			HALLEY_URL = properties.getProperty("HALLEY_URL");
			PLUTO_URL = properties.getProperty("PLUTO_URL");
			MASTERDATA_URL = properties.getProperty("MASTERDATA_URL");
			VENUS_URL = properties.getProperty("VENUS_URL");
			DEPLOY_IP = properties.getProperty("DEPLOY_IP");
			DEPLOY_PORT = Integer.parseInt(properties.getProperty("DEPLOY_PORT"));
			DEPLOY_USER = properties.getProperty("DEPLOY_USER");
			DEPLOY_PASSWD = properties.getProperty("DEPLOY_PASSWD");
			COMMAND_CANAAN = properties.getProperty("COMMAND_CANAAN");
			IDENTITY_FILE = properties.getProperty("IDENTITY_FILE");
			COMMAND_SUN = properties.getProperty("COMMAND_SUN");
			TOKEN = properties.getProperty("TOKEN");
			
			LOG.info("参数初始化成功");
			LOG.info("HALLEY_URL:"+HALLEY_URL);
			LOG.info("PLUTO_URL:"+PLUTO_URL);
			LOG.info("MASTERDATA_URL:"+MASTERDATA_URL);
			LOG.info("VENUS_URL:"+VENUS_URL);
			LOG.info("DEPLOY_IP:"+DEPLOY_IP);
			LOG.info("DEPLOY_PORT:"+DEPLOY_PORT);
			LOG.info("DEPLOY_USER:"+DEPLOY_USER);
			LOG.info("DEPLOY_PASSWD:"+DEPLOY_PASSWD);
			LOG.info("COMMAND_CANAAN:"+COMMAND_CANAAN);
			LOG.info("IDENTITY_FILE:"+IDENTITY_FILE);
			LOG.info("TOKEN:"+TOKEN);
		} catch (Exception e) {
			LOG.error(e);
		}
	}
}
