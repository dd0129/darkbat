package com.dianping.darkbat.service;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.springframework.stereotype.Service;

import net.sf.json.JSONObject;

import com.dianping.darkbat.common.GlobalResources;
import com.dianping.darkbat.entity.LoginUserInfo;

@Service
public class AclService {
    private static Log log = LogFactory.getLog(AclService.class);
	public static LoginUserInfo getLoginUserInfo(String token){
		String url = GlobalResources.PLUTO_URL+"getLoginUserInfo?token="+token;
		String context = null;
		try {
			context = Jsoup.connect(url).execute().body();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
		LoginUserInfo loginUserInfo = new LoginUserInfo();
		if(context != null){
			JSONObject jsonObj = JSONObject.fromObject(context);
			JSONObject var1 = jsonObj.getJSONObject("msg");
			loginUserInfo.setLogin_id(var1.getInt("loginID"));
			loginUserInfo.setEmployee_id(var1.getString("employee_id"));
			loginUserInfo.setEmployee_name(var1.getString("employee_name"));
			loginUserInfo.setEmployee_email(var1.getString("employee_email"));
			return loginUserInfo;
		}
		else
			return null;
	}
}
