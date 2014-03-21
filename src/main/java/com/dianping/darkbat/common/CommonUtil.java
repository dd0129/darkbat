package com.dianping.darkbat.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONObject;

public class CommonUtil {

    public static boolean isEmpty(Object obj) {
        if (obj instanceof String) {
            String str = (String) obj;
            return str == null || str.isEmpty();
        }
        if (obj instanceof Integer) {
            Integer i = (Integer) obj;
            return i == null;
        }
        return obj == null;
    }

    public static void validateParam(Object obj, String msg) {
        if (isEmpty(obj)) {
            throw new RuntimeException(msg + "不能为空");
        }
    }

    public static Date strToDate(String strDate, String strFormat) throws ParseException {
        Date date = null;
        SimpleDateFormat format = new SimpleDateFormat(strFormat);
        
        date = format.parse(strDate);
        return date;
    }
    
    public static String dateToStr(Date date, String strFormat) {
        SimpleDateFormat format = new SimpleDateFormat(strFormat);
        
        return format.format(date);
    }
    
    /**
     * 从请求中获取值，若为空，则返回空字符串
     * @param req
     * @param param
     * @return
     */
    public static String getValueFromReq(HttpServletRequest req, String param) {
        String val = req.getParameter(param);
        
        return val == null || val.isEmpty() ? null : val;
    }
    
    public static void validateParamRange(Object obj, String msg, Object[] values) {
        if (isEmpty(obj)) {
            throw new RuntimeException(msg + "不能为空");
        }
        boolean ok = false;
        if (obj instanceof Integer) {
            Integer iObj = (Integer) obj;
            for (int i = 0; i < values.length; ++i) {
                if (((Integer) values[i]).intValue() == iObj.intValue()) {
                    ok = true;
                    break;
                }
            }
        } else if (obj instanceof String) {
            String sObj = (String) obj;
            for (int i = 0; i < values.length; ++i) {
                if (((String) values[i]).equals(sObj)) {
                    ok = true;
                    break;
                }
            }
        }
        if (!ok) {
            throw new RuntimeException(msg + "不合法");
        }
    }

    public static Integer parseInt(String str) {
        if (null == str || str.trim().isEmpty()) {
            return null;
        } else {
            return Integer.parseInt(str);
        }
    }

    public static String parseStr(String str) {
        if (null == str) {
            return null;
        }
        str = str.trim();
        if (str.isEmpty()) {
            return null;
        }
        return str;
    }
    
    public static <T> String collectionToString(Collection<T> strList) {
		if (strList == null) {
			return null;
		}
		StringBuilder result = new StringBuilder();
		boolean flag = false;
		for (T string : strList) {
			if (flag) {
				result.append(",");
			} else {
				flag = true;
			}
			result.append(string);
		}
		return result.toString();
	}


    public static JSONObject getPubJson(Object o) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("code", Const.RET_CODE_200);
        if (o != null) {
            jsonObject.element("msg", o);
        } else {
            jsonObject.element("msg", "");
        }
        return jsonObject;
    }
    
    public static JSONObject getPubJson(Object o, Integer code) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("code", code);
        if (o != null) {
            jsonObject.element("msg", o);
        } else {
            jsonObject.element("msg", "");
        }
        return jsonObject;
    }

}
