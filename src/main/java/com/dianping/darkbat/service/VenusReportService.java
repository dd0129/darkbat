package com.dianping.darkbat.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Jsoup;
import org.springframework.stereotype.Service;

import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.common.GlobalResources;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@Service
public class VenusReportService {
    private final int RESOURCE_REPORT = 5,
                      RESOURCE_PAGE   = 7;
    
    public Map<Integer, JSONObject> buildIdToJsonMap(JSONArray items, String itemType) {
        Map<Integer, JSONObject> map = new HashMap<Integer, JSONObject>();
        
        for (Object obj : items) {
            JSONObject jObj = (JSONObject)obj;
            
            if (Const.MAIL_ITEM_CHART.equals(itemType)) {
                map.put(jObj.getInt("chartId"), jObj);
            } else if (Const.MAIL_ITEM_REPORT.equals(itemType) || Const.MAIL_ITEM_TABLE.equals(itemType)) {
                map.put(jObj.getInt("reportId"), jObj);
            }
        }
        
        return map;
    }
    
    /**
     * 获取chart列表
     * @param ids
     * @return
     */
    public JSONArray getChartList(String ids) {
        String url = GlobalResources.VENUS_URL +
                "config/chart/multiple?admin_token=" + 
                Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;

        Map<String, String> postMap = new HashMap<String, String>();
        postMap.put("chartIds", ids);
        
        String context = getJsonpContextNoNull(url, postMap);
        return JSONArray.fromObject(context);
    
    }
    
    /**
     * 获取page信息
     * @param pageId
     * @return
     */
    public JSONObject getPageInfo(String pageId) {
        String url = GlobalResources.VENUS_URL +
                "config/page/" + pageId + "?admin_token=" +
                Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;
        
        String ctx = getJsonpContextNoNull(url);
        return JSONObject.fromObject(ctx);
    }
    
    public JSONObject getPageList() {
        JSONObject ret = this.getResourceList(RESOURCE_PAGE);
        
        return ret;
    }
    
    public JSONObject getReportList() {
        JSONObject ret = this.getResourceList(RESOURCE_REPORT);
        
        return ret;
    }
    
    /**
     * 获取不带目录的报表信息
     * @return
     */
    public JSONArray getReportListWithoutDir(){
        String url = GlobalResources.VENUS_URL + 
                "config/report" +
                "?admin_token=" + Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;
        
        String context = getJsonpContextNoNull(url);
        return JSONArray.fromObject(context);
    }
    
    /**
     * 获取不带目录的页面信息
     * @return
     */
    public JSONArray getPageListWithoutDir(){
        String url = GlobalResources.VENUS_URL + 
                "config/page" +
                "?admin_token=" + Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;
        
        String context = getJsonpContextNoNull(url);
        return JSONArray.fromObject(context);
    }
    
    public JSONArray getReportListByIds(String ids) {
        String url = GlobalResources.VENUS_URL +
                "config/report/multiple?admin_token=" + 
                Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;
        Map<String, String> postMap = new HashMap<String, String>();
        postMap.put("reportIds", ids);
        
        String context = getJsonpContextNoNull(url, postMap);
        return JSONArray.fromObject(context);
    }
    
    /**
     * 从venus获取报表信息
     * @param reportId
     * @return
     */
    public JSONObject getReportInfo(String reportId) {
        String url = GlobalResources.VENUS_URL + "config/report/"
                + reportId + "?admin_token="
                + Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;
        String context = getJsonpContext(url, null);
        
        return JSONObject.fromObject(context);
    }
    
    /**
     * 从venus获取图形信息
     * @param chartId
     * @return
     */
    public JSONObject getChartInfo(String chartId) {
        String url = GlobalResources.VENUS_URL + "config/chart/"
                + chartId + "?admin_token="
                + Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;
        String context = getJsonpContext(url, null);
        
        return JSONObject.fromObject(context);
    }
    
    private JSONObject getResourceList(int resourceRootId) {
        String url = GlobalResources.VENUS_URL + 
                "result/resource/" + resourceRootId +
                "?admin_token=" + Const.VENUS_ADMIN_TOKEN_MAIL_SERVICE;
        
        String context = getJsonpContextNoNull(url);
        return JSONObject.fromObject(context);
    }
    
    private String getJsonpContextNoNull(String url) {
        return getJsonpContextNoNull(url, null);
    }
    
    private String getJsonpContextNoNull(String url, Map<String, String> postDataMap) {
        String context = getJsonpContext(url, postDataMap);
        context = nullStringFilter(context);
        
        return context;
    }
    
    private String getJsonpContext(String url, Map<String, String> postDataMap) {
        String context = null;

        try {
            context = visit(url, postDataMap);
        } catch (IOException e) {
            throw new RuntimeException("从Venus获取报表信息失败: " + e.getMessage());
        }
        
        if(context == null) {
            throw new RuntimeException("从Venus获取报表信息失败！");
        }
        
        return context;
    }
    
    private String visit(String url, Map<String, String> postDataMap) throws IOException {
        String ctx = null;
        int timeout = 30000;
        
        if(null != postDataMap) {
            //do post
            ctx = Jsoup.connect(url)
                    .ignoreContentType(true)
                    .timeout(timeout)
                    .data(postDataMap)
                    .post().text();
        } else {
            //do get
            ctx = Jsoup.connect(url).timeout(timeout).execute().body();
        }
        
        return ctx;
    }
    
    /**
     * 过滤null字符串
     * @param ctx
     */
    private String nullStringFilter(String ctx) {
        return ctx.replaceAll("null", "[]");
    }
}
