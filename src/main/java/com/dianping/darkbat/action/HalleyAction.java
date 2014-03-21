package com.dianping.darkbat.action;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.Const;
import com.dianping.darkbat.entity.TaskChildToParentEntity;
import com.dianping.darkbat.entity.TaskStatusEntity;
import com.dianping.darkbat.exception.BaseRuntimeException;
import com.dianping.darkbat.service.HalleyService;
import com.dianping.darkbat.service.TopologyService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.opensymphony.xwork2.Action;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import org.apache.log4j.Logger;
import org.apache.struts2.ServletActionContext;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class HalleyAction {
	@Autowired
	private HalleyService halleyService;
	
	private JSONObject jsonObject;

    private static Logger log = Logger.getLogger(HalleyAction.class);

	/**
	 * 重跑任务
	 * @return
	 */
	public String batchstopPrerunJob() {
		HttpServletRequest req = ServletActionContext.getRequest();

        Integer taskId = Integer.valueOf(req.getParameter("taskId"));
        if(taskId == null){
            setJsonObject(CommonUtil.getPubJson(2));
            return Action.ERROR;
        }

        try{
            Integer ret = halleyService.batchstopPrerunJob(taskId);
            setJsonObject(CommonUtil.getPubJson(0));
        }catch (Exception e){
            setJsonObject(CommonUtil.getPubJson(1));
            log.error(e.getMessage(),e);
            return Action.ERROR;
        }
		return Action.SUCCESS;
	}

	/**
	 * 重跑任务
	 * @return
	 */
	public String prerunJob() {
		HttpServletRequest req = ServletActionContext.getRequest();
        String begin = null;
        String end = null;
        JSONArray array = null;
        List<Integer> taskList = null;
        try{
            begin = req.getParameter("begin");
            end = req.getParameter("end");
            array = JSONArray.fromObject(req.getParameter("taskList"));
            taskList = JSONArray.toList(array,Integer.class,new JsonConfig());
        }catch(Exception e){
            log.error(e.getMessage(),e);
            setJsonObject(CommonUtil.getPubJson(2));
            return Action.ERROR;
        }

        try{
            Integer ret = halleyService.prerunJob(taskList,begin,end);
            setJsonObject(CommonUtil.getPubJson(0));
            return Action.SUCCESS;
        }catch(Exception e){
            log.error(e.getMessage(),e);
            setJsonObject(CommonUtil.getPubJson(1));
            return Action.ERROR;
        }
	}

    /**
     * 重跑任务
     * @return
     */
    public String prerunChildCascadeJob() {
        HttpServletRequest req = ServletActionContext.getRequest();
        String begin = null;
        String end = null;
        JSONArray array = null;
        List<Integer> boundaryTask = null;
        Integer childTask = null;
        try{
            begin = req.getParameter("begin");
            end = req.getParameter("end");
            childTask = Integer.valueOf(req.getParameter("childTask"));
            array = JSONArray.fromObject(req.getParameter("boundaryTask"));
            boundaryTask = JSONArray.toList(array,Integer.class,new JsonConfig());
        }catch(Exception e){
            log.error(e.getMessage(),e);
            setJsonObject(CommonUtil.getPubJson(2));
            return Action.ERROR;
        }

        try{
            Integer ret = halleyService.prerunChildCascdeJob(childTask,boundaryTask,begin,end);
            setJsonObject(CommonUtil.getPubJson(0));
            return Action.SUCCESS;
        }catch(Exception e){
            log.error(e.getMessage(),e);
            setJsonObject(CommonUtil.getPubJson(1));
            return Action.ERROR;
        }
    }

    public String temporaryRunJob() {
        HttpServletRequest req = ServletActionContext.getRequest();
        //force
        String taskName = req.getParameter("taskName");
        String para1 = req.getParameter("para1");
        String owner = req.getParameter("owner");
        //unforce
        String para2 = req.getParameter("para2");
        String para3 = req.getParameter("para3");
        String timeout = req.getParameter("timeout");

        Map<String,String> paras = new HashMap<String,String>();
        paras.put("taskName",taskName);
        paras.put("para1",para1);
        paras.put("owner",owner);

        paras.put("para2",para2);
        paras.put("para3",para3);
        paras.put("timeout",timeout);
        try{
            int ret = halleyService.temporaryRunJob(paras);
            setJsonObject(CommonUtil.getPubJson(0));
        }catch (Exception e){
            log.error(e.getMessage(),e);
            setJsonObject(CommonUtil.getPubJson(1));
            return Action.ERROR;
        }
        return Action.SUCCESS;
    }

	public JSONObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}

}
