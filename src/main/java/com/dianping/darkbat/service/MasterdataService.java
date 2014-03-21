package com.dianping.darkbat.service;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.common.GlobalResources;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class MasterdataService {

    private static Log log  = LogFactory.getLog(MasterdataService.class);

    public List<String> getTaskIds(Set<String> atomIds) {
        List<String> taskIds = new ArrayList<String>();
        String url = GlobalResources.MASTERDATA_URL + "getTaskIds?atomIds="
                + CommonUtil.collectionToString(atomIds);
        String context = null;

        try {
            context = Jsoup.connect(url).execute().body();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        if (context != null) {
            JSONObject jsonObj = JSONObject.fromObject(context);
            for (Object var : JSONArray.fromObject(jsonObj.get("msg"))) {
                taskIds.add(String.valueOf(var));
            }
        }

        return taskIds;
    }
    
    public List<String> getTaskIdsByTableName(Map<String,Set<String>> tableNames){
        List<String> taskIds = new ArrayList<String>();
        String url = GlobalResources.MASTERDATA_URL + "getParentTaskIdList?tableNames="
//                + CommonUtil.collectionToString(tableNames);
                + JSONObject.fromObject(tableNames).toString();
        String context = null;

        try {
            context = Jsoup.connect(url).execute().body();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        if (context != null) {
            JSONObject jsonObj = JSONObject.fromObject(context);
            for (Object var : JSONArray.fromObject(jsonObj.get("msg"))) {
                taskIds.add(String.valueOf(var));
            }
        }
        return taskIds;
    }
    
    public Map<Integer, Double> getAtomValue(List<Integer> calIds, String calDate) {
        Map<Integer, Double> atomValues = new HashMap<Integer, Double>();
        if(calIds.size() == 0)
            return atomValues;
        String url = GlobalResources.MASTERDATA_URL + "getAtomValue?calIds="
                + CommonUtil.collectionToString(calIds)
                + "&calDate=" + calDate;
        
        String context = null;

        try {
            context = Jsoup.connect(url).execute().body();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        if (context != null) {
            JSONObject jsonObj = JSONObject.fromObject(context);
            for (Object var : JSONArray.fromObject(jsonObj.get("msg"))) {
                JSONObject atomValue = JSONObject.fromObject(var);
                Integer calId = atomValue.getInt("cal_id");
                Double value = atomValue.getDouble("atom_value");
                atomValues.put(calId, value);
            }
        }

        return atomValues;
    }
}
