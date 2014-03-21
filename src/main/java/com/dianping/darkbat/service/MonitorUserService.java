package com.dianping.darkbat.service;

import com.dianping.darkbat.common.CommonUtil;
import com.dianping.darkbat.entity.MonitorUser;
import com.dianping.darkbat.mapper.MonitorUserMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.*;

@Service
@Scope("singleton")
public class MonitorUserService {
    
    @Autowired
    private MonitorUserMapper monitorUserMapper;
    
    private MonitorUser currentUser = null;

    private static Log log = LogFactory.getLog(MonitorUserService.class);
    
    /**
     * 上次调用日期
     */
    private Date lastCallDate = null;
    
    /**
     * 获取当前值班用户, 如果从内存读, 先检测当前请求是否是跨天请求, 如果是, 清空内存内数据
     * @return
     */
    public synchronized MonitorUser getCurrentUser()  {
        if (null == currentUser) {
            Date nowDt = new Date();
            
            List<MonitorUser> allUsers = monitorUserMapper.getAllMonitorUsers();
            MonitorUser lastUser = allUsers.get(0);
            Date lastMonitDate;
            try {
                lastMonitDate = CommonUtil.strToDate(lastUser.getBeginDate(), "yyyy-MM-dd");
            } catch (ParseException e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException("值班日期解析错误");
            }
            
            if (7 > getDayDiff(nowDt, lastMonitDate)) {
                currentUser = lastUser;
            } else {
                //改变当前值班用户，并将值班日期更新
                currentUser = getNextMonitorUser(allUsers, lastUser);
                
                Calendar lastDtCalendar = Calendar.getInstance();
                lastDtCalendar.setTime(lastMonitDate);
                lastDtCalendar.add(Calendar.DAY_OF_YEAR, 7);
                Date dt = lastDtCalendar.getTime();
                currentUser.setBeginDate(CommonUtil.dateToStr(dt, "yyyy-MM-dd"));
                monitorUserMapper.setMonitorBeginDate(currentUser);
            }
        } else {
            checkReload();
        }
        
        lastCallDate = new Date();
        return currentUser;
    }
    
    /**
     * 清理缓存在内存中的数据
     * @return
     */
    public synchronized int reload() {
        currentUser = null;
        getCurrentUser();
        
        return 1;
    }
    
    /**
     * 获取下一个值班用户
     * @param users
     * @param currentUser
     * @return
     */
    private MonitorUser getNextMonitorUser(List<MonitorUser> users, MonitorUser currentUser) {
        int currentOrderId = currentUser.getOrderId();
        int index = 0;
        
        Collections.sort(users, new Comparator<MonitorUser>() {
            public int compare(MonitorUser user0, MonitorUser user1) {
                return user0.getOrderId() - user1.getOrderId();
            }
        });
        
        for (; index < users.size(); index++) {
            if (users.get(index).getOrderId() == currentOrderId)
                break;
        }
        index = (index + 1) % users.size();
        return users.get(index);
    }
    
    /**
     * 如果第二天调用，重新清空内存内数据
     */
    private synchronized void checkReload() {
        if (null == lastCallDate) {
            reload();
        }
        
        Date nowDt = new Date();
        String format = "yyyy-MM-dd";
        if (!CommonUtil.dateToStr(nowDt, format).equals(CommonUtil.dateToStr(lastCallDate, format))) {
            reload();
        }
    }
    
    /**
     * 获取日期相差天数
     * @param date0
     * @param date1
     * @return
     */
    private int getDayDiff(Date date0, Date date1) {
        final int DAY_TO_MS = 1000 * 3600 * 24;
        int diff = (int)Math.floor(date0.getTime() - date1.getTime()) / DAY_TO_MS;
        
        return diff;
    }
}
