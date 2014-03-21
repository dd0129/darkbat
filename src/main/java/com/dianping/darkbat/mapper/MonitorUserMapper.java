package com.dianping.darkbat.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.dianping.darkbat.entity.MonitorUser;

public interface MonitorUserMapper {

    /**
     * order by 值班日期 desc
     * @return
     */
    List<MonitorUser> getAllMonitorUsers();
    
    Integer setMonitorBeginDate(@Param("user") MonitorUser user);
}
