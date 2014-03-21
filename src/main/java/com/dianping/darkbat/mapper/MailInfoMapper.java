package com.dianping.darkbat.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.dianping.darkbat.entity.MailDetailEntity;
import com.dianping.darkbat.entity.MailInfoEntity;

public interface MailInfoMapper {
    List<MailInfoEntity> getMailList(
            @Param("mailInfo")      MailInfoEntity mailInfo
            );
    List<MailInfoEntity> getMailListContainsReport(
            @Param("reportId")      Integer reportId
            );
   
    Integer insertMailInfo(
            @Param("mailInfo")      MailInfoEntity mailInfo
            );
    
    MailInfoEntity getMailByName(
            @Param("mailInfo")      MailInfoEntity mailInfo
            );
    
    
    Integer deleteMailInfo(
            @Param("mailId")        Integer mailId
            );
    
    
    Integer updateMailInfo(
            @Param("mailInfo")      MailInfoEntity mailInfo
            );
    
    MailInfoEntity getExistedMailInfo(
            @Param("mailTitle")     String mailTitle,
            @Param("mailId")        Integer mailId
            );
    
    Integer updateTaskId(
            @Param("mailId")        Integer mailId,
            @Param("taskId")        Integer taskId
            );
    
    Integer updateMailPreTasks(
            @Param("reportId")        Integer reportId
            );
    
    List<MailDetailEntity> getMailDetail(@Param("info") MailDetailEntity info);
    Integer setMailDetail(@Param("info") MailDetailEntity info);
    Integer addMailDetail(@Param("info") MailDetailEntity info);
    Integer delMailDetail(@Param("info") MailDetailEntity info);
}
