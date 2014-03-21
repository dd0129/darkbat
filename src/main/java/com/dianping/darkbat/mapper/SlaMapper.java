package com.dianping.darkbat.mapper;

import com.dianping.darkbat.entity.BottleneckTaskEntity;
import com.dianping.darkbat.entity.SlaEntity;
import com.dianping.darkbat.entity.SlaFailRecordEntity;
import com.dianping.darkbat.entity.SlaJobEntity;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-1-22
 * Time: 上午11:42
 * To change this template use File | Settings | File Templates.
 */
public interface SlaMapper {

    List<SlaJobEntity> getSlaJobs(@Param("timeId") String timeId);

    SlaJobEntity getSlaJobById(@Param("slaId")Integer slaId,@Param("timeId") String timeId);

    List<SlaEntity> getSlas();

    List<String> getUserEmail(@Param("userName") String userName);

    Integer insertSlaDim(@Param("sla") SlaEntity sla);

    Integer insertSlaJob(@Param("sla") SlaEntity sla,@Param("preTaskId") Integer preTaskId);

    Integer insertSlaUser(@Param("sla") SlaEntity sla);

    Integer updateSlaDim(@Param("sla") SlaEntity sla);

    Integer updateSlaJob(@Param("sla") SlaEntity sla,@Param("preTaskId") Integer preTaskId);

    Integer deleteSlaJob(@Param("sla") SlaEntity sla);

    List<BottleneckTaskEntity> getdelayinfo(@Param("timeId") String timeId);
    List<Map<String,Object>> getJobRunHis(@Param("timeId") String timeId, @Param("keyTaskId") List<Integer> keyTaskId);
}
