package com.dianping.darkbat.service;

import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import com.dianping.darkbat.entity.LoginUserInfo;

@Component
@Aspect
public class LogAspectJ {

    public Log LOG = LogFactory.getLog(LogAspectJ.class);

	@Pointcut("execution (* com.dianping.darkbat.action.*.*(..))")
    private void anyMethod() {
    }

    @Before("anyMethod()")
    public void beforeService(JoinPoint joinpoint) {
        // LOG.info(joinpoint.getTarget().getClass().getName()+" start");
		HttpServletRequest req = ServletActionContext.getRequest();
		if(req.getAttribute("guid") == null)
			req.setAttribute("guid", UUID.randomUUID());
		req.setAttribute("begintime", System.currentTimeMillis());
    }

    @After("anyMethod()")
    public void afterService(JoinPoint joinpoint) {
        // LOG.info(joinpoint.getTarget().getClass().getName()+" end");
    }

    @AfterThrowing(pointcut = "anyMethod()", throwing = "e")
    public void afterThrowing(JoinPoint joinpoint, Exception e) {
		String action = joinpoint.getTarget().getClass().getName();
		String method = joinpoint.getSignature().getName();
		StringBuffer logInfo = new StringBuffer(getOperatorInfo(action, method));
		logInfo.append("\t"+e);
		LOG.error(logInfo);
	}

    @Around("anyMethod()")
    public Object aroundAction(ProceedingJoinPoint pjp) throws Throwable {
		Object result = pjp.proceed();
		String action = pjp.getTarget().getClass().getName();
		String method = pjp.getSignature().getName();
		StringBuffer logInfo = new StringBuffer(getOperatorInfo(action, method));
		logInfo.append("\t"+(result.toString().length()>10000000?result.toString().substring(0, 10000000):result.toString()));
		LOG.info(logInfo);
		return result;
    }
	public String getOperatorInfo(String action, String method){
		StringBuffer logInfo = new StringBuffer();
		HttpServletRequest req = ServletActionContext.getRequest();
		if(req.getAttribute("guid") == null)
			req.setAttribute("guid", UUID.randomUUID());
		String ip = req.getHeader("X-Real-IP");
		if(ip == null || ip.isEmpty())
			ip = req.getRemoteAddr();
		logInfo.append(req.getAttribute("guid")+"\t"+ip+"\t"+req.getAttribute("begintime")+"\t"+System.currentTimeMillis()+"\t"+action+"."+method);
		try{
			HttpSession hs = req.getSession();
			if(hs != null && hs.getAttribute("token") != null){
				LoginUserInfo aum = AclService.getLoginUserInfo(hs.getAttribute("token").toString());
				if(aum != null){
					logInfo.append("\t"+aum.getLogin_id()+";"+aum.getEmployee_id()+";"+aum.getEmployee_name());
				}
				else
					logInfo.append("\t无用户信息");
			}
			else
				logInfo.append("\t无用户信息");
		}catch(Exception e1){
			logInfo.append("\t获取用户信息出错\t"+e1);
		}
		StringBuffer data = new StringBuffer();
		for(Object var:req.getParameterMap().keySet()){
			data.append(var+":"+req.getParameter(var.toString())+";");
		}
		logInfo.append("\t"+data.toString());
		return logInfo.toString();
	}
}
