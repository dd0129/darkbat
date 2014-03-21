package com.dianping.darkbat.interceptor;

import com.dianping.darkbat.exception.BaseException;
import com.opensymphony.xwork2.ActionInvocation;
import com.opensymphony.xwork2.interceptor.ExceptionMappingInterceptor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExceptionInterceptor extends ExceptionMappingInterceptor {

    private static final long serialVersionUID = -1525370835877930044L;

    private static Log log = LogFactory.getLog(ExceptionInterceptor.class);

    @Override
    public String intercept(ActionInvocation invocation) throws Exception {
        String res = "all";
        try {
            res = invocation.invoke();
        } catch (Exception e) {
        	log.error(e.getMessage(), e);
            throw new BaseException(e.getMessage());
//            throw e;
        }
        return res;
    }

}
