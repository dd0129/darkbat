package com.dianping.darkbat.exception;

public class InvalidInputException extends BaseException {

    /**
     * 序列UID生成失败，TODO 重新生成序列UID
     */
    private static final long serialVersionUID = 1L;

    public InvalidInputException(String message) {
        super(message, new ExceptionData());
    }
    
}
