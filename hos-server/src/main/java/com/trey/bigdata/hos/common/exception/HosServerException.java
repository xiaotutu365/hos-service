package com.trey.bigdata.hos.common.exception;

import com.trey.bigdata.hos.core.exception.HosException;

/**
 * 异常类
 */
public class HosServerException extends HosException {

    private int code;

    private String message;

    public HosServerException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.message = message;
    }

    public HosServerException(int code, String message) {
        super(message, null);
        this.code = code;
        this.message = message;
    }

    @Override
    public int errorCode() {
        return 0;
    }
}