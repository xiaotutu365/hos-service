package com.trey.bigdata.hos.core.usermgr;

import com.trey.bigdata.hos.core.exception.HosException;

public class HosUsermgrException extends HosException {

    private int code;

    private String message;

    public HosUsermgrException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.message = message;
    }

    public HosUsermgrException(int code, String message) {
        super(message, null);
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return this.code;
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    @Override
    public int errorCode() {
        return this.code;
    }
}
