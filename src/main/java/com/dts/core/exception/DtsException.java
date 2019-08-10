package com.dts.core.exception;


import org.apache.commons.lang.exception.NestableRuntimeException;

public class DtsException extends NestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public DtsException(String errorCode) {
        super(errorCode);
    }

    public DtsException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public DtsException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public DtsException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public DtsException(Throwable cause) {
        super(cause);
    }

}
