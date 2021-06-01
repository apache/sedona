package org.apache.sedona.core.exceptions;

public class SedonaException extends Exception{
    public SedonaException() {
    }

    public SedonaException(String message) {
        super(message);
    }

    public SedonaException(String message, Throwable cause) {
        super(message, cause);
    }

    public SedonaException(Throwable cause) {
        super(cause);
    }
    
    public SedonaException(SedonaRuntimeException e){
        super(e.getMessage(),e.getCause());
    }

    public SedonaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
