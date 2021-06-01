package org.apache.sedona.core.exceptions;

public class SedonaRuntimeException extends RuntimeException{
    public SedonaRuntimeException(SedonaException e) {
        super(e.getMessage(),e.getCause());
    }
}
