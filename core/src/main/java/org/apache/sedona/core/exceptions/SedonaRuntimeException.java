package org.apache.sedona.core.exceptions;

/**
 * Base Class for Handling Runtime Exceptions
 */
public class SedonaRuntimeException extends RuntimeException{
    public SedonaRuntimeException(SedonaException e) {
        super(e.getMessage(),e.getCause());
    }
}
