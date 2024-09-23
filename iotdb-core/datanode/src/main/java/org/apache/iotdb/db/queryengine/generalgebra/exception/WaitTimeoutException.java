package org.apache.iotdb.db.queryengine.generalgebra.exception;

public class WaitTimeoutException extends RuntimeException {
    public WaitTimeoutException(String message) {
        super(message);
    }
}
