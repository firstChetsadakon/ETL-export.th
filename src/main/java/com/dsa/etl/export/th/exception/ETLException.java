package com.dsa.etl.export.th.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class ETLException extends RuntimeException {
    public ETLException(String message) {
        super(message);
    }

    public ETLException(String message, Throwable cause) {
        super(message, cause);
    }
}
