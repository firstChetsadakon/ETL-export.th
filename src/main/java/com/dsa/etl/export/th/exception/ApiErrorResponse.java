package com.dsa.etl.export.th.exception;

import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponse;

import java.time.LocalDateTime;

@Getter
@Setter
public class ApiErrorResponse implements ErrorResponse {
    private String error;
    private String message;
    private LocalDateTime timestamp;

    public ApiErrorResponse(String error, String message, LocalDateTime timestamp) {
        this.error = error;
        this.message = message;
        this.timestamp = timestamp;
    }

    @Override
    public HttpStatusCode getStatusCode() {
        return null;
    }

    @Override
    public ProblemDetail getBody() {
        return null;
    }
}
