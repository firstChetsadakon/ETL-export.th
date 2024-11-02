package com.dsa.etl.export.th.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ETLResponse {
    private String status;
    private String processId;
    //private long recordsProcessed;
    private LocalDateTime timestamp;
    private String message;
}
