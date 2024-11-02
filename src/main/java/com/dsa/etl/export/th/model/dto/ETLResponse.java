package com.dsa.etl.export.th.model.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class ETLResponse {
    private String status;
    private String processId;
    private long recordsProcessed;
    private LocalDateTime timestamp;
    private String message;
}
