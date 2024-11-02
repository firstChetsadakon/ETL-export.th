package com.dsa.etl.export.th.model.dto;

import com.dsa.etl.export.th.model.enums.ETLProcessStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ETLStatus {
    private String processId;
    private ETLProcessStatus status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private long totalRecords;
    private long processedRecords;
    private long failedRecords;
    private String errorMessage;
    private double progressPercentage;
    private Map<String, String> additionalInfo;
}
