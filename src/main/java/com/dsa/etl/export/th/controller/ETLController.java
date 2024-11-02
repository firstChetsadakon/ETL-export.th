package com.dsa.etl.export.th.controller;

import com.dsa.etl.export.th.model.dto.ETLResponse;
import com.dsa.etl.export.th.service.ETLService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/etl")
@Slf4j
@RequiredArgsConstructor
public class ETLController {
    private final ETLService etlService;

    @GetMapping("/process/{year}")
    public ResponseEntity<ETLResponse> startETLForYear(@PathVariable String year) {
        try {
            etlService.performETL(year);
            //etlResponse.builder().message("ETL process completed successfully for year: " + year).status("COMPLETE").build()

            return ResponseEntity.ok(ETLResponse.builder().message("ETL process completed successfully for year: " + year)
                    .status("COMPLETE")
                    .timestamp(LocalDateTime.now())
                    .build());
        } catch (Exception e) {
            log.error("ETL process failed for year: {}", year, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ETLResponse.builder().message("ETL process failed: " + e.getMessage()).build());
        }
    }

    @GetMapping("/status/{year}")
    public ResponseEntity<Map<String, Object>> getETLStatus(@PathVariable String year) {
        log.info("Endpoint Status by year  : {}",year);
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("year", year);
            status.put("factRecords", etlService.getRecordCountForYear(year));
            status.put("dimensions", Map.of(
                    "countries", etlService.getCountryCount(),
                    "hs2", etlService.getHs2Count(),
                    "hs4", etlService.getHs4Count()
            ));
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            log.error("Failed to get status", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
