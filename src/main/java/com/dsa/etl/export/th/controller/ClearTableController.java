package com.dsa.etl.export.th.controller;

import com.dsa.etl.export.th.service.ClearTableService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/clear")
@RequiredArgsConstructor
@Slf4j
public class ClearTableController {
    private final ClearTableService clearTableService;

    @DeleteMapping("/all")
    public ResponseEntity<String> clearAllTables() {
        try {
            clearTableService.clearAllTables();
            return ResponseEntity.ok("All tables cleared successfully");
        } catch (Exception e) {
            log.error("Failed to clear tables", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to clear tables: " + e.getMessage());
        }
    }

    @DeleteMapping("/year/{year}")
    public ResponseEntity<String> clearTablesByYear(@PathVariable String year) {
        try {
            clearTableService.clearTablesByYear(year);
            return ResponseEntity.ok("Tables cleared for year " + year + " successfully");
        } catch (Exception e) {
            log.error("Failed to clear tables for year: {}", year, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to clear tables for year " + year + ": " + e.getMessage());
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Long>> getTableStatus() {
        return ResponseEntity.ok(clearTableService.getTableCounts());
    }
}
