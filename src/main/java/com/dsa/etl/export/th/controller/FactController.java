package com.dsa.etl.export.th.controller;

import com.dsa.etl.export.th.model.dto.FactDetailProjection;
import com.dsa.etl.export.th.model.dto.FactDetailResponse;
import com.dsa.etl.export.th.model.dto.FactSummaryProjection;
import com.dsa.etl.export.th.repository.FactExportThRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/facts")
@RequiredArgsConstructor
@Slf4j
public class FactController {
    private final FactExportThRepository factRepo;

    // 1. Get facts with pagination and filters
    // 1. Get facts with full dimension details
    @GetMapping
    public ResponseEntity<Page<FactDetailResponse>> getFactsWithDetails(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month) {

        Pageable pageable = PageRequest.of(page, size);
        Page<FactDetailProjection> facts = factRepo.findFactsWithDetails(year, month, pageable);

        Page<FactDetailResponse> response = facts.map(this::mapToDetailResponse);
        return ResponseEntity.ok(response);
    }

    

    // 2. Get summary statistics by year
    @GetMapping("/summary/year/{year}")
    public ResponseEntity<Map<String, Object>> getYearSummary(@PathVariable Integer year) {
        Map<String, Object> summary = new HashMap<>();
        summary.put("totalThaipValue", factRepo.sumThaipValueByYear(year));
        summary.put("totalDollarValue", factRepo.sumDollarValueByYear(year));
        summary.put("recordCount", factRepo.countByYear(year));
        return ResponseEntity.ok(summary);
    }

    // 3. Get summary by month for a specific year
    @GetMapping("/summary/year/{year}/month/{month}")
    public ResponseEntity<Map<String, Object>> getMonthSummary(
            @PathVariable Integer year,
            @PathVariable Integer month) {

        Map<String, Object> summary = new HashMap<>();
        summary.put("totalThaipValue", factRepo.sumThaipValueByYearAndMonth(year, month));
        summary.put("totalDollarValue", factRepo.sumDollarValueByYearAndMonth(year, month));
        summary.put("recordCount", factRepo.countByYearAndMonth(year, month));
        return ResponseEntity.ok(summary);
    }

    // 4. Get top countries by value for a year
    @GetMapping("/top-countries/{year}")
    public ResponseEntity<List<Map<String, Object>>> getTopCountries(
            @PathVariable Integer year,
            @RequestParam(defaultValue = "10") int limit) {

        List<Map<String, Object>> topCountries = factRepo.findTopCountriesByValue(year, limit);
        return ResponseEntity.ok(topCountries);
    }

    // 5. Get top HS2 categories by value
    @GetMapping("/top-hs2/{year}")
    public ResponseEntity<List<Map<String, Object>>> getTopHS2Categories(
            @PathVariable Integer year,
            @RequestParam(defaultValue = "10") int limit) {

        List<Map<String, Object>> topHS2 = factRepo.findTopHS2ByValue(year, limit);
        return ResponseEntity.ok(topHS2);
    }

    // ดึงข้อมูลสรุปรายปี
    @GetMapping("/summary/{year}")
    public ResponseEntity<List<FactSummaryProjection>> getYearSummary(
            @PathVariable Integer year,
            @RequestParam(defaultValue = "10") int limit) {

        List<FactSummaryProjection> summary = factRepo.findFactSummaryByYear(year, limit);
        return ResponseEntity.ok(summary);
    }

    // ดึงข้อมูลสรุปรายเดือน
    @GetMapping("/summary/{year}/{month}")
    public ResponseEntity<List<FactSummaryProjection>> getMonthSummary(
            @PathVariable Integer year,
            @PathVariable Integer month,
            @RequestParam(defaultValue = "10") int limit) {

        List<FactSummaryProjection> summary =
                factRepo.findFactSummaryByYearAndMonth(year, month, limit);
        return ResponseEntity.ok(summary);
    }

    private FactDetailResponse mapToDetailResponse(FactDetailProjection projection) {
        return FactDetailResponse.builder()
                .factId(projection.getFactId())
                .country(projection.getCountryName())
                .hs2Code(String.valueOf(projection.getHs2Code()))
                .hs2Description(projection.getHs2Description())
                .hs4Code(String.valueOf(projection.getHs4Code()))
                .hs4Description(projection.getHs4Description())
                .thaipValue(projection.getThaipValue())
                .dollarValue(projection.getDollarValue())
                .size(projection.getSize())
                .month(projection.getMonth())
                .year(projection.getYear())
                .build();
    }
}
