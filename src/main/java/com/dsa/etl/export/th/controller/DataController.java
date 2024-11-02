package com.dsa.etl.export.th.controller;

import com.dsa.etl.export.th.model.entities.DimCountryEntity;
import com.dsa.etl.export.th.model.entities.DimHs2Entity;
import com.dsa.etl.export.th.model.entities.DimHs4Entity;
import com.dsa.etl.export.th.model.entities.FactExportThEntity;
import com.dsa.etl.export.th.repository.DimCountryRepository;
import com.dsa.etl.export.th.repository.DimHs2Repository;
import com.dsa.etl.export.th.repository.DimHs4Repository;
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
@RequestMapping("/api/data")
@RequiredArgsConstructor
@Slf4j
public class DataController {
    private final DimHs2Repository hs2Repo;
    private final DimHs4Repository hs4Repo;
    private final DimCountryRepository countryRepo;
    private final FactExportThRepository factRepo;

    // Dimension APIs
    @GetMapping("/dimensions/hs2")
    public ResponseEntity<List<DimHs2Entity>> getAllHs2() {
        log.info("Get dimensions hs2");
        return ResponseEntity.ok(hs2Repo.findAll());
    }

//    @GetMapping("/dimensions/hs2/{id}")
//    public ResponseEntity<DimHs2Entity> getHs2ById(@PathVariable Long id) {
//        return hs2Repo.findById(id)
//                .map(ResponseEntity::ok)
//                .orElse(ResponseEntity.notFound().build());
//    }

    @GetMapping("/dimensions/hs4")
    public ResponseEntity<List<DimHs4Entity>> getAllHs4() {
        return ResponseEntity.ok(hs4Repo.findAll());
    }

//    @GetMapping("/dimensions/hs4/{id}")
//    public ResponseEntity<DimHs4Entity> getHs4ById(@PathVariable Long id) {
//        return hs4Repo.findById(id)
//                .map(ResponseEntity::ok)
//                .orElse(ResponseEntity.notFound().build());
//    }

    @GetMapping("/dimensions/countries")
    public ResponseEntity<List<DimCountryEntity>> getAllCountries() {
        return ResponseEntity.ok(countryRepo.findAll());
    }

//    @GetMapping("/dimensions/countries/{id}")
//    public ResponseEntity<DimCountryEntity> getCountryById(@PathVariable Long id) {
//        return countryRepo.findById(id)
//                .map(ResponseEntity::ok)
//                .orElse(ResponseEntity.notFound().build());
//    }

    // Fact table APIs
//    @GetMapping("/facts")
//    public ResponseEntity<Page<FactExportThEntity>> getFacts(
//            @RequestParam(defaultValue = "0") int page,
//            @RequestParam(defaultValue = "10") int size,
//            @RequestParam(required = false) Integer year,
//            @RequestParam(required = false) Integer month,
//            @RequestParam(required = false) String country) {
//
//        Pageable pageable = PageRequest.of(page, size);
//        Page<FactExportThEntity> facts;
//
//        if (year != null && month != null && country != null) {
//            facts = factRepo.findByYearAndMonthAndCountry_Country(year, month, country, pageable);
//        } else if (year != null && month != null) {
//            facts = factRepo.findByYearAndMonth(year, month, pageable);
//        } else if (year != null) {
//            facts = factRepo.findByYear(year, pageable);
//        } else {
//            facts = factRepo.findAll(pageable);
//        }
//
//        return ResponseEntity.ok(facts);
//    }

    @GetMapping("/facts/summary")
    public ResponseEntity<Map<String, Object>> getFactsSummary(
            @RequestParam Integer year,
            @RequestParam(required = false) Integer month) {
        Map<String, Object> summary = new HashMap<>();

        if (month != null) {
            summary.put("totalThaipValue", factRepo.sumThaipValueByYearAndMonth(year, month));
            summary.put("totalDollarValue", factRepo.sumDollarValueByYearAndMonth(year, month));
            summary.put("recordCount", factRepo.countByYearAndMonth(year, month));
        } else {
            summary.put("totalThaipValue", factRepo.sumThaipValueByYear(year));
            summary.put("totalDollarValue", factRepo.sumDollarValueByYear(year));
            summary.put("recordCount", factRepo.countByYear(year));
        }

        return ResponseEntity.ok(summary);
    }

//    @GetMapping("/facts/by-country")
//    public ResponseEntity<List<Map<String, Object>>> getFactsByCountry(
//            @RequestParam Integer year) {
//        return ResponseEntity.ok(factRepo.findSummaryByCountry(year));
//    }
//
//    @GetMapping("/facts/by-hs2")
//    public ResponseEntity<List<Map<String, Object>>> getFactsByHs2(
//            @RequestParam Integer year) {
//        return ResponseEntity.ok(factRepo.findSummaryByHs2(year));
//    }
}
