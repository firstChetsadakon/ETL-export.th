package com.dsa.etl.export.th.service;

import com.dsa.etl.export.th.exception.ETLException;
import com.dsa.etl.export.th.model.entities.*;
import com.dsa.etl.export.th.repository.*;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class ETLServiceTest {
    private final ExportThRepository sourceRepo;
    private final DimHs2Repository hs2Repo;
    private final DimHs4Repository hs4Repo;
    private final DimCountryRepository countryRepo;
    private final FactExportThRepository factRepo;

    private static final int BATCH_SIZE = 1000;

    @Transactional
    public void performETL(String year) {
        log.info("Starting ETL process for year: {}", year);
        try {
            // Clear existing data for the specified year
            clearExistingDataForYear(year);

            // Extract and load dimensions
            extractDimensions(year);

            // Extract and load facts
            extractFacts(year);

            log.info("ETL process completed successfully for year: {}", year);
        } catch (Exception e) {
            log.error("ETL process failed for year: {}", year, e);
            throw new RuntimeException("ETL process failed: " + e.getMessage(), e);
        }
    }

    private void clearExistingDataForYear(String year) {
        log.info("Clearing existing data for year {}...", year);
        factRepo.deleteByYear(Integer.parseInt(year));
        // Don't clear dimensions as they might be referenced by other years
    }

    @Transactional
    protected void extractDimensions(String year) {
        try {
            log.info("Extracting dimensions for year {}...", year);

            Set<DimHs2Entity> hs2Dimensions = new HashSet<>();
            Set<DimHs4Entity> hs4Dimensions = new HashSet<>();
            Set<DimCountryEntity> countryDimensions = new HashSet<>();

            try (Stream<ExportThEntity> stream = sourceRepo.streamAllByYear(year)) {
                stream.forEach(source -> {
                    // HS2 Dimension
                    DimHs2Entity hs2 = new DimHs2Entity();
                    hs2.setHs2dg(source.getHs2dg());
                    hs2.setDescription(source.getDescriptionHs2dg());
                    hs2Dimensions.add(hs2);

                    // HS4 Dimension
                    DimHs4Entity hs4 = new DimHs4Entity();
                    hs4.setHs4dg(source.getHs4dg());
                    hs4.setDescription(source.getDescriptionHs4dg());
                    hs4Dimensions.add(hs4);

                    // Country Dimension
                    DimCountryEntity country = new DimCountryEntity();
                    country.setCountry(source.getCountry());
                    countryDimensions.add(country);
                });
            }

            // Save dimensions (merge with existing)
            mergeAndSaveDimensions(hs2Dimensions, hs4Dimensions, countryDimensions);

        } catch (Exception e) {
            log.error("Failed to extract dimensions for year: {}", year, e);
            throw new RuntimeException("Failed to extract dimensions: " + e.getMessage(), e);
        }
    }

    private void mergeAndSaveDimensions(Set<DimHs2Entity> hs2Dimensions,
                                        Set<DimHs4Entity> hs4Dimensions,
                                        Set<DimCountryEntity> countryDimensions) {
        // Merge HS2 dimensions
        for (DimHs2Entity hs2 : hs2Dimensions) {
            if (!hs2Repo.existsByHs2dg(hs2.getHs2dg())) {
                hs2Repo.save(hs2);
            }
        }

        // Merge HS4 dimensions
        for (DimHs4Entity hs4 : hs4Dimensions) {
            if (!hs4Repo.existsByHs4dg(hs4.getHs4dg())) {
                hs4Repo.save(hs4);
            }
        }

        // Merge country dimensions
        for (DimCountryEntity country : countryDimensions) {
            if (!countryRepo.existsByCountry(country.getCountry())) {
                countryRepo.save(country);
            }
        }
    }

    @Transactional
    protected void extractFacts(String year) {
        AtomicInteger count = new AtomicInteger(0);
        List<FactExportThEntity> batchList = new ArrayList<>(BATCH_SIZE);

        try (Stream<ExportThEntity> stream = sourceRepo.streamAllByYear(year)) {
            stream.forEach(source -> {
                try {
                    FactExportThEntity fact = mapToFact(source);
                    batchList.add(fact);

                    if (batchList.size() >= BATCH_SIZE) {
                        factRepo.saveAll(batchList);
                        log.info("Processed {} records for year {}", count.addAndGet(batchList.size()), year);
                        batchList.clear();
                    }
                } catch (Exception e) {
                    log.error("Error processing record: {}", source, e);
                }
            });

            if (!batchList.isEmpty()) {
                factRepo.saveAll(batchList);
                log.info("Processed final {} records for year {}", count.addAndGet(batchList.size()), year);
            }
        }
    }

    private FactExportThEntity mapToFact(ExportThEntity source) {
        try {
            FactExportThEntity fact = new FactExportThEntity();

            // Get dimension references
            DimCountryEntity country = countryRepo.findByCountry(source.getCountry())
                    .orElseThrow(() -> new ETLException("Country not found: " + source.getCountry()));
            DimHs2Entity hs2 = hs2Repo.findByHs2dg(source.getHs2dg())
                    .orElseThrow(() -> new ETLException("HS2 not found: " + source.getHs2dg()));
            DimHs4Entity hs4 = hs4Repo.findByHs4dg(source.getHs4dg())
                    .orElseThrow(() -> new ETLException("HS4 not found: " + source.getHs4dg()));

            // Set dimension references
            fact.setCountry(country);
            fact.setHs2(hs2);
            fact.setHs4(hs4);

            // Transform values
            fact.setThaipValue(parseValue(source.getThaipValue()));
            fact.setDollarValue(parseValue(source.getDollarValue()));
            fact.setSize(source.getSize());
            fact.setMonth(parseInteger(source.getMonth(), "month"));
            fact.setYear(parseInteger(source.getYear(), "year"));

            validateFactData(fact);
            return fact;
        } catch (Exception e) {
            log.error("Error mapping fact data for record: {}", source, e);
            throw new ETLException("Failed to map fact data: " + e.getMessage());
        }
    }

    private BigDecimal parseValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return BigDecimal.ZERO;
        }
        String cleaned = value.replaceAll("[^\\d.]", "");
        try {
            return new BigDecimal(cleaned);
        } catch (NumberFormatException e) {
            log.warn("Invalid numeric value: {}, using 0", value);
            return BigDecimal.ZERO;
        }
    }

    private Integer parseInteger(String value, String fieldName) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid {} format: {}, using 0", fieldName, value);
            return 0;
        }
    }

    private void validateFactData(FactExportThEntity fact) {
        List<String> errors = new ArrayList<>();

        if (fact.getCountry() == null) {
            errors.add("Country reference cannot be null");
        }
        if (fact.getHs2() == null) {
            errors.add("HS2 reference cannot be null");
        }
        if (fact.getHs4() == null) {
            errors.add("HS4 reference cannot be null");
        }
        if (fact.getMonth() != null && (fact.getMonth() < 1 || fact.getMonth() > 12)) {
            errors.add("Month must be between 1 and 12");
        }
        if (fact.getYear() != null && fact.getYear() < 1900) {
            errors.add("Year must be after 1900");
        }

        if (!errors.isEmpty()) {
            throw new ETLException("Fact data validation failed: " + String.join(", ", errors));
        }
    }

    public List<String> getAllAvailableYears() {
        return sourceRepo.findDistinctYears();
    }

//    public long getRecordCountForYear(String year) {
//        return factRepo.countByYear(Integer.parseInt(year));
//    }
//
//    public long getCountryCount() {
//        return countryRepo.count();
//    }
//
//    public long getHs2Count() {
//        return hs2Repo.count();
//    }
//
//    public long getHs4Count() {
//        return hs4Repo.count();
//    }
}
