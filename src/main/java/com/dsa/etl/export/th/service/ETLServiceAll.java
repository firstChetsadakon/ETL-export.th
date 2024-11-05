package com.dsa.etl.export.th.service;

import com.dsa.etl.export.th.exception.ETLException;
import com.dsa.etl.export.th.model.entities.*;
import com.dsa.etl.export.th.repository.*;
import com.google.common.collect.Lists;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class ETLServiceAll {
    private final ExportThRepository sourceRepo;
    private final DimHs2Repository hs2Repo;
    private final DimHs4Repository hs4Repo;
    private final DimCountryRepository countryRepo;
    private final FactExportThRepository factRepo;
    private final DataSource dataSource;
    private static final int MAX_CONCURRENT_CHUNKS = 4;  // Reduce from default
    private final ClearTableService clearTableService;
    private static final int BATCH_SIZE = 50000; // Increased batch size
    private final Executor executorService;
    private final EntityManager entityManager;

    public void performETL() {  // removed year parameter
        log.info("Starting ETL process for all records");
        StopWatch watch = new StopWatch();
        watch.start();

        try {
//            // Clear tables
//            clearTables();

            // First extract and save dimensions
            log.info("Extracting dimensions...");
            extractAndSaveDimensions();

            // Extract dimensions and load maps
            Map<Integer, DimHs2Entity> hs2Map = loadHs2Map();
            Map<Integer, DimHs4Entity> hs4Map = loadHs4Map();
            Map<String, DimCountryEntity> countryMap = loadCountryMap();

            // Get total count for all records
            long totalRecords = sourceRepo.count();  // count all records
            int chunkSize = 50000;
            int totalChunks = (int) Math.ceil((double) totalRecords / chunkSize);

            log.info("Processing {} total records in {} chunks", totalRecords, totalChunks);

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < totalChunks; i++) {
                int offset = i * chunkSize;
                futures.add(processChunk(offset, chunkSize, hs2Map, hs4Map, countryMap));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        } catch (Exception e) {
            log.error("ETL process failed: {}", e.getMessage(), e);
            throw new ETLException("ETL process failed: " + e.getMessage());
        } finally {
            watch.stop();
            log.info("ETL process completed in {} seconds", watch.getTotalTimeSeconds());
        }
    }

//    private void extractAndSaveDimensions() {
//        log.info("Starting dimension extraction");
//
//        List<DimHs2Entity> hs2Batch = new ArrayList<>();
//        List<DimHs4Entity> hs4Batch = new ArrayList<>();
//        List<DimCountryEntity> countryBatch = new ArrayList<>();
//
//        Set<Integer> processedHs2 = new HashSet<>();
//        Set<Integer> processedHs4 = new HashSet<>();
//        Set<String> processedCountries = new HashSet<>();
//
//        int offset = 0;
//        int limit = 500000;
//        int batchSize = 500000;
//        List<ExportThEntity> chunk;
//
//        do {
//            chunk = sourceRepo.findAllWithPagination(offset, limit);
//
//            for (ExportThEntity source : chunk) {
//                // HS2 Dimensions
//                if (!processedHs2.contains(source.getHs2dg())) {
//                    DimHs2Entity hs2 = new DimHs2Entity();
//                    hs2.setHs2dg(source.getHs2dg());
//                    hs2.setDescription(source.getDescriptionHs2dg());
//                    hs2Batch.add(hs2);
//                    processedHs2.add(source.getHs2dg());
//
//                    if (hs2Batch.size() >= batchSize) {
//                        hs2Repo.saveAll(hs2Batch);
//                        log.info("Saved batch of {} HS2 dimensions", hs2Batch.size());
//                        hs2Batch.clear();
//                    }
//                }
//
//                // HS4 Dimensions
//                if (!processedHs4.contains(source.getHs4dg())) {
//                    DimHs4Entity hs4 = new DimHs4Entity();
//                    hs4.setHs4dg(source.getHs4dg());
//                    hs4.setDescription(source.getDescriptionHs4dg());
//                    hs4Batch.add(hs4);
//                    processedHs4.add(source.getHs4dg());
//
//                    if (hs4Batch.size() >= batchSize) {
//                        hs4Repo.saveAll(hs4Batch);
//                        log.info("Saved batch of {} HS4 dimensions", hs4Batch.size());
//                        hs4Batch.clear();
//                    }
//                }
//
//                // Country Dimensions
//                if (!processedCountries.contains(source.getCountry())) {
//                    DimCountryEntity country = new DimCountryEntity();
//                    country.setCountry(source.getCountry());
//                    countryBatch.add(country);
//                    processedCountries.add(source.getCountry());
//
//                    if (countryBatch.size() >= batchSize) {
//                        countryRepo.saveAll(countryBatch);
//                        log.info("Saved batch of {} country dimensions", countryBatch.size());
//                        countryBatch.clear();
//                    }
//                }
//            }
//
//            offset += limit;
//            log.info("Processed {} records for dimension extraction", offset);
//
//        } while (!chunk.isEmpty());
//
//        // Save remaining batches
//        if (!hs2Batch.isEmpty()) {
//            hs2Repo.saveAll(hs2Batch);
//            log.info("Saved final batch of {} HS2 dimensions", hs2Batch.size());
//        }
//
//        if (!hs4Batch.isEmpty()) {
//            hs4Repo.saveAll(hs4Batch);
//            log.info("Saved final batch of {} HS4 dimensions", hs4Batch.size());
//        }
//
//        if (!countryBatch.isEmpty()) {
//            countryRepo.saveAll(countryBatch);
//            log.info("Saved final batch of {} country dimensions", countryBatch.size());
//        }
//
//        log.info("Completed dimension extraction and save. Total dimensions: HS2={}, HS4={}, Countries={}",
//                processedHs2.size(), processedHs4.size(), processedCountries.size());
//    }

    private void extractAndSaveDimensions() {
        log.info("Starting dimension extraction");

        // Get existing dimensions first
        Set<Integer> existingHs2 = new HashSet<>(hs2Repo.findAllHs2Codes());
        Set<Integer> existingHs4 = new HashSet<>(hs4Repo.findAllHs4Codes());
        Set<String> existingCountries = new HashSet<>(countryRepo.findAllCountries());

        List<DimHs2Entity> hs2Batch = new ArrayList<>();
        List<DimHs4Entity> hs4Batch = new ArrayList<>();
        List<DimCountryEntity> countryBatch = new ArrayList<>();

        Set<Integer> processedHs2 = new HashSet<>(existingHs2);  // Initialize with existing
        Set<Integer> processedHs4 = new HashSet<>(existingHs4);  // Initialize with existing
        Set<String> processedCountries = new HashSet<>(existingCountries);  // Initialize with existing

        int offset = 0;
        int limit = 500000;
        int batchSize = 500000;
        List<ExportThEntity> chunk;

        try {
            do {
                chunk = sourceRepo.findAllWithPagination(offset, limit);
                int chunkSize = chunk.size();
                if (chunkSize > 0) {
                    log.info("Processing chunk of {} records starting from offset {}", chunkSize, offset);
                }

                for (ExportThEntity source : chunk) {
                    try {
                        // HS2 Dimensions
                        if (source.getHs2dg() != null && !processedHs2.contains(source.getHs2dg())) {
                            DimHs2Entity hs2 = new DimHs2Entity();
                            hs2.setHs2dg(source.getHs2dg());
                            hs2.setDescription(source.getDescriptionHs2dg());
                            hs2Batch.add(hs2);
                            processedHs2.add(source.getHs2dg());

                            if (hs2Batch.size() >= batchSize) {
                                saveHs2Batch(hs2Batch);
                            }
                        }

                        // HS4 Dimensions
                        if (source.getHs4dg() != null && !processedHs4.contains(source.getHs4dg())) {
                            DimHs4Entity hs4 = new DimHs4Entity();
                            hs4.setHs4dg(source.getHs4dg());
                            hs4.setDescription(source.getDescriptionHs4dg());
                            hs4Batch.add(hs4);
                            processedHs4.add(source.getHs4dg());

                            if (hs4Batch.size() >= batchSize) {
                                saveHs4Batch(hs4Batch);
                            }
                        }

                        // Country Dimensions
                        if (source.getCountry() != null && !processedCountries.contains(source.getCountry())) {
                            DimCountryEntity country = new DimCountryEntity();
                            country.setCountry(source.getCountry());
                            countryBatch.add(country);
                            processedCountries.add(source.getCountry());

                            if (countryBatch.size() >= batchSize) {
                                saveCountryBatch(countryBatch);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error processing record: {}", source, e);
                    }
                }

                offset += limit;
                log.info("Processed {} records for dimension extraction", offset);

            } while (!chunk.isEmpty());

            // Save remaining batches
            saveHs2Batch(hs2Batch);
            saveHs4Batch(hs4Batch);
            saveCountryBatch(countryBatch);

            log.info("Completed dimension extraction and save. New dimensions added: HS2={}, HS4={}, Countries={}",
                    processedHs2.size() - existingHs2.size(),
                    processedHs4.size() - existingHs4.size(),
                    processedCountries.size() - existingCountries.size());

        } catch (Exception e) {
            log.error("Error during dimension extraction", e);
            throw new RuntimeException("Failed to extract dimensions", e);
        }
    }

    @Transactional
    protected void saveHs2Batch(List<DimHs2Entity> batch) {
        if (!batch.isEmpty()) {
            try {
                hs2Repo.saveAll(batch);
                log.info("Saved batch of {} HS2 dimensions", batch.size());
                batch.clear();
            } catch (Exception e) {
                log.error("Error saving HS2 batch", e);
                throw e;
            }
        }
    }

    @Transactional
    protected void saveHs4Batch(List<DimHs4Entity> batch) {
        if (!batch.isEmpty()) {
            try {
                hs4Repo.saveAll(batch);
                log.info("Saved batch of {} HS4 dimensions", batch.size());
                batch.clear();
            } catch (Exception e) {
                log.error("Error saving HS4 batch", e);
                throw e;
            }
        }
    }

    @Transactional
    protected void saveCountryBatch(List<DimCountryEntity> batch) {
        if (!batch.isEmpty()) {
            try {
                countryRepo.saveAll(batch);
                log.info("Saved batch of {} country dimensions", batch.size());
                batch.clear();
            } catch (Exception e) {
                log.error("Error saving country batch", e);
                throw e;
            }
        }
    }

    private CompletableFuture<Void> processChunk(int offset, int limit,
                                                 Map<Integer, DimHs2Entity> hs2Map,
                                                 Map<Integer, DimHs4Entity> hs4Map,
                                                 Map<String, DimCountryEntity> countryMap) {
        return CompletableFuture.runAsync(() -> {
            List<ExportThEntity> records = sourceRepo.findAllWithPagination(offset, limit);  // modified method
            List<FactExportThEntity> facts = new ArrayList<>(records.size());

            records.forEach(source -> {
                try {
                    facts.add(mapToFact(source, hs2Map, hs4Map, countryMap));
                } catch (Exception e) {
                    log.error("Error processing record: {}", source, e);
                }
            });

            // Save in batches
            Lists.partition(facts, BATCH_SIZE).forEach(batch -> {
                try {
                    factRepo.saveAll(batch);
                } catch (Exception e) {
                    log.error("Error saving batch at offset {}", offset, e);
                }
            });

            log.info("Processed chunk of {} records at offset {}", records.size(), offset);
        }, executorService);
    }

    private void clearTables() {
        log.info("Starting to clear tables...");

        // Disable foreign key checks
        entityManager.createNativeQuery("SET FOREIGN_KEY_CHECKS = 0").executeUpdate();

        try {
            // Truncate tables for faster deletion
            entityManager.createNativeQuery("TRUNCATE TABLE fact_export_th").executeUpdate();
            entityManager.createNativeQuery("TRUNCATE TABLE dim_hs2").executeUpdate();
            entityManager.createNativeQuery("TRUNCATE TABLE dim_hs4").executeUpdate();
            entityManager.createNativeQuery("TRUNCATE TABLE dim_country").executeUpdate();

            // Force commit
            entityManager.flush();

        } finally {
            // Re-enable foreign key checks
            entityManager.createNativeQuery("SET FOREIGN_KEY_CHECKS = 1").executeUpdate();
        }

        log.info("Tables cleared successfully");
    }

    @PostConstruct
    private void initializeDatabase() {
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Basic settings
                stmt.execute("SET SESSION sql_mode = ''");
                // Performance settings
                stmt.execute("SET SESSION unique_checks = 0");
                stmt.execute("SET SESSION foreign_key_checks = 0");
                stmt.execute("SET SESSION autocommit = 0");
            }
        } catch (SQLException e) {
            log.error("Error configuring database: {}", e.getMessage(), e);
        }
    }

    private FactExportThEntity mapToFact(ExportThEntity source,
                                         Map<Integer, DimHs2Entity> hs2Map,
                                         Map<Integer, DimHs4Entity> hs4Map,
                                         Map<String, DimCountryEntity> countryMap) {
        FactExportThEntity fact = new FactExportThEntity();

        // Get dimension entities
        DimCountryEntity country = countryMap.get(source.getCountry());
        DimHs2Entity hs2 = hs2Map.get(source.getHs2dg());
        DimHs4Entity hs4 = hs4Map.get(source.getHs4dg());

        // Set IDs instead of entity references
        fact.setCountryId(country != null ? country.getCountryId() : null);
        fact.setHs2Id(hs2 != null ? hs2.getHs2Id() : null);
        fact.setHs4Id(hs4 != null ? hs4.getHs4Id() : null);

        // Set other fields
        fact.setThaipValue(parseValue(source.getThaipValue()));
        fact.setDollarValue(parseValue(source.getDollarValue()));
        fact.setSize(source.getSize());
        fact.setMonth(Integer.parseInt(source.getMonth()));
        fact.setYear(Integer.parseInt(source.getYear()));

        return fact;
    }

    private Map<Integer, DimHs2Entity> loadHs2Map() {
        return hs2Repo.findAll().stream()
                .collect(Collectors.toMap(DimHs2Entity::getHs2dg, Function.identity()));
    }

    private Map<Integer, DimHs4Entity> loadHs4Map() {
        return hs4Repo.findAll().stream()
                .collect(Collectors.toMap(DimHs4Entity::getHs4dg, Function.identity()));
    }

    private Map<String, DimCountryEntity> loadCountryMap() {
        return countryRepo.findAll().stream()
                .collect(Collectors.toMap(DimCountryEntity::getCountry, Function.identity()));
    }


    private BigDecimal parseValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return BigDecimal.ZERO;
        }
        String cleaned = value.replaceAll("[^\\d.]", "");
        try {
            return new BigDecimal(cleaned);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    public long getRecordCountForYear(String year) {
        return factRepo.countByYear(Integer.parseInt(year));
    }

    public long getCountryCount() {
        return countryRepo.count();
    }

    public long getHs2Count() {
        return hs2Repo.count();
    }

    public long getHs4Count() {
        return hs4Repo.count();
    }
}