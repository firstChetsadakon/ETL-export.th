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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private static final int BATCH_SIZE = 5000; // Increased batch size
    private final Executor executorService;
    private final EntityManager entityManager;

    public void performETL() {  // removed year parameter
        log.info("Starting ETL process for all records");
        StopWatch watch = new StopWatch();
        watch.start();

        try {
            // Clear tables
            clearTables();

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
