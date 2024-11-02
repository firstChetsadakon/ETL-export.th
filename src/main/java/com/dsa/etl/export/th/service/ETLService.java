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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class ETLService {
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



    public void performETL(String year) {
//        clearTables();
        //clearTableService.clearAllTables();
        log.info("Starting ETL process for year: {}", year);
        StopWatch watch = new StopWatch();
        watch.start();

        try {
            clearTables();
            // First extract and save dimensions from source data
            extractAndSaveDimensions(year);

            // Clear existing data
            factRepo.deleteByYear(Integer.parseInt(year));

            // Load all dimension data into memory
            Map<Integer, DimHs2Entity> hs2Map = loadHs2Map();
            Map<Integer, DimHs4Entity> hs4Map = loadHs4Map();
            Map<String, DimCountryEntity> countryMap = loadCountryMap();

            // Get total count and calculate chunks
            long totalRecords = sourceRepo.countByYear(year);
            int chunkSize = 50000; // Process 50k records per chunk
            int totalChunks = (int) Math.ceil((double) totalRecords / chunkSize);
            int actualChunks = Math.min(totalChunks, MAX_CONCURRENT_CHUNKS);
            log.info("Processing {} total records in {} chunks", totalRecords, totalChunks);//
            // Process chunks in parallel
//            List<CompletableFuture<Void>> futures = new ArrayList<>();
//            for (int i = 0; i < totalChunks; i++) {
//                int offset = i * chunkSize;
//                futures.add(processChunk(year, offset, chunkSize, hs2Map, hs4Map, countryMap));
//            }
            //works before loop chunk
//            List<CompletableFuture<Void>> futures = new ArrayList<>();
//            for (int i = 0; i < actualChunks; i++) {
//                int offset =(int) (i * (totalRecords / actualChunks));
//                int limit = (i == actualChunks - 1) ?
//                        (int) (totalRecords - offset) :
//                        (int) (totalRecords / actualChunks);
//                futures.add(processChunk(year, offset, limit, hs2Map, hs4Map, countryMap));
//            }
//            TODO แก้เรื่อง sql ของ fact เพื่อดึงชื่อ ประเทศ กับ hs2 hs4
//            TODO ETL ALL Table 3M record
//            TODO now แก้เรื่อง log loop
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < totalChunks; i++) {
                int offset = i * chunkSize;
                futures.add(processChunk(year, offset, chunkSize, hs2Map, hs4Map, countryMap));
            }

            // Wait for all chunks to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        } catch (Exception e) {
            log.error("ETL process failed for year: {}", year, e);
            throw new ETLException("ETL process failed: " + e.getMessage());
        } finally {
            watch.stop();
            log.info("ETL process completed in {} seconds", watch.getTotalTimeSeconds());
        }
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

    private void extractAndSaveDimensions(String year) {
        log.info("Starting dimension extraction for year: {}", year);

        Set<DimHs2Entity> hs2Dimensions = new HashSet<>();
        Set<DimHs4Entity> hs4Dimensions = new HashSet<>();
        Set<DimCountryEntity> countryDimensions = new HashSet<>();

        try (Stream<ExportThEntity> stream = sourceRepo.streamAllByYear(year)) {
            stream.forEach(source -> {
                // Extract HS2 Dimensions
                DimHs2Entity hs2 = new DimHs2Entity();
                hs2.setHs2dg(source.getHs2dg());
                hs2.setDescription(source.getDescriptionHs2dg());
                hs2Dimensions.add(hs2);

                // Extract HS4 Dimensions
                DimHs4Entity hs4 = new DimHs4Entity();
                hs4.setHs4dg(source.getHs4dg());
                hs4.setDescription(source.getDescriptionHs4dg());
                hs4Dimensions.add(hs4);

                // Extract Country Dimensions
                DimCountryEntity country = new DimCountryEntity();
                country.setCountry(source.getCountry());
                countryDimensions.add(country);
            });
        }

        // Save dimensions to tables
        log.info("Saving {} unique HS2 dimensions", hs2Dimensions.size());
        hs2Repo.saveAll(hs2Dimensions);

        log.info("Saving {} unique HS4 dimensions", hs4Dimensions.size());
        hs4Repo.saveAll(hs4Dimensions);

        log.info("Saving {} unique country dimensions", countryDimensions.size());
        countryRepo.saveAll(countryDimensions);

        log.info("Completed dimension extraction and save");
    }

    private CompletableFuture<Void> processChunk(String year, int offset, int limit,
                                                 Map<Integer, DimHs2Entity> hs2Map,
                                                 Map<Integer, DimHs4Entity> hs4Map,
                                                 Map<String, DimCountryEntity> countryMap) {
        return CompletableFuture.runAsync(() -> {
            log.info("Starting to process chunk at offset {} with limit {}", offset, limit);//
            List<ExportThEntity> records = sourceRepo.findByYearWithPagination(year, offset, limit);

            List<List<ExportThEntity>> batches = Lists.partition(records, BATCH_SIZE);//

            log.info("Processing {} records in {} batches for offset {}",
                    records.size(), batches.size(), offset);//

//            // Process in smaller sub-batches
//            for (List<ExportThEntity> batch : Lists.partition(records, BATCH_SIZE)) {
//                try {
//                    List<FactExportThEntity> facts = batch.stream()
//                            .map(source -> mapToFact(source, hs2Map, hs4Map, countryMap))
//                            .collect(Collectors.toList());
//
//                    // Retry logic for saving
//                    retry(() -> factRepo.saveAll(facts), 3);
//
//
//                } catch (Exception e) {
//                    log.error("Error processing batch at offset {}: {}", offset, e.getMessage());
//                }
//            }

            // Process each batch once
            for (int i = 0; i < batches.size(); i++) {
                List<ExportThEntity> batch = batches.get(i);
                try {
                    List<FactExportThEntity> facts = batch.stream()
                            .map(source -> mapToFact(source, hs2Map, hs4Map, countryMap))
                            .collect(Collectors.toList());

                    factRepo.saveAll(facts);
                    log.info("Processed batch {}/{} ({} records) at offset {}",
                            i + 1, batches.size(), batch.size(), offset);
                } catch (Exception e) {
                    log.error("Error processing batch {}/{} at offset {}: {}",
                            i + 1, batches.size(), offset, e.getMessage());
                }
            }

            log.info("Completed processing chunk at offset {}", offset);
        }, executorService);
    }

    private <T> T retry(Supplier<T> operation, int maxAttempts) {
        int attempt = 0;
        while (attempt < maxAttempts) {
            try {
                return operation.get();
            } catch (Exception e) {
                attempt++;
                if (attempt == maxAttempts) {
                    throw e;
                }
                try {
                    Thread.sleep(1000 * attempt); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
                log.warn("Retry attempt {} after error: {}", attempt, e.getMessage());
            }
        }
        throw new RuntimeException("Failed after " + maxAttempts + " attempts");
    }

//    private CompletableFuture<Void> processChunk(String year, int offset, int limit,
//                                                 Map<Integer, DimHs2Entity> hs2Map,
//                                                 Map<Integer, DimHs4Entity> hs4Map,
//                                                 Map<String, DimCountryEntity> countryMap) {
//        return CompletableFuture.runAsync(() -> {
//            List<ExportThEntity> records = sourceRepo.findByYearWithPagination(year, offset, limit);
//            List<FactExportThEntity> facts = new ArrayList<>(records.size());
//
//            records.forEach(source -> {
//                try {
//                    facts.add(mapToFact(source, hs2Map, hs4Map, countryMap));
//                } catch (Exception e) {
//                    log.error("Error processing record: {}", source, e);
//                }
//            });
//
//            // Save in batches
//            Lists.partition(facts, BATCH_SIZE).forEach(batch -> {
//                try {
//                    factRepo.saveAll(batch);
//                } catch (Exception e) {
//                    log.error("Error saving batch", e);
//                }
//            });
//
//            log.info("Processed chunk of {} records at offset {}", records.size(), offset);
//        }, executorService);
//    }

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





    //---------------------------------------/-/--

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

//    @Transactional
//    protected void processRecords(String year,
//                                  Map<Integer, DimHs2Entity> hs2Map,
//                                  Map<Integer, DimHs4Entity> hs4Map,
//                                  Map<String, DimCountryEntity> countryMap) {
//        List<FactExportThEntity> batch = new ArrayList<>();
//        AtomicInteger count = new AtomicInteger(0);
//        int pageSize = 10000;
//        int pageNumber = 0;
//
//        while (true) {
//            Page<ExportThEntity> page = sourceRepo.findByYear(year,
//                    PageRequest.of(pageNumber, pageSize));
//
//            if (page.isEmpty()) {
//                break;
//            }
//
//            for (ExportThEntity source : page.getContent()) {
//                try {
//                    // Process dimensions first
//                    processDimensions(source, hs2Map, hs4Map, countryMap);
//
//                    // Create fact record
//                    FactExportThEntity fact = mapToFact(source, hs2Map, hs4Map, countryMap);
//                    batch.add(fact);
//
//                    if (batch.size() >= BATCH_SIZE) {
//                        factRepo.saveAll(batch);
//                        log.info("Processed {} records", count.addAndGet(batch.size()));
//                        batch.clear();
//                    }
//                } catch (Exception e) {
//                    log.error("Error processing record: {}", source, e);
//                }
//            }
//
//            pageNumber++;
//        }
//
//        // Save remaining batch
//        if (!batch.isEmpty()) {
//            factRepo.saveAll(batch);
//            log.info("Processed final {} records", count.addAndGet(batch.size()));
//        }
//    }
//
//    private void processDimensions(ExportThEntity source,
//                                   Map<Integer, DimHs2Entity> hs2Map,
//                                   Map<Integer, DimHs4Entity> hs4Map,
//                                   Map<String, DimCountryEntity> countryMap) {
//        // Process HS2
//        if (!hs2Map.containsKey(source.getHs2dg())) {
//            DimHs2Entity hs2 = new DimHs2Entity();
//            hs2.setHs2dg(source.getHs2dg());
//            hs2.setDescription(source.getDescriptionHs2dg());
//            hs2 = hs2Repo.save(hs2);
//            hs2Map.put(hs2.getHs2dg(), hs2);
//        }
//
//        // Process HS4
//        if (!hs4Map.containsKey(source.getHs4dg())) {
//            DimHs4Entity hs4 = new DimHs4Entity();
//            hs4.setHs4dg(source.getHs4dg());
//            hs4.setDescription(source.getDescriptionHs4dg());
//            hs4 = hs4Repo.save(hs4);
//            hs4Map.put(hs4.getHs4dg(), hs4);
//        }
//
//        // Process Country
//        if (!countryMap.containsKey(source.getCountry())) {
//            DimCountryEntity country = new DimCountryEntity();
//            country.setCountry(source.getCountry());
//            country = countryRepo.save(country);
//            countryMap.put(country.getCountry(), country);
//        }
//    }


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
