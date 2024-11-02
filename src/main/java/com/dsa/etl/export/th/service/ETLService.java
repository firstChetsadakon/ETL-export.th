package com.dsa.etl.export.th.service;

import com.dsa.etl.export.th.exception.ETLException;
import com.dsa.etl.export.th.model.entities.*;
import com.dsa.etl.export.th.repository.*;
import com.google.common.collect.Lists;
import jakarta.annotation.PostConstruct;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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

    private static final int BATCH_SIZE = 5000; // Increased batch size
    private final Executor executorService;



    public void performETL(String year) {
        log.info("Starting ETL process for year: {}", year);
        StopWatch watch = new StopWatch();
        watch.start();

        try {
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

            // Process chunks in parallel
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

    private CompletableFuture<Void> processChunk(String year, int offset, int limit,
                                                 Map<Integer, DimHs2Entity> hs2Map,
                                                 Map<Integer, DimHs4Entity> hs4Map,
                                                 Map<String, DimCountryEntity> countryMap) {
        return CompletableFuture.runAsync(() -> {
            List<ExportThEntity> records = sourceRepo.findByYearWithPagination(year, offset, limit);
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
                    log.error("Error saving batch", e);
                }
            });

            log.info("Processed chunk of {} records at offset {}", records.size(), offset);
        }, executorService);
    }

    @PostConstruct
    private void initializeDatabase() {
        // Configure database for batch operations
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                // MySQL optimizations
                stmt.execute("SET SESSION sql_mode = ''");
                stmt.execute("SET SESSION bulk_insert_buffer_size = 1048576");
            }
        } catch (SQLException e) {
            log.error("Error configuring database", e);
        }
    }

    private FactExportThEntity mapToFact(ExportThEntity source,
                                         Map<Integer, DimHs2Entity> hs2Map,
                                         Map<Integer, DimHs4Entity> hs4Map,
                                         Map<String, DimCountryEntity> countryMap) {
        return FactExportThEntity.builder()
                .country(countryMap.get(source.getCountry()))
                .hs2(hs2Map.get(source.getHs2dg()))
                .hs4(hs4Map.get(source.getHs4dg()))
                .thaipValue(parseValue(source.getThaipValue()))
                .dollarValue(parseValue(source.getDollarValue()))
                .size(source.getSize())
                .month(Integer.parseInt(source.getMonth()))
                .year(Integer.parseInt(source.getYear()))
                .build();
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

    @Transactional
    protected void processRecords(String year,
                                  Map<Integer, DimHs2Entity> hs2Map,
                                  Map<Integer, DimHs4Entity> hs4Map,
                                  Map<String, DimCountryEntity> countryMap) {
        List<FactExportThEntity> batch = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(0);
        int pageSize = 10000;
        int pageNumber = 0;

        while (true) {
            Page<ExportThEntity> page = sourceRepo.findByYear(year,
                    PageRequest.of(pageNumber, pageSize));

            if (page.isEmpty()) {
                break;
            }

            for (ExportThEntity source : page.getContent()) {
                try {
                    // Process dimensions first
                    processDimensions(source, hs2Map, hs4Map, countryMap);

                    // Create fact record
                    FactExportThEntity fact = mapToFact(source, hs2Map, hs4Map, countryMap);
                    batch.add(fact);

                    if (batch.size() >= BATCH_SIZE) {
                        factRepo.saveAll(batch);
                        log.info("Processed {} records", count.addAndGet(batch.size()));
                        batch.clear();
                    }
                } catch (Exception e) {
                    log.error("Error processing record: {}", source, e);
                }
            }

            pageNumber++;
        }

        // Save remaining batch
        if (!batch.isEmpty()) {
            factRepo.saveAll(batch);
            log.info("Processed final {} records", count.addAndGet(batch.size()));
        }
    }

    private void processDimensions(ExportThEntity source,
                                   Map<Integer, DimHs2Entity> hs2Map,
                                   Map<Integer, DimHs4Entity> hs4Map,
                                   Map<String, DimCountryEntity> countryMap) {
        // Process HS2
        if (!hs2Map.containsKey(source.getHs2dg())) {
            DimHs2Entity hs2 = new DimHs2Entity();
            hs2.setHs2dg(source.getHs2dg());
            hs2.setDescription(source.getDescriptionHs2dg());
            hs2 = hs2Repo.save(hs2);
            hs2Map.put(hs2.getHs2dg(), hs2);
        }

        // Process HS4
        if (!hs4Map.containsKey(source.getHs4dg())) {
            DimHs4Entity hs4 = new DimHs4Entity();
            hs4.setHs4dg(source.getHs4dg());
            hs4.setDescription(source.getDescriptionHs4dg());
            hs4 = hs4Repo.save(hs4);
            hs4Map.put(hs4.getHs4dg(), hs4);
        }

        // Process Country
        if (!countryMap.containsKey(source.getCountry())) {
            DimCountryEntity country = new DimCountryEntity();
            country.setCountry(source.getCountry());
            country = countryRepo.save(country);
            countryMap.put(country.getCountry(), country);
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
