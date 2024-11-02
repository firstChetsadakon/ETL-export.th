package com.dsa.etl.export.th.service;

import com.dsa.etl.export.th.exception.ETLException;
import com.dsa.etl.export.th.model.entities.DimCountryEntity;
import com.dsa.etl.export.th.model.entities.DimHs2Entity;
import com.dsa.etl.export.th.model.entities.DimHs4Entity;
import com.dsa.etl.export.th.repository.DimCountryRepository;
import com.dsa.etl.export.th.repository.DimHs2Repository;
import com.dsa.etl.export.th.repository.DimHs4Repository;
import com.dsa.etl.export.th.repository.FactExportThRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class ClearTableService {
    //private final SourceDataRepository sourceRepo;
    private final DimHs2Repository hs2Repo;
    private final DimHs4Repository hs4Repo;
    private final DimCountryRepository countryRepo;
    private final FactExportThRepository factRepo;
    private final DataSource dataSource;

    @Transactional
    public void clearAllTables() {
        try {
            log.info("Starting to clear all tables");
            StopWatch watch = new StopWatch();
            watch.start();

            // Disable foreign key checks
            disableForeignKeyChecks();

            // Clear tables
            log.info("Clearing fact table...");
            factRepo.deleteAllInBatch();

            log.info("Clearing dimension tables...");
            hs2Repo.deleteAllInBatch();
            hs4Repo.deleteAllInBatch();
            countryRepo.deleteAllInBatch();

            // Enable foreign key checks
            enableForeignKeyChecks();

            watch.stop();
            log.info("All tables cleared successfully in {} seconds", watch.getTotalTimeSeconds());
        } catch (Exception e) {
            log.error("Error clearing tables", e);
            throw new ETLException("Failed to clear tables: " + e.getMessage());
        }
    }

    @Transactional
    public void clearTablesByYear(String year) {
        try {
            log.info("Starting to clear tables for year: {}", year);
            StopWatch watch = new StopWatch();
            watch.start();

            int yearInt = Integer.parseInt(year);

            // Clear fact table for specific year
            log.info("Clearing fact table data for year {}...", year);
            factRepo.deleteByYear(yearInt);

            // Clear unused dimensions
            log.info("Clearing unused dimensions...");
            clearUnusedDimensions();

            watch.stop();
            log.info("Tables cleared for year {} in {} seconds", year, watch.getTotalTimeSeconds());
        } catch (Exception e) {
            log.error("Error clearing tables for year: {}", year, e);
            throw new ETLException("Failed to clear tables for year " + year + ": " + e.getMessage());
        }
    }

    private void clearUnusedDimensions() {
        // Clear unused HS2 dimensions
        List<DimHs2Entity> unusedHs2 = hs2Repo.findUnusedHs2Dimensions();
        if (!unusedHs2.isEmpty()) {
            hs2Repo.deleteAll(unusedHs2);
            log.info("Cleared {} unused HS2 dimensions", unusedHs2.size());
        }

        // Clear unused HS4 dimensions
        List<DimHs4Entity> unusedHs4 = hs4Repo.findUnusedHs4Dimensions();
        if (!unusedHs4.isEmpty()) {
            hs4Repo.deleteAll(unusedHs4);
            log.info("Cleared {} unused HS4 dimensions", unusedHs4.size());
        }

        // Clear unused country dimensions
        List<DimCountryEntity> unusedCountries = countryRepo.findUnusedCountryDimensions();
        if (!unusedCountries.isEmpty()) {
            countryRepo.deleteAll(unusedCountries);
            log.info("Cleared {} unused country dimensions", unusedCountries.size());
        }
    }

    private void disableForeignKeyChecks() {
        executeSql("SET FOREIGN_KEY_CHECKS = 0");
    }

    private void enableForeignKeyChecks() {
        executeSql("SET FOREIGN_KEY_CHECKS = 1");
    }

    private void executeSql(String sql) {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            log.error("Error executing SQL: {}", sql, e);
            throw new ETLException("Failed to execute SQL: " + e.getMessage());
        }
    }

    // Additional utility methods
    public long getTotalRecords() {
        return factRepo.count();
    }

    public Map<String, Long> getTableCounts() {
        return Map.of(
                "fact_table", factRepo.count(),
                "hs2_dimensions", hs2Repo.count(),
                "hs4_dimensions", hs4Repo.count(),
                "country_dimensions", countryRepo.count()
        );
    }
}
