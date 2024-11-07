package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.dto.FactDetailProjection;
import com.dsa.etl.export.th.model.dto.FactSummaryProjection;
import com.dsa.etl.export.th.model.entities.FactExportThEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Repository
public interface FactExportThRepository extends JpaRepository<FactExportThEntity, Long> {


    void deleteByYear(Integer year);

    // Count queries
    long countByYear(Integer year);

    long countByYearAndMonth(Integer year, Integer month);

    // Basic queries
    Page<FactExportThEntity> findByYear(Integer year, Pageable pageable);

    Page<FactExportThEntity> findByYearAndMonth(Integer year, Integer month, Pageable pageable);

    Page<FactExportThEntity> findByYearAndMonthAndCountryId(Integer year, Integer month, Long countryId, Pageable pageable);

    // Sum queries
    @Query("SELECT SUM(f.thaipValue) FROM FactExportThEntity f WHERE f.year = :year")
    BigDecimal sumThaipValueByYear(@Param("year") Integer year);

    @Query("SELECT SUM(f.dollarValue) FROM FactExportThEntity f WHERE f.year = :year")
    BigDecimal sumDollarValueByYear(@Param("year") Integer year);

    @Query("SELECT SUM(f.thaipValue) FROM FactExportThEntity f WHERE f.year = :year AND f.month = :month")
    BigDecimal sumThaipValueByYearAndMonth(@Param("year") Integer year, @Param("month") Integer month);

    @Query("SELECT SUM(f.dollarValue) FROM FactExportThEntity f WHERE f.year = :year AND f.month = :month")
    BigDecimal sumDollarValueByYearAndMonth(@Param("year") Integer year, @Param("month") Integer month);

    // Top countries query
    @Query("""
        SELECT c.country as country, 
               SUM(f.thaipValue) as totalValue,
               COUNT(f) as recordCount 
        FROM FactExportThEntity f 
        JOIN DimCountryEntity c ON f.countryId = c.countryId 
        WHERE f.year = :year 
        GROUP BY c.country 
        ORDER BY totalValue DESC 
        LIMIT :limit
    """)
    List<Map<String, Object>> findTopCountriesByValue(@Param("year") Integer year, @Param("limit") int limit);

    // Top HS2 categories query
    @Query("""
        SELECT h.description as category, 
               SUM(f.thaipValue) as totalValue,
               COUNT(f) as recordCount 
        FROM FactExportThEntity f 
        JOIN DimHs2Entity h ON f.hs2Id = h.hs2Id 
        WHERE f.year = :year 
        GROUP BY h.description 
        ORDER BY totalValue DESC 
        LIMIT :limit
    """)
    List<Map<String, Object>> findTopHS2ByValue(@Param("year") Integer year, @Param("limit") int limit);

    @Query("""
        SELECT 
            f.id as factId,
            c.country as countryName,
            h2.hs2dg as hs2Code,
            h2.description as hs2Description,
            h4.hs4dg as hs4Code,
            h4.description as hs4Description,
            f.thaipValue as thaipValue,
            f.dollarValue as dollarValue,
            f.size as size,
            f.month as month,
            f.year as year
        FROM FactExportThEntity f
        LEFT JOIN DimCountryEntity c ON f.countryId = c.countryId
        LEFT JOIN DimHs2Entity h2 ON f.hs2Id = h2.hs2Id
        LEFT JOIN DimHs4Entity h4 ON f.hs4Id = h4.hs4Id
        WHERE (:year IS NULL OR f.year = :year)
        AND (:month IS NULL OR f.month = :month)
    """)
    Page<FactDetailProjection> findFactsWithDetails(
            @Param("year") Integer year,
            @Param("month") Integer month,
            Pageable pageable
    );

    // Get summary with dimension details
    @Query("""
        SELECT 
            c.country as country,
            h2.description as hs2Category,
            SUM(f.thaipValue) as totalThaipValue,
            SUM(f.dollarValue) as totalDollarValue,
            COUNT(f) as recordCount
        FROM FactExportThEntity f
        LEFT JOIN DimCountryEntity c ON f.countryId = c.countryId
        LEFT JOIN DimHs2Entity h2 ON f.hs2Id = h2.hs2Id
        WHERE f.year = :year
        GROUP BY c.country, h2.description
        ORDER BY totalThaipValue DESC
        LIMIT :limit
    """)
    List<FactSummaryProjection> findFactSummaryByYear(
            @Param("year") Integer year,
            @Param("limit") int limit
    );

    @Query("""
        SELECT 
            c.country as country,
            h2.description as hs2Category,
            SUM(f.thaipValue) as totalThaipValue,
            SUM(f.dollarValue) as totalDollarValue,
            COUNT(f) as recordCount
        FROM FactExportThEntity f
        LEFT JOIN DimCountryEntity c ON f.countryId = c.countryId
        LEFT JOIN DimHs2Entity h2 ON f.hs2Id = h2.hs2Id
        WHERE f.year = :year AND f.month = :month
        GROUP BY c.country, h2.description
        ORDER BY totalThaipValue DESC
        LIMIT :limit
    """)
    List<FactSummaryProjection> findFactSummaryByYearAndMonth(
            @Param("year") Integer year,
            @Param("month") Integer month,
            @Param("limit") int limit
    );
}
