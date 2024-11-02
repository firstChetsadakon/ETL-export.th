package com.dsa.etl.export.th.repository;

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
    long countByYear(Integer year);

    Page<FactExportThEntity> findByYear(Integer year, Pageable pageable);
    Page<FactExportThEntity> findByYearAndMonth(Integer year, Integer month, Pageable pageable);
//    Page<FactExportThEntity> findByYearAndMonthAndCountry_Country(Integer year, Integer month, String country, Pageable pageable);
    long countByYearAndMonth(Integer year, Integer month);

    @Query("SELECT SUM(f.thaipValue) FROM FactExportThEntity f WHERE f.year = :year")
    BigDecimal sumThaipValueByYear(@Param("year") Integer year);

    @Query("SELECT SUM(f.dollarValue) FROM FactExportThEntity f WHERE f.year = :year")
    BigDecimal sumDollarValueByYear(@Param("year") Integer year);

    @Query("SELECT SUM(f.thaipValue) FROM FactExportThEntity f WHERE f.year = :year AND f.month = :month")
    BigDecimal sumThaipValueByYearAndMonth(@Param("year") Integer year, @Param("month") Integer month);

    @Query("SELECT SUM(f.dollarValue) FROM FactExportThEntity f WHERE f.year = :year AND f.month = :month")
    BigDecimal sumDollarValueByYearAndMonth(@Param("year") Integer year, @Param("month") Integer month);

//    @Query("SELECT new map(" +
//            "f.country.country as country, " +
//            "SUM(f.thaipValue) as totalThaipValue, " +
//            "SUM(f.dollarValue) as totalDollarValue, " +
//            "COUNT(f) as recordCount) " +
//            "FROM FactExportThEntity f " +
//            "WHERE f.year = :year " +
//            "GROUP BY f.country.country")
//    List<Map<String, Object>> findSummaryByCountry(@Param("year") Integer year);
//
//    @Query("SELECT new map(" +
//            "f.hs2.hs2dg as hs2Code, " +
//            "f.hs2.description as description, " +
//            "SUM(f.thaipValue) as totalThaipValue, " +
//            "SUM(f.dollarValue) as totalDollarValue, " +
//            "COUNT(f) as recordCount) " +
//            "FROM FactExportThEntity f " +
//            "WHERE f.year = :year " +
//            "GROUP BY f.hs2.hs2dg, f.hs2.description")
//    List<Map<String, Object>> findSummaryByHs2(@Param("year") Integer year);
}
