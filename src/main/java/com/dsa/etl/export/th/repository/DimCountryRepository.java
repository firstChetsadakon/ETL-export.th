package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.entities.DimCountryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DimCountryRepository extends JpaRepository<DimCountryEntity, Long> {
    Optional<DimCountryEntity> findByCountry(String country);

    boolean existsByCountry(String country);

    @Query("SELECT c FROM DimCountryEntity c WHERE c.countryId NOT IN (SELECT DISTINCT f.countryId FROM FactExportThEntity f)")
    List<DimCountryEntity> findUnusedCountryDimensions();

    @Query("SELECT d.country FROM DimCountryEntity d")
    List<String> findAllCountries();
}
