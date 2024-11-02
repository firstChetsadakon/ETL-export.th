package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.entities.DimCountryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DimCountryRepository extends JpaRepository<DimCountryEntity, Long> {
    Optional<DimCountryEntity> findByCountry(String country);
    boolean existsByCountry(String country);
}
