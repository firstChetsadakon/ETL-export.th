package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.entities.FactExportThEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FactExportThRepository extends JpaRepository<FactExportThEntity, Long> {
    void deleteByYear(Integer year);
    long countByYear(Integer year);
}
