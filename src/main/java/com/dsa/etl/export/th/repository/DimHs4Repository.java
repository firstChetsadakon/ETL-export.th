package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.entities.DimHs4Entity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DimHs4Repository extends JpaRepository<DimHs4Entity, Long> {
    Optional<DimHs4Entity> findByHs4dg(Integer hs4dg);
    boolean existsByHs4dg(Integer hs4dg);
}
