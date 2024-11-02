package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.entities.DimHs2Entity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DimHs2Repository extends JpaRepository<DimHs2Entity, Long> {
    Optional<DimHs2Entity> findByHs2dg(Integer hs2dg);
    boolean existsByHs2dg(Integer hs2dg);
}
