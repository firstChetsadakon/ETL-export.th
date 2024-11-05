package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.entities.DimHs2Entity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DimHs2Repository extends JpaRepository<DimHs2Entity, Long> {
    Optional<DimHs2Entity> findByHs2dg(Integer hs2dg);

    boolean existsByHs2dg(Integer hs2dg);

    @Query("SELECT h FROM DimHs2Entity h WHERE h.hs2Id NOT IN (SELECT DISTINCT f.hs2Id FROM FactExportThEntity f)")
    List<DimHs2Entity> findUnusedHs2Dimensions();

    @Query("SELECT d.hs2dg FROM DimHs2Entity d")
    List<Integer> findAllHs2Codes();
}
