package com.dsa.etl.export.th.repository;

import com.dsa.etl.export.th.model.entities.ExportThEntity;
import com.dsa.etl.export.th.model.entities.ExportThId;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.stream.Stream;

import static org.hibernate.jpa.HibernateHints.HINT_CACHEABLE;
import static org.hibernate.jpa.HibernateHints.HINT_FETCH_SIZE;

@Repository
public interface ExportThRepository extends JpaRepository<ExportThEntity, ExportThId> {

    @Query("SELECT e FROM ExportThEntity e WHERE e.year = :year")
    Stream<ExportThEntity> streamAllByYear(String year);

    @Query("SELECT DISTINCT e.year FROM ExportThEntity e ORDER BY e.year")
    List<String> findDistinctYears();

//    List<ExportThEntity> findByYear(String year);
//    long countByYear(String year);

    Page<ExportThEntity> findByYear(String year, Pageable pageable);


    @Query(value = "SELECT * FROM export_th WHERE year = :year LIMIT :limit OFFSET :offset",
            nativeQuery = true)
    List<ExportThEntity> findByYearWithPagination(@Param("year") String year,
                                                  @Param("offset") int offset,
                                                  @Param("limit") int limit);

    @Query(value = "SELECT COUNT(*) FROM export_th WHERE year = :year",
            nativeQuery = true)
    long countByYear(@Param("year") String year);
}


