package com.dsa.etl.export.th.model.entities;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Table(name = "fact_export_th")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FactExportThEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "country_id")
    private Long countryId;

    @Column(name = "hs2_id")
    private Long hs2Id;

    @Column(name = "hs4_id")
    private Long hs4Id;

    @Column(name = "thaip_value", precision = 20, scale = 2)
    private BigDecimal thaipValue;

    @Column(name = "dollar_value", precision = 20, scale = 2)
    private BigDecimal dollarValue;

    private String size;
    private Integer month;
    private Integer year;
}

