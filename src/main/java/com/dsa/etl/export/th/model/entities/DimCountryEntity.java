package com.dsa.etl.export.th.model.entities;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "dim_country")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DimCountryEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "country_id")
    private Long countryId;

    @Column(unique = true)
    private String country;
}