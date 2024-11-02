package com.dsa.etl.export.th.model.entities;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "dim_hs4")
@Data
public class DimHs4Entity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "hs4_id")  // Make sure this matches your database column name
    private Long hs4Id;

    @Column(name = "hs4dg")   // Column for the HS4 code
    private Integer hs4dg;

    @Column(name = "description")
    private String description;
}
