package com.dsa.etl.export.th.model.entities;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "dim_hs2")
@Data
public class DimHs2Entity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "hs2_id")  // Make sure this matches your database column name
    private Long hs2Id;

    @Column(name = "hs2dg")   // Column for the HS2 code
    private Integer hs2dg;

    @Column(name = "description")
    private String description;
}

