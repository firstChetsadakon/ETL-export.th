package com.dsa.etl.export.th.model.entities;

import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportThId implements Serializable {
    private String country;
    private Integer hs2dg;
    private Integer hs4dg;
    private String month;
    private String year;
}
