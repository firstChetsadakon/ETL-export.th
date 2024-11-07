package com.dsa.etl.export.th.model.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class FactDetailResponse {
    private Long factId;
    private String country;
    private String hs2Code;
    private String hs2Description;
    private String hs4Code;
    private String hs4Description;
    private BigDecimal thaipValue;
    private BigDecimal dollarValue;
    private String size;
    private Integer month;
    private Integer year;
}
