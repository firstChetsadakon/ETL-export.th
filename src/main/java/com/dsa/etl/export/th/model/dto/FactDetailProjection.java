package com.dsa.etl.export.th.model.dto;

import java.math.BigDecimal;

public interface FactDetailProjection {
    Long getFactId();
    String getCountryName();
    Integer getHs2Code();
    String getHs2Description();
    Integer getHs4Code();
    String getHs4Description();
    BigDecimal getThaipValue();
    BigDecimal getDollarValue();
    String getSize();
    Integer getMonth();
    Integer getYear();
}
