package com.dsa.etl.export.th.model.dto;

import java.math.BigDecimal;

public interface FactSummaryProjection {
    String getCountry();
    String getHs2Category();
    BigDecimal getTotalThaipValue();
    BigDecimal getTotalDollarValue();
    Long getRecordCount();
}
