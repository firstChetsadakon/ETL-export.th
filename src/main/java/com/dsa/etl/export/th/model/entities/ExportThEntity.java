package com.dsa.etl.export.th.model.entities;

import jakarta.persistence.*;
import lombok.*;


@Table(name = "export_th", schema = "export_th")
@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportThEntity {
    @Id
    private Long id;

    @Column(insertable=false, updatable=false)
    private String country;

    @Column(insertable=false, updatable=false)
    private Integer hs2dg;

    @Column(name = "description_hs2dg")
    private String descriptionHs2dg;

    @Column(insertable=false, updatable=false)
    private Integer hs4dg;

    @Column(name = "description_hs4dg", insertable=false, updatable=false)
    private String descriptionHs4dg;

    @Column(name = "thaip_value", insertable=false, updatable=false)
    private String thaipValue;

    @Column(name = "dollar_value", insertable=false, updatable=false)
    private String dollarValue;

    @Column(insertable=false, updatable=false)
    private String size;

    @Column(insertable=false, updatable=false)
    private String month;

    @Column(insertable=false, updatable=false)
    private String year;
}

//@Entity
//@Table(name = "export_th", schema = "export_th")
//@Data
//public class ExportThEntity {
//    @Id
//    private Long id;
//    private String country;
//    private Integer hs2dg;
//    @Column(name = "description_hs2dg")
//    private String descriptionHs2dg;
//    private Integer hs4dg;
//    @Column(name = "description_hs4dg")
//    private String descriptionHs4dg;
//    @Column(name = "thaip_value")
//    private String thaipValue;
//    @Column(name = "dollar_value")
//    private String dollarValue;
//    private String size;
//    private String month;
//    private String year;
//}