package com.tencent.supersonic.semantic.api.core.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Measure {


    private String name;

    private String agg;

    private String expr;

    private String constraint;

    private String alias;

    private String create_metric;

    private String bizName;

    private Integer isCreateMetric = 0;

    private Long datasourceId;


}
