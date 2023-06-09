package com.tencent.supersonic.semantic.api.core.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimensionTimeTypeParams {

    private String isPrimary;

    private String timeGranularity;

}
