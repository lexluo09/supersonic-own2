package com.tencent.supersonic.semantic.api.core.request;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class DateInfoReq {

    private String type;
    private Long itemId;
    private String dateFormat;
    private String startDate;
    private String endDate;
    private List<String> unavailableDateList = new ArrayList<>();

    public DateInfoReq(String type, Long itemId, String dateFormat, String startDate, String endDate) {
        this.type = type;
        this.itemId = itemId;
        this.dateFormat = dateFormat;
        this.startDate = startDate;
        this.endDate = endDate;
    }
}