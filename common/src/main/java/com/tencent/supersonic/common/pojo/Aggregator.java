package com.tencent.supersonic.common.pojo;

import com.tencent.supersonic.common.enums.AggOperatorEnum;

import java.util.List;
import javax.validation.constraints.NotBlank;

import lombok.Data;

@Data
public class Aggregator {

    @NotBlank(message = "Invalid aggregator column")
    private String column;

    private AggOperatorEnum func = AggOperatorEnum.SUM;

    private String nameCh;

    private List<String> args;

    public Aggregator() {
    }

    public Aggregator(String column, AggOperatorEnum func) {
        this.column = column;
        this.func = func;
    }

    public Aggregator(String column, AggOperatorEnum func, List<String> args) {
        this.column = column;
        this.func = func;
        this.args = args;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"column\":\"")
                .append(column).append('\"');
        sb.append(",\"func\":")
                .append(func);
        sb.append(",\"nameCh\":\"")
                .append(nameCh).append('\"');
        sb.append(",\"args\":")
                .append(args);
        sb.append('}');
        return sb.toString();
    }
}
