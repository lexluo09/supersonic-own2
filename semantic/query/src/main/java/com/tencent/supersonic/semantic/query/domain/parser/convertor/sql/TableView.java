package com.tencent.supersonic.semantic.query.domain.parser.convertor.sql;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

@Data
public class TableView {

    private List<SqlNode> filter = new ArrayList<>();
    private List<SqlNode> dimension = new ArrayList<>();
    private List<SqlNode> measure = new ArrayList<>();
    private SqlNodeList order;
    private SqlNode fetch;
    private SqlNode offset;
    private SqlNode table;

    private String alias;
    private List<String> primary;

    public SqlNode build() {
        measure.addAll(dimension);
        SqlNodeList dimensionNodeList = null;
        if (dimension.size() > 0) {
            dimensionNodeList = new SqlNodeList(dimension, SqlParserPos.ZERO);
        }
        SqlNodeList filterNodeList = null;
        if (filter.size() > 0) {
            filterNodeList = new SqlNodeList(filter, SqlParserPos.ZERO);
        }
        return new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(measure, SqlParserPos.ZERO), table,
                filterNodeList, dimensionNodeList, null, null, null, order, offset, fetch, null);
    }

}
