package com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node;

import lombok.Data;
import org.apache.calcite.sql.SqlNode;

@Data
public class JoinNode extends SemanticNode {

    private SqlNode join;
    private SqlNode on;
    private SqlNode left;
    private SqlNode right;
}
