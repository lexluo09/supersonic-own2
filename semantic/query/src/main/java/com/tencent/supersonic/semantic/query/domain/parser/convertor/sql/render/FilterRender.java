package com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.render;


import com.tencent.supersonic.semantic.api.query.request.MetricReq;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.Renderer;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.TableView;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.MetricNode;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.SemanticNode;
import com.tencent.supersonic.semantic.query.domain.parser.dsl.DataSource;
import com.tencent.supersonic.semantic.query.domain.parser.dsl.Metric;
import com.tencent.supersonic.semantic.query.domain.parser.schema.SemanticSchema;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class FilterRender extends Renderer {

    @Override
    public void render(MetricReq metricCommand, List<DataSource> dataSources, SqlValidatorScope scope,
            SemanticSchema schema, boolean nonAgg) throws Exception {
        TableView tableView = super.tableView;
        for (String dimension : metricCommand.getDimensions()) {
            tableView.getMeasure().add(SemanticNode.parse(dimension, scope));
        }
        for (String metric : metricCommand.getMetrics()) {
            Optional<Metric> optionalMetric = Renderer.getMetricByName(metric, schema);
            if (optionalMetric.isPresent()) {
                tableView.getMeasure().add(MetricNode.build(optionalMetric.get(), scope));
            } else {
                tableView.getMeasure().add(SemanticNode.parse(metric, scope));
            }
        }
    }
}
