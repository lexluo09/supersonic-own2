package com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.render;


import com.tencent.supersonic.semantic.api.query.request.MetricReq;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.Renderer;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.TableView;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.DataSourceNode;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.DimensionNode;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.FilterNode;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.IdentifyNode;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.MetricNode;
import com.tencent.supersonic.semantic.query.domain.parser.convertor.sql.node.SemanticNode;
import com.tencent.supersonic.semantic.query.domain.parser.dsl.Constants;
import com.tencent.supersonic.semantic.query.domain.parser.dsl.DataSource;
import com.tencent.supersonic.semantic.query.domain.parser.dsl.Dimension;
import com.tencent.supersonic.semantic.query.domain.parser.dsl.Identify;
import com.tencent.supersonic.semantic.query.domain.parser.schema.SemanticSchema;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.springframework.util.CollectionUtils;

@Slf4j
public class SourceRender extends Renderer {

    public void render(MetricReq metricCommand, List<DataSource> dataSources, SqlValidatorScope scope,
            SemanticSchema schema, boolean nonAgg) throws Exception {
        String queryWhere = metricCommand.getWhere();
        Set<String> whereFields = new HashSet<>();
        List<String> fieldWhere = new ArrayList<>();
        if (queryWhere != null && !queryWhere.isEmpty()) {
            SqlNode sqlNode = SemanticNode.parse(queryWhere, scope);
            FilterNode.getFilterField(sqlNode, whereFields);
            fieldWhere = whereFields.stream().collect(Collectors.toList());
        }
        if (dataSources.size() == 1) {
            DataSource dataSource = dataSources.get(0);
            super.tableView = renderOne(true, "", fieldWhere, metricCommand.getMetrics(), metricCommand.getDimensions(),
                    metricCommand.getWhere(), dataSource, scope, schema, nonAgg);
            return;
        }
        JoinRender joinRender = new JoinRender();
        joinRender.render(metricCommand, dataSources, scope, schema, nonAgg);
        super.tableView = joinRender.getTableView();
    }


    public static TableView renderOne(boolean addWhere, String alias, List<String> fieldWhere,
            List<String> queryMetrics, List<String> queryDimensions, String queryWhere, DataSource datasource,
            SqlValidatorScope scope, SemanticSchema schema, boolean nonAgg) throws Exception {

        TableView dataSet = new TableView();
        TableView output = new TableView();
        if (!fieldWhere.isEmpty()) {
            boolean whereHasMetric = isWhereHasMetric(fieldWhere, datasource);
            if (whereHasMetric) {
                //output.getFilter().add(sqlNode) ;
            } else {
                SqlNode sqlNode = SemanticNode.parse(queryWhere, scope);
                if (addWhere) {
                    output.getFilter().add(sqlNode);
                }
                mergeWhere(fieldWhere, dataSet, queryMetrics, queryDimensions, datasource, scope, schema, nonAgg);
            }
        }

        for (String metric : queryMetrics) {
            MetricNode metricNode = buildMetricNode(metric, datasource, scope, schema, nonAgg, alias);
            if (!metricNode.getAggNode().isEmpty()) {
                metricNode.getAggNode().entrySet().stream().forEach(m -> output.getMeasure().add(m.getValue()));
            }
            if (metricNode.getNonAggNode() != null) {
                metricNode.getNonAggNode().entrySet().stream().forEach(m -> dataSet.getMeasure().add(m.getValue()));
            }
            if (metricNode.getMeasureFilter() != null) {
                metricNode.getMeasureFilter().entrySet().stream().forEach(m -> dataSet.getFilter().add(m.getValue()));
            }
        }
        for (String dimension : queryDimensions) {
            List<Dimension> dimensionList = schema.getDimension().get(datasource.getName());
            boolean isAdd = false;
            if (!CollectionUtils.isEmpty(dimensionList)) {
                for (Dimension dim : dimensionList) {
                    if (!dim.getName().equalsIgnoreCase(dimension)) {
                        continue;
                    }
                    if (nonAgg) {
                        dataSet.getMeasure().addAll(DimensionNode.expand(dim, scope));
                        output.getMeasure().add(DimensionNode.buildName(dim, scope));
                        isAdd = true;
                        continue;
                    }
                    dataSet.getMeasure().addAll(DimensionNode.expand(dim, scope));
                    output.getDimension().add(DimensionNode.buildName(dim, scope));
                    isAdd = true;
                }
            }
            if (!isAdd) {
                Optional<Identify> identify = datasource.getIdentifiers().stream()
                        .filter(i -> i.getName().equalsIgnoreCase(dimension)).findFirst();
                if (identify.isPresent()) {
                    if (nonAgg) {
                        dataSet.getMeasure().add(SemanticNode.parse(identify.get().getName(), scope));
                        output.getMeasure().add(SemanticNode.parse(identify.get().getName(), scope));
                    } else {
                        dataSet.getMeasure().add(SemanticNode.parse(identify.get().getName(), scope));
                        output.getDimension().add(SemanticNode.parse(identify.get().getName(), scope));
                    }
                    isAdd = true;
                }
            }
            if (isAdd) {
                continue;
            }
            Optional<Dimension> dimensionOptional = getDimensionByName(dimension, datasource);
            if (dimensionOptional.isPresent()) {
                if (nonAgg) {
                    dataSet.getMeasure().add(DimensionNode.build(dimensionOptional.get(), scope));
                    output.getMeasure().add(DimensionNode.buildName(dimensionOptional.get(), scope));
                    continue;
                }
                dataSet.getMeasure().add(DimensionNode.build(dimensionOptional.get(), scope));
                output.getDimension().add(DimensionNode.buildName(dimensionOptional.get(), scope));
            }
        }
        SqlNode tableNode = DataSourceNode.build(datasource, scope);
        dataSet.setTable(tableNode);
        output.setTable(SemanticNode.buildAs(
                Constants.DATASOURCE_TABLE_OUT_PREFIX + datasource.getName() + "_" + UUID.randomUUID().toString()
                        .substring(32), dataSet.build()));
        return output;
    }

    private static boolean isWhereHasMetric(List<String> fields, DataSource datasource) {
        Long metricNum = datasource.getMeasures().stream().filter(m -> fields.contains(m.getName().toLowerCase()))
                .count();
        Long measureNum = datasource.getMeasures().stream().filter(m -> fields.contains(m.getName().toLowerCase()))
                .count();
        return metricNum > 0 || measureNum > 0;
    }

    private static List<SqlNode> getWhereMeasure(List<String> fields, List<String> queryMetrics,
            List<String> queryDimensions, DataSource datasource, SqlValidatorScope scope, SemanticSchema schema,
            boolean nonAgg) throws Exception {
        Iterator<String> iterator = fields.iterator();
        List<SqlNode> whereNode = new ArrayList<>();
        while (iterator.hasNext()) {
            String cur = iterator.next();
            if (queryDimensions.contains(cur) || queryMetrics.contains(cur)) {
                iterator.remove();
            }
        }
        for (String where : fields) {
            List<Dimension> dimensionList = schema.getDimension().get(datasource.getName());
            boolean isAdd = false;
            if (!CollectionUtils.isEmpty(dimensionList)) {
                for (Dimension dim : dimensionList) {
                    if (!dim.getName().equalsIgnoreCase(where)) {
                        continue;
                    }
                    if (nonAgg) {
                        whereNode.addAll(DimensionNode.expand(dim, scope));
                        isAdd = true;
                        continue;
                    }
                    whereNode.addAll(DimensionNode.expand(dim, scope));
                    isAdd = true;
                }
            }
            Optional<Identify> identify = getIdentifyByName(where, datasource);
            if (identify.isPresent()) {
                whereNode.add(IdentifyNode.build(identify.get(), scope));
                isAdd = true;
            }
            if (isAdd) {
                continue;
            }
            Optional<Dimension> dimensionOptional = getDimensionByName(where, datasource);
            if (dimensionOptional.isPresent()) {
                if (nonAgg) {
                    whereNode.add(DimensionNode.build(dimensionOptional.get(), scope));
                    continue;
                }
                whereNode.add(DimensionNode.build(dimensionOptional.get(), scope));
            }
        }
        return whereNode;
    }

    private static void mergeWhere(List<String> fields, TableView dataSet, List<String> queryMetrics,
            List<String> queryDimensions, DataSource datasource, SqlValidatorScope scope, SemanticSchema schema,
            boolean nonAgg) throws Exception {
        List<SqlNode> whereNode = getWhereMeasure(fields, queryMetrics, queryDimensions, datasource, scope, schema,
                nonAgg);
        dataSet.getMeasure().addAll(whereNode);
    }


    private static void expandWhere(MetricReq metricCommand, TableView tableView, SqlValidatorScope scope)
            throws Exception {
        if (metricCommand.getWhere() != null && !metricCommand.getWhere().isEmpty()) {
            SqlNode sqlNode = SemanticNode.parse(metricCommand.getWhere(), scope);
            Set<String> fieldWhere = new HashSet<>();
            FilterNode.getFilterField(sqlNode, fieldWhere);
            //super.tableView.getFilter().add(sqlNode);
            tableView.getFilter().add(sqlNode);
        }
    }


}
