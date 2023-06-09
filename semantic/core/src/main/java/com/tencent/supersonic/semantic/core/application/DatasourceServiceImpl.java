package com.tencent.supersonic.semantic.core.application;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.tencent.supersonic.auth.api.authentication.pojo.User;
import com.tencent.supersonic.semantic.api.core.pojo.DatasourceDetail;
import com.tencent.supersonic.semantic.api.core.pojo.Dim;
import com.tencent.supersonic.semantic.api.core.pojo.ItemDateFilter;
import com.tencent.supersonic.semantic.api.core.pojo.Measure;
import com.tencent.supersonic.semantic.api.core.pojo.yaml.DatasourceYamlTpl;
import com.tencent.supersonic.semantic.api.core.pojo.yaml.DimensionYamlTpl;
import com.tencent.supersonic.semantic.api.core.pojo.yaml.MetricYamlTpl;
import com.tencent.supersonic.semantic.api.core.request.DatasourceRelaReq;
import com.tencent.supersonic.semantic.api.core.request.DatasourceReq;
import com.tencent.supersonic.semantic.api.core.request.DateInfoReq;
import com.tencent.supersonic.semantic.api.core.request.DimensionReq;
import com.tencent.supersonic.semantic.api.core.request.MetricReq;
import com.tencent.supersonic.semantic.api.core.response.DatabaseResp;
import com.tencent.supersonic.semantic.api.core.response.DatasourceRelaResp;
import com.tencent.supersonic.semantic.api.core.response.DatasourceResp;
import com.tencent.supersonic.semantic.api.core.response.DimensionResp;
import com.tencent.supersonic.semantic.api.core.response.ItemDateResp;
import com.tencent.supersonic.semantic.api.core.response.MeasureResp;
import com.tencent.supersonic.semantic.api.core.response.MetricResp;
import com.tencent.supersonic.common.util.json.JsonUtil;
import com.tencent.supersonic.semantic.core.domain.dataobject.DatasourceDO;
import com.tencent.supersonic.semantic.core.domain.dataobject.DatasourceRelaDO;
import com.tencent.supersonic.semantic.core.domain.dataobject.DateInfoDO;
import com.tencent.supersonic.semantic.core.domain.manager.DatasourceYamlManager;
import com.tencent.supersonic.semantic.core.domain.manager.DimensionYamlManager;
import com.tencent.supersonic.semantic.core.domain.manager.MetricYamlManager;
import com.tencent.supersonic.semantic.core.domain.repository.DatasourceRepository;
import com.tencent.supersonic.semantic.core.domain.repository.DateInfoRepository;
import com.tencent.supersonic.semantic.core.domain.utils.DatasourceConverter;
import com.tencent.supersonic.semantic.core.domain.utils.DimensionConverter;
import com.tencent.supersonic.semantic.core.domain.utils.MetricConverter;
import com.tencent.supersonic.semantic.core.domain.DatabaseService;
import com.tencent.supersonic.semantic.core.domain.DatasourceService;
import com.tencent.supersonic.semantic.core.domain.DimensionService;
import com.tencent.supersonic.semantic.core.domain.DomainService;
import com.tencent.supersonic.semantic.core.domain.MetricService;
import com.tencent.supersonic.semantic.core.domain.pojo.Datasource;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.BeanUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;


@Service
@Slf4j
public class DatasourceServiceImpl implements DatasourceService {

    private DatasourceRepository datasourceRepository;

    private DatasourceYamlManager datasourceYamlManager;

    private DatabaseService databaseService;

    private DimensionService dimensionService;

    private MetricService metricService;

    private DateInfoRepository dateInfoRepository;

    private DomainService domainService;


    public DatasourceServiceImpl(DatasourceRepository datasourceRepository,
            DatasourceYamlManager datasourceYamlManager,
            DomainService domainService,
            DatabaseService databaseService,
            @Lazy DimensionService dimensionService,
            @Lazy MetricService metricService,
            DateInfoRepository dateInfoRepository) {
        this.domainService = domainService;
        this.datasourceRepository = datasourceRepository;
        this.datasourceYamlManager = datasourceYamlManager;
        this.databaseService = databaseService;
        this.dimensionService = dimensionService;
        this.metricService = metricService;
        this.dateInfoRepository = dateInfoRepository;
    }

    @Override
    public DatasourceResp createDatasource(DatasourceReq datasourceReq, User user) throws Exception {
        preCheck(datasourceReq);
        Datasource datasource = DatasourceConverter.convert(datasourceReq);
        log.info("[create datasource] object:{}", JSONObject.toJSONString(datasource));
        saveDatasource(datasource, user);
        Optional<DatasourceResp> datasourceDescOptional = getDatasource(datasourceReq.getDomainId(),
                datasourceReq.getBizName());
        if (!datasourceDescOptional.isPresent()) {
            throw new RuntimeException("create datasource failed");
        }
        DatasourceResp datasourceDesc = datasourceDescOptional.get();
        datasource.setId(datasourceDesc.getId());
        batchCreateDimension(datasource, user);
        batchCreateMetric(datasource, user);
        List<DimensionResp> dimensionDescsExist = dimensionService.getDimensionsByDatasource(datasource.getId());
        DatabaseResp databaseResp = databaseService.getDatabase(datasource.getDatabaseId());
        datasourceYamlManager.generateYamlFile(datasource, databaseResp,
                domainService.getDomainFullPath(datasource.getDomainId()), dimensionDescsExist);
        return datasourceDesc;
    }


    @Override
    public DatasourceResp updateDatasource(DatasourceReq datasourceReq, User user) throws Exception {
        preCheck(datasourceReq);
        Datasource datasource = DatasourceConverter.convert(datasourceReq);

        log.info("[update datasource] object:{}", JSONObject.toJSONString(datasource));

        batchCreateDimension(datasource, user);
        batchCreateMetric(datasource, user);
        List<DimensionResp> dimensionDescsExist = dimensionService.getDimensionsByDatasource(datasource.getId());
        DatabaseResp databaseResp = databaseService.getDatabase(datasource.getDatabaseId());
        datasourceYamlManager.generateYamlFile(datasource, databaseResp,
                domainService.getDomainFullPath(datasource.getDomainId()), dimensionDescsExist);
        DatasourceDO datasourceDO = updateDatasource(datasource, user);
        return DatasourceConverter.convert(datasourceDO);
    }

    private DatasourceDO updateDatasource(Datasource datasource, User user) {
        DatasourceDO datasourceDO = datasourceRepository.getDatasourceById(datasource.getId());
        datasource.updatedBy(user.getName());
        datasourceRepository.updateDatasource(DatasourceConverter.convert(datasourceDO, datasource));
        return datasourceDO;
    }

    @Override
    public String getSourceBizNameById(Long id) {
        DatasourceDO datasourceDO = getDatasourceById(id);
        if (datasourceDO == null) {
            String message = String.format("datasource with id:%s not exsit", id);
            throw new RuntimeException(message);
        }
        return datasourceDO.getBizName();
    }


    private DatasourceDO getDatasourceById(Long id) {
        return datasourceRepository.getDatasourceById(id);
    }


    @Override
    public List<MeasureResp> getMeasureListOfDomain(Long domainId) {
        List<DatasourceResp> datasourceDescs = getDatasourceList(domainId);
        List<MeasureResp> measureDescs = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(datasourceDescs)) {
            for (DatasourceResp datasourceDesc : datasourceDescs) {
                DatasourceDetail datasourceDetail = datasourceDesc.getDatasourceDetail();
                List<Measure> measures = datasourceDetail.getMeasures();
                if (!CollectionUtils.isEmpty(measures)) {
                    measureDescs.addAll(
                            measures.stream().map(measure -> DatasourceConverter.convert(measure, datasourceDesc))
                                    .collect(Collectors.toList()));
                }
            }
        }
        return measureDescs;
    }


    private void batchCreateDimension(Datasource datasource, User user) throws Exception {
        List<DimensionReq> dimensionReqs = DatasourceConverter.convertDimensionList(datasource);
        dimensionService.createDimensionBatch(dimensionReqs, user);
    }

    private void batchCreateMetric(Datasource datasource, User user) throws Exception {
        List<MetricReq> exprMetricReqs = DatasourceConverter.convertMetricList(datasource);
        metricService.createMetricBatch(exprMetricReqs, user);
    }


    private Optional<DatasourceResp> getDatasource(Long domainId, String bizName) {
        List<DatasourceResp> datasourceDescs = getDatasourceList(domainId);
        if (CollectionUtils.isEmpty(datasourceDescs)) {
            return Optional.empty();
        }
        for (DatasourceResp datasourceDesc : datasourceDescs) {
            if (datasourceDesc.getBizName().equals(bizName)) {
                return Optional.of(datasourceDesc);
            }
        }
        return Optional.empty();
    }

    //保存并获取自增ID
    private void saveDatasource(Datasource datasource, User user) {
        DatasourceDO datasourceDO = DatasourceConverter.convert(datasource, user);
        log.info("[save datasource] datasourceDO:{}", JSONObject.toJSONString(datasourceDO));
        datasourceRepository.createDatasource(datasourceDO);
        datasource.setId(datasourceDO.getId());
    }




    private void preCheck(DatasourceReq datasourceReq) {
        List<Dim> dims = datasourceReq.getDimensions();
        if (CollectionUtils.isEmpty(dims)) {
            throw new RuntimeException("lack of dimension");
        }
    }

    @Override
    public List<DatasourceResp> getDatasourceList(Long domainId) {
        return DatasourceConverter.convertList(datasourceRepository.getDatasourceList(domainId));
    }

    @Override
    public List<DatasourceResp> getDatasourceList() {
        return DatasourceConverter.convertList(datasourceRepository.getDatasourceList());
    }

    @Override
    public List<DatasourceResp> getDatasourceListNoMeasurePrefix(Long domainId) {
        List<DatasourceResp> datasourceResps = getDatasourceList(domainId);
        for (DatasourceResp datasourceResp : datasourceResps) {
            if (!CollectionUtils.isEmpty(datasourceResp.getDatasourceDetail().getMeasures())) {
                for (Measure measure : datasourceResp.getDatasourceDetail().getMeasures()) {
                    measure.setBizName(Optional.ofNullable(measure.getBizName()).orElse("")
                            .replace(getDatasourcePrefix(datasourceResp.getBizName()), ""));
                }
            }
        }
        return datasourceResps;
    }

    private String getDatasourcePrefix(String datasourceBizName) {
        return String.format("%s_", datasourceBizName);
    }




    @Override
    public Map<Long, DatasourceResp> getDatasourceMap() {
        Map<Long, DatasourceResp> map = new HashMap<>();
        List<DatasourceResp> datasourceDescs = getDatasourceList();
        if (CollectionUtils.isEmpty(datasourceDescs)) {
            return map;
        }
        return datasourceDescs.stream().collect(Collectors.toMap(DatasourceResp::getId, a -> a, (k1, k2) -> k1));
    }


    @Override
    public void deleteDatasource(Long id) throws Exception {
        DatasourceDO datasourceDO = datasourceRepository.getDatasourceById(id);
        if (datasourceDO == null) {
            return;
        }
        datasourceRepository.deleteDatasource(id);
        datasourceYamlManager.deleteYamlFile(datasourceDO.getBizName(),
                domainService.getDomainFullPath(datasourceDO.getDomainId()));
    }


    private List<DatasourceRelaResp> convertDatasourceRelaList(List<DatasourceRelaDO> datasourceRelaDOS) {
        List<DatasourceRelaResp> datasourceRelaResps = Lists.newArrayList();
        if (CollectionUtils.isEmpty(datasourceRelaDOS)) {
            return datasourceRelaResps;
        }
        return datasourceRelaDOS.stream().map(DatasourceConverter::convert).collect(Collectors.toList());


    }


    @Override

    public DatasourceRelaResp createOrUpdateDatasourceRela(DatasourceRelaReq datasourceRelaReq, User user) {
        if (datasourceRelaReq.getId() == null) {
            DatasourceRelaDO datasourceRelaDO = new DatasourceRelaDO();
            BeanUtils.copyProperties(datasourceRelaReq, datasourceRelaDO);
            datasourceRelaDO.setCreatedAt(new Date());
            datasourceRelaDO.setCreatedBy(user.getName());
            datasourceRelaDO.setUpdatedAt(new Date());
            datasourceRelaDO.setUpdatedBy(user.getName());
            datasourceRepository.createDatasourceRela(datasourceRelaDO);
            return DatasourceConverter.convert(datasourceRelaDO);
        }
        Long id = datasourceRelaReq.getId();
        DatasourceRelaDO datasourceRelaDO = datasourceRepository.getDatasourceRelaById(id);
        BeanUtils.copyProperties(datasourceRelaDO, datasourceRelaReq);
        datasourceRelaDO.setUpdatedAt(new Date());
        datasourceRelaDO.setUpdatedBy(user.getName());
        datasourceRepository.updateDatasourceRela(datasourceRelaDO);
        return DatasourceConverter.convert(datasourceRelaDO);
    }

    @Override
    public List<DatasourceRelaResp> getDatasourceRelaList(Long domainId) {
        return convertDatasourceRelaList(datasourceRepository.getDatasourceRelaList(domainId));
    }


    @Override
    public void deleteDatasourceRela(Long id) {
        datasourceRepository.deleteDatasourceRela(id);
    }


    public ItemDateResp getDateDate(ItemDateFilter dimension, ItemDateFilter metric) {
        List<DateInfoReq> itemDates = new ArrayList<>();
        List<DateInfoDO> dimensions = dateInfoRepository.getDateInfos(dimension);
        List<DateInfoDO> metrics = dateInfoRepository.getDateInfos(metric);

        log.info("getDateDate, dimension:{}, dimensions dateInfo:{}", dimension, dimensions);
        log.info("getDateDate, metric:{}, metrics dateInfo:{}", metric, metrics);
        itemDates.addAll(convert(dimensions));
        itemDates.addAll(convert(metrics));

        ItemDateResp itemDateDescriptor = calculateDateInternal(itemDates);
        log.info("itemDateDescriptor:{}", itemDateDescriptor);

        return itemDateDescriptor;
    }

    private List<DateInfoReq> convert(List<DateInfoDO> dateInfoDOList) {
        List<DateInfoReq> dateInfoCommendList = new ArrayList<>();
        dateInfoDOList.stream().forEach(dateInfoDO -> {
            DateInfoReq dateInfoCommend = new DateInfoReq();
            BeanUtils.copyProperties(dateInfoDO, dateInfoCommend);
            dateInfoCommend.setUnavailableDateList(JsonUtil.toList(dateInfoDO.getUnavailableDateList(), String.class));
            dateInfoCommendList.add(dateInfoCommend);
        });
        return dateInfoCommendList;
    }

    private ItemDateResp calculateDateInternal(List<DateInfoReq> itemDates) {
        if (CollectionUtils.isEmpty(itemDates)) {
            log.warn("itemDates is empty!");
            return null;
        }
        String dateFormat = itemDates.get(0).getDateFormat();
        String startDate = itemDates.get(0).getStartDate();
        String endDate = itemDates.get(0).getEndDate();
        List<String> unavailableDateList = itemDates.get(0).getUnavailableDateList();
        for (DateInfoReq item : itemDates) {
            String startDate1 = item.getStartDate();
            String endDate1 = item.getEndDate();
            List<String> unavailableDateList1 = item.getUnavailableDateList();
            if (Strings.isNotEmpty(startDate1) && startDate1.compareTo(startDate) < 0) {
                startDate = startDate1;
            }
            if (Strings.isNotEmpty(endDate1) && startDate1.compareTo(endDate1) > 0) {
                endDate = endDate1;
            }
            if (!CollectionUtils.isEmpty(unavailableDateList1)) {
                unavailableDateList.addAll(unavailableDateList1);
            }
        }

        return new ItemDateResp(dateFormat, startDate, endDate, unavailableDateList);

    }

    @Override
    public void getModelYamlTplByDomainIds(Set<Long> domainIds, Map<String, List<DimensionYamlTpl>> dimensionYamlMap,
            List<DatasourceYamlTpl> datasourceYamlTplList, List<MetricYamlTpl> metricYamlTplList) {
        for (Long domainId : domainIds) {
            List<DatasourceResp> datasourceResps = getDatasourceList(domainId);
            List<MetricResp> metricResps = metricService.getMetrics(domainId);
            metricYamlTplList.addAll(MetricYamlManager.convert2YamlObj(MetricConverter.metricInfo2Metric(metricResps)));
            DatabaseResp databaseResp = databaseService.getDatabaseByDomainId(domainId);
            List<DimensionResp> dimensionResps = dimensionService.getDimensions(domainId);
            for (DatasourceResp datasourceResp : datasourceResps) {
                datasourceYamlTplList.add(DatasourceYamlManager.convert2YamlObj(
                        DatasourceConverter.datasourceInfo2Datasource(datasourceResp), databaseResp));
                if (!dimensionYamlMap.containsKey(datasourceResp.getBizName())) {
                    dimensionYamlMap.put(datasourceResp.getBizName(), new ArrayList<>());
                }
                List<DimensionResp> dimensionRespList = dimensionResps.stream()
                        .filter(d -> d.getDatasourceName().equalsIgnoreCase(datasourceResp.getBizName()))
                        .collect(Collectors.toList());
                dimensionYamlMap.get(datasourceResp.getBizName()).addAll(DimensionYamlManager.convert2DimensionYaml(
                        DimensionConverter.dimensionInfo2Dimension(dimensionRespList)));
            }
        }

    }
}
