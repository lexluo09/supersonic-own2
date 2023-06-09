package com.tencent.supersonic.chat.infrastructure.semantic;

import static com.tencent.supersonic.common.constant.Constants.TRUE_LOWER;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.tencent.supersonic.auth.api.authentication.constant.UserConstants;
import com.tencent.supersonic.auth.api.authentication.pojo.User;
import com.tencent.supersonic.chat.api.service.SemanticLayer;
import com.tencent.supersonic.semantic.api.core.request.DomainSchemaFilterReq;
import com.tencent.supersonic.semantic.api.core.response.DimSchemaResp;
import com.tencent.supersonic.semantic.api.core.response.DomainSchemaResp;
import com.tencent.supersonic.semantic.api.core.response.MetricSchemaResp;
import com.tencent.supersonic.semantic.api.core.response.QueryResultWithSchemaResp;
import com.tencent.supersonic.semantic.api.query.request.QueryStructReq;
import com.tencent.supersonic.chat.application.ConfigServiceImpl;
import com.tencent.supersonic.chat.domain.pojo.config.ChatConfigInfo;
import com.tencent.supersonic.chat.domain.pojo.config.ItemVisibility;
import com.tencent.supersonic.chat.domain.utils.DefaultSemanticInternalUtils;
import com.tencent.supersonic.common.exception.CommonException;
import com.tencent.supersonic.common.result.ResultData;
import com.tencent.supersonic.common.result.ReturnCode;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Service
public class DefaultSemanticLayerImpl implements SemanticLayer {

    private final Logger logger = LoggerFactory.getLogger(DefaultSemanticLayerImpl.class);

    @Value("${semantic.url.prefix:http://localhost:8081}")
    private String semanticUrl;

    @Value("${searchByStruct.path:/api/semantic/query/struct}")
    private String searchByStructPath;

    @Value("${fetchDomainSchemaPath.path:/api/semantic/schema}")
    private String fetchDomainSchemaPath;

    @Autowired
    private DefaultSemanticInternalUtils defaultSemanticInternalUtils;

    private ParameterizedTypeReference<ResultData<QueryResultWithSchemaResp>> structTypeRef =
            new ParameterizedTypeReference<ResultData<QueryResultWithSchemaResp>>() {
            };

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private ConfigServiceImpl chaConfigService;

    @Override
    public QueryResultWithSchemaResp queryByStruct(QueryStructReq queryStructCmd, User user) {
        deletionDuplicated(queryStructCmd);
        return searchByStruct(semanticUrl + searchByStructPath, queryStructCmd);
    }

    public QueryResultWithSchemaResp searchByStruct(String url, QueryStructReq queryStructCmd) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        defaultSemanticInternalUtils.fillToken(headers);
        URI requestUrl = UriComponentsBuilder.fromHttpUrl(url).build().encode().toUri();
        Gson gson = new Gson();
        HttpEntity<String> entity = new HttpEntity<>(gson.toJson(queryStructCmd), headers);
        logger.info("searchByStruct {}", entity.getBody());
        ResultData<QueryResultWithSchemaResp> responseBody;
        try {
            ResponseEntity<ResultData<QueryResultWithSchemaResp>> responseEntity = restTemplate.exchange(requestUrl,
                    HttpMethod.POST, entity, structTypeRef);
            responseBody = responseEntity.getBody();
            logger.debug("ApiResponse<QueryResultWithColumns> responseBody:{}", responseBody);
            QueryResultWithSchemaResp semanticQuery = new QueryResultWithSchemaResp();
            if (ReturnCode.SUCCESS.getCode() == responseBody.getCode()) {
                QueryResultWithSchemaResp data = responseBody.getData();
                semanticQuery.setColumns(data.getColumns());
                semanticQuery.setResultList(data.getResultList());
                semanticQuery.setSql(data.getSql());
                semanticQuery.setQueryAuthorization(data.getQueryAuthorization());
                return semanticQuery;
            }
        } catch (Exception e) {
            throw new RuntimeException("search semantic struct interface error", e);
        }
        throw new CommonException(responseBody.getCode(), responseBody.getMsg());
    }

    public List<DomainSchemaResp> fetchDomainSchemaAll(List<Long> ids) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(UserConstants.INTERNAL, TRUE_LOWER);
        headers.setContentType(MediaType.APPLICATION_JSON);
        defaultSemanticInternalUtils.fillToken(headers);
        URI requestUrl = UriComponentsBuilder.fromHttpUrl(semanticUrl + fetchDomainSchemaPath).build().encode().toUri();
        DomainSchemaFilterReq filter = new DomainSchemaFilterReq();
        filter.setDomainIds(ids);
        ParameterizedTypeReference<ResultData<List<DomainSchemaResp>>> responseTypeRef =
                new ParameterizedTypeReference<ResultData<List<DomainSchemaResp>>>() {
                };

        HttpEntity<String> entity = new HttpEntity<>(JSON.toJSONString(filter), headers);
        try {
            ResponseEntity<ResultData<List<DomainSchemaResp>>> responseEntity = restTemplate.exchange(requestUrl,
                    HttpMethod.POST, entity, responseTypeRef);
            ResultData<List<DomainSchemaResp>> responseBody = responseEntity.getBody();
            logger.debug("ApiResponse<fetchDomainSchema> responseBody:{}", responseBody);
            if (ReturnCode.SUCCESS.getCode() == responseBody.getCode()) {
                List<DomainSchemaResp> data = responseBody.getData();
                return data;
            }
        } catch (Exception e) {
            throw new RuntimeException("fetchDomainSchema interface error", e);
        }
        throw new RuntimeException("fetchDomainSchema interface error");
    }


    public List<DomainSchemaResp> fetchDomainSchema(List<Long> ids) {
        List<DomainSchemaResp> data = fetchDomainSchemaAll(ids);
        fillEntityNameAndFilterBlackElement(data);
        return data;
    }

    @Override
    public DomainSchemaResp getDomainSchemaInfo(Long domain) {
        List<Long> ids = new ArrayList<>();
        ids.add(domain);
        List<DomainSchemaResp> domainSchemaDescs = fetchDomainSchema(ids);
        if (!CollectionUtils.isEmpty(domainSchemaDescs)) {
            Optional<DomainSchemaResp> domainSchemaDesc = domainSchemaDescs.stream()
                    .filter(d -> d.getId().equals(domain)).findFirst();
            if (domainSchemaDesc.isPresent()) {
                DomainSchemaResp domainSchema = domainSchemaDesc.get();
                return domainSchema;
            }
        }
        return null;
    }

    @Override
    public List<DomainSchemaResp> getDomainSchemaInfo(List<Long> ids) {
        return fetchDomainSchema(ids);
    }

    public DomainSchemaResp fillEntityNameAndFilterBlackElement(DomainSchemaResp domainSchemaResp) {
        if (Objects.isNull(domainSchemaResp) || Objects.isNull(domainSchemaResp.getId())) {
            return domainSchemaResp;
        }
        ChatConfigInfo chaConfigDesc = getConfigBaseInfo(domainSchemaResp.getId());

        // fill entity names
        fillEntityNamesInfo(domainSchemaResp, chaConfigDesc);

        // filter black element
        filterBlackDim(domainSchemaResp, chaConfigDesc);
        filterBlackMetric(domainSchemaResp, chaConfigDesc);
        return domainSchemaResp;
    }

    public void fillEntityNameAndFilterBlackElement(List<DomainSchemaResp> domainSchemaDescList) {
        if (!CollectionUtils.isEmpty(domainSchemaDescList)) {
            domainSchemaDescList.stream()
                    .forEach(domainSchemaDesc -> fillEntityNameAndFilterBlackElement(domainSchemaDesc));
        }
    }

    private void filterBlackMetric(DomainSchemaResp domainSchemaDesc, ChatConfigInfo chaConfigDesc) {
        ItemVisibility visibility = chaConfigDesc.getVisibility();
        if (Objects.nonNull(chaConfigDesc) && Objects.nonNull(visibility)
                && !CollectionUtils.isEmpty(visibility.getBlackMetricIdList())
                && !CollectionUtils.isEmpty(domainSchemaDesc.getMetrics())) {
            List<MetricSchemaResp> metric4Chat = domainSchemaDesc.getMetrics().stream()
                    .filter(metric -> !visibility.getBlackMetricIdList().contains(metric.getId()))
                    .collect(Collectors.toList());
            domainSchemaDesc.setMetrics(metric4Chat);
        }
    }

    private void filterBlackDim(DomainSchemaResp domainSchemaDesc, ChatConfigInfo chaConfigDesc) {
        ItemVisibility visibility = chaConfigDesc.getVisibility();
        if (Objects.nonNull(chaConfigDesc) && Objects.nonNull(visibility)
                && !CollectionUtils.isEmpty(visibility.getBlackDimIdList())
                && !CollectionUtils.isEmpty(domainSchemaDesc.getDimensions())) {
            List<DimSchemaResp> dim4Chat = domainSchemaDesc.getDimensions().stream()
                    .filter(dim -> !visibility.getBlackDimIdList().contains(dim.getId()))
                    .collect(Collectors.toList());
            domainSchemaDesc.setDimensions(dim4Chat);
        }
    }

    private void fillEntityNamesInfo(DomainSchemaResp domainSchemaDesc, ChatConfigInfo chaConfigDesc) {
        if (Objects.nonNull(chaConfigDesc) && Objects.nonNull(chaConfigDesc.getEntity())
                && !CollectionUtils.isEmpty(chaConfigDesc.getEntity().getNames())) {
            domainSchemaDesc.setEntityNames(chaConfigDesc.getEntity().getNames());
        }
    }

    private void deletionDuplicated(QueryStructReq queryStructCmd) {
        if (!CollectionUtils.isEmpty(queryStructCmd.getGroups()) && queryStructCmd.getGroups().size() > 1) {
            Set<String> groups = new HashSet<>();
            groups.addAll(queryStructCmd.getGroups());
            queryStructCmd.getGroups().clear();
            ;
            queryStructCmd.getGroups().addAll(groups);
        }
    }

    public ChatConfigInfo getConfigBaseInfo(Long domain) {
        return chaConfigService.fetchConfigByDomainId(domain);
    }

}
