package com.tencent.supersonic.semantic.query.domain.utils;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.supersonic.auth.api.authentication.pojo.User;
import com.tencent.supersonic.semantic.api.core.enums.QueryTypeBackEnum;
import com.tencent.supersonic.semantic.api.core.enums.QueryTypeEnum;
import com.tencent.supersonic.semantic.api.core.pojo.QueryStat;
import com.tencent.supersonic.semantic.api.query.request.ItemUseReq;
import com.tencent.supersonic.semantic.api.query.request.QueryStructReq;
import com.tencent.supersonic.semantic.api.query.response.ItemUseResp;
import com.tencent.supersonic.common.enums.TaskStatusEnum;
import com.tencent.supersonic.semantic.query.domain.repository.StatRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StatUtils {

    private final StatRepository statRepository;
    private final SqlFilterUtils sqlFilterUtils;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final TransmittableThreadLocal<QueryStat> STATS = new TransmittableThreadLocal<>();

    public StatUtils(StatRepository statRepository,
            SqlFilterUtils sqlFilterUtils) {

        this.statRepository = statRepository;
        this.sqlFilterUtils = sqlFilterUtils;
    }

    public static QueryStat get() {
        return STATS.get();
    }

    public static void set(QueryStat queryStatInfo) {
        STATS.set(queryStatInfo);
    }

    public static void remove() {
        STATS.remove();
    }

    public void statInfo2DbAsync(TaskStatusEnum state) {
        QueryStat queryStatInfo = get();
        queryStatInfo.setElapsedMs(System.currentTimeMillis() - queryStatInfo.getStartTime());
        queryStatInfo.setQueryState(state.getStatus());
        log.info("queryStatInfo: {}", queryStatInfo);
        CompletableFuture.runAsync(() -> {
            statRepository.createRecord(queryStatInfo);
        }).exceptionally(exception -> {
            log.warn("queryStatInfo, exception:", exception);
            return null;
        });

        remove();
    }

    public Boolean updateResultCacheKey(String key) {
        STATS.get().setResultCacheKey(key);
        return true;
    }

    public void initStatInfo(QueryStructReq queryStructCmd, User facadeUser) {
        QueryStat queryStatInfo = new QueryStat();
        String traceId = "";
        List<String> dimensions = queryStructCmd.getGroups();

        List<String> metrics = new ArrayList<>();
        queryStructCmd.getAggregators().stream().forEach(aggregator -> metrics.add(aggregator.getColumn()));
        String user = (Objects.nonNull(facadeUser) && Strings.isNotEmpty(facadeUser.getName())) ? facadeUser.getName()
                : "Admin";

        try {
            queryStatInfo.setTraceId(traceId)
                    .setClassId(queryStructCmd.getDomainId())
                    .setUser(user)
                    .setQueryType(QueryTypeEnum.STRUCT.getValue())
                    .setQueryTypeBack(QueryTypeBackEnum.NORMAL.getState())
                    .setQueryStructCmd(queryStructCmd.toString())
                    .setQueryStructCmdMd5(DigestUtils.md5Hex(queryStructCmd.toString()))
                    .setStartTime(System.currentTimeMillis())
                    .setNativeQuery(queryStructCmd.getNativeQuery())
                    .setGroupByCols(objectMapper.writeValueAsString(queryStructCmd.getGroups()))
                    .setAggCols(objectMapper.writeValueAsString(queryStructCmd.getAggregators()))
                    .setOrderByCols(objectMapper.writeValueAsString(queryStructCmd.getOrders()))
                    .setFilterCols(objectMapper.writeValueAsString(
                            sqlFilterUtils.getFiltersCol(queryStructCmd.getOriginalFilter())))
                    .setUseResultCache(true)
                    .setUseSqlCache(true)
                    .setMetrics(objectMapper.writeValueAsString(metrics))
                    .setDimensions(objectMapper.writeValueAsString(dimensions));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        StatUtils.set(queryStatInfo);

    }

    public List<ItemUseResp> getStatInfo(ItemUseReq itemUseCommend) {
        return statRepository.getStatInfo(itemUseCommend);
    }

    public List<QueryStat> getQueryStatInfoWithoutCache(ItemUseReq itemUseCommend) {
        return statRepository.getQueryStatInfoWithoutCache(itemUseCommend);
    }
}