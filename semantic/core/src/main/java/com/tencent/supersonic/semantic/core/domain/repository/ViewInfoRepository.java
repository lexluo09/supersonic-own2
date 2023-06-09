package com.tencent.supersonic.semantic.core.domain.repository;

import com.tencent.supersonic.semantic.core.domain.dataobject.ViewInfoDO;
import java.util.List;

public interface ViewInfoRepository {

    List<ViewInfoDO> getViewInfoList(Long domainId);

    ViewInfoDO getViewInfoById(Long id);

    void deleteViewInfo(Long id);

    void createViewInfo(ViewInfoDO viewInfoDO);

    void updateViewInfo(ViewInfoDO viewInfoDO);
}
