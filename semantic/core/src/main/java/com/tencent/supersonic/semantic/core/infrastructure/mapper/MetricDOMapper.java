package com.tencent.supersonic.semantic.core.infrastructure.mapper;

import com.tencent.supersonic.semantic.core.domain.dataobject.MetricDO;
import com.tencent.supersonic.semantic.core.domain.dataobject.MetricDOExample;

import java.util.List;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface MetricDOMapper {

    /**
     * @mbg.generated
     */
    long countByExample(MetricDOExample example);

    /**
     * @mbg.generated
     */
    int deleteByPrimaryKey(Long id);

    /**
     * @mbg.generated
     */
    int insert(MetricDO record);

    /**
     * @mbg.generated
     */
    int insertSelective(MetricDO record);

    /**
     * @mbg.generated
     */
    List<MetricDO> selectByExampleWithBLOBs(MetricDOExample example);

    /**
     * @mbg.generated
     */
    List<MetricDO> selectByExample(MetricDOExample example);

    /**
     * @mbg.generated
     */
    MetricDO selectByPrimaryKey(Long id);

    /**
     * @mbg.generated
     */
    int updateByPrimaryKeySelective(MetricDO record);

    /**
     * @mbg.generated
     */
    int updateByPrimaryKeyWithBLOBs(MetricDO record);

    /**
     * @mbg.generated
     */
    int updateByPrimaryKey(MetricDO record);
}
