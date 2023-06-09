package com.tencent.supersonic.semantic.api.query.request;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.codec.digest.DigestUtils;

@Data
@ToString
public class QueryMultiStructReq {


    List<QueryStructReq> queryStructCmds;


    public String toCustomizedString() {
        return JSONObject.toJSONString(queryStructCmds);
    }

    public String generateCommandMd5() {
        return DigestUtils.md5Hex(this.toCustomizedString());
    }
}
