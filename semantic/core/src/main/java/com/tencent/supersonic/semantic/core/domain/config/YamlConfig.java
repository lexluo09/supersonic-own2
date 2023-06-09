package com.tencent.supersonic.semantic.core.domain.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Configuration
public class YamlConfig {


    @Value("${model.yaml.file.dir:/data/services/semantic_parse_sit-1.0/conf/models/}")
    private String metaYamlFileDir;

    public String getmetaYamlFileDir() {
        return metaYamlFileDir;
    }

}
