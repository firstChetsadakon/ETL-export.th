package com.dsa.etl.export.th.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import lombok.Data;
@Configuration
@ConfigurationProperties(prefix = "spring.datasource")
@Data
public class DatabaseConfig {
    private String url;
    private String username;
    private String password;
}