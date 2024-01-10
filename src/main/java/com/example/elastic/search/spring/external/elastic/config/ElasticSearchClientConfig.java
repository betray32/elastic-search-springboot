package com.example.elastic.search.spring.external.elastic.config;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

/**
 * Configuration class for Elasticsearch client.
 * This class sets up the RestHighLevelClient used to connect to an Elasticsearch cluster.
 */
@Configuration
public class ElasticSearchClientConfig {

    @Value("${elastic.host.url}")
    private String elasticHost;

    @Value("${elastic.schema:https}")
    private String elasticSchema;

    RestHighLevelClient restHighLevelClient;

    /**
     * Initializes the RestHighLevelClient. This method is called after the bean's properties have been set.
     */
    @PostConstruct
    public void init() {
        // Creating the RestHighLevelClient using the specified host, port, and schema.
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticHost, 443, elasticSchema)));
    }

    @PreDestroy
    public void destroy() throws IOException {
        restHighLevelClient.close();
    }

    /**
     * Bean definition for the RestHighLevelClient.
     * This method provides a synchronized, singleton instance of RestHighLevelClient.
     *
     * @return An instance of RestHighLevelClient.
     */
    @Bean
    public synchronized RestHighLevelClient restHighLevelClient() {
        return restHighLevelClient;
    }

}

