package com.example.elastic.search.spring.service;

import com.example.elastic.search.spring.external.elastic.config.ElasticOperationsFactory;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ElasticService {

    public static final String FETCH_SOURCE = "FETCH_SOURCE";
    public static final String QUERY_PARAM = "QUERY_PARAM";
    @Autowired
    private ElasticOperationsFactory elasticOperationsFactory;

    /**
     * Elastic document to query
     */
    @Value("${elastic.document.index}")
    private String flightIndex;

    public List<Map<String, Object>> getFlightByNoseNbr(String noseNumber) {
        List<Map<String, Object>> resultList = new ArrayList<>();

        try {
            // set
            SearchRequest searchRequest = new SearchRequest(flightIndex);
            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.from(0);
            searchBuilder.size(1); // limit to 1 result, specific
            searchBuilder.fetchSource(FETCH_SOURCE, null);
            searchBuilder.query(QueryBuilders.matchQuery(QUERY_PARAM, noseNumber));
            searchRequest.source(searchBuilder);

            // do
            SearchResponse searchResponse = elasticOperationsFactory.search(searchRequest, RequestOptions.DEFAULT);

            // check
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit : hits.getHits()) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                resultList.add(sourceAsMap);
            }
        } catch (Exception e) {
            log.error("Error querying Elastic search, Detail > {}", e.getMessage());
        }

        return resultList;
    }

}
