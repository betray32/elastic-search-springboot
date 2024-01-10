package com.example.elastic.search.spring.external.elastic.config;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * Service for performing operations on Elasticsearch.
 * This class provides methods for querying, inserting, and deleting documents in an Elasticsearch index.
 */
@Component
public class ElasticOperationsFactory {

    // Constants used in the service.
    public static final String EMPTY_STRING = "";
    public static final String FIELD_ID = "_id";
    public static final int LIST_SIZE = 10000;

    // Client to connect with Elasticsearch.
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * Retrieves a document from Elasticsearch based on a GetRequest.
     *
     * @param getRequest The GetRequest specifying the document to retrieve.
     * @param aDefault   Default request options.
     * @return GetResponse containing the retrieved document.
     * @throws IOException If there is an issue with the request.
     */
    public GetResponse get(GetRequest getRequest, RequestOptions aDefault) throws IOException {
        return restHighLevelClient.get(getRequest, aDefault);
    }

    /**
     * Searches for documents in Elasticsearch based on a SearchRequest.
     *
     * @param searchRequest The SearchRequest specifying the search criteria.
     * @param aDefault      Default request options.
     * @return SearchResponse containing the search results.
     * @throws IOException If there is an issue with the request.
     */
    public SearchResponse search(SearchRequest searchRequest, RequestOptions aDefault) throws IOException {
        return restHighLevelClient.search(searchRequest, aDefault);
    }

    /**
     * Deletes a document from Elasticsearch based on a DeleteRequest.
     *
     * @param deleteRequest The DeleteRequest specifying the document to delete.
     * @param aDefault      Default request options.
     * @return DeleteResponse indicating the outcome of the delete operation.
     * @throws IOException If there is an issue with the request.
     */
    public DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions aDefault) throws IOException {
        return restHighLevelClient.delete(deleteRequest, aDefault);
    }

    /**
     * Indexes a document in Elasticsearch based on an IndexRequest.
     *
     * @param indexRequest The IndexRequest with the document data to index.
     * @param aDefault     Default request options.
     * @return IndexResponse indicating the outcome of the indexing operation.
     * @throws IOException If there is an issue with the request.
     */
    public IndexResponse index(IndexRequest indexRequest, RequestOptions aDefault) throws IOException {
        return restHighLevelClient.index(indexRequest, aDefault);
    }

    /**
     * Creates a SearchSourceBuilder with a specified size.
     * It is used to build search queries for Elasticsearch.
     *
     * @param size The size of the search results to return.
     * @return SearchSourceBuilder configured with the specified size and default sort order.
     */
    public SearchSourceBuilder getSearchSourceBuilder(int size) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(size);
        final FieldSortBuilder fieldSortBuilder = new FieldSortBuilder(FIELD_ID).order(SortOrder.ASC);
        searchSourceBuilder.sort(fieldSortBuilder);
        return searchSourceBuilder;
    }

    /**
     * Retrieves the last ID from an array of SearchHits.
     * This is useful for pagination in search queries.
     *
     * @param searchHits Array of SearchHit objects.
     * @return The ID of the last SearchHit if available, or an empty string if not.
     */
    public String getLastId(SearchHit[] searchHits) {
        if (searchHits.length > 0) {
            return searchHits[searchHits.length - 1].getId();
        }
        return EMPTY_STRING;
    }

    /**
     * Adds a 'search after' parameter to a SearchSourceBuilder based on the last ID in a set of search hits.
     * This method is used to implement efficient search pagination.
     *
     * @param searchSourceBuilder The SearchSourceBuilder to modify.
     * @param searchHits          Array of SearchHit objects.
     * @return The last ID used for the 'search after' parameter.
     */
    public String addSearchAfter(SearchSourceBuilder searchSourceBuilder, SearchHit[] searchHits) {
        String lastId = getLastId(searchHits);
        if (!lastId.isEmpty()) {
            Object[] listId = {lastId};
            searchSourceBuilder.searchAfter(listId);
        }
        return lastId;
    }

    /**
     * Checks if there is a duplicated ID in the set of search hits.
     * This method is useful for detecting when the end of a search result set has been reached.
     *
     * @param searchHits Array of SearchHit objects.
     * @param oldLastId  The previous last ID for comparison.
     * @return True if the last ID in searchHits is the same as oldLastId, indicating a duplicate.
     */
    public boolean checkDuplicatedId(SearchHit[] searchHits, String oldLastId) {
        return getLastId(searchHits).equals(oldLastId);
    }

    /**
     * Retrieves a set of IDs from Elasticsearch based on a SearchSourceBuilder.
     * This method is used to fetch a large set of IDs efficiently.
     *
     * @param searchSourceBuilder The SearchSourceBuilder with the search query.
     * @param indexName           The name of the Elasticsearch index to search.
     * @return A Set of IDs from the search results.
     * @throws IOException If there is an issue with the search request.
     */
    public Set<String> getIds(SearchSourceBuilder searchSourceBuilder, String indexName) throws IOException {
        String id;
        searchSourceBuilder.sort(new FieldSortBuilder(FIELD_ID).order(SortOrder.ASC));
        searchSourceBuilder.size(LIST_SIZE);
        SearchRequest searchRequests = new SearchRequest();
        searchRequests.indices(indexName).source(searchSourceBuilder);
        SearchResponse searchResponses = search(searchRequests, RequestOptions.DEFAULT);
        SearchHits hits = searchResponses.getHits();
        SearchHit[] searchHits = hits.getHits();
        Set<String> resultIds = new HashSet<>();
        for (SearchHit hit : searchHits) {
            id = hit.getId();
            resultIds.add(id);
        }
        return resultIds;
    }
}
