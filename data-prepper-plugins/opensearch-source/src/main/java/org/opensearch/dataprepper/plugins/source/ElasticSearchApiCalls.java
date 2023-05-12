/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.simple.JSONObject;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class ElasticSearchApiCalls implements SearchAPICalls {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchApiCalls.class);
    private static final String KEEP_ALIVE_VALUE = "24h";
    private static final String TIME_VALUE = "24h";
    private static final int ELASTICSEARCH_VERSION = 7100;
    private static final int SEARCH_AFTER_SIZE = 100;

    private SourceInfoProvider sourceInfoProvider = new SourceInfoProvider();

    @Override
    public String generatePitId(final OpenSearchSourceConfig openSearchSourceConfig,final ElasticsearchClient client) {
        OpenPointInTimeResponse response = null;
        OpenPointInTimeRequest request = new OpenPointInTimeRequest.Builder().
                index(openSearchSourceConfig.getIndexValue()).
                keepAlive(new Time.Builder().time(KEEP_ALIVE_VALUE).build()).build();
        LOG.info("Request is : {} ", request);
            try {
                response = client.openPointInTime(request);
                LOG.debug("Response is {} ",response);
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }

        return response.id();
    }
    @Override
    public String searchPitIndexes(final OpenSearchSourceConfig openSearchSourceConfig,final ElasticsearchClient client) {
        SearchResponse<ObjectNode> searchResponse = null;
        try {
            searchResponse = client.search(req ->
                            req.index(openSearchSourceConfig.getIndexValue()),
                    ObjectNode.class);
            searchResponse.hits().hits().stream()
                    .map(Hit::source).collect(Collectors.toList());
            LOG.debug("Search Response {} ", searchResponse);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
        return searchResponse.toString();
    }
    @Override
    public String generateScrollId(final OpenSearchSourceConfig openSearchSourceConfig,final ElasticsearchClient client) {
        SearchResponse response = null;
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfig);
        SearchRequest searchRequest = SearchRequest
                .of(e -> e.index(indexList.toString()).size(openSearchSourceConfig.getSearchOptions().getBatchSize()).scroll(scr -> scr.time(TIME_VALUE)));
        try {
            response = client.search(searchRequest, ObjectNode.class);
            LOG.info("Response is : {} ",response);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return response.scrollId();
    }
    public ScrollRequest nextScrollRequest(final String scrollId) {
        return ScrollRequest
                .of(scrollRequest -> scrollRequest.scrollId(scrollId).scroll(Time.of(t -> t.time(TIME_VALUE))));
    }
    @Override
    public String searchScrollIndexes(OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client) {
        return null;
    }


    @Override
    public Boolean delete(final String id,final ElasticsearchClient client,final Integer elasticSearchVersion) {
        LOG.info("PIT or Scroll ID to be deleted - " + id);
        try {
            if (elasticSearchVersion.intValue() >= ELASTICSEARCH_VERSION) {
                return deletePitId(id, client);
            } else {
                return deleteScrollId(id, client);
            }

        } catch (IOException e) {
            LOG.error("Error occured while closing PIT " + e);
        }
        return false;
    }

    private boolean deleteScrollId(String id, ElasticsearchClient client) throws IOException {
        ClearScrollRequest scrollRequest=new ClearScrollRequest.Builder().scrollId(id).build();
        ClearScrollResponse clearScrollResponse = client.clearScroll(scrollRequest);
        LOG.info("Delete Scroll ID Response "+clearScrollResponse);
        return  clearScrollResponse.succeeded();
    }

    private boolean deletePitId(String id, ElasticsearchClient client) throws IOException {
        ClosePointInTimeRequest request = new ClosePointInTimeRequest.Builder().id(id).build();
        ClosePointInTimeResponse closePointInTimeResponse = client.closePointInTime(request);
        LOG.info("Delete PIT ID Response " + closePointInTimeResponse);
        return closePointInTimeResponse.succeeded();
    }

    SearchResponse getSearchForSort(final OpenSearchSourceConfig openSearchSourceConfig, final ElasticsearchClient client, long searchAfter){

        SearchResponse response = null;
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfig);
        LOG.info("indexList: " + indexList);
        String sortOrder = openSearchSourceConfig.getSearchOptions().getSorting().getOrder();
        SortOrder order = sortOrder.toLowerCase().equalsIgnoreCase("asc")?SortOrder.Asc: SortOrder.Desc;
        SortOptions sort = new SortOptions.Builder().field(f -> f.field(openSearchSourceConfig.getSearchOptions().getSorting().getSortKey().get(0)).order(order)).build();
        SearchRequest searchRequest = null;
        if(!openSearchSourceConfig.getQueryParameters().getFields().isEmpty()) {
            String[] queryParam = openSearchSourceConfig.getQueryParameters().getFields().get(0).split(":");
            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).query(q->q.match(t -> t
                                    .field(queryParam[0].trim())
                                    .query(queryParam[1].trim()))).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                            .sort(sort));
        }else {
            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).query(q->q.match(t -> t
                                    .field("")
                                    .query(""))).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                            .sort(sort));
        }
         try {
            response = client.search(searchRequest, JSONObject.class);
            LOG.info("Response of getSearchForSort : {} ",response);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
         return response;
    }

    public long extractSortValue(SearchResponse response, Buffer<Record<Event>> buffer) throws TimeoutException {
        HitsMetadata hitsMetadata = response.hits();

        int size = hitsMetadata.hits().size();
        long sortValue = 0;
        if(size != 0) {
            try {
                sortValue = ((Hit<Object>) hitsMetadata.hits().get(size - 1)).sort().get(0).longValue();
                LOG.info("extractSortValue : " + sortValue);
            }catch(Exception e){
                LOG.error(e.getMessage());
            }
        }

        // Write to Buffer
        sourceInfoProvider.writeClusterDataToBuffer(response.fields().toString(),buffer);
        return sortValue;
    }

    public void searchPitIndexesForPagination(final OpenSearchSourceConfig openSearchSourceConfig, final ElasticsearchClient client,long currentSearchAfterValue, Buffer<Record<Event>> buffer) throws TimeoutException {
        int batchSize = openSearchSourceConfig.getSearchOptions().getBatchSize();
        SearchResponse response = getSearchForSort(openSearchSourceConfig,client,currentSearchAfterValue);
        currentSearchAfterValue = extractSortValue(response, buffer);
        if(currentSearchAfterValue != 0) {
            searchPitIndexesForPagination(openSearchSourceConfig, client, currentSearchAfterValue,buffer);
        }
        else {
            LOG.info("---------- END OF PAGINATION MECHANISM -------------");
        }
    }
}