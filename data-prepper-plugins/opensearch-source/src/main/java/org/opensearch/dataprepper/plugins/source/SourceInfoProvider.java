/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class SourceInfoProvider {
    private String datasource;
    private static final Logger LOG = LoggerFactory.getLogger(SourceInfoProvider.class);
    private static final String GET_REQUEST_METHOD = "GET";
    private static final String CONTENT_TYPE = "content-type";
    private static final String CONTENT_TYPE_VALUE = "application/json";
    private static final String VERSION = "version";
    private static final String DISTRIBUTION = "distribution";
    private static final String ELASTIC_SEARCH = "elasticsearch";
    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";
    private static final String CLUSTER_HEALTH_STATUS = "status";
    private static final String CLUSTER_HEALTH_STATUS_RED = "red";
    private static final String NODES = "nodes";
    private static final String VERSIONS = "versions";
    private static final String REGULAR_EXPRESSION = "[^a-zA-Z0-9]";
    private static final int VERSION_7_10_0 = 7100;
    private  final JsonFactory jsonFactory = new JsonFactory();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public String getsourceInfo(final OpenSearchSourceConfig openSearchSourceConfig) {
        try {
            JSONParser jsonParser = new JSONParser();
            StringBuilder response = new StringBuilder();
            if (StringUtils.isBlank(openSearchSourceConfig.getHosts().get(0)))
                throw new IllegalArgumentException("Hostname cannot be null or empty");
            URL obj = new URL(openSearchSourceConfig.getHosts().get(0));
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod(GET_REQUEST_METHOD);
            con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
            int responseCode = con.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
               LOG.info("Response is  : {} " , response);
            } else {
                LOG.error("GET request did not work.");
            }
            JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
            Map<String,String> versionMap = ((Map) jsonObject.get(VERSION));
            for (Map.Entry<String,String> entry : versionMap.entrySet())
            {
                if (entry.getKey().equals(DISTRIBUTION)) {
                    datasource = String.valueOf(entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (datasource == null)
            datasource = ELASTIC_SEARCH;
        return datasource;
    }
    public SourceInfo checkStatus(final OpenSearchSourceConfig openSearchSourceConfig,final SourceInfo sourceInfo) throws IOException, ParseException {
        String osVersion = null;
        URL obj = new URL(openSearchSourceConfig.getHosts().get(0) + CLUSTER_STATS_ENDPOINTS);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod(GET_REQUEST_METHOD);
        con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
        int responseCode = con.getResponseCode();
        JSONParser jsonParser = new JSONParser();
        StringBuilder response = new StringBuilder();
        String status;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            LOG.info("Response is {} " ,response);
        } else {
            LOG.info("GET request did not work.");
        }
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
        status = (String) jsonObject.get(CLUSTER_HEALTH_STATUS);
        if (status.equalsIgnoreCase(CLUSTER_HEALTH_STATUS_RED))
            sourceInfo.setHealthStatus(false);
        Map<String,String> nodesMap = ((Map) jsonObject.get(NODES));
        for (Map.Entry<String,String> entry : nodesMap.entrySet())
        {
            if (entry.getKey().equals(VERSIONS)) {
                osVersion = String.valueOf(entry.getValue());
                sourceInfo.setOsVersion(osVersion);
            }
        }
        LOG.info("version Number  : {} " , osVersion);
        return sourceInfo;
    }
    public void versionCheck(final OpenSearchSourceConfig openSearchSourceConfig, final SourceInfo sourceInfo, final ElasticsearchClient client, Buffer<Record<Event>> buffer) throws TimeoutException, IOException {
        int osVersionIntegerValue = Integer.parseInt(sourceInfo.getOsVersion().replaceAll(REGULAR_EXPRESSION, ""));
        if ((sourceInfo.getDataSource().equalsIgnoreCase(ELASTIC_SEARCH))
                && (osVersionIntegerValue >= VERSION_7_10_0)) {
            ElasticSearchApiCalls elasticSearchApiCalls = new ElasticSearchApiCalls();
            for (String index : openSearchSourceConfig.getIndexNames().keySet()) {
                openSearchSourceConfig.setIndexValue(index);
                // TODO: need to confirm if Pit Id is required
              //  String pitId = elasticSearchApiCalls.generatePitId(openSearchSourceConfig, client);
              //  LOG.info("Pit Id is  {} ", pitId);
                if(openSearchSourceConfig.getSearchOptions().getBatchSize() > 1000) {
                    if(!openSearchSourceConfig.getSearchOptions().getSorting().getSortKey().isEmpty()) {
                        elasticSearchApiCalls.searchPitIndexesForPagination(openSearchSourceConfig, client, 0, buffer);
                    }
                    else{
                        LOG.info("Sort must contain at least one field");
                    }
                }
                else {
                    elasticSearchApiCalls.searchPitIndexes(openSearchSourceConfig,client);
                }
               /* LOG.info("Search After Response :{} ", getSearchResponseBody);
                LOG.info("Delete Operation starts");
                Boolean deleteResult = elasticSearchApiCalls.delete(pitId, client, osVersionIntegerValue);
                if (deleteResult) {
                    LOG.info("Delete operation performed successfully");
                } else {
                    LOG.info("Delete operation failed");
                }
                LOG.info("Delete operation ends"); */
            }

        } else if (sourceInfo.getDataSource().equalsIgnoreCase(ELASTIC_SEARCH) && (osVersionIntegerValue < VERSION_7_10_0)) {
            ElasticSearchApiCalls elasticSearchApiCalls = new ElasticSearchApiCalls();
            String scrollId = elasticSearchApiCalls.generateScrollId(openSearchSourceConfig, client);
            ScrollRequest scrollRequest = elasticSearchApiCalls.nextScrollRequest(scrollId);
            ScrollResponse<ObjectNode> NextsearchResponse = client.scroll(scrollRequest, ObjectNode.class);
            LOG.info("NextsearchResponse is {} " , NextsearchResponse);
            LOG.info("Delete Operation starts");
            elasticSearchApiCalls.delete(scrollId,client,osVersionIntegerValue);
            LOG.info("Delete operation ends");
        }
    }

    public void writeClusterDataToBuffer(String responseBody, Buffer<Record<Event>> buffer) throws TimeoutException {
        try {
            LOG.info("Write to buffer code started {} ",buffer);
            final JsonParser jsonParser = jsonFactory.createParser(responseBody);
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);
            Event event = JacksonLog.builder().withData(innerJson).build();
            Record<Event> jsonRecord = new Record<>(event);
            LOG.info("Data is pushed to buffer {} ",jsonRecord);
            buffer.write(jsonRecord, 1200);
        }
        catch (Exception e)
        {
            LOG.error("Unable to parse json data [{}], assuming plain text", responseBody, e);
            final Map<String, Object> plainMap = new HashMap<>();
            plainMap.put("message", responseBody);
            Event event = JacksonLog.builder().withData(plainMap).build();
            Record<Event> jsonRecord = new Record<>(event);
            buffer.write(jsonRecord, 1200);
        }
    }
    public List<IndicesRecord> callCatIndices(final ElasticsearchClient client) throws IOException {
        List<IndicesRecord> indexInfoList = client.cat().indices().valueBody();
        return indexInfoList;
    }
    public HashMap<String, String> getIndexMap(final List<IndicesRecord> indexInfos) {
        HashMap<String,String> indexMap = new HashMap<>();
        for(IndicesRecord indexInfo : indexInfos) {
            String indexname = indexInfo.index();
            String indexSize = indexInfo.storeSize();
            indexMap.put(indexname,indexSize);
        }
        return indexMap;
    }

    public  List<IndicesRecord> getIndicesRecords(final OpenSearchSourceConfig openSearchSourceConfig,
                                                  final List<IndicesRecord>  indicesRecords) {
        if (openSearchSourceConfig.getIndexParameters().getExclude() != null
                && !openSearchSourceConfig.getIndexParameters().getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = openSearchSourceConfig.getIndexParameters().getInclude().
                    stream()
                    .filter(index -> !(openSearchSourceConfig.getIndexParameters().getExclude().contains(index)))
                    .collect(Collectors.toList());
            openSearchSourceConfig.getIndexParameters().setInclude(filteredIncludeIndexes);
        }
        openSearchSourceConfig.getIndexParameters().getInclude().forEach(index -> {
            IndicesRecord indexRecord =
                    new IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }

}