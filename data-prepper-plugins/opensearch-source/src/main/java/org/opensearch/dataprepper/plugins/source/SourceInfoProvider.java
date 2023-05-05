package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
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
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class SourceInfoProvider {
    private String datasource;
    private static final Logger LOG = LoggerFactory.getLogger(SourceInfoProvider.class);
    private static final String GET_REQUEST_MEHTOD = "GET";
    private static final String CONTENT_TYPE = "content-type";
    private static final String CONTENT_TYPE_VALUE = "application/json";
    private static final String VERSION = "version";
    private static final String DISTRIBUTION = "distribution";
    private static final String ELASTICSEARCH = "elasticsearch";
    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";
    private static final String CLUSTER_HEALTHSTATUS = "status";
    private static final String CLUSTER_HEALTHSTATUS_RED = "red";
    private static final String NODES = "nodes";
    private static final String VERSIONS = "versions";
    private static final String REGULAR_EXPRESSION = "[^a-zA-Z0-9]";
    private static final String OPENSEARCH ="opensearch";
    private static final int VERSION_1_3_0 = 130;
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
            con.setRequestMethod(GET_REQUEST_MEHTOD);
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
            datasource = ELASTICSEARCH;
        return datasource;
    }
    public SourceInfo checkStatus(final OpenSearchSourceConfig openSearchSourceConfig,final SourceInfo sourceInfo) throws IOException, ParseException {
        String osVersion = null;
        URL obj = new URL(openSearchSourceConfig.getHosts().get(0) + CLUSTER_STATS_ENDPOINTS);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod(GET_REQUEST_MEHTOD);
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
        status = (String) jsonObject.get(CLUSTER_HEALTHSTATUS);
        if (status.equalsIgnoreCase(CLUSTER_HEALTHSTATUS_RED))
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

    public void versionCheck(final OpenSearchSourceConfig openSearchSourceConfig, final SourceInfo sourceInfo, final OpenSearchClient client,
                             Buffer<Record<Event>> buffer)  throws TimeoutException, IOException {
        int osVersionIntegerValue = Integer.parseInt(sourceInfo.getOsVersion().replaceAll(REGULAR_EXPRESSION, ""));
        // osVersionIntegerValue = 123; to test Scroll API
        if ((sourceInfo.getDataSource().equalsIgnoreCase(OPENSEARCH))
                && (osVersionIntegerValue >= VERSION_1_3_0)) {
            OpenSearchApiCalls openSearchApiCalls = new OpenSearchApiCalls();
            for (String index : openSearchSourceConfig.getIndexNames().keySet()) {
                openSearchSourceConfig.setIndexValue(index);
                String pitId = openSearchApiCalls.generatePitId(openSearchSourceConfig, client);
                String getSearchResponseBody = openSearchApiCalls.searchPitIndexes(pitId, openSearchSourceConfig, client);
                LOG.info("Search After Response :{} ", getSearchResponseBody);
                //  writeClusterDataToBuffer(getSearchResponseBody,buffer);
                if (pitId != null && !pitId.isBlank()) {
                    openSearchApiCalls.delete(pitId, client, osVersionIntegerValue);
                }
            }

        } else if (sourceInfo.getDataSource().equalsIgnoreCase(OPENSEARCH) && (osVersionIntegerValue < VERSION_1_3_0)) {
            OpenSearchApiCalls openSearchApiCalls = new OpenSearchApiCalls();
            String scrollId = openSearchApiCalls.generateScrollId(openSearchSourceConfig, client);
            LOG.info("Scroll Response : {} ", scrollId);
            String getData = openSearchApiCalls.searchScrollIndexes(openSearchSourceConfig, client);
            LOG.info("Data in batches : {} ", getData);
            // writeClusterDataToBuffer(getData,buffer);
            if (scrollId != null && !scrollId.isBlank()) {
                openSearchApiCalls.delete(scrollId, client, osVersionIntegerValue);
            }
        }
    }
    public void writeClusterDataToBuffer(final String responseBoday,final Buffer<Record<Event>> buffer) throws TimeoutException {
        try {
            LOG.info("Write to buffer code started {} ",buffer);
            final JsonParser jsonParser = jsonFactory.createParser(responseBoday);
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);
            Event event = JacksonLog.builder().withData(innerJson).build();
            Record<Event> jsonRecord = new Record<>(event);
            LOG.info("Data is pushed to buffer {} ",jsonRecord);
            buffer.write(jsonRecord, 1200);
        }
        catch (Exception e)
        {
            LOG.error("Unable to parse json data [{}], assuming plain text", responseBoday, e);
            final Map<String, Object> plainMap = new HashMap<>();
            plainMap.put("message", responseBoday);
            Event event = JacksonLog.builder().withData(plainMap).build();
            Record<Event> jsonRecord = new Record<>(event);
            buffer.write(jsonRecord, 1200);
        }
    }
    public List<IndicesRecord> callCatIndices(final OpenSearchClient client) throws IOException,ParseException {
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

    public static StringBuilder getIndexList(final OpenSearchSourceConfig openSearchSourceConfig)
    {
        List<String> include = openSearchSourceConfig.getIndex().getInclude();
        List<String> exclude = openSearchSourceConfig.getIndex().getExclude();
        String includeIndexes = null;
        String excludeIndexes = null;
        StringBuilder indexList = new StringBuilder();
        if(!include.isEmpty())
            includeIndexes = include.stream().collect(Collectors.joining(","));
        if(!exclude.isEmpty())
            excludeIndexes = exclude.stream().collect(Collectors.joining(",-*"));
        indexList.append(includeIndexes);
        indexList.append(",-*"+excludeIndexes);
        return indexList;
    }

    public  List<IndicesRecord> getIndicesRecords(final OpenSearchSourceConfig openSearchSourceConfig,
                                                  final List<IndicesRecord>  indicesRecords) {
        if (openSearchSourceConfig.getIndex().getExclude() != null
                && !openSearchSourceConfig.getIndex().getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = openSearchSourceConfig.getIndex().getInclude().stream()
                    .filter(index -> !(openSearchSourceConfig.getIndex().getExclude().contains(index))).collect(Collectors.toList());
            openSearchSourceConfig.getIndex().setInclude(filteredIncludeIndexes);
        }
        openSearchSourceConfig.getIndex().getInclude().forEach(index -> {
            IndicesRecord indexRecord =
                    new IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }
}