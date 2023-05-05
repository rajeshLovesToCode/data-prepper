package org.opensearch.dataprepper.plugins.source;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.http.HttpEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.ClearScrollRequest;
import org.opensearch.client.opensearch.core.ClearScrollResponse;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.IoUtils;

import java.io.*;
import java.lang.invoke.MethodType;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class OpenSearchApiCalls implements SearchAPICalls {
    private static final String POINT_IN_TIME_KEEP_ALIVE = "keep_alive";
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchApiCalls.class);
    private static final String KEEP_ALIVE_VALUE = "24h";
    private static final String BATCH_SIZE_VALUE = "10000";
    private static final int OPENSEARCH_VERSION = 130;

    @Override
    public String generatePitId(final OpenSearchSourceConfig openSearchSourceConfig, final OpenSearchClient client) throws IOException {
            PITRequest pitRequest = new PITRequest(new PITBuilder());
            pitRequest.setIndex(new StringBuilder(openSearchSourceConfig.getIndexValue()));
            Map<String,String> params = new HashMap<>();
            params.put(POINT_IN_TIME_KEEP_ALIVE,KEEP_ALIVE_VALUE);
            pitRequest.setQueryParameters(params);
            Map  pitResponse =  client._transport().performRequest(pitRequest,PITRequest.ENDPOINT,client._transportOptions());
            LOG.debug("PIT Response is : {}  " , pitResponse);
            LOG.debug("PIT ID is :  {} "  , pitResponse.get("pit_id"));
            return pitResponse.get("pit_id").toString();
    }
    @Override
    public String searchPitIndexes(String pitId, OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client) {
        return "getResponseBody";
    }
    @Override
    public String generateScrollId(final OpenSearchSourceConfig openSearchSourceConfig,final OpenSearchClient client) {
        ScrollRequest scrollRequest = new ScrollRequest(new ScrollBuilder());
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfig);
        scrollRequest.setIndex(indexList);
        scrollRequest.setSize(BATCH_SIZE_VALUE);
        Map<String, Object> response = null;
        try
        {
           response =  client._transport().performRequest(scrollRequest,ScrollRequest.ENDPOINT,client._transportOptions());
           LOG.debug("Response is {}  " , response);
        }
       catch (Exception e)
       {
           LOG.error("Error occured "+e);
       }
        return response.toString();
    }
    @Override
    public String searchScrollIndexes(final OpenSearchSourceConfig openSearchSourceConfig,final OpenSearchClient client) {

        return "responseBody";
    }

    @Override
    public void delete(final String id,final OpenSearchClient client,final Integer openSearchVersion) {
        LOG.info("PIT or Scroll ID to be deleted - " + id);
        try {
            if (openSearchVersion.intValue() >= OPENSEARCH_VERSION) {
                 deletePitId(id, client);
            } else {
                 deleteScrollId(id, client);

            }

        } catch (IOException e) {
            LOG.error("Error occured while closing PIT " + e);
        }

    }

    private void deleteScrollId(String id, OpenSearchClient client) throws IOException {
        ClearScrollRequest scrollRequest=new ClearScrollRequest.Builder().scrollId(id).build();
        ClearScrollResponse clearScrollResponse = client.clearScroll(scrollRequest);
        LOG.debug("Delete Scroll ID Response "+clearScrollResponse);
        LOG.debug("Delete successful "+ clearScrollResponse.succeeded());

    }

    private void deletePitId(String id, OpenSearchClient client) throws IOException {
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put("pit_id", id);
        LOG.debug("Request Object "+inputMap);
        Map executeResponse = execute(Map.class, "_search/point_in_time", inputMap,"DELETE");
        LOG.debug("Delete Pit ID Response " + executeResponse);
        List<Map> pits = (List<Map>) executeResponse.get("pits");
        LOG.debug("Delete successful "+ pits.get(0).get("successful"));

    }


    private <T> T execute(Class<T> responseType, String uri, Map<String,String> inputMap, String method) throws IOException {
        StringEntity requestEntity = new StringEntity(new ObjectMapper().writeValueAsString(inputMap));
        final String finalURL = "http://localhost:9200/" + uri;
        LOG.debug("Final URL "+finalURL);
        URI httpUri = URI.create(finalURL);
        HttpUriRequestBase operationRequest = new HttpUriRequestBase(method, httpUri);
        operationRequest.setHeader("Accept", ContentType.APPLICATION_JSON);
        operationRequest.setHeader("Content-type", ContentType.APPLICATION_JSON);
        operationRequest.setEntity(requestEntity);

        CloseableHttpResponse pitResponse = getCloseableHttpResponse(operationRequest);
        BufferedReader reader = new BufferedReader(new InputStreamReader(pitResponse.getEntity().getContent()));
        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(result.toString(), responseType);
    }

    @Deprecated
    private CloseableHttpResponse getCloseableHttpResponse( HttpUriRequestBase operationRequest) throws IOException {
        CloseableHttpClient httpClient= HttpClients.createDefault();
        CloseableHttpResponse pitResponse = httpClient.execute(operationRequest);
        LOG.debug("Pit Response "+pitResponse);
        return pitResponse;
    }
}
