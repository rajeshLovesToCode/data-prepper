/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import java.util.List;
public class PrepareConnection {
    private static final String HEADER_NAME = "X-Elastic-Product";
    private static final String HEADER_VALUE = "Elasticsearch";
    public ElasticsearchClient prepareElasticSearchConnection() {
        RestClient client = org.elasticsearch.client.RestClient.builder(new org.apache.http.HttpHost("localhost", 9200)).
                setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultHeaders(List.of(new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())))
                        .addInterceptorLast((HttpResponseInterceptor) (response, context) -> response.addHeader(HEADER_NAME, HEADER_VALUE))).build();
        JacksonJsonpMapper jacksonJsonpMapper = new JacksonJsonpMapper();
        ElasticsearchTransport transport = new RestClientTransport(client, jacksonJsonpMapper);
        return new ElasticsearchClient(transport);
    }
}