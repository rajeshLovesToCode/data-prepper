package org.opensearch.dataprepper.plugins.source;

import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
public class PrepareConnection {
    public OpenSearchClient prepareOpensearchConnection() {
        final HttpHost host = new HttpHost("http", "localhost", 9200);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        final OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
                .builder(host)
                .setMapper(new JacksonJsonpMapper())
                //    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_2))
                .build();
        OpenSearchClient osClient = new OpenSearchClient(transport);
        return osClient;
    }
}
