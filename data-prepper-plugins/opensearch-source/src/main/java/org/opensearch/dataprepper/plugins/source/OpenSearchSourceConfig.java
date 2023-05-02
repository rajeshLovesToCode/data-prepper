/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.config.*;
import java.util.HashMap;
import java.util.List;
public class OpenSearchSourceConfig {
    @JsonProperty("hosts")
    @NotNull
    @Valid
    private List<String> hosts;
    @JsonProperty("indices")
    private IndexParameters indexParameters;
    @JsonProperty("aws")
    private AwsAuthenticationOptions awsAuthenticationOptions;
    @JsonProperty("scheduling")
    private SchedulingParameters schedulingParameters;
    @JsonProperty("query")
    private QueryParameters queryParameters;
    @JsonProperty("search_options")
    private SearchOptions searchOptions;
    @JsonProperty("insecure")
    private String insecure;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @JsonProperty("cert")
    private String cert;
    @JsonProperty("socket_timeout")
    private String socketTimeout;
    @JsonProperty("connection_timeout")
    private String connectionTimeout;
    private HashMap<String,String> indexNames;
    public HashMap<String, String> getIndexNames() {
        return indexNames;
    }
    public void setIndexNames(HashMap<String, String> indexNames) {
        this.indexNames = indexNames;
    }
    public List<String> getHosts() {
        return hosts;
    }
    public String getInsecure() {
        return insecure;
    }
    public String getUsername() {
        return username;
    }
    public String getPassword() {
        return password;
    }
    public String getCert() {
        return cert;
    }
    public String getSocketTimeout() {
        return socketTimeout;
    }
    public IndexParameters getIndexParameters() {
        return indexParameters;
    }
    public SearchOptions getSearchOptions() {
        return searchOptions;
    }
    public String getConnectionTimeout() {
        return connectionTimeout;
    }
    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }
    public SchedulingParameters getSchedulingParameters() {
        return schedulingParameters;
    }
    public QueryParameters getQueryParameters() {
        return queryParameters;
    }
}
