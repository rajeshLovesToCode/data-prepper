package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;

public class OpenSearchSourceConfig {
    @JsonProperty("hosts")
    @NotNull
    @Valid
    private ArrayList<String> hosts;
    @JsonProperty("indices")
    private IndexParameters index;
    @JsonProperty("aws")
    private AwsAuthenticationOptions metrics;
    @JsonProperty("scheduling")
    private SchedulingParameters scheduling;
    @JsonProperty("query")
    private QueryParameters query;
    @JsonProperty("search_options")
    private SearchOptionClass searchOption;
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
    private String connection_timeout;
    private HashMap<String,String> indexNames;
    public HashMap<String, String> getIndexNames() {
        return indexNames;
    }
    public void setIndexNames(HashMap<String, String> indexNames) {
        this.indexNames = indexNames;
    }
    public ArrayList<String> getHosts() {
        return hosts;
    }
    public IndexParameters getIndex() {
        return index;
    }
    public AwsAuthenticationOptions getMetrics() {
        return metrics;
    }

    public SchedulingParameters getScheduling() {
        return scheduling;
    }
    public QueryParameters getQuery() {
        return query;
    }
}
