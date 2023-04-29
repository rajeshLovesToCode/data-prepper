package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;

public class SortingParameters {

    @JsonProperty("sort_key")
    private ArrayList<String> sortKey;
    @JsonProperty("order")
    private String order;
    public ArrayList<String> getSortKey() {
        return sortKey;
    }
    public String getOrder() {
        return order;
    }
}
