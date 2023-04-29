package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
public class QueryParameters {
    @JsonProperty("fields")
    private ArrayList<String> fields;
    public ArrayList<String> getFields() {
        return fields;
    }
}
