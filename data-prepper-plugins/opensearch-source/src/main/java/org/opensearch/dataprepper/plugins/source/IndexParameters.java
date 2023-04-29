package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
public class IndexParameters {
    @JsonProperty("include")
    private ArrayList<String> include;
    @JsonProperty("exclude")
    private ArrayList<String> exclude;
    public void setInclude(ArrayList<String> include) {
        this.include = include;
    }
    public void setExclude(ArrayList<String> exclude) {
        this.exclude = exclude;
    }
    public ArrayList<String> getInclude() {
        return include;
    }
    public ArrayList<String> getExclude() {
        return exclude;
    }
}
