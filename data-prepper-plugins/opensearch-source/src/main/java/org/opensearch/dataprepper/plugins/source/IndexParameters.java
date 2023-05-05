package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class IndexParameters {
    @JsonProperty("include")
    private List<String> include;
    @JsonProperty("exclude")
    private List<String> exclude;
    public void setInclude(List<String> include) {
        this.include = include;
    }
    public void setExclude(ArrayList<String> exclude) {
        this.exclude = exclude;
    }
    public List<String> getInclude() {
        return include;
    }
    public List<String> getExclude() {
        return exclude;
    }
}
