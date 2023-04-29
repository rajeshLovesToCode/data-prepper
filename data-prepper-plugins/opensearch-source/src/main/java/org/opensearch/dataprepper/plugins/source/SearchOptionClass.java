package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
public class SearchOptionClass {
    @JsonProperty("batch_size")
    private int batchSize;
    @JsonProperty("expand_wildcards")
    private String expand_wildcards;
    @JsonProperty("sorting")
    private SortingParameters sorting;
}
