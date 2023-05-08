/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchOptions {
    @JsonProperty("batch_size")
    private int batchSize;
    @JsonProperty("expand_wildcards")
    private String expand_wildcards;
    @JsonProperty("sorting")
    private SortingParameters sorting;
    public int getBatchSize() {
        return batchSize;
    }
    public String getExpand_wildcards() {
        return expand_wildcards;
    }
    public SortingParameters getSorting() {
        return sorting;
    }
}
