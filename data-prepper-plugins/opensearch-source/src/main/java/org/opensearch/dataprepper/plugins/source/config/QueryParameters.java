/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class QueryParameters {
    @JsonProperty("fields")
    private List<String> fields;
    public List<String> getFields() {
        return fields;
    }
}
