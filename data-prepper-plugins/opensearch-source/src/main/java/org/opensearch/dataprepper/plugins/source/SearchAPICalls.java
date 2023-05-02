/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
public interface SearchAPICalls {
    String generatePitId(final OpenSearchSourceConfig openSearchSourceConfig,final ElasticsearchClient client);
    String searchPitIndexes(final String pitId,final OpenSearchSourceConfig openSearchSourceConfig,final ElasticsearchClient client);
    String generateScrollId(final OpenSearchSourceConfig openSearchSourceConfig, final ElasticsearchClient client);
    String searchScrollIndexes(final OpenSearchSourceConfig openSearchSourceConfig,final ElasticsearchClient client);
}
