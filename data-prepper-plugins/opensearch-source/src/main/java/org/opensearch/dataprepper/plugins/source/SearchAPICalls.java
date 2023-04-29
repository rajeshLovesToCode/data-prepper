package org.opensearch.dataprepper.plugins.source;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;

public interface SearchAPICalls {
      String generatePitId(OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client) throws IOException;
      String searchPitIndexes(String pitId, OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client);
      String generateScrollId(OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client);
      String searchScrollIndexes(OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client);
}
